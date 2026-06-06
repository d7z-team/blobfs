package blobfs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero"
)

// Store manages BlobFS metadata, content-addressed chunks, and segment files.
type Store struct {
	fs          afero.Fs
	baseDir     string
	metaDir     string
	segmentsDir string
	stagingDir  string
	lockPath    string
	lockFile    afero.File
	cfg         Config

	metaMu                 sync.RWMutex
	meta                   *metadata
	metaLog                afero.File
	metaLogName            string
	commitsSinceCheckpoint int
	lastCheckpointErr      error
	recoveryWarnings       []metadataReplayWarning

	pinMu sync.Mutex
	pins  map[string]int

	writeSessionMu    sync.Mutex
	openWriteSessions int

	backgroundMu        sync.Mutex
	lastBackgroundGCAt  time.Time
	lastBackgroundGC    *GCResult
	lastBackgroundGCErr error

	handleMu sync.Mutex
	handles  map[storeHandle]struct{}

	lifeMu  sync.Mutex
	closing bool
	bgRuns  bool
	opWG    sync.WaitGroup
	bgWG    sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	closeOnce sync.Once
	closed    chan struct{}
}

// PutResult describes the committed file record and manifest created or reused by Put.
type PutResult struct {
	FileID       string
	TenantID     string
	Path         string
	Size         int64
	FileHash     string
	ManifestID   string
	ChunkCount   int
	ChunkingType string
	Generation   uint64
}

// ObjectInfo describes an active file and its user metadata.
type ObjectInfo struct {
	FileID     string
	TenantID   string
	Path       string
	Size       int64
	FileHash   string
	ManifestID string
	Generation uint64
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Options    map[string]string
}

type preparedObject struct {
	tenantID     string
	path         string
	scopeID      string
	fileHash     string
	size         int64
	chunkingType string
	refs         []manifestChunk
	chunks       map[string]*chunkRecord
	segments     []*segmentRecord
	pinned       []string
	reusedChunks map[string]bool
	manifest     *manifestRecord
}

type putCommitOptions struct {
	baseGeneration  uint64
	checkGeneration bool
	mode            os.FileMode
	modTime         int64
	options         map[string]string
}

type metadataCommitError struct {
	err error
}

func (e metadataCommitError) Error() string {
	return e.err.Error()
}

func (e metadataCommitError) Unwrap() error {
	return e.err
}

// Open opens a store on the local operating system filesystem.
func Open(baseDir string, cfg Config) (*Store, error) {
	baseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, err
	}
	return OpenFS(afero.NewOsFs(), baseDir, cfg)
}

// OpenFS opens a store rooted at baseDir on the provided afero filesystem.
func OpenFS(fs afero.Fs, baseDir string, cfg Config) (*Store, error) {
	if fs == nil {
		return nil, ErrNilFilesystem
	}
	baseDir = filepath.Clean(baseDir)
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	storeCtx, cancel := context.WithCancel(context.Background())
	store := &Store{
		fs:          fs,
		baseDir:     baseDir,
		metaDir:     filepath.Join(baseDir, "meta"),
		segmentsDir: filepath.Join(baseDir, "data", "segments"),
		stagingDir:  filepath.Join(baseDir, "data", "staging"),
		lockPath:    filepath.Join(baseDir, "meta", "LOCK"),
		cfg:         cfg,
		pins:        map[string]int{},
		handles:     map[storeHandle]struct{}{},
		ctx:         storeCtx,
		cancel:      cancel,
		closed:      make(chan struct{}),
	}
	if err := fs.MkdirAll(store.metaDir, 0o755); err != nil {
		return nil, err
	}
	lockFile, err := fs.OpenFile(store.lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	store.lockFile = lockFile
	if err := fs.MkdirAll(store.segmentsDir, 0o755); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := fs.MkdirAll(store.stagingDir, 0o700); err != nil {
		_ = store.Close()
		return nil, err
	}
	var loadReport metadataLoadReport
	store.meta, store.metaLogName, loadReport, err = loadMetadata(fs, store.metaDir)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	store.recoveryWarnings = append([]metadataReplayWarning(nil), loadReport.ReplayWarnings...)
	if err := store.cleanupStagingAndOrphans(); err != nil {
		_ = store.Close()
		return nil, err
	}
	logPath := filepath.Join(store.metaDir, "txlog", store.metaLogName)
	metaLog, err := fs.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	store.metaLog = metaLog
	if err := saveSuperBlock(store.fs, store.metaDir, store.meta.TxID, store.metaLogName); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) beginOp(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.lifeMu.Lock()
	if s.closing {
		s.lifeMu.Unlock()
		return os.ErrClosed
	}
	s.opWG.Add(1)
	s.lifeMu.Unlock()
	if err := contextError(ctx); err != nil {
		s.endOp()
		return err
	}
	return nil
}

func (s *Store) endOp() {
	s.opWG.Done()
}

type storeHandle interface {
	forceCloseFromStore() error
}

func (s *Store) registerHandle(handle storeHandle) error {
	s.lifeMu.Lock()
	if s.closing {
		s.lifeMu.Unlock()
		return os.ErrClosed
	}
	s.handleMu.Lock()
	s.handles[handle] = struct{}{}
	s.handleMu.Unlock()
	s.lifeMu.Unlock()
	return nil
}

func (s *Store) unregisterHandle(handle storeHandle) {
	s.handleMu.Lock()
	delete(s.handles, handle)
	s.handleMu.Unlock()
}

func (s *Store) closeHandles() error {
	s.handleMu.Lock()
	handles := make([]storeHandle, 0, len(s.handles))
	for handle := range s.handles {
		handles = append(handles, handle)
	}
	s.handleMu.Unlock()

	var err error
	for _, handle := range handles {
		err = errors.Join(err, handle.forceCloseFromStore())
	}
	return err
}

// Put stores or replaces a file and records optional string metadata.
func (s *Store) Put(ctx context.Context, tenantID, path string, input io.Reader, options map[string]string) (*PutResult, error) {
	return s.putObject(ctx, tenantID, path, input, putCommitOptions{options: copyOptions(options)})
}

func (s *Store) putObject(ctx context.Context, tenantID, path string, input io.Reader, opts putCommitOptions) (*PutResult, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	if input == nil {
		return nil, ErrNilReader
	}
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("put", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("put", path, err)
	}
	prepared, err := s.prepareObject(ctx, tenantID, path, input)
	if err != nil {
		return nil, err
	}
	defer s.releasePreparedPins(prepared)
	result, err := s.commitPreparedObject(ctx, prepared, opts)
	if err != nil {
		var commitErr metadataCommitError
		if !errors.As(err, &commitErr) {
			if cleanupErr := s.removePreparedSegments(prepared); cleanupErr != nil {
				return nil, errors.Join(err, cleanupErr)
			}
		}
		return nil, err
	}
	return result, nil
}

func (s *Store) prepareObject(ctx context.Context, tenantID, path string, input io.Reader) (*preparedObject, error) {
	scopeID := s.dedupScopeID(tenantID)
	scoped := scopeID != ""
	fileHasher := scopedHasher(scopeID, scoped)
	prepared := &preparedObject{
		tenantID:     tenantID,
		path:         path,
		scopeID:      scopeID,
		chunks:       map[string]*chunkRecord{},
		reusedChunks: map[string]bool{},
	}
	success := false
	defer func() {
		if !success {
			s.releasePreparedPins(prepared)
		}
	}()
	writer := &segmentBatchWriter{store: s}
	defer writer.cleanup()
	if err := s.streamChunks(ctx, input, fileHasher, func(offset int64, raw []byte) error {
		if int64(len(raw))+prepared.size > s.cfg.MaxFileSize {
			return ErrTooLarge
		}
		chunkID := hashBytes(scopeID, scoped, raw)
		if _, ok := prepared.chunks[chunkID]; ok {
			prepared.refs = append(prepared.refs, manifestChunk{Index: len(prepared.refs), ChunkID: chunkID, FileOffset: offset, ChunkSize: int64(len(raw))})
			prepared.size += int64(len(raw))
			return nil
		}
		if existing := s.pinChunkSnapshot(chunkID); existing != nil {
			prepared.chunks[chunkID] = existing
			prepared.pinned = append(prepared.pinned, existing.SegmentID)
			prepared.reusedChunks[chunkID] = true
			prepared.refs = append(prepared.refs, manifestChunk{Index: len(prepared.refs), ChunkID: chunkID, FileOffset: offset, ChunkSize: int64(len(raw))})
			prepared.size += int64(len(raw))
			return nil
		}
		chunk, err := writer.appendChunk(scopeID, chunkID, raw)
		if err != nil {
			return err
		}
		prepared.chunks[chunkID] = &chunk
		prepared.refs = append(prepared.refs, manifestChunk{Index: len(prepared.refs), ChunkID: chunkID, FileOffset: offset, ChunkSize: int64(len(raw))})
		prepared.size += int64(len(raw))
		return nil
	}); err != nil {
		return nil, err
	}
	if err := writer.finish(); err != nil {
		return nil, err
	}
	writer.current = nil
	prepared.segments = writer.segments
	prepared.fileHash = hex.EncodeToString(fileHasher.Sum(nil))
	prepared.chunkingType = chunkingSingle
	if len(prepared.refs) > 1 {
		prepared.chunkingType = chunkingFastCDC
	}
	now := nowUnix()
	manifestID := manifestID(scopeID, prepared.fileHash, prepared.size, prepared.chunkingType, prepared.refs)
	for i := range prepared.refs {
		prepared.refs[i].ManifestID = manifestID
	}
	prepared.manifest = &manifestRecord{
		ManifestID:   manifestID,
		TenantID:     scopeID,
		FileSize:     prepared.size,
		FileHash:     prepared.fileHash,
		ChunkCount:   len(prepared.refs),
		ChunkingType: prepared.chunkingType,
		State:        manifestStateActive,
		Chunks:       append([]manifestChunk(nil), prepared.refs...),
		CreatedAt:    now,
		LastLiveAt:   now,
	}
	success = true
	return prepared, nil
}

func (s *Store) streamChunks(ctx context.Context, input io.Reader, fileHasher hash.Hash, emit func(offset int64, raw []byte) error) error {
	maxChunk := s.cfg.Chunking.MaxSize
	if maxChunk <= 0 {
		maxChunk = DefaultConfig().Chunking.MaxSize
	}
	minChunk := s.cfg.Chunking.MinSize
	if minChunk <= 0 || minChunk > maxChunk {
		minChunk = maxChunk
	}
	mask := uint64(nextPowerOfTwo(s.cfg.Chunking.AvgSize) - 1)
	pending := make([]byte, 0, maxChunk+128*1024)
	readBuf := make([]byte, 128*1024)
	var offset int64
	for {
		if err := contextError(ctx); err != nil {
			return err
		}
		n, readErr := input.Read(readBuf)
		if n > 0 {
			pending = append(pending, readBuf[:n]...)
			for len(pending) >= maxChunk {
				cut := findChunkCut(pending[:maxChunk], minChunk, maxChunk, mask)
				raw := append([]byte(nil), pending[:cut]...)
				fileHasher.Write(raw)
				if err := emit(offset, raw); err != nil {
					return err
				}
				offset += int64(len(raw))
				copy(pending, pending[cut:])
				pending = pending[:len(pending)-cut]
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if len(pending) == 0 && offset == 0 {
		fileHasher.Write(nil)
		return emit(0, nil)
	}
	if len(pending) > 0 {
		raw := append([]byte(nil), pending...)
		fileHasher.Write(raw)
		return emit(offset, raw)
	}
	return nil
}

func findChunkCut(buf []byte, minSize, maxSize int, mask uint64) int {
	if len(buf) <= maxSize {
		maxSize = len(buf)
	}
	if minSize > maxSize {
		return maxSize
	}
	fp := uint64(0)
	for i := 0; i < maxSize; i++ {
		fp = (fp << 1) + gearTable[buf[i]]
		if i+1 >= minSize && (fp&mask) == 0 {
			return i + 1
		}
	}
	return maxSize
}

func (s *Store) commitPreparedObject(ctx context.Context, prepared *preparedObject, opts putCommitOptions) (*PutResult, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if opts.modTime == 0 {
		opts.modTime = nowUnix()
	}
	if opts.options == nil {
		opts.options = map[string]string{}
	}
	if err := s.ensureTenantRoot(prepared.tenantID); err != nil {
		return nil, err
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	parentID, name, err := s.resolveParentLocked(prepared.tenantID, prepared.path)
	if err != nil {
		return nil, pathError("put", prepared.path, err)
	}
	var existing *inodeRecord
	if childID := s.meta.DirEntries[parentID][name]; childID != 0 {
		existing = s.activeInodeLocked(childID)
	}
	if existing != nil && existing.Kind != fileKindFile {
		return nil, pathError("put", prepared.path, ErrIsDir)
	}
	if opts.checkGeneration {
		if opts.baseGeneration == 0 && existing != nil {
			return nil, pathError("put", prepared.path, ErrConflict)
		}
		if opts.baseGeneration != 0 {
			if existing == nil {
				return nil, notExist("put", prepared.path)
			}
			if existing.Generation != opts.baseGeneration {
				return nil, pathError("put", prepared.path, ErrConflict)
			}
		}
	}
	now := nowUnix()
	for chunkID := range prepared.reusedChunks {
		chunk := prepared.chunks[chunkID]
		current := s.meta.Chunks[chunkID]
		segment := s.meta.Segments[chunk.SegmentID]
		if current == nil || current.State != chunkStateActive ||
			segment == nil || segment.State == segmentStateDeleted || segment.State == segmentStateCorrupt {
			return nil, errChunkNotReadable
		}
	}
	ops := make([]metaOp, 0, len(prepared.segments)+len(prepared.chunks)+4)
	for _, seg := range prepared.segments {
		segCopy := *seg
		ops = append(ops, metaOp{Type: "put_segment", Segment: &segCopy})
	}
	newChunkRef := map[string]bool{}
	for _, ref := range prepared.refs {
		newChunkRef[ref.ChunkID] = true
	}
	for _, chunk := range prepared.chunks {
		current := s.meta.Chunks[chunk.ChunkID]
		if current != nil && current.State == chunkStateActive && current.RefCount > 0 {
			continue
		}
		chunkCopy := *chunk
		if newChunkRef[chunkCopy.ChunkID] {
			chunkCopy.RefCount = 0
			if current != nil && current.State == chunkStateCorrupt {
				chunkCopy.RefCount = current.RefCount
			}
		}
		ops = append(ops, metaOp{Type: "put_chunk", Chunk: &chunkCopy})
	}
	manifest := prepared.manifest
	if current := s.meta.Manifests[prepared.manifest.ManifestID]; current != nil {
		manifest = cloneManifest(current)
		manifest.State = manifestStateActive
		manifest.DeletedAt = 0
		manifest.LastLiveAt = now
	}
	addManifestRef := existing == nil || existing.ManifestID != manifest.ManifestID
	manifestDeltas := map[string]int{}
	chunkDeltas := map[string]int{}
	manifestRecords := map[string]*manifestRecord{manifest.ManifestID: manifest}
	if addManifestRef {
		addManifestRefDelta(manifest, 1, manifestDeltas, chunkDeltas)
	}
	if existing != nil && existing.ManifestID != "" && existing.ManifestID != manifest.ManifestID {
		oldManifest := s.meta.Manifests[existing.ManifestID]
		if oldManifest != nil {
			manifestRecords[oldManifest.ManifestID] = oldManifest
			addManifestRefDelta(oldManifest, -1, manifestDeltas, chunkDeltas)
		}
	}
	appendRefDeltaOpsLocked(s.meta, &ops, manifestRecords, manifestDeltas, chunkDeltas, now)
	var inode *inodeRecord
	if existing == nil {
		inode = &inodeRecord{
			InodeID:             s.nextInodeIDLocked(),
			TenantID:            prepared.tenantID,
			Kind:                fileKindFile,
			ParentInode:         parentID,
			Name:                name,
			State:               fileStateActive,
			Generation:          1,
			ContentGeneration:   1,
			MetadataGeneration:  1,
			NamespaceGeneration: 1,
			CreatedAt:           now,
		}
		ops = append(ops, metaOp{Type: "put_dirent", ParentID: parentID, Name: name, ChildID: inode.InodeID})
	} else {
		inode = cloneInode(existing)
	}
	inode.Size = prepared.size
	inode.FileHash = prepared.fileHash
	inode.ManifestID = manifest.ManifestID
	inode.Kind = fileKindFile
	inode.State = fileStateActive
	inode.Options = copyOptions(opts.options)
	inode.Mode = uint32(s.regularFileMode(opts.mode))
	inode.ModTime = opts.modTime
	inode.MTime = opts.modTime
	inode.CTime = now
	inode.UpdatedAt = now
	inode.Generation++
	inode.ContentGeneration++
	ops = append(ops, metaOp{Type: "put_inode", Inode: inode})
	if err := s.commitMetaLocked(ops); err != nil {
		return nil, metadataCommitError{err: err}
	}
	return &PutResult{
		FileID:       inodeFileID(inode.InodeID),
		TenantID:     prepared.tenantID,
		Path:         prepared.path,
		Size:         prepared.size,
		FileHash:     prepared.fileHash,
		ManifestID:   manifest.ManifestID,
		ChunkCount:   manifest.ChunkCount,
		ChunkingType: manifest.ChunkingType,
		Generation:   inode.Generation,
	}, nil
}

// OpenObject opens an immutable reader for the active object at tenantID/path.
// The returned reader pins referenced segments until Close is called.
func (s *Store) OpenObject(ctx context.Context, tenantID, path string) (*ObjectReader, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	return s.openReader(tenantID, path, 0, -1)
}

// OpenRange opens a reader limited to [offset, offset+length). If length extends
// past the object size, the reader stops at EOF.
func (s *Store) OpenRange(ctx context.Context, tenantID, path string, offset, length int64) (io.ReadCloser, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	if offset < 0 || length < 0 {
		return nil, ErrInvalidRange
	}
	return s.openReader(tenantID, path, offset, length)
}

// StatObject returns metadata for an active file object without opening its content.
func (s *Store) StatObject(ctx context.Context, tenantID, path string) (*ObjectInfo, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("stat", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("stat", path, err)
	}
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return nil, pathError("stat", path, err)
	}
	if inode.Kind != fileKindFile {
		return nil, pathError("stat", path, ErrIsDir)
	}
	info := objectInfoFromInode(inode, path)
	return &info, nil
}

// UpdateMetadata replaces user options on an active file without changing its content.
func (s *Store) UpdateMetadata(ctx context.Context, tenantID, path string, options map[string]string) (*ObjectInfo, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("update metadata", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("update metadata", path, err)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return nil, pathError("update metadata", path, err)
	}
	if inode.Kind != fileKindFile {
		return nil, pathError("update metadata", path, ErrIsDir)
	}
	next := cloneInode(inode)
	now := nowUnix()
	next.Options = copyOptions(options)
	next.Generation++
	next.MetadataGeneration++
	next.UpdatedAt = now
	next.CTime = now
	if err := s.commitMetaLocked([]metaOp{{Type: "put_inode", Inode: next}}); err != nil {
		return nil, err
	}
	info := objectInfoFromInode(next, path)
	return &info, nil
}

// DeleteObject removes one active file from the namespace and releases its references.
func (s *Store) DeleteObject(ctx context.Context, tenantID, path string) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return pathError("delete", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return pathError("delete", path, err)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return pathError("delete", path, err)
	}
	if inode.Kind != fileKindFile {
		return pathError("delete", path, ErrIsDir)
	}
	parentID, name, err := s.resolveParentLocked(tenantID, path)
	if err != nil {
		return pathError("delete", path, err)
	}
	now := nowUnix()
	next := cloneInode(inode)
	next.State = fileStateDeleted
	next.DeletedAt = now
	next.UpdatedAt = now
	next.CTime = now
	next.Generation++
	ops := []metaOp{
		{Type: "put_inode", Inode: next},
		{Type: "delete_dirent", ParentID: parentID, Name: name},
	}
	addDeletedManifestOpsLocked(s.meta, inode.ManifestID, &ops, now)
	return s.commitMetaLocked(ops)
}

// StartBackground starts periodic compacting GC until ctx is canceled or the
// store is closed. The latest background GC status is exposed through Health and Stats.
func (s *Store) StartBackground(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.lifeMu.Lock()
	if s.closing {
		s.lifeMu.Unlock()
		return os.ErrClosed
	}
	if s.bgRuns {
		s.lifeMu.Unlock()
		return ErrBackgroundRunning
	}
	s.bgRuns = true
	s.bgWG.Add(1)
	s.lifeMu.Unlock()
	ticker := time.NewTicker(time.Minute)
	go func() {
		defer func() {
			ticker.Stop()
			s.lifeMu.Lock()
			s.bgRuns = false
			s.lifeMu.Unlock()
			s.bgWG.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.ctx.Done():
				return
			case <-s.closed:
				return
			case <-ticker.C:
				result, err := s.RunGC(s.ctx, GCOptions{Compact: true})
				s.backgroundMu.Lock()
				s.lastBackgroundGCAt = time.Now()
				if result != nil {
					copyResult := *result
					s.lastBackgroundGC = &copyResult
				} else {
					s.lastBackgroundGC = nil
				}
				s.lastBackgroundGCErr = err
				s.backgroundMu.Unlock()
			}
		}
	}()
	return nil
}

// Close stops background work, waits for in-flight operations, checkpoints
// metadata, closes resources, and removes the process lock.
func (s *Store) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.lifeMu.Lock()
		s.closing = true
		if s.cancel != nil {
			s.cancel()
		}
		close(s.closed)
		s.lifeMu.Unlock()

		s.bgWG.Wait()
		s.opWG.Wait()
		closeErr = errors.Join(closeErr, s.closeHandles())

		s.metaMu.Lock()
		closeErr = errors.Join(closeErr, s.checkpointMetaLocked())
		if s.metaLog != nil {
			closeErr = errors.Join(closeErr, s.metaLog.Close())
			s.metaLog = nil
		}
		s.metaMu.Unlock()
		if s.lockFile != nil {
			closeErr = errors.Join(closeErr, s.lockFile.Close())
			if err := s.fs.Remove(s.lockPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				closeErr = errors.Join(closeErr, err)
			}
			s.lockFile = nil
		}
	})
	return closeErr
}

func (s *Store) ensureTenantRoot(tenantID string) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.meta.Tenants[tenantID] != 0 {
		return nil
	}
	now := nowUnix()
	root := &inodeRecord{
		InodeID:             s.nextInodeIDLocked(),
		TenantID:            tenantID,
		Kind:                fileKindDir,
		Name:                "",
		State:               fileStateActive,
		Mode:                uint32(os.ModeDir | 0o755),
		Generation:          1,
		MetadataGeneration:  1,
		NamespaceGeneration: 1,
		CreatedAt:           now,
		UpdatedAt:           now,
		CTime:               now,
		MTime:               now,
		ModTime:             now,
	}
	return s.commitMetaLocked([]metaOp{
		{Type: "put_tenant", TenantID: tenantID, ChildID: root.InodeID},
		{Type: "put_inode", Inode: root},
	})
}

func (s *Store) commitMetaLocked(ops []metaOp) error {
	if len(ops) == 0 {
		return nil
	}
	txid := s.meta.TxID + 1
	tx := metaTx{TxID: txid, Ops: ops}
	if err := writeMetaTx(s.metaLog, tx); err != nil {
		return err
	}
	applyMetaTx(s.meta, tx)
	s.commitsSinceCheckpoint++
	if err := saveSuperBlock(s.fs, s.metaDir, s.meta.TxID, s.metaLogName); err != nil {
		s.lastCheckpointErr = err
		return err
	}
	s.lastCheckpointErr = nil
	if s.commitsSinceCheckpoint >= metaCheckpointInterval {
		if err := s.checkpointMetaLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) checkpointMetaLocked() error {
	if s.metaLog == nil {
		return errMetadataLogClosed
	}
	compactMetadata(s.meta)
	if err := saveMetaCheckpoint(s.fs, s.metaDir, s.meta); err != nil {
		s.lastCheckpointErr = err
		return err
	}
	newLog, newName, err := s.createMetaLogGenerationLocked(nextMetaLogName(s.metaLogName))
	if err != nil {
		s.lastCheckpointErr = err
		return err
	}
	if err := newLog.Sync(); err != nil {
		_ = newLog.Close()
		_ = s.fs.Remove(filepath.Join(s.metaDir, "txlog", newName))
		s.lastCheckpointErr = err
		return err
	}
	if err := saveSuperBlock(s.fs, s.metaDir, s.meta.TxID, newName); err != nil {
		_ = newLog.Close()
		_ = s.fs.Remove(filepath.Join(s.metaDir, "txlog", newName))
		s.lastCheckpointErr = err
		return err
	}
	oldLog := s.metaLog
	oldName := s.metaLogName
	s.metaLog = newLog
	s.metaLogName = newName
	s.commitsSinceCheckpoint = 0
	s.lastCheckpointErr = nil
	var cleanupErr error
	if oldLog != nil {
		cleanupErr = errors.Join(cleanupErr, oldLog.Close())
	}
	if oldName != "" && oldName != newName {
		if err := s.fs.Remove(filepath.Join(s.metaDir, "txlog", oldName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if cleanupErr != nil {
		s.lastCheckpointErr = cleanupErr
	}
	return cleanupErr
}

func (s *Store) createMetaLogGenerationLocked(startName string) (afero.File, string, error) {
	name := startName
	for i := 0; i < 1000; i++ {
		path := filepath.Join(s.metaDir, "txlog", name)
		file, err := s.fs.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0o600)
		if os.IsExist(err) {
			name = nextMetaLogName(name)
			continue
		}
		return file, name, err
	}
	return nil, "", errors.New("create metadata log generation: exhausted attempts")
}

func (s *Store) nextInodeIDLocked() uint64 {
	id := s.meta.NextInodeID
	s.meta.NextInodeID++
	return id
}

func (s *Store) activeInodeLocked(id uint64) *inodeRecord {
	inode := s.meta.Inodes[id]
	if inode == nil || inode.State != fileStateActive {
		return nil
	}
	return inode
}

func (s *Store) resolvePathLocked(tenantID, path string) (*inodeRecord, error) {
	rootID := s.meta.Tenants[tenantID]
	if rootID == 0 {
		return nil, fs.ErrNotExist
	}
	current := s.activeInodeLocked(rootID)
	if current == nil {
		return nil, fs.ErrNotExist
	}
	if path == "" {
		return current, nil
	}
	for _, name := range strings.Split(path, "/") {
		if current.Kind != fileKindDir {
			return nil, ErrNotDir
		}
		childID := s.meta.DirEntries[current.InodeID][name]
		if childID == 0 {
			return nil, fs.ErrNotExist
		}
		current = s.activeInodeLocked(childID)
		if current == nil {
			return nil, fs.ErrNotExist
		}
	}
	return current, nil
}

func (s *Store) resolveParentLocked(tenantID, path string) (uint64, string, error) {
	parent := parentPath(path)
	name := pathBase(path)
	rootID := s.meta.Tenants[tenantID]
	if rootID == 0 {
		return 0, "", fs.ErrNotExist
	}
	if parent == "" {
		return rootID, name, nil
	}
	inode, err := s.resolvePathLocked(tenantID, parent)
	if err != nil {
		return 0, "", err
	}
	if inode.Kind != fileKindDir {
		return 0, "", ErrNotDir
	}
	return inode.InodeID, name, nil
}

func (s *Store) pinChunkSnapshot(chunkID string) *chunkRecord {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	chunk := s.meta.Chunks[chunkID]
	if chunk == nil || chunk.RefCount <= 0 || chunk.State != chunkStateActive {
		return nil
	}
	segment := s.meta.Segments[chunk.SegmentID]
	if segment == nil || segment.State == segmentStateDeleted || segment.State == segmentStateCorrupt {
		return nil
	}
	s.pinSegment(chunk.SegmentID)
	next := *chunk
	return &next
}

func addManifestRefDelta(manifest *manifestRecord, delta int, manifestDeltas, chunkDeltas map[string]int) {
	if manifest == nil || delta == 0 {
		return
	}
	manifestDeltas[manifest.ManifestID] += delta
	seen := map[string]bool{}
	for _, ref := range manifest.Chunks {
		if seen[ref.ChunkID] {
			continue
		}
		seen[ref.ChunkID] = true
		chunkDeltas[ref.ChunkID] += delta
	}
}

func appendRefDeltaOpsLocked(meta *metadata, ops *[]metaOp, manifestRecords map[string]*manifestRecord, manifestDeltas, chunkDeltas map[string]int, now int64) {
	for manifestID, delta := range manifestDeltas {
		manifest := manifestRecords[manifestID]
		if manifest == nil {
			manifest = meta.Manifests[manifestID]
		}
		if manifest == nil {
			continue
		}
		next := cloneManifest(manifest)
		next.RefCount += delta
		if next.RefCount < 0 {
			next.RefCount = 0
		}
		if next.RefCount == 0 {
			next.State = manifestStateDeleted
			next.DeletedAt = now
		} else {
			next.State = manifestStateActive
			next.DeletedAt = 0
			next.LastLiveAt = now
		}
		*ops = append(*ops, metaOp{Type: "put_manifest", Manifest: next})
	}
	pendingChunks := map[string]*chunkRecord{}
	for i := range *ops {
		op := &(*ops)[i]
		if op.Type == "put_chunk" && op.Chunk != nil {
			pendingChunks[op.Chunk.ChunkID] = op.Chunk
		}
	}
	for chunkID, delta := range chunkDeltas {
		chunk := pendingChunks[chunkID]
		if chunk == nil {
			chunk = meta.Chunks[chunkID]
			if chunk == nil {
				continue
			}
		}
		next := *chunk
		next.RefCount += delta
		if next.RefCount < 0 {
			next.RefCount = 0
		}
		if next.RefCount > 0 {
			next.State = chunkStateActive
			next.LastSeenAt = now
			next.DeletedAt = 0
			next.GarbageCandidateAt = 0
			next.GarbageSeenCount = 0
		}
		*ops = append(*ops, metaOp{Type: "put_chunk", Chunk: &next})
	}
}

func addDeletedManifestOpsLocked(meta *metadata, manifestID string, ops *[]metaOp, now int64) {
	manifest := meta.Manifests[manifestID]
	if manifest == nil {
		return
	}
	manifestDeltas := map[string]int{}
	chunkDeltas := map[string]int{}
	addManifestRefDelta(manifest, -1, manifestDeltas, chunkDeltas)
	appendRefDeltaOpsLocked(meta, ops, map[string]*manifestRecord{manifestID: manifest}, manifestDeltas, chunkDeltas, now)
}

func cloneInode(inode *inodeRecord) *inodeRecord {
	next := *inode
	next.Options = copyOptions(inode.Options)
	return &next
}

func cloneManifest(manifest *manifestRecord) *manifestRecord {
	next := *manifest
	next.Chunks = append([]manifestChunk(nil), manifest.Chunks...)
	return &next
}

func objectInfoFromInode(inode *inodeRecord, path string) ObjectInfo {
	return ObjectInfo{
		FileID:     inodeFileID(inode.InodeID),
		TenantID:   inode.TenantID,
		Path:       path,
		Size:       inode.Size,
		FileHash:   inode.FileHash,
		ManifestID: inode.ManifestID,
		Generation: inode.Generation,
		CreatedAt:  time.Unix(0, inode.CreatedAt),
		UpdatedAt:  time.Unix(0, inode.UpdatedAt),
		Options:    copyOptions(inode.Options),
	}
}

func inodeFileID(id uint64) string {
	return fmt.Sprintf("inode-%016d", id)
}

func (s *Store) dedupScopeID(tenantID string) string {
	if s.cfg.DedupScope == DedupScopeTenant {
		return tenantID
	}
	return ""
}

func (s *Store) pinSegment(segmentID string) {
	s.pinMu.Lock()
	s.pins[segmentID]++
	s.pinMu.Unlock()
}

func (s *Store) unpinSegment(segmentID string) {
	s.pinMu.Lock()
	if s.pins[segmentID] <= 1 {
		delete(s.pins, segmentID)
	} else {
		s.pins[segmentID]--
	}
	s.pinMu.Unlock()
}

func (s *Store) segmentPinned(segmentID string) bool {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	return s.pins[segmentID] > 0
}

func (s *Store) releasePreparedPins(prepared *preparedObject) {
	if prepared == nil {
		return
	}
	for _, segmentID := range prepared.pinned {
		s.unpinSegment(segmentID)
	}
	prepared.pinned = nil
}

func (s *Store) removePreparedSegments(prepared *preparedObject) error {
	var errs []error
	for _, seg := range prepared.segments {
		segmentPath := s.segmentPath(seg)
		if err := s.fs.Remove(segmentPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove prepared segment %s: %w", segmentPath, err))
		}
		stagingPath := s.stagingSegmentPath(seg)
		if err := s.fs.Remove(stagingPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove prepared staging segment %s: %w", stagingPath, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Store) cleanupStagingAndOrphans() error {
	if err := afero.Walk(s.fs, s.stagingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return err
		}
		return s.fs.Remove(path)
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	referenced := map[string]bool{}
	for _, seg := range s.meta.Segments {
		if seg.State != segmentStateDeleted {
			referenced[s.segmentPath(seg)] = true
		}
	}
	return afero.Walk(s.fs, s.segmentsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return err
		}
		if !referenced[path] {
			return s.fs.Remove(path)
		}
		return nil
	})
}

func copyOptions(options map[string]string) map[string]string {
	if options == nil {
		return map[string]string{}
	}
	copied := make(map[string]string, len(options))
	for k, v := range options {
		copied[k] = v
	}
	return copied
}

func sortedNames(entries map[string]uint64) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
