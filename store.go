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
	commitsSinceCheckpoint int

	pinMu sync.Mutex
	pins  map[string]int

	writeSessionMu    sync.Mutex
	openWriteSessions int

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
		return nil, errors.New("filesystem is nil")
	}
	baseDir = filepath.Clean(baseDir)
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	store := &Store{
		fs:          fs,
		baseDir:     baseDir,
		metaDir:     filepath.Join(baseDir, "meta"),
		segmentsDir: filepath.Join(baseDir, "data", "segments"),
		stagingDir:  filepath.Join(baseDir, "data", "staging"),
		lockPath:    filepath.Join(baseDir, "meta", "LOCK"),
		cfg:         cfg,
		pins:        map[string]int{},
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
	store.meta, err = loadMetadata(fs, store.metaDir)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := store.cleanupStagingAndOrphans(); err != nil {
		_ = store.Close()
		return nil, err
	}
	logPath := filepath.Join(store.metaDir, "txlog", metaLogFile)
	metaLog, err := fs.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	store.metaLog = metaLog
	if err := saveSuperBlock(store.fs, store.metaDir, store.meta.TxID); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// Put stores or replaces a file and records optional string metadata.
func (s *Store) Put(ctx context.Context, tenantID, path string, input io.Reader, options map[string]string) (*PutResult, error) {
	return s.putObject(ctx, tenantID, path, input, putCommitOptions{options: copyOptions(options)})
}

func (s *Store) putObject(ctx context.Context, tenantID, path string, input io.Reader, opts putCommitOptions) (*PutResult, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errors.New("input reader is nil")
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
	result, err := s.commitPreparedObject(ctx, prepared, opts)
	if err != nil {
		var commitErr metadataCommitError
		if !errors.As(err, &commitErr) {
			s.removePreparedSegments(prepared)
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
		tenantID: tenantID,
		path:     path,
		scopeID:  scopeID,
		chunks:   map[string]*chunkRecord{},
	}
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
		if existing := s.chunkSnapshot(chunkID); existing != nil {
			prepared.chunks[chunkID] = existing
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
		if current != nil && current.State != chunkStateDeleted && current.State != chunkStateCorrupt {
			continue
		}
		chunkCopy := *chunk
		if newChunkRef[chunkCopy.ChunkID] {
			chunkCopy.RefCount = 1
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

func (s *Store) OpenObject(ctx context.Context, tenantID, path string) (*ObjectReader, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return s.openReader(tenantID, path, 0, -1)
}

func (s *Store) OpenRange(ctx context.Context, tenantID, path string, offset, length int64) (io.ReadCloser, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("range offset and length must be non-negative")
	}
	return s.openReader(tenantID, path, offset, length)
}

func (s *Store) StatObject(ctx context.Context, tenantID, path string) (*ObjectInfo, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
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

func (s *Store) UpdateMetadata(ctx context.Context, tenantID, path string, options map[string]string) (*ObjectInfo, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
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

func (s *Store) DeleteObject(ctx context.Context, tenantID, path string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
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

func (s *Store) StartBackground(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	select {
	case <-s.closed:
		return os.ErrClosed
	default:
	}
	ticker := time.NewTicker(time.Minute)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.closed:
				return
			case <-ticker.C:
				_, _ = s.RunGC(ctx, GCOptions{Compact: true})
			}
		}
	}()
	return nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.metaMu.Lock()
		_ = s.checkpointMetaLocked()
		s.metaMu.Unlock()
		close(s.closed)
		if s.metaLog != nil {
			_ = s.metaLog.Close()
		}
		if s.lockFile != nil {
			_ = s.lockFile.Close()
			_ = s.fs.Remove(s.lockPath)
		}
	})
	return nil
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
	_ = saveSuperBlock(s.fs, s.metaDir, s.meta.TxID)
	if s.commitsSinceCheckpoint >= metaCheckpointInterval {
		_ = s.checkpointMetaLocked()
	}
	return nil
}

func (s *Store) checkpointMetaLocked() error {
	if s.metaLog == nil {
		return nil
	}
	if err := saveMetaCheckpoint(s.fs, s.metaDir, s.meta); err != nil {
		return err
	}
	if err := saveSuperBlock(s.fs, s.metaDir, s.meta.TxID); err != nil {
		return err
	}
	logPath := filepath.Join(s.metaDir, "txlog", metaLogFile)
	_ = s.metaLog.Close()
	truncated, err := s.fs.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		s.metaLog, _ = s.fs.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		return err
	}
	if err := truncated.Sync(); err != nil {
		_ = truncated.Close()
		s.metaLog, _ = s.fs.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		return err
	}
	if err := truncated.Close(); err != nil {
		s.metaLog, _ = s.fs.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		return err
	}
	metaLog, err := s.fs.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	s.metaLog = metaLog
	s.commitsSinceCheckpoint = 0
	return nil
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

func (s *Store) chunkSnapshot(chunkID string) *chunkRecord {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	chunk := s.meta.Chunks[chunkID]
	if chunk == nil || chunk.State == chunkStateDeleted || chunk.State == chunkStateCorrupt {
		return nil
	}
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
	for chunkID, delta := range chunkDeltas {
		chunk := meta.Chunks[chunkID]
		if chunk == nil {
			continue
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

func (s *Store) removePreparedSegments(prepared *preparedObject) {
	for _, seg := range prepared.segments {
		_ = s.fs.Remove(s.segmentPath(seg))
		_ = s.fs.Remove(s.stagingSegmentPath(seg))
	}
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
