package blobfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/afero"
)

// Store manages content-addressed file data, manifests, metadata, and background cleanup.
type Store struct {
	fs          afero.Fs
	metaPath    string
	segmentsDir string
	sessionsDir string
	lockPath    string
	lockFile    afero.File
	cfg         Config

	mu                sync.Mutex
	meta              *metadata
	pathLocks         *rwLockGroup
	openWriteSessions int

	pinMu sync.Mutex
	pins  map[string]int

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
		metaPath:    filepath.Join(baseDir, "meta", "blobfs.json"),
		segmentsDir: filepath.Join(baseDir, "data", "segments"),
		sessionsDir: filepath.Join(baseDir, "tmp", "write-sessions"),
		lockPath:    filepath.Join(baseDir, "meta", "LOCK"),
		cfg:         cfg,
		pathLocks:   newRWLockGroup(),
		pins:        map[string]int{},
		closed:      make(chan struct{}),
	}
	if err := fs.MkdirAll(filepath.Dir(store.metaPath), 0o755); err != nil {
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
	if err := fs.MkdirAll(store.sessionsDir, 0o700); err != nil {
		_ = store.Close()
		return nil, err
	}
	store.meta, err = loadMetadata(fs, store.metaPath)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	store.mu.Lock()
	err = store.recoverSegmentsLocked()
	if err == nil {
		err = saveMetadata(store.fs, store.metaPath, store.meta)
	}
	store.mu.Unlock()
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// Put stores or replaces a file and records optional string metadata.
func (s *Store) Put(ctx context.Context, tenantID, path string, input io.Reader, options map[string]string) (*PutResult, error) {
	if err := ctx.Err(); err != nil {
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
	pathLock := s.pathLocks.Open(fileKey(tenantID, path)).Lock(false)
	defer pathLock.Close()

	data, err := readInput(ctx, input, s.cfg.MaxFileSize)
	if err != nil {
		return nil, err
	}
	scopeID := s.dedupScopeID(tenantID)
	fileHash := hashBytes(scopeID, scopeID != "", data)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := nowUnix()
	manifest := s.findReusableManifestLocked(scopeID, fileHash, int64(len(data)))
	if manifest == nil {
		manifest, err = s.ingestManifestLocked(scopeID, fileHash, data, now)
		if err != nil {
			return nil, err
		}
	}
	manifest.State = manifestStateActive
	manifest.DeletedAt = 0
	manifest.LastLiveAt = now
	record, err := s.commitFileRecordLocked(commitFileOptions{
		Op:         "put",
		TenantID:   tenantID,
		Path:       path,
		Size:       int64(len(data)),
		FileHash:   fileHash,
		ManifestID: manifest.ManifestID,
		Options:    copyOptions(options),
		Mode:       s.regularFileMode(0),
		ModTime:    now,
		Now:        now,
	})
	if err != nil {
		return nil, err
	}
	if err = saveMetadata(s.fs, s.metaPath, s.meta); err != nil {
		return nil, err
	}
	return &PutResult{
		FileID:       record.FileID,
		TenantID:     tenantID,
		Path:         path,
		Size:         int64(len(data)),
		FileHash:     fileHash,
		ManifestID:   manifest.ManifestID,
		ChunkCount:   manifest.ChunkCount,
		ChunkingType: manifest.ChunkingType,
		Generation:   record.Generation,
	}, nil
}

type commitFileOptions struct {
	Op              string
	TenantID        string
	Path            string
	Size            int64
	FileHash        string
	ManifestID      string
	Options         map[string]string
	Mode            os.FileMode
	ModTime         int64
	Now             int64
	BaseGeneration  uint64
	CheckGeneration bool
}

func (s *Store) commitFileRecordLocked(opts commitFileOptions) (*fileRecord, error) {
	if err := s.ensureParentDirLocked(opts.Op, opts.TenantID, opts.Path); err != nil {
		return nil, err
	}
	key := fileKey(opts.TenantID, opts.Path)
	record := s.meta.Files[key]
	if record != nil && record.State == fileStateActive && record.Kind != fileKindFile {
		return nil, pathError(opts.Op, opts.Path, ErrIsDir)
	}
	if opts.CheckGeneration {
		if opts.BaseGeneration == 0 {
			if record != nil && record.State == fileStateActive {
				return nil, pathError(opts.Op, opts.Path, ErrConflict)
			}
		} else {
			if record == nil || record.State != fileStateActive {
				return nil, notExist(opts.Op, opts.Path)
			}
			if record.Generation != opts.BaseGeneration {
				return nil, pathError(opts.Op, opts.Path, ErrConflict)
			}
		}
	}
	previousManifestID := ""
	if record == nil || record.State != fileStateActive {
		record = s.newFileRecordLocked(opts.TenantID, opts.Path, fileKindFile, opts.Now)
		record.Mode = uint32(s.regularFileMode(opts.Mode))
		s.meta.Files[key] = record
		s.addDirEntryLocked(record)
	} else {
		previousManifestID = record.ManifestID
		if opts.Mode == 0 {
			opts.Mode = os.FileMode(record.Mode)
		}
	}
	s.applyFileContentLocked(record, opts.Size, opts.FileHash, opts.ManifestID, opts.Options, opts.Now)
	record.Mode = uint32(s.regularFileMode(opts.Mode))
	record.ModTime = opts.ModTime
	record.MTime = opts.ModTime
	if previousManifestID != "" && previousManifestID != opts.ManifestID {
		s.markManifestDeletedIfUnreferencedLocked(previousManifestID, opts.Now)
	}
	return record, nil
}

// OpenObject returns a reader for an active BlobFS object.
func (s *Store) OpenObject(ctx context.Context, tenantID, path string) (*ObjectReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.openReader(tenantID, path, 0, -1)
}

// OpenRange returns a reader over the requested byte range of an active file.
func (s *Store) OpenRange(ctx context.Context, tenantID, path string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("range offset and length must be non-negative")
	}
	reader, err := s.openReader(tenantID, path, offset, length)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// StatObject returns metadata for an active BlobFS object.
func (s *Store) StatObject(ctx context.Context, tenantID, path string) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("stat", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("stat", path, err)
	}
	lock := s.pathLocks.Open(fileKey(tenantID, path)).Lock(true)
	defer lock.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, info, err := s.activeFileInfoLocked(tenantID, path)
	if err != nil {
		return nil, pathError("stat", path, err)
	}
	return &info, nil
}

// UpdateMetadata replaces a file's string metadata without rewriting content or changing its manifest.
func (s *Store) UpdateMetadata(ctx context.Context, tenantID, path string, options map[string]string) (*ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("update metadata", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("update metadata", path, err)
	}
	lock := s.pathLocks.Open(fileKey(tenantID, path)).Lock(false)
	defer lock.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	file, _, err := s.activeFileInfoLocked(tenantID, path)
	if err != nil {
		return nil, pathError("update metadata", path, err)
	}
	file.Options = copyOptions(options)
	now := nowUnix()
	file.Generation++
	file.MetadataGeneration++
	file.UpdatedAt = now
	file.CTime = now
	if err = saveMetadata(s.fs, s.metaPath, s.meta); err != nil {
		return nil, err
	}
	_, info, err := s.activeFileInfoLocked(tenantID, path)
	if err != nil {
		return nil, pathError("update metadata", path, err)
	}
	return &info, nil
}

func (s *Store) activeFileInfoLocked(tenantID, path string) (*fileRecord, ObjectInfo, error) {
	file := s.meta.Files[fileKey(tenantID, path)]
	if file == nil || file.State != fileStateActive {
		return nil, ObjectInfo{}, os.ErrNotExist
	}
	if file.Kind != fileKindFile {
		return nil, ObjectInfo{}, ErrIsDir
	}
	return file, ObjectInfo{
		FileID:     file.FileID,
		TenantID:   file.TenantID,
		Path:       file.Path,
		Size:       file.Size,
		FileHash:   file.FileHash,
		ManifestID: file.ManifestID,
		Generation: file.Generation,
		CreatedAt:  time.Unix(0, file.CreatedAt),
		UpdatedAt:  time.Unix(0, file.UpdatedAt),
		Options:    copyOptions(file.Options),
	}, nil
}

func (s *Store) newFileRecordLocked(tenantID, path, kind string, now int64) *fileRecord {
	fileID := fmt.Sprintf("file-%016d", s.meta.NextFileSeq)
	s.meta.NextFileSeq++
	return &fileRecord{
		FileID:             fileID,
		TenantID:           tenantID,
		Path:               path,
		ParentPath:         parentPath(path),
		Name:               pathBase(path),
		Kind:               kind,
		State:              fileStateActive,
		Generation:         1,
		ContentGeneration:  1,
		MetadataGeneration: 1,
		CTime:              now,
		MTime:              now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

func (s *Store) applyFileContentLocked(record *fileRecord, size int64, fileHash, manifestID string, options map[string]string, now int64) {
	record.Size = size
	record.FileHash = fileHash
	record.ManifestID = manifestID
	record.Kind = fileKindFile
	record.State = fileStateActive
	record.Options = options
	record.ModTime = now
	record.MTime = now
	record.CTime = now
	record.UpdatedAt = now
	record.Generation++
	record.ContentGeneration++
}

func (s *Store) addDirEntryLocked(record *fileRecord) {
	if record.Path == "" || record.State != fileStateActive {
		return
	}
	key := dirKey(record.TenantID, record.ParentPath)
	if s.meta.DirEntries[key] == nil {
		s.meta.DirEntries[key] = map[string]string{}
	}
	s.meta.DirEntries[key][record.Name] = fileKey(record.TenantID, record.Path)
}

func (s *Store) removeDirEntryLocked(record *fileRecord) {
	key := dirKey(record.TenantID, record.ParentPath)
	if entries := s.meta.DirEntries[key]; entries != nil {
		delete(entries, record.Name)
		if len(entries) == 0 {
			delete(s.meta.DirEntries, key)
		}
	}
}

func (s *Store) ensureParentDirLocked(op, tenantID, path string) error {
	parent := parentPath(path)
	if parent == "" {
		return nil
	}
	record := s.activeRecordLocked(tenantID, parent)
	if record == nil {
		return notExist(op, parent)
	}
	if record.Kind != fileKindDir {
		return pathError(op, parent, ErrNotDir)
	}
	return nil
}

func (s *Store) activeChildrenLocked(tenantID, path string) map[string]string {
	entries := s.meta.DirEntries[dirKey(tenantID, path)]
	if len(entries) == 0 {
		return nil
	}
	copied := make(map[string]string, len(entries))
	for name, key := range entries {
		if record := s.meta.Files[key]; record != nil && record.State == fileStateActive {
			copied[name] = key
		}
	}
	return copied
}

// DeleteObject marks a file and its unreferenced manifest deleted without removing chunk data synchronously.
func (s *Store) DeleteObject(ctx context.Context, tenantID, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return pathError("delete", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return pathError("delete", path, err)
	}
	lock := s.pathLocks.Open(fileKey(tenantID, path)).Lock(false)
	defer lock.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fileKey(tenantID, path)
	file := s.meta.Files[key]
	if file == nil || file.State != fileStateActive {
		return notExist("delete", path)
	}
	if file.Kind != fileKindFile {
		return pathError("delete", path, ErrIsDir)
	}
	now := nowUnix()
	file.State = fileStateDeleted
	file.DeletedAt = now
	file.UpdatedAt = now
	file.Generation++
	s.removeDirEntryLocked(file)
	s.markManifestDeletedIfUnreferencedLocked(file.ManifestID, now)
	return saveMetadata(s.fs, s.metaPath, s.meta)
}

// StartBackground starts periodic asynchronous GC until ctx is canceled or the store is closed.
func (s *Store) StartBackground(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
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

// Close stops background workers started by this store.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.lockFile != nil {
			_ = s.lockFile.Close()
			_ = s.fs.Remove(s.lockPath)
		}
	})
	return nil
}

func readInput(ctx context.Context, input io.Reader, maxSize int64) ([]byte, error) {
	var data []byte
	buf := make([]byte, 128*1024)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := input.Read(buf)
		if n > 0 {
			if int64(len(data))+int64(n) > maxSize {
				return nil, ErrTooLarge
			}
			data = append(data, buf[:n]...)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return data, nil
}

func (s *Store) ingestManifestLocked(scopeID, fileHash string, data []byte, now int64) (*manifestRecord, error) {
	var refs []manifestChunk
	chunkingType := chunkingSingle
	if int64(len(data)) <= s.cfg.LargeFileThreshold {
		refs = []manifestChunk{{
			Index:      0,
			ChunkID:    fileHash,
			FileOffset: 0,
			ChunkSize:  int64(len(data)),
		}}
		if err := s.ensureChunkLocked(scopeID, fileHash, data, now); err != nil {
			return nil, err
		}
	} else {
		chunkingType = chunkingFastCDC
		slices := splitFastCDC(data, s.cfg.Chunking)
		refs = make([]manifestChunk, 0, len(slices))
		for i, slice := range slices {
			end := slice.Offset + slice.Size
			chunkData := data[slice.Offset:end]
			chunkID := hashBytes(scopeID, scopeID != "", chunkData)
			if err := s.ensureChunkLocked(scopeID, chunkID, chunkData, now); err != nil {
				return nil, err
			}
			refs = append(refs, manifestChunk{
				Index:      i,
				ChunkID:    chunkID,
				FileOffset: slice.Offset,
				ChunkSize:  slice.Size,
			})
		}
	}
	id := manifestID(scopeID, fileHash, int64(len(data)), chunkingType, refs)
	manifest := &manifestRecord{
		ManifestID:   id,
		TenantID:     scopeID,
		FileSize:     int64(len(data)),
		FileHash:     fileHash,
		ChunkCount:   len(refs),
		ChunkingType: chunkingType,
		State:        manifestStateActive,
		CreatedAt:    now,
		LastLiveAt:   now,
	}
	manifest.Chunks = refs
	for i := range manifest.Chunks {
		manifest.Chunks[i].ManifestID = id
	}
	if existing := s.meta.Manifests[id]; existing != nil {
		existing.State = manifestStateActive
		existing.DeletedAt = 0
		existing.LastLiveAt = now
		return existing, nil
	}
	s.meta.Manifests[id] = manifest
	return manifest, nil
}

func (s *Store) ensureChunkLocked(scopeID, chunkID string, raw []byte, now int64) error {
	if chunk := s.meta.Chunks[chunkID]; chunk != nil {
		if chunk.State != chunkStateDeleted && chunk.State != chunkStateCorrupt && chunk.SegmentID != "" {
			chunk.State = chunkStateActive
			chunk.LastSeenAt = now
			chunk.GarbageSeenCount = 0
			chunk.GarbageCandidateAt = 0
			chunk.DeletingAt = 0
			chunk.DeletedAt = 0
			return nil
		}
	}
	s.meta.Chunks[chunkID] = &chunkRecord{
		ChunkID:    chunkID,
		TenantID:   scopeID,
		RawSize:    int64(len(raw)),
		State:      chunkStateWriting,
		CreatedAt:  now,
		LastSeenAt: now,
	}
	loc, err := s.appendChunkRecordLocked(chunkID, raw)
	if err != nil {
		return err
	}
	s.meta.Chunks[chunkID] = &chunkRecord{
		ChunkID:        chunkID,
		TenantID:       scopeID,
		RawSize:        int64(len(raw)),
		StoredSize:     loc.StoredSize,
		State:          chunkStateActive,
		SegmentID:      loc.SegmentID,
		SegmentOffset:  loc.SegmentOffset,
		SegmentLength:  loc.SegmentLength,
		ChecksumCRC32C: loc.Checksum,
		Compression:    loc.Compression,
		CreatedAt:      now,
		LastSeenAt:     now,
	}
	return nil
}

func (s *Store) findReusableManifestLocked(scopeID, fileHash string, size int64) *manifestRecord {
	for _, manifest := range s.meta.Manifests {
		if manifest.TenantID == scopeID && manifest.FileHash == fileHash && manifest.FileSize == size && s.manifestReadableLocked(manifest) {
			return manifest
		}
	}
	return nil
}

func (s *Store) dedupScopeID(tenantID string) string {
	if s.cfg.DedupScope == DedupScopeTenant {
		return tenantID
	}
	return ""
}

func (s *Store) manifestReadableLocked(manifest *manifestRecord) bool {
	for _, ref := range s.manifestRefs(manifest) {
		chunk := s.meta.Chunks[ref.ChunkID]
		if chunk == nil || chunk.SegmentID == "" || chunk.State == chunkStateDeleted || chunk.State == chunkStateDeleting || chunk.State == chunkStateCorrupt {
			return false
		}
		seg := s.meta.Segments[chunk.SegmentID]
		if seg == nil || seg.State == segmentStateDeleted || seg.State == segmentStateCorrupt {
			return false
		}
	}
	return true
}

func (s *Store) markManifestDeletedIfUnreferencedLocked(manifestID string, now int64) {
	manifest := s.meta.Manifests[manifestID]
	if manifest == nil {
		return
	}
	for _, file := range s.meta.Files {
		if file.ManifestID == manifestID && file.State == fileStateActive {
			return
		}
	}
	manifest.State = manifestStateDeleted
	manifest.DeletedAt = now
}

func (s *Store) manifestRefs(manifest *manifestRecord) []manifestChunk {
	return manifest.Chunks
}

func (s *Store) segmentSnapshot(segmentID string) (segmentRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seg := s.meta.Segments[segmentID]
	if seg == nil {
		return segmentRecord{}, false
	}
	return *seg, true
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
