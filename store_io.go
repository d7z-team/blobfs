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
	"time"
)

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

type RepairObjectOptions struct {
	BaseGeneration uint64
	Options        map[string]string
}

type LeaseOptions struct {
	TTL           time.Duration
	Holder        string
	AutoRenew     bool
	RenewInterval time.Duration
}

type CleanupOptions struct {
	Workers          int
	BatchSize        int
	Filter           func(*CleanupInfo) bool
	ErrorHandler     func(error, *CleanupInfo)
	ProgressCallback func(processed, total int64, current *TenantCount) bool
	DryRun           bool
}

type CleanupInfo struct {
	TenantID  string
	Path      string
	Inode     inodeRecord
	Depth     int
	Size      int64
	CreatedAt time.Time
	UpdatedAt time.Time
	State     string
}

// ObjectInfo describes an active file and its user metadata.
type ObjectInfo struct {
	FileID     string
	TenantID   string
	Path       string
	Size       int64
	FileHash   string
	ManifestID string
	State      string
	Readable   bool
	Writable   bool
	Reason     string
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
	allowDegraded   bool
	requireDegraded bool
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

// Put stores or replaces a file and records optional string metadata.
func (s *Store) Put(ctx context.Context, tenantID, path string, input io.Reader, options map[string]string) (*PutResult, error) {
	return s.putObject(ctx, tenantID, path, input, putCommitOptions{options: copyOptions(options)})
}

func (s *Store) RepairObject(ctx context.Context, tenantID, path string, input io.Reader, opts RepairObjectOptions) (*PutResult, error) {
	return s.putObject(ctx, tenantID, path, input, putCommitOptions{
		baseGeneration:  opts.BaseGeneration,
		checkGeneration: opts.BaseGeneration != 0,
		allowDegraded:   true,
		requireDegraded: true,
		options:         copyOptions(opts.Options),
	})
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
		existing = s.visibleInodeLocked(childID)
	}
	if existing != nil && existing.Kind != fileKindFile {
		return nil, pathError("put", prepared.path, ErrIsDir)
	}
	if opts.requireDegraded {
		if existing == nil {
			return nil, notExist("repair", prepared.path)
		}
		if existing.State != fileStateDegraded {
			return nil, ErrConflict
		}
	}
	if existing != nil && existing.State == fileStateDegraded && !opts.allowDegraded {
		return nil, ErrObjectDegraded
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
		if current == nil || !chunkReadableState(current.State) ||
			segment == nil || segment.State == segmentStateDeleted || segment.State == segmentStateMissing {
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
			if current != nil && current.State == chunkStateMissing {
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
	appendRefDeltaOpsLocked(s.meta, &ops, manifestRecords, manifestDeltas, chunkDeltas, now, &s.refCountWarnings)
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
	inode.DegradedAt = 0
	inode.DegradedReason = ""
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

func (s *Store) OpenObjectWithLease(ctx context.Context, tenantID, path string, opts *LeaseOptions) (*ObjectReader, *LeaseHandle, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, nil, err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, nil, pathError("open", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, nil, pathError("open", path, err)
	}

	reader, err := s.openReader(tenantID, path, 0, -1)
	if err != nil {
		return nil, nil, err
	}

	holder := "object-reader"
	requestedTTL := time.Duration(0)
	autoRenew := true
	if opts != nil {
		if opts.Holder != "" {
			holder = opts.Holder
		}
		requestedTTL = opts.TTL
		if !opts.AutoRenew {
			autoRenew = false
		}
	}

	var handles []*LeaseHandle
	for _, segID := range reader.pinnedSegments {
		h, err := s.grantLease(segID, holder, requestedTTL)
		if err != nil {
			for _, h := range handles {
				_ = h.Release(ctx)
			}
			reader.Close()
			return nil, nil, err
		}
		h.autoRenew = autoRenew
		handles = append(handles, h)
	}
	reader.leaseHandles = handles
	return reader, handles[0], nil
}

func (s *Store) ExtendObjectLease(ctx context.Context, reader *ObjectReader, ttl time.Duration) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()
	if reader == nil {
		return errors.New("reader is nil")
	}
	for _, h := range reader.leaseHandles {
		if err := h.Renew(ctx, ttl); err != nil {
			return err
		}
	}
	return nil
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

func (s *Store) pinChunkSnapshot(chunkID string) *chunkRecord {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	chunk := s.meta.Chunks[chunkID]
	if chunk == nil || (chunk.RefCount <= 0 && chunk.State != chunkStateGarbageCandidate) {
		return nil
	}
	if !chunkReadableState(chunk.State) {
		return nil
	}
	segment := s.meta.Segments[chunk.SegmentID]
	if segment == nil || segment.State == segmentStateDeleted || segment.State == segmentStateMissing {
		return nil
	}
	s.pinSegment(chunk.SegmentID)
	next := *chunk
	return &next
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
