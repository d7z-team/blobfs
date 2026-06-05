package blobfs

import (
	"errors"
	"io"
	"sort"
	"sync"
)

// ObjectReader reads an immutable snapshot of a file's manifest and chunks.
type ObjectReader struct {
	mu             sync.Mutex
	store          *Store
	size           int64
	offset         int64
	limitEnd       int64
	refs           []chunkSnapshot
	buf            []byte
	bufStart       int64
	bufEnd         int64
	closed         bool
	fileHash       string
	info           ObjectInfo
	pinnedSegments []string
}

type chunkSnapshot struct {
	Ref     manifestChunk
	Chunk   chunkRecord
	Segment segmentRecord
}

func (s *Store) openReader(tenantID, path string, rangeOffset, rangeLength int64) (*ObjectReader, error) {
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("open", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("open", path, err)
	}
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return nil, pathError("open", path, err)
	}
	if inode.Kind != fileKindFile {
		return nil, pathError("open", path, ErrIsDir)
	}
	manifest := s.meta.Manifests[inode.ManifestID]
	if manifest == nil || manifest.State == manifestStateDeleted {
		return nil, errors.New("manifest not found")
	}
	refs := append([]manifestChunk(nil), manifest.Chunks...)
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].FileOffset < refs[j].FileOffset
	})
	snapshots := make([]chunkSnapshot, 0, len(refs))
	for _, ref := range refs {
		chunk := s.meta.Chunks[ref.ChunkID]
		if chunk == nil || chunk.State == chunkStateDeleted || chunk.State == chunkStateCorrupt {
			return nil, errors.New("chunk not readable")
		}
		seg := s.meta.Segments[chunk.SegmentID]
		if seg == nil || seg.State == segmentStateDeleted || seg.State == segmentStateCorrupt {
			return nil, errors.New("chunk not readable")
		}
		snapshots = append(snapshots, chunkSnapshot{Ref: ref, Chunk: *chunk, Segment: *seg})
	}
	pinned := make([]string, 0, len(snapshots))
	seenPins := map[string]bool{}
	for _, snap := range snapshots {
		if seenPins[snap.Segment.SegmentID] {
			continue
		}
		seenPins[snap.Segment.SegmentID] = true
		s.pinSegment(snap.Segment.SegmentID)
		pinned = append(pinned, snap.Segment.SegmentID)
	}
	if rangeOffset > inode.Size {
		for _, segmentID := range pinned {
			s.unpinSegment(segmentID)
		}
		return nil, io.EOF
	}
	limitEnd := inode.Size
	if rangeLength >= 0 {
		if rangeLength > inode.Size-rangeOffset {
			limitEnd = inode.Size
		} else {
			limitEnd = rangeOffset + rangeLength
		}
	}
	return &ObjectReader{
		store:          s,
		size:           inode.Size,
		offset:         rangeOffset,
		limitEnd:       limitEnd,
		refs:           snapshots,
		fileHash:       inode.FileHash,
		info:           objectInfoFromInode(inode, path),
		pinnedSegments: pinned,
	}, nil
}

func (r *ObjectReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, errors.New("reader is closed")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.offset >= r.limitEnd {
		return 0, io.EOF
	}
	total := 0
	for len(p) > 0 && r.offset < r.limitEnd {
		if r.offset < r.bufStart || r.offset >= r.bufEnd {
			if err := r.loadChunkAtOffset(r.offset); err != nil {
				if total > 0 {
					return total, nil
				}
				return 0, err
			}
		}
		available := r.bufEnd - r.offset
		if available > r.limitEnd-r.offset {
			available = r.limitEnd - r.offset
		}
		if available > int64(len(p)) {
			available = int64(len(p))
		}
		start := r.offset - r.bufStart
		copy(p[:available], r.buf[start:start+available])
		p = p[available:]
		total += int(available)
		r.offset += available
	}
	if r.offset >= r.limitEnd {
		return total, io.EOF
	}
	return total, nil
}

func (r *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, errors.New("reader is closed")
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, errors.New("invalid seek whence")
	}
	if next < 0 {
		return 0, errors.New("negative seek offset")
	}
	if next > r.limitEnd {
		next = r.limitEnd
	}
	r.offset = next
	return r.offset, nil
}

func (r *ObjectReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	pinned := r.pinnedSegments
	r.pinnedSegments = nil
	r.buf = nil
	r.mu.Unlock()
	for _, segmentID := range pinned {
		r.store.unpinSegment(segmentID)
	}
	return nil
}

// Info returns metadata captured when the reader was opened.
func (r *ObjectReader) Info() ObjectInfo {
	return r.info
}

// ETag returns the file content hash for the opened object.
func (r *ObjectReader) ETag() string {
	return r.fileHash
}

func (r *ObjectReader) loadChunkAtOffset(offset int64) error {
	for _, ref := range r.refs {
		start := ref.Ref.FileOffset
		end := start + ref.Ref.ChunkSize
		if offset >= start && offset < end {
			data, err := r.store.readChunkPayloadAt(ref.Segment, ref.Chunk)
			if err != nil {
				return err
			}
			r.buf = data
			r.bufStart = start
			r.bufEnd = end
			return nil
		}
	}
	return io.EOF
}

var _ io.ReadSeekCloser = (*ObjectReader)(nil)
