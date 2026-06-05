package blobfs

import (
	"errors"
	"io"
	"sort"
)

// ObjectReader reads an immutable snapshot of a file's manifest and chunks.
type ObjectReader struct {
	store    *Store
	size     int64
	offset   int64
	limitEnd int64
	refs     []chunkSnapshot
	buf      []byte
	bufStart int64
	bufEnd   int64
	closed   bool
	fileHash string
	info     ObjectInfo
}

type chunkSnapshot struct {
	Ref   manifestChunk
	Chunk chunkRecord
}

func (s *Store) openReader(tenantID, path string, rangeOffset, rangeLength int64) (*ObjectReader, error) {
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("open", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("open", path, err)
	}
	lock := s.pathLocks.Open(fileKey(tenantID, path)).Lock(true)
	defer lock.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	file, info, err := s.activeFileInfoLocked(tenantID, path)
	if err != nil {
		return nil, pathError("open", path, err)
	}
	manifest := s.meta.Manifests[file.ManifestID]
	if manifest == nil {
		return nil, errors.New("manifest not found")
	}
	refs := s.manifestRefs(manifest)
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].FileOffset < refs[j].FileOffset
	})
	snapshots := make([]chunkSnapshot, 0, len(refs))
	for _, ref := range refs {
		chunk := s.meta.Chunks[ref.ChunkID]
		if chunk == nil || chunk.State == chunkStateDeleted || chunk.State == chunkStateDeleting || chunk.State == chunkStateCorrupt {
			return nil, errors.New("chunk not readable")
		}
		seg := s.meta.Segments[chunk.SegmentID]
		if seg == nil || seg.State == segmentStateDeleted || seg.State == segmentStateCorrupt {
			return nil, errors.New("chunk not readable")
		}
		snapshots = append(snapshots, chunkSnapshot{Ref: ref, Chunk: *chunk})
	}
	if rangeOffset > file.Size {
		return nil, io.EOF
	}
	limitEnd := file.Size
	if rangeLength >= 0 {
		if rangeLength > file.Size-rangeOffset {
			limitEnd = file.Size
		} else {
			limitEnd = rangeOffset + rangeLength
		}
	}
	return &ObjectReader{
		store:    s,
		size:     file.Size,
		offset:   rangeOffset,
		limitEnd: limitEnd,
		refs:     snapshots,
		fileHash: file.FileHash,
		info:     info,
	}, nil
}

func (r *ObjectReader) Read(p []byte) (int, error) {
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
	r.closed = true
	r.buf = nil
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
			data, err := r.store.readChunkPayload(ref.Chunk)
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
