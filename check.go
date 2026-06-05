package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
)

var ErrCorrupt = errors.New("blobfs corruption detected")

type chunkCheckSnapshot struct {
	TenantID string
	Path     string
	Ref      manifestChunk
	Chunk    chunkRecord
	Segment  segmentRecord
	HasSeg   bool
}

// CheckObject verifies one active object from metadata references through chunk and file hashes.
func (s *Store) CheckObject(ctx context.Context, tenantID, path string) (*CheckResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("check", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("check", path, err)
	}
	lock := s.pathLocks.Open(fileKey(tenantID, path)).Lock(true)
	defer lock.Close()

	s.mu.Lock()
	file, _, err := s.activeFileInfoLocked(tenantID, path)
	if err != nil {
		s.mu.Unlock()
		return nil, pathError("check", path, err)
	}
	manifest := s.meta.Manifests[file.ManifestID]
	if manifest == nil {
		s.mu.Unlock()
		return nil, errors.New("manifest not found")
	}
	refs := s.manifestRefs(manifest)
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].FileOffset < refs[j].FileOffset
	})
	snapshots := make([]chunkCheckSnapshot, 0, len(refs))
	for _, ref := range refs {
		chunk := s.meta.Chunks[ref.ChunkID]
		if chunk == nil {
			snapshots = append(snapshots, chunkCheckSnapshot{
				TenantID: tenantID,
				Path:     path,
				Ref:      ref,
			})
			continue
		}
		seg, ok := s.meta.Segments[chunk.SegmentID]
		snap := chunkCheckSnapshot{TenantID: tenantID, Path: path, Ref: ref, Chunk: *chunk, HasSeg: ok}
		if ok {
			snap.Segment = *seg
		}
		snapshots = append(snapshots, snap)
	}
	fileHash := file.FileHash
	fileSize := file.Size
	scopeID := s.dedupScopeID(tenantID)
	s.mu.Unlock()

	result := &CheckResult{TenantID: tenantID, Path: path, Healthy: true}
	content := bytes.NewBuffer(make([]byte, 0, fileSize))
	for _, snap := range snapshots {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		raw, issue := s.checkChunkSnapshot(snap)
		result.CheckedChunks++
		if snap.HasSeg {
			result.CheckedSegments++
		}
		if issue != nil {
			result.Issues = append(result.Issues, *issue)
			continue
		}
		result.CheckedBytes += int64(len(raw))
		content.Write(raw)
	}
	if len(result.Issues) == 0 {
		gotHash := hashBytes(scopeID, scopeID != "", content.Bytes())
		if gotHash != fileHash || int64(content.Len()) != fileSize {
			result.Issues = append(result.Issues, CheckIssue{
				Kind:     "file_hash_mismatch",
				ID:       fileHash,
				Path:     path,
				TenantID: tenantID,
				Reason:   "file content hash or size mismatch",
			})
		}
	}
	if len(result.Issues) > 0 {
		result.Healthy = false
		if err := s.markCorruption(result.Issues); err != nil {
			return result, err
		}
		return result, ErrCorrupt
	}
	return result, nil
}

// Scrub verifies stored chunks and optionally active file hashes across the whole store.
func (s *Store) Scrub(ctx context.Context, opts ScrubOptions) (*ScrubResult, error) {
	s.mu.Lock()
	snapshots := make([]chunkCheckSnapshot, 0, len(s.meta.Chunks))
	affected := map[string][]string{}
	for _, file := range s.meta.Files {
		if file.State != fileStateActive || file.Kind != fileKindFile {
			continue
		}
		manifest := s.meta.Manifests[file.ManifestID]
		if manifest == nil {
			continue
		}
		for _, ref := range s.manifestRefs(manifest) {
			affected[ref.ChunkID] = append(affected[ref.ChunkID], fileKey(file.TenantID, file.Path))
		}
	}
	for _, chunk := range s.meta.Chunks {
		if chunk.State == chunkStateDeleted || chunk.SegmentID == "" {
			continue
		}
		seg, ok := s.meta.Segments[chunk.SegmentID]
		snap := chunkCheckSnapshot{Chunk: *chunk, HasSeg: ok}
		if ok {
			snap.Segment = *seg
		}
		snapshots = append(snapshots, snap)
	}
	files := make([]fileRecord, 0, len(s.meta.Files))
	if opts.CheckFiles {
		for _, file := range s.meta.Files {
			if file.State == fileStateActive && file.Kind == fileKindFile {
				files = append(files, *file)
			}
		}
	}
	s.mu.Unlock()

	result := &ScrubResult{Healthy: true}
	seenSegments := map[string]bool{}
	seenCorruptChunks := map[string]bool{}
	seenCorruptSegments := map[string]bool{}
	seenAffected := map[string]bool{}
	for _, snap := range snapshots {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		raw, issue := s.checkChunkSnapshot(snap)
		result.CheckedChunks++
		if snap.HasSeg && !seenSegments[snap.Segment.SegmentID] {
			result.CheckedSegments++
			seenSegments[snap.Segment.SegmentID] = true
		}
		if issue == nil {
			result.CheckedBytes += int64(len(raw))
			continue
		}
		result.Issues = append(result.Issues, *issue)
		if issue.ChunkID != "" {
			seenCorruptChunks[issue.ChunkID] = true
			for _, fileKey := range affected[issue.ChunkID] {
				seenAffected[fileKey] = true
			}
		}
		if issue.SegmentID != "" {
			seenCorruptSegments[issue.SegmentID] = true
		}
	}
	if len(result.Issues) > 0 {
		if err := s.markCorruption(result.Issues); err != nil {
			return result, err
		}
	}
	for _, file := range files {
		check, err := s.CheckObject(ctx, file.TenantID, file.Path)
		result.CheckedFiles++
		if err != nil && !errors.Is(err, ErrCorrupt) {
			return result, err
		}
		if check != nil && len(check.Issues) > 0 {
			result.Issues = append(result.Issues, check.Issues...)
			seenAffected[fileKey(file.TenantID, file.Path)] = true
		}
	}
	for id := range seenCorruptChunks {
		result.CorruptChunks = append(result.CorruptChunks, id)
	}
	for id := range seenCorruptSegments {
		result.CorruptSegments = append(result.CorruptSegments, id)
	}
	for key := range seenAffected {
		result.AffectedFiles = append(result.AffectedFiles, key)
	}
	sort.Strings(result.CorruptChunks)
	sort.Strings(result.CorruptSegments)
	sort.Strings(result.AffectedFiles)
	if len(result.Issues) > 0 {
		result.Healthy = false
		return result, ErrCorrupt
	}
	return result, nil
}

func (s *Store) checkChunkSnapshot(snap chunkCheckSnapshot) ([]byte, *CheckIssue) {
	if snap.Chunk.ChunkID == "" {
		return nil, &CheckIssue{Kind: "chunk_missing", Path: snap.Path, TenantID: snap.TenantID, ChunkID: snap.Ref.ChunkID, Reason: "chunk metadata is missing"}
	}
	if !snap.HasSeg {
		return nil, &CheckIssue{Kind: "segment_missing", Path: snap.Path, TenantID: snap.TenantID, ChunkID: snap.Chunk.ChunkID, SegmentID: snap.Chunk.SegmentID, Reason: "segment metadata is missing"}
	}
	if snap.Chunk.State == chunkStateCorrupt {
		return nil, &CheckIssue{Kind: "chunk_corrupt", Path: snap.Path, TenantID: snap.TenantID, ChunkID: snap.Chunk.ChunkID, SegmentID: snap.Chunk.SegmentID, Reason: snap.Chunk.CorruptReason}
	}
	if snap.Segment.State == segmentStateCorrupt {
		return nil, &CheckIssue{Kind: "segment_corrupt", Path: snap.Path, TenantID: snap.TenantID, ChunkID: snap.Chunk.ChunkID, SegmentID: snap.Segment.SegmentID, Reason: snap.Segment.CorruptReason}
	}
	raw, err := s.readChunkPayloadAt(snap.Segment, snap.Chunk)
	if err != nil {
		kind := "chunk_read_failed"
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			kind = "segment_read_failed"
		}
		return nil, &CheckIssue{Kind: kind, Path: snap.Path, TenantID: snap.TenantID, ChunkID: snap.Chunk.ChunkID, SegmentID: snap.Segment.SegmentID, Reason: err.Error()}
	}
	gotChunkID := hashBytes(snap.Chunk.TenantID, snap.Chunk.TenantID != "", raw)
	if gotChunkID != snap.Chunk.ChunkID {
		return nil, &CheckIssue{Kind: "chunk_hash_mismatch", Path: snap.Path, TenantID: snap.TenantID, ChunkID: snap.Chunk.ChunkID, SegmentID: snap.Segment.SegmentID, Reason: "chunk sha256 mismatch"}
	}
	return raw, nil
}

func (s *Store) markCorruption(issues []CheckIssue) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := nowUnix()
	for _, issue := range issues {
		if issue.ChunkID != "" {
			if chunk := s.meta.Chunks[issue.ChunkID]; chunk != nil && chunk.State != chunkStateDeleted {
				chunk.State = chunkStateCorrupt
				chunk.CorruptAt = now
				chunk.CorruptReason = issue.Reason
			}
		}
		if issue.SegmentID != "" {
			if seg := s.meta.Segments[issue.SegmentID]; seg != nil && seg.State != segmentStateDeleted {
				seg.State = segmentStateCorrupt
				seg.CorruptAt = now
				seg.CorruptReason = issue.Reason
			}
		}
	}
	return saveMetadata(s.fs, s.metaPath, s.meta)
}
