package blobfs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

var ErrCorrupt = errors.New("blobfs corruption detected")

type chunkCheckSnapshot struct {
	TenantID string
	Path     string
	Ref      manifestChunk
	Chunk    chunkRecord
	Segment  segmentRecord
	HasChunk bool
	HasSeg   bool
}

// CheckObject verifies one active object from metadata references through chunk and file hashes.
func (s *Store) CheckObject(ctx context.Context, tenantID, path string) (*CheckResult, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	return s.checkObject(ctx, tenantID, path)
}

func (s *Store) checkObject(ctx context.Context, tenantID, path string) (*CheckResult, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("check", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("check", path, err)
	}
	snapshots, fileHash, fileSize, scopeID, err := s.checkSnapshots(tenantID, path)
	if err != nil {
		return nil, pathError("check", path, err)
	}
	result := &CheckResult{TenantID: tenantID, Path: path, Healthy: true}
	contentHash := scopedHasher(scopeID, scopeID != "")
	var contentSize int64
	for _, snap := range snapshots {
		if err := contextError(ctx); err != nil {
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
		contentHash.Write(raw)
		contentSize += int64(len(raw))
	}
	if len(result.Issues) == 0 {
		gotHash := hex.EncodeToString(contentHash.Sum(nil))
		if gotHash != fileHash || contentSize != fileSize {
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

func (s *Store) checkSnapshots(tenantID, path string) ([]chunkCheckSnapshot, string, int64, string, error) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return nil, "", 0, "", err
	}
	if inode.Kind != fileKindFile {
		return nil, "", 0, "", ErrIsDir
	}
	manifest := s.meta.Manifests[inode.ManifestID]
	if manifest == nil {
		return nil, "", 0, "", errors.New("manifest not found")
	}
	refs := append([]manifestChunk(nil), manifest.Chunks...)
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].FileOffset < refs[j].FileOffset
	})
	snapshots := make([]chunkCheckSnapshot, 0, len(refs))
	for _, ref := range refs {
		snap := chunkCheckSnapshot{TenantID: tenantID, Path: path, Ref: ref}
		chunk := s.meta.Chunks[ref.ChunkID]
		if chunk != nil {
			snap.Chunk = *chunk
			snap.HasChunk = true
			seg := s.meta.Segments[chunk.SegmentID]
			if seg != nil {
				snap.Segment = *seg
				snap.HasSeg = true
			}
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, inode.FileHash, inode.Size, s.dedupScopeID(tenantID), nil
}

// Scrub verifies stored chunks and optionally active file hashes across the whole store.
func (s *Store) Scrub(ctx context.Context, opts ScrubOptions) (*ScrubResult, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()

	s.metaMu.RLock()
	chunkIDs := make([]string, 0, len(s.meta.Chunks))
	for chunkID, chunk := range s.meta.Chunks {
		if chunk == nil || chunk.State == chunkStateDeleted || chunk.SegmentID == "" {
			continue
		}
		chunkIDs = append(chunkIDs, chunkID)
	}
	fileIDs := make([]uint64, 0, len(s.meta.Inodes))
	if opts.CheckFiles {
		for inodeID, inode := range s.meta.Inodes {
			if inode != nil && inode.State == fileStateActive && inode.Kind == fileKindFile {
				fileIDs = append(fileIDs, inodeID)
			}
		}
	}
	s.metaMu.RUnlock()

	result := &ScrubResult{Healthy: true}
	seenSegments := map[string]bool{}
	seenCorruptChunks := map[string]bool{}
	seenCorruptSegments := map[string]bool{}
	seenAffected := map[string]bool{}
	for _, chunkID := range chunkIDs {
		if err := contextError(ctx); err != nil {
			return result, err
		}
		s.metaMu.RLock()
		chunk := s.meta.Chunks[chunkID]
		if chunk == nil || chunk.State == chunkStateDeleted || chunk.SegmentID == "" {
			s.metaMu.RUnlock()
			continue
		}
		snap := chunkCheckSnapshot{Chunk: *chunk, HasChunk: true}
		if seg := s.meta.Segments[chunk.SegmentID]; seg != nil {
			snap.Segment = *seg
			snap.HasSeg = true
		}
		s.metaMu.RUnlock()
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
			paths, pathIssues := s.pathsForChunk(issue.ChunkID)
			result.Issues = append(result.Issues, pathIssues...)
			for _, fileKey := range paths {
				seenAffected[fileKey] = true
			}
		}
		if issue.SegmentID != "" {
			seenCorruptSegments[issue.SegmentID] = true
		}
	}
	for _, fileID := range fileIDs {
		s.metaMu.RLock()
		inode := s.meta.Inodes[fileID]
		if inode == nil || inode.State != fileStateActive || inode.Kind != fileKindFile {
			s.metaMu.RUnlock()
			continue
		}
		tenantID := inode.TenantID
		path, pathErr := s.pathForInodeLocked(fileID)
		s.metaMu.RUnlock()
		result.CheckedFiles++
		if pathErr != nil {
			result.Issues = append(result.Issues, CheckIssue{Kind: "inode_parent_invalid", ID: inodeFileID(fileID), TenantID: tenantID, Reason: pathErr.Error()})
			continue
		}
		check, err := s.checkObject(ctx, tenantID, path)
		if err != nil && !errors.Is(err, ErrCorrupt) {
			return result, err
		}
		if check != nil && len(check.Issues) > 0 {
			result.Issues = append(result.Issues, check.Issues...)
			seenAffected[tenantID+"/"+path] = true
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
		if err := s.markCorruption(result.Issues); err != nil {
			return result, err
		}
		return result, ErrCorrupt
	}
	return result, nil
}

func (s *Store) pathsForChunk(chunkID string) ([]string, []CheckIssue) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	var paths []string
	var issues []CheckIssue
	for _, inode := range s.meta.Inodes {
		if inode == nil || inode.State != fileStateActive || inode.Kind != fileKindFile {
			continue
		}
		manifest := s.meta.Manifests[inode.ManifestID]
		if manifest == nil {
			continue
		}
		matches := false
		for _, ref := range manifest.Chunks {
			if ref.ChunkID == chunkID {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		path, err := s.pathForInodeLocked(inode.InodeID)
		if err != nil {
			issues = append(issues, CheckIssue{Kind: "inode_parent_invalid", ID: inodeFileID(inode.InodeID), TenantID: inode.TenantID, ChunkID: chunkID, Reason: err.Error()})
			continue
		}
		paths = append(paths, inode.TenantID+"/"+path)
	}
	return paths, issues
}

func (s *Store) checkChunkSnapshot(snap chunkCheckSnapshot) ([]byte, *CheckIssue) {
	if !snap.HasChunk {
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
	s.pinSegment(snap.Segment.SegmentID)
	raw, err := s.readChunkPayloadAt(snap.Segment, snap.Chunk)
	s.unpinSegment(snap.Segment.SegmentID)
	if err != nil {
		kind := "chunk_read_failed"
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			kind = "segment_read_failed"
		} else if errors.Is(err, errChunkHashMismatch) {
			kind = "chunk_hash_mismatch"
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
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	now := nowUnix()
	ops := make([]metaOp, 0, len(issues)*2)
	for _, issue := range issues {
		if issue.ChunkID != "" {
			if chunk := s.meta.Chunks[issue.ChunkID]; chunk != nil && chunk.State != chunkStateDeleted {
				next := *chunk
				next.State = chunkStateCorrupt
				next.CorruptAt = now
				next.CorruptReason = issue.Reason
				ops = append(ops, metaOp{Type: "put_chunk", Chunk: &next})
			}
		}
		if issue.SegmentID != "" {
			if seg := s.meta.Segments[issue.SegmentID]; seg != nil && seg.State != segmentStateDeleted {
				next := *seg
				next.State = segmentStateCorrupt
				next.CorruptAt = now
				next.CorruptReason = issue.Reason
				ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
			}
		}
	}
	return s.commitMetaLocked(ops)
}

func (s *Store) pathForInodeLocked(id uint64) (string, error) {
	var parts []string
	seen := map[uint64]bool{}
	for {
		if seen[id] {
			return "", fmt.Errorf("inode parent cycle at inode %d", id)
		}
		seen[id] = true
		inode := s.meta.Inodes[id]
		if inode == nil {
			return "", fmt.Errorf("inode %d not found", id)
		}
		if inode.ParentInode == 0 {
			break
		}
		parts = append(parts, inode.Name)
		id = inode.ParentInode
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, "/"), nil
}
