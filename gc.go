package blobfs

import (
	"context"
	"os"
	"time"
)

type compactCandidate struct {
	Source segmentRecord
	Chunks []chunkRecord
}

type compactResult struct {
	Source   segmentRecord
	Segments []*segmentRecord
	Original []chunkRecord
	Moved    []chunkRecord
}

// RunGC marks unreferenced chunks, optionally compacts fragmented segments, and
// deletes fully dead segments without holding metadata locks during filesystem IO.
func (s *Store) RunGC(ctx context.Context, opts GCOptions) (*GCResult, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	nowTime := time.Now()
	now := nowTime.UnixNano()
	segmentDeleteCutoff := now - int64(s.cfg.GC.SegmentDeleteDelay)
	safetyWindow := s.cfg.GC.SafetyWindow
	if opts.SafetyWindow != 0 {
		safetyWindow = opts.SafetyWindow
	}
	if safetyWindow < 0 {
		safetyWindow = 0
	}
	confirmCycles := s.cfg.GC.CandidateConfirmCycles
	if opts.CandidateConfirmCycles > 0 {
		confirmCycles = opts.CandidateConfirmCycles
	}
	if confirmCycles < 1 {
		confirmCycles = 1
	}

	result := &GCResult{}
	var removeSegments []segmentRecord
	var compactCandidates []compactCandidate

	s.metaMu.Lock()
	epoch := s.meta.NextGCEpoch
	s.meta.NextGCEpoch++
	result.Epoch = epoch
	ops := []metaOp{{Type: "append_gcrun", GCRun: &gcRun{Epoch: epoch, State: "DONE", StartedAt: now, FinishedAt: now, SafetyCutoff: nowTime.Add(-safetyWindow).UnixNano()}}}
	s.collectUnreachableInodesLocked(now, &ops)
	if err := s.commitMetaLocked(ops); err != nil {
		s.metaMu.Unlock()
		return nil, err
	}

	ops = ops[:0]
	s.markUnreferencedChunksLocked(now, nowTime.Add(-safetyWindow).UnixNano(), confirmCycles, result, &ops)
	if err := s.commitMetaLocked(ops); err != nil {
		s.metaMu.Unlock()
		return nil, err
	}

	if opts.Compact {
		compactCandidates = s.collectCompactCandidatesLocked()
		ops = ops[:0]
		for _, candidate := range compactCandidates {
			next := candidate.Source
			next.State = segmentStateCompacting
			ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
		}
		if err := s.commitMetaLocked(ops); err != nil {
			s.metaMu.Unlock()
			return nil, err
		}
	}
	removeSegments = s.collectDeadSegmentsLocked(segmentDeleteCutoff)
	s.metaMu.Unlock()

	if len(compactCandidates) > 0 {
		compacted, err := s.compactCandidates(ctx, compactCandidates)
		if err != nil {
			s.rollbackCompaction(compactCandidates)
			return result, err
		}
		deleted, err := s.commitCompactionResults(compacted, result, now, segmentDeleteCutoff)
		if err != nil {
			for _, item := range compacted {
				for _, seg := range item.Segments {
					_ = s.fs.Remove(s.segmentPath(seg))
				}
			}
			s.rollbackCompaction(compactCandidates)
			return result, err
		}
		removeSegments = append(removeSegments, deleted...)
		s.metaMu.RLock()
		removeSegments = append(removeSegments, s.collectDeadSegmentsLocked(segmentDeleteCutoff)...)
		s.metaMu.RUnlock()
	}

	deleted, err := s.removeSegmentFiles(ctx, removeSegments)
	if err != nil || len(deleted) == 0 {
		return result, err
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	ops = ops[:0]
	for _, seg := range deleted {
		if current := s.meta.Segments[seg.SegmentID]; current != nil && current.State != segmentStateDeleted && s.segmentReadyForRemovalLocked(current, segmentDeleteCutoff) {
			next := *current
			next.State = segmentStateDeleted
			next.DeletedAt = now
			ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
			result.SegmentsDeleted++
		}
	}
	return result, s.commitMetaLocked(ops)
}

func (s *Store) markUnreferencedChunksLocked(now, cutoff int64, confirmCycles int, result *GCResult, ops *[]metaOp) {
	for _, chunk := range s.meta.Chunks {
		if chunk.State == chunkStateDeleted || chunk.RefCount > 0 || chunk.CreatedAt >= cutoff {
			if chunk.RefCount > 0 {
				result.LiveChunks++
			}
			continue
		}
		next := *chunk
		switch chunk.State {
		case chunkStateActive:
			if confirmCycles <= 1 {
				next.State = chunkStateDeleted
				next.DeletedAt = now
				result.ChunksDeleted++
				result.BytesMadeGarbage += chunk.StoredSize
			} else {
				next.State = chunkStateGarbageCandidate
				next.GarbageSeenCount = 1
				next.GarbageCandidateAt = now
				result.CandidatesMarked++
			}
		case chunkStateGarbageCandidate:
			if chunk.GarbageSeenCount+1 >= confirmCycles {
				next.State = chunkStateDeleted
				next.DeletedAt = now
				result.ChunksDeleted++
				result.BytesMadeGarbage += chunk.StoredSize
			} else {
				next.GarbageSeenCount++
				result.CandidatesMarked++
			}
		}
		*ops = append(*ops, metaOp{Type: "put_chunk", Chunk: &next})
	}
}

func (s *Store) collectCompactCandidatesLocked() []compactCandidate {
	var candidates []compactCandidate
	for _, seg := range s.meta.Segments {
		if seg.State != segmentStateSealed || s.segmentPinned(seg.SegmentID) {
			continue
		}
		var liveBytes, garbageBytes int64
		var liveChunks []chunkRecord
		for _, chunk := range s.meta.Chunks {
			if chunk.SegmentID != seg.SegmentID {
				continue
			}
			if chunk.State == chunkStateDeleted || chunk.RefCount == 0 {
				garbageBytes += chunk.SegmentLength
				continue
			}
			liveBytes += chunk.SegmentLength
			liveChunks = append(liveChunks, *chunk)
		}
		total := liveBytes + garbageBytes
		if liveBytes == 0 || garbageBytes == 0 || total == 0 {
			continue
		}
		if float64(garbageBytes)/float64(total) >= s.cfg.GC.CompactGarbageRatio {
			candidates = append(candidates, compactCandidate{Source: *seg, Chunks: liveChunks})
		}
	}
	return candidates
}

func (s *Store) compactCandidates(ctx context.Context, candidates []compactCandidate) ([]compactResult, error) {
	results := make([]compactResult, 0, len(candidates))
	for _, candidate := range candidates {
		if err := contextError(ctx); err != nil {
			s.removeCompactedSegments(results)
			return nil, err
		}
		writer := &segmentBatchWriter{store: s}
		result := compactResult{Source: candidate.Source}
		for _, chunk := range candidate.Chunks {
			raw, err := s.readChunkPayloadAt(candidate.Source, chunk)
			if err != nil {
				writer.cleanup()
				s.removeCompactedSegments(results)
				return nil, err
			}
			next, err := writer.appendChunk(chunk.TenantID, chunk.ChunkID, raw)
			if err != nil {
				writer.cleanup()
				s.removeCompactedSegments(results)
				return nil, err
			}
			next.RefCount = chunk.RefCount
			result.Original = append(result.Original, chunk)
			result.Moved = append(result.Moved, next)
		}
		if err := writer.finish(); err != nil {
			writer.cleanup()
			s.removeCompactedSegments(results)
			return nil, err
		}
		result.Segments = writer.segments
		results = append(results, result)
	}
	return results, nil
}

func (s *Store) removeCompactedSegments(results []compactResult) {
	for _, item := range results {
		for _, seg := range item.Segments {
			_ = s.fs.Remove(s.segmentPath(seg))
		}
	}
}

func (s *Store) commitCompactionResults(results []compactResult, gcResult *GCResult, now, segmentDeleteCutoff int64) ([]segmentRecord, error) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	var deleteSegments []segmentRecord
	ops := []metaOp{}
	for _, item := range results {
		source := s.meta.Segments[item.Source.SegmentID]
		if source == nil || source.State != segmentStateCompacting {
			continue
		}
		if len(item.Original) != len(item.Moved) {
			for _, seg := range item.Segments {
				deleteSegments = append(deleteSegments, *seg)
			}
			continue
		}
		var chunkUpdates []chunkRecord
		valid := true
		for i, moved := range item.Moved {
			original := item.Original[i]
			current := s.meta.Chunks[moved.ChunkID]
			if current == nil ||
				current.SegmentID != item.Source.SegmentID ||
				current.SegmentOffset != original.SegmentOffset ||
				current.SegmentLength != original.SegmentLength ||
				current.State == chunkStateDeleted ||
				current.RefCount == 0 {
				valid = false
				break
			}
			next := *current
			next.SegmentID = moved.SegmentID
			next.SegmentOffset = moved.SegmentOffset
			next.SegmentLength = moved.SegmentLength
			next.StoredSize = moved.StoredSize
			next.ChecksumCRC32C = moved.ChecksumCRC32C
			next.Compression = moved.Compression
			chunkUpdates = append(chunkUpdates, next)
		}
		if !valid {
			rollback := *source
			rollback.State = segmentStateSealed
			ops = append(ops, metaOp{Type: "put_segment", Segment: &rollback})
			for _, seg := range item.Segments {
				deleteSegments = append(deleteSegments, *seg)
			}
			continue
		}
		for _, seg := range item.Segments {
			next := *seg
			ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
		}
		for _, chunk := range chunkUpdates {
			next := chunk
			ops = append(ops, metaOp{Type: "put_chunk", Chunk: &next})
			gcResult.BytesRewritten += next.StoredSize
		}
		nextSource := *source
		nextSource.State = segmentStateSealed
		nextSource.CompactedAt = now
		if !s.segmentPinned(source.SegmentID) && nextSource.CompactedAt <= segmentDeleteCutoff {
			deleteSegments = append(deleteSegments, *source)
		}
		ops = append(ops, metaOp{Type: "put_segment", Segment: &nextSource})
		gcResult.SegmentsCompacted++
	}
	return deleteSegments, s.commitMetaLocked(ops)
}

func (s *Store) rollbackCompaction(candidates []compactCandidate) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	ops := []metaOp{}
	for _, candidate := range candidates {
		seg := s.meta.Segments[candidate.Source.SegmentID]
		if seg == nil || seg.State != segmentStateCompacting {
			continue
		}
		next := *seg
		next.State = segmentStateSealed
		ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
	}
	_ = s.commitMetaLocked(ops)
}

func (s *Store) collectDeadSegmentsLocked(segmentDeleteCutoff int64) []segmentRecord {
	var segments []segmentRecord
	for _, seg := range s.meta.Segments {
		if seg.State == segmentStateDeleted || seg.State == segmentStateCorrupt || s.segmentPinned(seg.SegmentID) {
			continue
		}
		if s.segmentReadyForRemovalLocked(seg, segmentDeleteCutoff) {
			segments = append(segments, *seg)
		}
	}
	return segments
}

func (s *Store) removeSegmentFiles(ctx context.Context, segments []segmentRecord) ([]segmentRecord, error) {
	var deleted []segmentRecord
	seen := map[string]bool{}
	for _, seg := range segments {
		if seen[seg.SegmentID] {
			continue
		}
		seen[seg.SegmentID] = true
		if err := contextError(ctx); err != nil {
			return deleted, err
		}
		if err := s.fs.Remove(s.segmentPath(&seg)); err != nil && !os.IsNotExist(err) {
			return deleted, err
		}
		deleted = append(deleted, seg)
	}
	return deleted, nil
}

func (s *Store) segmentHasLiveChunksLocked(segmentID string) bool {
	for _, chunk := range s.meta.Chunks {
		if chunk.SegmentID == segmentID && chunk.State != chunkStateDeleted {
			return true
		}
	}
	return false
}

func (s *Store) segmentReadyForRemovalLocked(seg *segmentRecord, segmentDeleteCutoff int64) bool {
	if s.segmentHasLiveChunksLocked(seg.SegmentID) {
		return false
	}
	deadAt := seg.CompactedAt
	for _, chunk := range s.meta.Chunks {
		if chunk.SegmentID != seg.SegmentID {
			continue
		}
		if chunk.DeletedAt == 0 {
			return false
		}
		if chunk.DeletedAt > deadAt {
			deadAt = chunk.DeletedAt
		}
	}
	if deadAt == 0 {
		deadAt = seg.SealedAt
	}
	if deadAt == 0 {
		deadAt = seg.CreatedAt
	}
	return deadAt <= segmentDeleteCutoff
}

func (s *Store) collectUnreachableInodesLocked(now int64, ops *[]metaOp) {
	reachable := map[uint64]bool{}
	for _, rootID := range s.meta.Tenants {
		s.markReachableLocked(rootID, reachable)
	}
	manifestRecords := map[string]*manifestRecord{}
	manifestDeltas := map[string]int{}
	chunkDeltas := map[string]int{}
	for _, inode := range s.meta.Inodes {
		if inode.State != fileStateActive || reachable[inode.InodeID] {
			continue
		}
		next := cloneInode(inode)
		next.State = fileStateDeleted
		next.DeletedAt = now
		next.UpdatedAt = now
		next.Generation++
		*ops = append(*ops, metaOp{Type: "put_inode", Inode: next})
		if inode.Kind == fileKindFile {
			if manifest := s.meta.Manifests[inode.ManifestID]; manifest != nil {
				manifestRecords[manifest.ManifestID] = manifest
				addManifestRefDelta(manifest, -1, manifestDeltas, chunkDeltas)
			}
		}
	}
	appendRefDeltaOpsLocked(s.meta, ops, manifestRecords, manifestDeltas, chunkDeltas, now)
}

func (s *Store) markReachableLocked(inodeID uint64, reachable map[uint64]bool) {
	inode := s.activeInodeLocked(inodeID)
	if inode == nil || reachable[inodeID] {
		return
	}
	reachable[inodeID] = true
	for _, childID := range s.meta.DirEntries[inodeID] {
		s.markReachableLocked(childID, reachable)
	}
}
