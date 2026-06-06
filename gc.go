package blobfs

import (
	"context"
	"errors"
	"fmt"
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

type segmentGCStats struct {
	Segment       segmentRecord
	LiveBytes     int64
	GarbageBytes  int64
	LiveChunks    []chunkRecord
	BlocksRemoval bool
	DeadAt        int64
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
	segmentDeleteDelay := s.cfg.GC.SegmentDeleteDelay
	if segmentDeleteDelay < 0 {
		segmentDeleteDelay = 0
	}
	segmentDeleteCutoff := now - int64(segmentDeleteDelay)
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
	startedAt := now
	safetyCutoff := nowTime.Add(-safetyWindow).UnixNano()
	ops := []metaOp{{Type: "append_gcrun", GCRun: &gcRun{Epoch: epoch, State: "STARTED", StartedAt: startedAt, SafetyCutoff: safetyCutoff}}}
	s.collectUnreachableInodesLocked(now, &ops)
	if err := s.commitMetaLocked(ops); err != nil {
		s.metaMu.Unlock()
		return nil, err
	}

	ops = ops[:0]
	s.markUnreferencedChunksLocked(now, nowTime.Add(-safetyWindow).UnixNano(), confirmCycles, result, &ops)
	if err := s.commitMetaLocked(ops); err != nil {
		s.metaMu.Unlock()
		return result, errors.Join(err, s.recordGCRun(epoch, "FAILED", startedAt, safetyCutoff, err.Error()))
	}

	compactCandidates, removeSegments = s.collectSegmentWorkLocked(segmentDeleteCutoff, opts.Compact)
	if opts.Compact {
		ops = ops[:0]
		for _, candidate := range compactCandidates {
			next := candidate.Source
			next.State = segmentStateCompacting
			ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
		}
		if err := s.commitMetaLocked(ops); err != nil {
			s.metaMu.Unlock()
			return result, errors.Join(err, s.recordGCRun(epoch, "FAILED", startedAt, safetyCutoff, err.Error()))
		}
	}
	s.metaMu.Unlock()

	if len(compactCandidates) > 0 {
		compacted, err := s.compactCandidates(ctx, compactCandidates)
		if err != nil {
			err = errors.Join(err, s.rollbackCompaction(compactCandidates))
			return result, errors.Join(err, s.recordGCRun(epoch, "FAILED", startedAt, safetyCutoff, err.Error()))
		}
		deleted, err := s.commitCompactionResults(compacted, result, now, segmentDeleteCutoff)
		if err != nil {
			err = errors.Join(err, s.removeCompactedSegments(compacted), s.rollbackCompaction(compactCandidates))
			return result, errors.Join(err, s.recordGCRun(epoch, "FAILED", startedAt, safetyCutoff, err.Error()))
		}
		removeSegments = append(removeSegments, deleted...)
		s.metaMu.RLock()
		_, dead := s.collectSegmentWorkLocked(segmentDeleteCutoff, false)
		removeSegments = append(removeSegments, dead...)
		s.metaMu.RUnlock()
	}

	deleted, err := s.removeSegmentFiles(ctx, removeSegments)
	if err != nil || len(deleted) == 0 {
		if err != nil {
			return result, errors.Join(err, s.recordGCRun(epoch, "FAILED", startedAt, safetyCutoff, err.Error()))
		}
		return result, s.recordGCRun(epoch, "DONE", startedAt, safetyCutoff, "")
	}
	s.metaMu.Lock()
	ops = ops[:0]
	_, removable := s.collectSegmentWorkLocked(segmentDeleteCutoff, false)
	ready := make(map[string]bool, len(removable))
	for _, seg := range removable {
		ready[seg.SegmentID] = true
	}
	for _, seg := range deleted {
		if current := s.meta.Segments[seg.SegmentID]; current != nil && current.State != segmentStateDeleted && ready[seg.SegmentID] {
			next := *current
			next.State = segmentStateDeleted
			next.DeletedAt = now
			ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
			result.SegmentsDeleted++
		}
	}
	if err := s.commitMetaLocked(ops); err != nil {
		s.metaMu.Unlock()
		return result, errors.Join(err, s.recordGCRun(epoch, "FAILED", startedAt, safetyCutoff, err.Error()))
	}
	s.metaMu.Unlock()
	if err := s.recordGCRun(epoch, "DONE", startedAt, safetyCutoff, ""); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) recordGCRun(epoch int64, state string, startedAt, safetyCutoff int64, notes string) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	run := &gcRun{
		Epoch:        epoch,
		State:        state,
		StartedAt:    startedAt,
		FinishedAt:   nowUnix(),
		SafetyCutoff: safetyCutoff,
		Notes:        notes,
	}
	return s.commitMetaLocked([]metaOp{{Type: "put_gcrun", GCRun: run}})
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
		changed := false
		switch chunk.State {
		case chunkStateActive:
			if confirmCycles <= 1 {
				next.State = chunkStateDeleted
				next.DeletedAt = now
				result.ChunksDeleted++
				result.BytesMadeGarbage += chunk.StoredSize
				changed = true
			} else {
				next.State = chunkStateGarbageCandidate
				next.GarbageSeenCount = 1
				next.GarbageCandidateAt = now
				result.CandidatesMarked++
				changed = true
			}
		case chunkStateGarbageCandidate:
			if chunk.GarbageSeenCount+1 >= confirmCycles {
				next.State = chunkStateDeleted
				next.DeletedAt = now
				result.ChunksDeleted++
				result.BytesMadeGarbage += chunk.StoredSize
				changed = true
			} else {
				next.GarbageSeenCount++
				result.CandidatesMarked++
				changed = true
			}
		}
		if changed {
			*ops = append(*ops, metaOp{Type: "put_chunk", Chunk: &next})
		}
	}
}

func (s *Store) collectSegmentWorkLocked(segmentDeleteCutoff int64, compact bool) ([]compactCandidate, []segmentRecord) {
	stats := s.collectSegmentStatsLocked()
	var candidates []compactCandidate
	var deadSegments []segmentRecord
	for _, stat := range stats {
		seg := stat.Segment
		pinned := s.segmentPinned(seg.SegmentID)
		if compact && seg.State == segmentStateSealed && !pinned {
			total := stat.LiveBytes + stat.GarbageBytes
			if stat.LiveBytes > 0 && stat.GarbageBytes > 0 && total > 0 && float64(stat.GarbageBytes)/float64(total) >= s.cfg.GC.CompactGarbageRatio {
				candidates = append(candidates, compactCandidate{Source: seg, Chunks: stat.LiveChunks})
			}
		}
		if seg.State == segmentStateDeleted || seg.State == segmentStateCorrupt || pinned || stat.BlocksRemoval {
			continue
		}
		deadAt := stat.DeadAt
		if deadAt == 0 {
			deadAt = seg.SealedAt
		}
		if deadAt == 0 {
			deadAt = seg.CreatedAt
		}
		if deadAt <= segmentDeleteCutoff {
			deadSegments = append(deadSegments, seg)
		}
	}
	return candidates, deadSegments
}

func (s *Store) collectSegmentStatsLocked() map[string]*segmentGCStats {
	stats := make(map[string]*segmentGCStats, len(s.meta.Segments))
	for id, seg := range s.meta.Segments {
		if seg == nil {
			continue
		}
		stats[id] = &segmentGCStats{Segment: *seg, DeadAt: seg.CompactedAt}
	}
	for _, chunk := range s.meta.Chunks {
		if chunk == nil || chunk.SegmentID == "" {
			continue
		}
		stat := stats[chunk.SegmentID]
		if stat == nil {
			continue
		}
		if chunk.State == chunkStateDeleted {
			stat.GarbageBytes += chunk.SegmentLength
			if chunk.DeletedAt == 0 {
				stat.BlocksRemoval = true
			} else if chunk.DeletedAt > stat.DeadAt {
				stat.DeadAt = chunk.DeletedAt
			}
			continue
		}
		if chunk.RefCount == 0 {
			stat.GarbageBytes += chunk.SegmentLength
			stat.BlocksRemoval = true
			continue
		}
		stat.LiveBytes += chunk.SegmentLength
		stat.LiveChunks = append(stat.LiveChunks, *chunk)
		stat.BlocksRemoval = true
	}
	return stats
}

func (s *Store) compactCandidates(ctx context.Context, candidates []compactCandidate) ([]compactResult, error) {
	results := make([]compactResult, 0, len(candidates))
	for _, candidate := range candidates {
		if err := contextError(ctx); err != nil {
			return nil, errors.Join(err, s.removeCompactedSegments(results))
		}
		writer := &segmentBatchWriter{store: s}
		result := compactResult{Source: candidate.Source}
		for _, chunk := range candidate.Chunks {
			raw, err := s.readChunkPayloadAt(candidate.Source, chunk)
			if err != nil {
				writer.cleanup()
				return nil, errors.Join(err, s.removeCompactedSegments(results))
			}
			next, err := writer.appendChunk(chunk.TenantID, chunk.ChunkID, raw)
			if err != nil {
				writer.cleanup()
				return nil, errors.Join(err, s.removeCompactedSegments(results))
			}
			next.RefCount = chunk.RefCount
			result.Original = append(result.Original, chunk)
			result.Moved = append(result.Moved, next)
		}
		if err := writer.finish(); err != nil {
			writer.cleanup()
			return nil, errors.Join(err, s.removeCompactedSegments(results))
		}
		result.Segments = writer.segments
		results = append(results, result)
	}
	return results, nil
}

func (s *Store) removeCompactedSegments(results []compactResult) error {
	var errs []error
	for _, item := range results {
		for _, seg := range item.Segments {
			path := s.segmentPath(seg)
			if err := s.fs.Remove(path); err != nil && !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("remove compacted segment %s: %w", path, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (s *Store) commitCompactionResults(results []compactResult, gcResult *GCResult, now, segmentDeleteCutoff int64) ([]segmentRecord, error) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	var deleteSegments []segmentRecord
	ops := []metaOp{}
	for _, item := range results {
		source := s.meta.Segments[item.Source.SegmentID]
		if source == nil || source.State != segmentStateCompacting {
			for _, seg := range item.Segments {
				deleteSegments = append(deleteSegments, *seg)
			}
			continue
		}
		if len(item.Original) != len(item.Moved) {
			rollback := *source
			rollback.State = segmentStateSealed
			ops = append(ops, metaOp{Type: "put_segment", Segment: &rollback})
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

func (s *Store) rollbackCompaction(candidates []compactCandidate) error {
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
	return s.commitMetaLocked(ops)
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
