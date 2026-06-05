package blobfs

import (
	"context"
	"os"
	"time"
)

// RunGC performs one mark/sweep pass and optionally compacts eligible segments.
func (s *Store) RunGC(ctx context.Context, opts GCOptions) (*GCResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	nowTime := time.Now()
	now := nowTime.UnixNano()
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
	epoch := s.meta.NextGCEpoch
	s.meta.NextGCEpoch++
	result := &GCResult{Epoch: epoch}
	cutoff := nowTime.Add(-safetyWindow).UnixNano()
	s.meta.GCRuns = append(s.meta.GCRuns, gcRun{
		Epoch:        epoch,
		State:        "RUNNING",
		StartedAt:    now,
		SafetyCutoff: cutoff,
	})

	liveManifests := map[string]bool{}
	for _, file := range s.meta.Files {
		if file.State == fileStateActive {
			liveManifests[file.ManifestID] = true
		}
	}
	liveChunks := map[string]bool{}
	for manifestID := range liveManifests {
		manifest := s.meta.Manifests[manifestID]
		if manifest == nil {
			continue
		}
		manifest.State = manifestStateActive
		manifest.DeletedAt = 0
		manifest.LastLiveAt = now
		for _, ref := range s.manifestRefs(manifest) {
			liveChunks[ref.ChunkID] = true
		}
	}
	for id, manifest := range s.meta.Manifests {
		if !liveManifests[id] && manifest.State == manifestStateActive {
			manifest.State = manifestStateDeleted
			manifest.DeletedAt = now
			result.ManifestsReclaimed++
		}
	}
	for id := range liveChunks {
		chunk := s.meta.Chunks[id]
		if chunk == nil {
			continue
		}
		chunk.LastLiveEpoch = epoch
		chunk.LastSeenAt = now
		if chunk.State == chunkStateGarbageCandidate || chunk.State == chunkStateDeleting {
			chunk.State = chunkStateActive
			chunk.GarbageSeenCount = 0
			chunk.GarbageCandidateAt = 0
			chunk.DeletingAt = 0
		}
		result.LiveChunks++
	}

	for _, chunk := range s.meta.Chunks {
		if liveChunks[chunk.ChunkID] || chunk.State == chunkStateDeleted {
			continue
		}
		if chunk.CreatedAt >= cutoff {
			continue
		}
		switch chunk.State {
		case chunkStateWriting:
			chunk.State = chunkStateDeleted
			chunk.DeletedAt = now
			result.ChunksDeleted++
		case chunkStateActive:
			chunk.State = chunkStateGarbageCandidate
			chunk.GarbageSeenCount = 1
			chunk.GarbageCandidateAt = now
			result.CandidatesMarked++
		case chunkStateGarbageCandidate:
			if chunk.GarbageSeenCount+1 >= confirmCycles {
				chunk.State = chunkStateDeleting
				chunk.DeletingAt = now
				result.ChunksDeleting++
				result.BytesMadeGarbage += chunk.StoredSize
			} else {
				chunk.GarbageSeenCount++
				result.CandidatesMarked++
			}
		}
	}

	s.recomputeSegmentEstimatesLocked()
	if opts.Compact {
		if err := s.compactEligibleSegmentsLocked(ctx, now, result); err != nil {
			return nil, err
		}
	}
	if err := s.deleteCompactedSegmentsLocked(now, result); err != nil {
		return nil, err
	}
	s.finishGCRunLocked(epoch, now, "DONE")
	if err := saveMetadata(s.fs, s.metaPath, s.meta); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) recomputeSegmentEstimatesLocked() {
	for _, seg := range s.meta.Segments {
		seg.LiveBytesEstimate = 0
		seg.GarbageBytesEstimate = 0
	}
	for _, chunk := range s.meta.Chunks {
		seg := s.meta.Segments[chunk.SegmentID]
		if seg == nil {
			continue
		}
		switch chunk.State {
		case chunkStateDeleting, chunkStateDeleted:
			seg.GarbageBytesEstimate += chunk.SegmentLength
		default:
			seg.LiveBytesEstimate += chunk.SegmentLength
		}
	}
}

func (s *Store) compactEligibleSegmentsLocked(ctx context.Context, now int64, result *GCResult) error {
	segments := make([]*segmentRecord, 0, len(s.meta.Segments))
	for _, seg := range s.meta.Segments {
		if seg.State == segmentStateDeleted || seg.State == segmentStateCorrupt || seg.State == segmentStateCompacted || seg.State == segmentStateCompacting {
			continue
		}
		if seg.GarbageBytesEstimate <= 0 || seg.TotalBytes <= 0 {
			continue
		}
		if float64(seg.GarbageBytesEstimate)/float64(seg.TotalBytes) >= s.cfg.GC.CompactGarbageRatio {
			segments = append(segments, seg)
		}
	}
	for _, seg := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.compactSegmentLocked(seg, now, result); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) compactSegmentLocked(sourceSeg *segmentRecord, now int64, result *GCResult) error {
	sourceSeg.State = segmentStateCompacting
	copiedSegmentIDs := map[string]bool{}
	for _, chunk := range s.meta.Chunks {
		if chunk.SegmentID != sourceSeg.SegmentID {
			continue
		}
		if chunk.State == chunkStateDeleting || chunk.State == chunkStateDeleted {
			continue
		}
		sourceLocation := *chunk
		raw, err := s.readChunkPayloadAt(*sourceSeg, sourceLocation)
		if err != nil {
			sourceSeg.State = segmentStateSealed
			return err
		}
		loc, err := s.appendChunkRecordLocked(chunk.ChunkID, raw)
		if err != nil {
			sourceSeg.State = segmentStateSealed
			return err
		}
		current := s.meta.Chunks[chunk.ChunkID]
		if current != nil &&
			current.SegmentID == sourceLocation.SegmentID &&
			current.SegmentOffset == sourceLocation.SegmentOffset &&
			current.SegmentLength == sourceLocation.SegmentLength {
			current.SegmentID = loc.SegmentID
			current.SegmentOffset = loc.SegmentOffset
			current.SegmentLength = loc.SegmentLength
			current.StoredSize = loc.StoredSize
			current.ChecksumCRC32C = loc.Checksum
			current.Compression = loc.Compression
			copiedSegmentIDs[loc.SegmentID] = true
			result.BytesRewritten += loc.StoredSize
		}
	}
	for _, chunk := range s.meta.Chunks {
		if chunk.SegmentID == sourceSeg.SegmentID && (chunk.State == chunkStateDeleting || chunk.State == chunkStateDeleted) {
			chunk.State = chunkStateDeleted
			if chunk.DeletedAt == 0 {
				chunk.DeletedAt = now
				result.ChunksDeleted++
			}
		}
	}
	for id := range copiedSegmentIDs {
		if seg := s.meta.Segments[id]; seg != nil && seg.State == segmentStateOpen {
			seg.State = segmentStateSealed
			seg.SealedAt = now
		}
	}
	sourceSeg.State = segmentStateCompacted
	sourceSeg.CompactedAt = now
	result.SegmentsCompacted++
	s.recomputeSegmentEstimatesLocked()
	return nil
}

func (s *Store) deleteCompactedSegmentsLocked(now int64, result *GCResult) error {
	delay := s.cfg.GC.SegmentDeleteDelay
	if delay < 0 {
		delay = 0
	}
	for _, seg := range s.meta.Segments {
		if seg.State != segmentStateCompacted || seg.CompactedAt == 0 {
			continue
		}
		if time.Duration(now-seg.CompactedAt) < delay {
			continue
		}
		if s.segmentPinned(seg.SegmentID) {
			continue
		}
		if err := s.fs.Remove(s.segmentPath(seg)); err != nil && !os.IsNotExist(err) {
			return err
		}
		seg.State = segmentStateDeleted
		seg.DeletedAt = now
		result.SegmentsDeleted++
	}
	return nil
}

func (s *Store) finishGCRunLocked(epoch, now int64, state string) {
	for i := len(s.meta.GCRuns) - 1; i >= 0; i-- {
		if s.meta.GCRuns[i].Epoch == epoch {
			s.meta.GCRuns[i].State = state
			s.meta.GCRuns[i].FinishedAt = now
			return
		}
	}
}
