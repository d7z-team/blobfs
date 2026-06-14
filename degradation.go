package blobfs

import (
	"context"
	"sort"
	"strings"
	"time"
)

func (s *Store) AssessImpact(ctx context.Context, opts ImpactOptions) (*ImpactReport, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()

	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.assessImpactLocked(opts), nil
}

func (s *Store) assessImpactLocked(opts ImpactOptions) *ImpactReport {
	report := &ImpactReport{GeneratedAt: nowTime()}
	tenantSet := map[string]bool{}
	chunkSet := map[string]bool{}
	segmentSet := map[string]bool{}

	for _, inode := range s.meta.Inodes {
		if inode == nil || inode.Kind != fileKindFile || !inodeVisibleState(inode.State) {
			continue
		}
		if opts.OnlyDegraded && inode.State != fileStateDegraded {
			continue
		}
		if opts.TenantID != "" && inode.TenantID != opts.TenantID {
			continue
		}
		path, err := s.pathForInodeLocked(inode.InodeID)
		if err != nil {
			continue
		}
		if opts.ObjectPath != "" && path != opts.ObjectPath {
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(path, opts.Prefix) {
			continue
		}
		manifest := s.meta.Manifests[inode.ManifestID]
		if manifest == nil || !manifestVisibleState(manifest.State) {
			continue
		}

		object := ImpactedObject{
			TenantID:   inode.TenantID,
			Path:       path,
			State:      inode.State,
			Size:       inode.Size,
			Reason:     inode.DegradedReason,
			ManifestID: inode.ManifestID,
		}
		objectChunks := map[string]bool{}
		objectSegments := map[string]bool{}
		for _, ref := range manifest.Chunks {
			chunk := s.meta.Chunks[ref.ChunkID]
			if chunk == nil {
				continue
			}
			if chunk.State == chunkStateMissing && !objectChunks[chunk.ChunkID] {
				objectChunks[chunk.ChunkID] = true
				object.MissingChunks = append(object.MissingChunks, chunk.ChunkID)
				chunkSet[chunk.ChunkID] = true
			}
			if seg := s.meta.Segments[chunk.SegmentID]; seg != nil && seg.State == segmentStateMissing && !objectSegments[seg.SegmentID] {
				objectSegments[seg.SegmentID] = true
				object.MissingSegments = append(object.MissingSegments, seg.SegmentID)
				segmentSet[seg.SegmentID] = true
			}
		}
		sort.Strings(object.MissingChunks)
		sort.Strings(object.MissingSegments)
		if opts.ChunkID != "" && !containsString(object.MissingChunks, opts.ChunkID) {
			continue
		}
		if opts.SegmentID != "" && !containsString(object.MissingSegments, opts.SegmentID) {
			continue
		}
		if len(object.MissingChunks) == 0 && len(object.MissingSegments) == 0 && inode.State != fileStateDegraded {
			continue
		}

		report.AffectedObjects = append(report.AffectedObjects, object)
		report.LogicalBytes += inode.Size
		tenantSet[inode.TenantID] = true
	}

	sort.Slice(report.AffectedObjects, func(i, j int) bool {
		if report.AffectedObjects[i].TenantID != report.AffectedObjects[j].TenantID {
			return report.AffectedObjects[i].TenantID < report.AffectedObjects[j].TenantID
		}
		return report.AffectedObjects[i].Path < report.AffectedObjects[j].Path
	})
	for tenantID := range tenantSet {
		report.AffectedTenants = append(report.AffectedTenants, tenantID)
	}
	for chunkID := range chunkSet {
		report.AffectedChunks = append(report.AffectedChunks, chunkID)
		if chunk := s.meta.Chunks[chunkID]; chunk != nil {
			report.StoredBytes += chunk.StoredSize
		}
	}
	for segmentID := range segmentSet {
		report.AffectedSegments = append(report.AffectedSegments, segmentID)
	}
	sort.Strings(report.AffectedTenants)
	sort.Strings(report.AffectedChunks)
	sort.Strings(report.AffectedSegments)
	report.ObjectCount = len(report.AffectedObjects)
	report.TenantCount = len(report.AffectedTenants)
	return report
}

func (s *Store) applyDegradation(ctx context.Context, issues []CheckIssue) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()

	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	ops := []metaOp{}
	s.degradeIssuesLocked(issues, nowUnix(), &ops)
	return s.commitMetaLocked(ops)
}

func (s *Store) degradeIssuesLocked(issues []CheckIssue, now int64, ops *[]metaOp) {
	missingSegments := map[string]string{}
	missingChunks := map[string]string{}
	for _, issue := range issues {
		if issue.SegmentID != "" && issue.Kind != string(IssueOrphanSegment) {
			if _, ok := missingSegments[issue.SegmentID]; !ok {
				missingSegments[issue.SegmentID] = issue.Reason
			}
		}
		if issue.ChunkID != "" {
			if _, ok := missingChunks[issue.ChunkID]; !ok {
				missingChunks[issue.ChunkID] = issue.Reason
			}
		}
	}
	for segmentID, reason := range missingSegments {
		if seg := s.meta.Segments[segmentID]; seg != nil && seg.State != segmentStateDeleted {
			next := *seg
			next.State = segmentStateMissing
			next.MissingAt = now
			next.MissingReason = reason
			*ops = append(*ops, metaOp{Type: "put_segment", Segment: &next})
			for _, chunk := range s.meta.Chunks {
				if chunk == nil || chunk.SegmentID != segmentID || chunk.State == chunkStateDeleted {
					continue
				}
				missingChunks[chunk.ChunkID] = reason
			}
		}
	}
	for chunkID, reason := range missingChunks {
		if chunk := s.meta.Chunks[chunkID]; chunk != nil && chunk.State != chunkStateDeleted {
			next := *chunk
			next.State = chunkStateMissing
			next.MissingAt = now
			next.MissingReason = reason
			*ops = append(*ops, metaOp{Type: "put_chunk", Chunk: &next})
		}
	}
	if len(missingChunks) == 0 {
		return
	}

	degradedManifests := map[string]string{}
	for _, manifest := range s.meta.Manifests {
		if manifest == nil || manifest.State == manifestStateDeleted {
			continue
		}
		for _, ref := range manifest.Chunks {
			if reason, ok := missingChunks[ref.ChunkID]; ok {
				degradedManifests[manifest.ManifestID] = reason
				break
			}
			chunk := s.meta.Chunks[ref.ChunkID]
			if chunk != nil && chunk.State == chunkStateMissing {
				degradedManifests[manifest.ManifestID] = chunk.MissingReason
				break
			}
		}
	}
	for manifestID, reason := range degradedManifests {
		manifest := s.meta.Manifests[manifestID]
		if manifest == nil || manifest.State == manifestStateDeleted {
			continue
		}
		next := cloneManifest(manifest)
		next.State = manifestStateDegraded
		next.DegradedAt = now
		next.DegradedReason = reason
		*ops = append(*ops, metaOp{Type: "put_manifest", Manifest: next})
	}
	for _, inode := range s.meta.Inodes {
		if inode == nil || inode.Kind != fileKindFile || !inodeVisibleState(inode.State) {
			continue
		}
		reason, ok := degradedManifests[inode.ManifestID]
		if !ok {
			continue
		}
		next := cloneInode(inode)
		next.State = fileStateDegraded
		next.DegradedAt = now
		next.DegradedReason = reason
		*ops = append(*ops, metaOp{Type: "put_inode", Inode: next})
	}
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func nowTime() time.Time {
	return time.Unix(0, nowUnix())
}
