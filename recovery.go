package blobfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/spf13/afero"
)

// HealthState is the coarse availability state of a Store.
type HealthState string

const (
	// HealthOK means the store is open, readable, writable, and has no known corruption.
	HealthOK HealthState = "OK"
	// HealthDegraded means the store is usable but has repairable metadata state.
	HealthDegraded HealthState = "DEGRADED"
	// HealthReadOnly means metadata is loaded but basic writable paths are unavailable.
	HealthReadOnly HealthState = "READ_ONLY"
	// HealthClosed means the store has been closed.
	HealthClosed HealthState = "CLOSED"
)

// HealthReport is a lightweight availability report.
type HealthReport struct {
	State           HealthState
	Readable        bool
	Writable        bool
	DegradedObjects int
	AffectedTenants []string
	Checks          []HealthCheck
	GeneratedAt     time.Time
}

// HealthCheck is one lightweight health assertion.
type HealthCheck struct {
	Name    string
	OK      bool
	Message string
}

// ManifestStats groups manifest counts by state.
type ManifestStats struct {
	Active  int
	Deleted int
}

// ChunkStats groups chunk counts by state.
type ChunkStats struct {
	Active           int
	GarbageCandidate int
	Missing          int
	Deleted          int
}

// SegmentStats groups segment counts by state.
type SegmentStats struct {
	Sealed     int
	Compacting int
	Missing    int
	Deleted    int
}

// ByteStats summarizes logical and physical byte counters from metadata.
type ByteStats struct {
	LogicalObjectBytes int64
	RawChunkBytes      int64
	StoredChunkBytes   int64
}

// GCStats summarizes recorded GC runs.
type GCStats struct {
	Runs      int
	LastEpoch int64
	// LastRunState is the final state recorded for the newest GC run.
	LastRunState string
	// LastBackgroundAt is the wall-clock time of the latest background GC attempt.
	LastBackgroundAt time.Time
	// LastBackgroundEpoch is the GC epoch returned by the latest background run.
	LastBackgroundEpoch int64
	// LastBackgroundError is the latest background GC error message, if any.
	LastBackgroundError string
}

// StatsSnapshot is a point-in-time metadata-only statistics snapshot.
type StatsSnapshot struct {
	TxID            uint64
	Tenants         int
	Inodes          int
	Objects         int
	DegradedObjects int
	Directories     int
	Manifests       ManifestStats
	Chunks          ChunkStats
	Segments        SegmentStats
	Bytes           ByteStats
	GC              GCStats
	GeneratedAt     time.Time
}

// DiagnoseOptions controls optional filesystem checks.
type DiagnoseOptions struct {
	CheckFiles   bool
	CheckOrphans bool
	CheckStaging bool
	MaxIssues    int
}

// DiagnoseReport contains recovery-oriented issues found without modifying the store.
type DiagnoseReport struct {
	Healthy     bool
	Issues      []Issue
	GeneratedAt time.Time
}

// IssueKind identifies a diagnosable store issue.
type IssueKind string

const (
	// IssueStagingLeftover is a leftover staging file.
	IssueStagingLeftover IssueKind = "staging_leftover"
	// IssueOrphanSegment is a segment file not referenced by metadata.
	IssueOrphanSegment IssueKind = "orphan_segment"
	// IssueMissingSegment is metadata pointing to a missing segment file.
	IssueMissingSegment IssueKind = "missing_segment"
	// IssueCompactingSegment is a lingering COMPACTING segment.
	IssueCompactingSegment IssueKind = "compacting_segment"
	// IssueMissingChunk is a chunk marked missing.
	IssueMissingChunk IssueKind = "missing_chunk"
	// IssueDegradedObject is an object degraded by missing data.
	IssueDegradedObject IssueKind = "degraded_object"
	// IssueChunkWithoutSegment is a live chunk without usable segment metadata.
	IssueChunkWithoutSegment IssueKind = "chunk_without_segment"
	// IssueSegmentWithoutChunks is a live segment with no chunk references.
	IssueSegmentWithoutChunks IssueKind = "segment_without_chunks"
	// IssueMetadataLogTornTail is a crash-torn metadata log tail ignored during replay.
	IssueMetadataLogTornTail IssueKind = "metadata_log_torn_tail"
	// IssueRefCountWarning is a refcount inconsistency warning.
	IssueRefCountWarning IssueKind = "refcount_warning"
)

// IssueSeverity is the severity of a diagnostic issue.
type IssueSeverity string

const (
	// SeverityInfo is informational.
	SeverityInfo IssueSeverity = "info"
	// SeverityWarn indicates degraded state.
	SeverityWarn IssueSeverity = "warn"
	// SeverityError indicates data may be unavailable.
	SeverityError IssueSeverity = "error"
)

// Issue is a single diagnostic finding.
type Issue struct {
	Kind       IssueKind
	Severity   IssueSeverity
	SegmentID  string
	ChunkID    string
	Path       string
	Message    string
	Repairable bool
}

// RepairOptions controls low-risk repair actions.
type RepairOptions struct {
	DryRun bool
	// Apply must be true to execute actions. Without Apply, Repair only returns a dry-run plan.
	Apply                 bool
	CleanStaging          bool
	CleanOrphans          bool
	ResetCompacting       bool
	DegradeMissing        bool
	ClearRefCountWarnings bool
	MaxActions            int
}

// RepairReport lists planned or applied repair actions.
type RepairReport struct {
	DryRun      bool
	Actions     []RepairAction
	GeneratedAt time.Time
}

// RepairActionType identifies one repair action.
type RepairActionType string

const (
	// RepairCleanStaging removes a staging file.
	RepairCleanStaging RepairActionType = "clean_staging"
	// RepairCleanOrphanSegment removes an unreferenced segment file.
	RepairCleanOrphanSegment RepairActionType = "clean_orphan_segment"
	// RepairResetCompacting resets a leftover COMPACTING segment to SEALED.
	RepairResetCompacting RepairActionType = "reset_compacting"
	// RepairDegradeMissing marks missing data and degrades affected objects.
	RepairDegradeMissing RepairActionType = "degrade_missing"
	// RepairClearRefCountWarnings clears recorded refcount warnings.
	RepairClearRefCountWarnings RepairActionType = "clear_refcount_warnings"
)

// RepairAction is one planned or applied repair operation.
type RepairAction struct {
	Type    RepairActionType
	Target  string
	Applied bool
	Message string
}

// RemoveStaleLock removes a stale store lock on the local filesystem.
// The caller must first ensure no live process is using the store.
func RemoveStaleLock(baseDir string) error {
	baseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return err
	}
	return RemoveFSStaleLock(afero.NewOsFs(), baseDir)
}

// RemoveFSStaleLock removes a stale store lock on an afero filesystem.
// The caller must first ensure no live process is using the store.
func RemoveFSStaleLock(filesystem afero.Fs, baseDir string) error {
	if filesystem == nil {
		return ErrNilFilesystem
	}
	err := filesystem.Remove(filepath.Join(filepath.Clean(baseDir), "meta", "LOCK"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// Health returns a lightweight store health report without scanning all data files.
func (s *Store) Health(ctx context.Context) (*HealthReport, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	report := &HealthReport{State: HealthOK, Readable: true, Writable: true, GeneratedAt: time.Now()}
	if s.isClosed() {
		report.State = HealthClosed
		report.Readable = false
		report.Writable = false
		report.Checks = append(report.Checks, HealthCheck{Name: "store_open", OK: false, Message: "store is closed"})
		return report, nil
	}
	report.Checks = append(report.Checks, HealthCheck{Name: "store_open", OK: true, Message: "store is open"})
	s.metaMu.RLock()
	metaLoaded := s.meta != nil
	txlogOK := s.metaLog != nil
	checkpointOK := s.lastCheckpointErr == nil
	checkpointMessage := "checkpoint is healthy"
	if s.lastCheckpointErr != nil {
		checkpointMessage = s.lastCheckpointErr.Error()
	}
	replayWarnings := append([]metadataReplayWarning(nil), s.recoveryWarnings...)
	degradedTenants := map[string]bool{}
	degradedObjects := 0
	hasMissingChunks := false
	hasMissingSegments := false
	hasCompactingSegments := false
	if metaLoaded {
		for _, inode := range s.meta.Inodes {
			if inode != nil && inode.State == fileStateDegraded {
				degradedObjects++
				degradedTenants[inode.TenantID] = true
			}
		}
		for _, chunk := range s.meta.Chunks {
			if chunk == nil {
				continue
			}
			if chunk.State == chunkStateMissing {
				hasMissingChunks = true
				break
			}
		}
		for _, seg := range s.meta.Segments {
			if seg == nil {
				continue
			}
			switch seg.State {
			case segmentStateMissing:
				hasMissingSegments = true
			case segmentStateCompacting:
				hasCompactingSegments = true
			}
		}
	}
	s.metaMu.RUnlock()
	s.backgroundMu.Lock()
	backgroundErr := s.lastBackgroundGCErr
	s.backgroundMu.Unlock()
	report.Checks = append(report.Checks, HealthCheck{Name: "metadata_loaded", OK: metaLoaded, Message: healthMessage(metaLoaded, "metadata loaded", "metadata is nil")})
	report.Checks = append(report.Checks, HealthCheck{Name: "txlog_available", OK: txlogOK, Message: healthMessage(txlogOK, "metadata log is open", "metadata log is unavailable")})
	report.Checks = append(report.Checks, HealthCheck{Name: "checkpoint_healthy", OK: checkpointOK, Message: checkpointMessage})
	backgroundOK := backgroundErr == nil
	backgroundMessage := "background GC is healthy"
	if backgroundErr != nil {
		backgroundMessage = backgroundErr.Error()
	}
	report.Checks = append(report.Checks, HealthCheck{Name: "background_gc", OK: backgroundOK, Message: backgroundMessage})
	report.Checks = append(report.Checks, HealthCheck{
		Name:    "metadata_log_replay",
		OK:      len(replayWarnings) == 0,
		Message: healthMessage(len(replayWarnings) == 0, "metadata log replay was clean", metadataReplayWarningMessage(replayWarnings)),
	})
	txlogDirOK := s.pathAccessible(metaTxLogDir(s.metaDir))
	report.Checks = append(report.Checks, HealthCheck{Name: "txlog_dir_available", OK: txlogDirOK, Message: healthMessage(txlogDirOK, "metadata log directory is accessible", "metadata log directory is not accessible")})
	segmentsOK := s.pathAccessible(s.segmentsDir)
	report.Checks = append(report.Checks, HealthCheck{Name: "segments_dir_available", OK: segmentsOK, Message: healthMessage(segmentsOK, "segments directory is accessible", "segments directory is not accessible")})
	stagingOK := s.pathAccessible(s.stagingDir)
	report.Checks = append(report.Checks, HealthCheck{Name: "staging_dir_available", OK: stagingOK, Message: healthMessage(stagingOK, "staging directory is accessible", "staging directory is not accessible")})
	report.Checks = append(report.Checks,
		HealthCheck{Name: "no_missing_chunks", OK: !hasMissingChunks, Message: healthMessage(!hasMissingChunks, "no missing chunks", "missing chunks exist")},
		HealthCheck{Name: "no_missing_segments", OK: !hasMissingSegments, Message: healthMessage(!hasMissingSegments, "no missing segments", "missing segments exist")},
		HealthCheck{Name: "no_compacting_segments", OK: !hasCompactingSegments, Message: healthMessage(!hasCompactingSegments, "no compacting segments", "compacting segments exist")},
	)
	hasRefCountWarnings := s.refCountWarningCount() > 0
	report.Checks = append(report.Checks, HealthCheck{Name: "refcount_integrity", OK: !hasRefCountWarnings, Message: healthMessage(!hasRefCountWarnings, "refcounts are consistent", "refcount inconsistencies detected")})
	report.DegradedObjects = degradedObjects
	for tenantID := range degradedTenants {
		report.AffectedTenants = append(report.AffectedTenants, tenantID)
	}
	sort.Strings(report.AffectedTenants)
	if !metaLoaded || !segmentsOK {
		report.State = HealthReadOnly
		report.Readable = false
		report.Writable = false
		return report, nil
	}
	if !txlogOK || !txlogDirOK || !stagingOK {
		report.State = HealthReadOnly
		report.Writable = false
		return report, nil
	}
	if degradedObjects > 0 || hasMissingChunks || hasMissingSegments || !checkpointOK || !backgroundOK || hasCompactingSegments || len(replayWarnings) > 0 || hasRefCountWarnings {
		report.State = HealthDegraded
	}
	return report, nil
}

// Stats returns metadata-only counters without filesystem scanning.
func (s *Store) Stats(ctx context.Context) (*StatsSnapshot, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	stats := &StatsSnapshot{TxID: s.meta.TxID, Tenants: len(s.meta.Tenants), GeneratedAt: time.Now()}
	for _, inode := range s.meta.Inodes {
		if !inodeVisibleState(inode.State) {
			continue
		}
		stats.Inodes++
		switch inode.Kind {
		case fileKindFile:
			if inode.State == fileStateDegraded {
				stats.DegradedObjects++
			} else {
				stats.Objects++
			}
			stats.Bytes.LogicalObjectBytes += inode.Size
		case fileKindDir:
			stats.Directories++
		}
	}
	for _, manifest := range s.meta.Manifests {
		if manifest.State == manifestStateDeleted {
			stats.Manifests.Deleted++
		} else {
			stats.Manifests.Active++
		}
	}
	for _, chunk := range s.meta.Chunks {
		if chunk == nil {
			continue
		}
		switch chunk.State {
		case chunkStateGarbageCandidate:
			stats.Chunks.GarbageCandidate++
		case chunkStateMissing:
			stats.Chunks.Missing++
		case chunkStateDeleted:
			stats.Chunks.Deleted++
			continue
		default:
			stats.Chunks.Active++
		}
		stats.Bytes.RawChunkBytes += chunk.RawSize
		stats.Bytes.StoredChunkBytes += chunk.StoredSize
	}
	for _, seg := range s.meta.Segments {
		if seg == nil {
			continue
		}
		switch seg.State {
		case segmentStateCompacting:
			stats.Segments.Compacting++
		case segmentStateMissing:
			stats.Segments.Missing++
		case segmentStateDeleted:
			stats.Segments.Deleted++
		default:
			stats.Segments.Sealed++
		}
	}
	stats.GC.Runs = int(s.meta.GC.TotalRuns)
	stats.GC.LastEpoch = s.meta.GC.LastEpoch
	if len(s.meta.GC.Recent) > 0 {
		stats.GC.LastRunState = s.meta.GC.Recent[len(s.meta.GC.Recent)-1].State
	}
	s.backgroundMu.Lock()
	stats.GC.LastBackgroundAt = s.lastBackgroundGCAt
	if s.lastBackgroundGC != nil {
		stats.GC.LastBackgroundEpoch = s.lastBackgroundGC.Epoch
	}
	if s.lastBackgroundGCErr != nil {
		stats.GC.LastBackgroundError = s.lastBackgroundGCErr.Error()
	}
	s.backgroundMu.Unlock()
	return stats, nil
}

// Diagnose scans metadata and optional filesystem state for recoverable issues.
func (s *Store) Diagnose(ctx context.Context, opts DiagnoseOptions) (*DiagnoseReport, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	report := &DiagnoseReport{Healthy: true, GeneratedAt: time.Now()}
	limitReached := false
	addIssue := func(issue Issue) {
		if limitReached {
			return
		}
		if opts.MaxIssues > 0 && len(report.Issues) >= opts.MaxIssues {
			limitReached = true
			return
		}
		report.Healthy = false
		report.Issues = append(report.Issues, issue)
	}
	s.metaMu.RLock()
	for _, warning := range s.recoveryWarnings {
		addIssue(Issue{
			Kind:       IssueMetadataLogTornTail,
			Severity:   SeverityWarn,
			Path:       warning.Path,
			Message:    metadataReplayWarningMessage([]metadataReplayWarning{warning}),
			Repairable: false,
		})
	}
	s.diagMu.Lock()
	for _, msg := range s.refCountWarnings {
		addIssue(Issue{
			Kind:       IssueRefCountWarning,
			Severity:   SeverityError,
			Message:    msg,
			Repairable: true,
		})
	}
	s.diagMu.Unlock()
	referencedPaths := s.referencedSegmentPathsLocked()
	chunksBySegment := map[string]int{}
	segments := make([]segmentRecord, 0, len(s.meta.Segments))
	for _, chunk := range s.meta.Chunks {
		if chunk == nil {
			continue
		}
		if chunk.State == chunkStateDeleted {
			continue
		}
		seg := s.meta.Segments[chunk.SegmentID]
		if chunk.SegmentID == "" || seg == nil || seg.State == segmentStateDeleted {
			addIssue(Issue{Kind: IssueChunkWithoutSegment, Severity: SeverityError, ChunkID: chunk.ChunkID, SegmentID: chunk.SegmentID, Message: "chunk has no usable segment metadata", Repairable: false})
			continue
		}
		chunksBySegment[chunk.SegmentID]++
		if chunk.State == chunkStateMissing {
			addIssue(Issue{Kind: IssueMissingChunk, Severity: SeverityError, ChunkID: chunk.ChunkID, SegmentID: chunk.SegmentID, Message: "chunk is marked missing", Repairable: true})
		}
	}
	degradedReported := map[string]bool{}
	for inodeID, inode := range s.meta.Inodes {
		if inode == nil || inode.Kind != fileKindFile || inode.State != fileStateDegraded {
			continue
		}
		path, err := s.pathForInodeLocked(inodeID)
		if err != nil {
			continue
		}
		key := inode.TenantID + "/" + path
		if degradedReported[key] {
			continue
		}
		degradedReported[key] = true
		addIssue(Issue{Kind: IssueDegradedObject, Severity: SeverityError, Path: key, Message: inode.DegradedReason, Repairable: true})
	}
	for _, seg := range s.meta.Segments {
		if seg == nil {
			continue
		}
		segments = append(segments, *seg)
		if seg.State == segmentStateDeleted {
			continue
		}
		switch seg.State {
		case segmentStateCompacting:
			addIssue(Issue{Kind: IssueCompactingSegment, Severity: SeverityWarn, SegmentID: seg.SegmentID, Message: "segment is left in compacting state", Repairable: true})
		case segmentStateMissing:
			addIssue(Issue{Kind: IssueMissingSegment, Severity: SeverityError, SegmentID: seg.SegmentID, Message: "segment is marked missing", Repairable: true})
		}
		if chunksBySegment[seg.SegmentID] == 0 {
			addIssue(Issue{Kind: IssueSegmentWithoutChunks, Severity: SeverityWarn, SegmentID: seg.SegmentID, Message: "segment has no live chunk references", Repairable: false})
		}
	}
	s.metaMu.RUnlock()
	if opts.CheckFiles {
		for _, seg := range segments {
			if err := contextError(ctx); err != nil {
				return report, err
			}
			if seg.State == segmentStateDeleted {
				continue
			}
			if err := s.statSegment(seg); errors.Is(err, fs.ErrNotExist) {
				addIssue(Issue{Kind: IssueMissingSegment, Severity: SeverityError, SegmentID: seg.SegmentID, Path: s.segmentPath(&seg), Message: "segment file is missing", Repairable: true})
			} else if err != nil {
				return report, err
			}
		}
	}
	if opts.CheckStaging {
		if err := s.walkFiles(ctx, s.stagingDir, func(path string) error {
			addIssue(Issue{Kind: IssueStagingLeftover, Severity: SeverityInfo, Path: path, Message: "staging file is left over", Repairable: true})
			return nil
		}); err != nil {
			return report, err
		}
	}
	if opts.CheckOrphans {
		if err := s.walkFiles(ctx, s.segmentsDir, func(path string) error {
			if !referencedPaths[path] {
				addIssue(Issue{Kind: IssueOrphanSegment, Severity: SeverityWarn, Path: path, Message: "segment file is not referenced by metadata", Repairable: true})
			}
			return nil
		}); err != nil {
			return report, err
		}
	}
	return report, nil
}

func metadataReplayWarningMessage(warnings []metadataReplayWarning) string {
	if len(warnings) == 0 {
		return ""
	}
	first := warnings[0]
	msg := fmt.Sprintf("%s at offset %d", first.Reason, first.Offset)
	if first.Bytes > 0 {
		msg = fmt.Sprintf("%s after %d bytes", msg, first.Bytes)
	}
	if len(warnings) > 1 {
		msg = fmt.Sprintf("%s; %d total warnings", msg, len(warnings))
	}
	return msg
}

// Repair applies or previews low-risk recovery actions selected by RepairOptions.
func (s *Store) Repair(ctx context.Context, opts RepairOptions) (*RepairReport, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	dryRun := opts.DryRun || !opts.Apply
	report := &RepairReport{DryRun: dryRun, GeneratedAt: time.Now()}
	addAction := func(action RepairAction) bool {
		if opts.MaxActions > 0 && len(report.Actions) >= opts.MaxActions {
			return false
		}
		action.Applied = !dryRun
		report.Actions = append(report.Actions, action)
		return true
	}
	if opts.CleanStaging {
		if err := s.walkFiles(ctx, s.stagingDir, func(path string) error {
			if !addAction(RepairAction{Type: RepairCleanStaging, Target: path, Message: "remove staging file"}) {
				return nil
			}
			if !dryRun {
				return s.fs.Remove(path)
			}
			return nil
		}); err != nil {
			return report, err
		}
	}
	if opts.CleanOrphans {
		s.metaMu.RLock()
		referencedPaths := s.referencedSegmentPathsLocked()
		s.metaMu.RUnlock()
		if err := s.walkFiles(ctx, s.segmentsDir, func(path string) error {
			if referencedPaths[path] {
				return nil
			}
			if !addAction(RepairAction{Type: RepairCleanOrphanSegment, Target: path, Message: "remove orphan segment file"}) {
				return nil
			}
			if !dryRun {
				return s.fs.Remove(path)
			}
			return nil
		}); err != nil {
			return report, err
		}
	}
	if opts.ResetCompacting {
		if err := s.repairCompactingSegments(ctx, dryRun, addAction); err != nil {
			return report, err
		}
	}
	if opts.DegradeMissing {
		if err := s.repairMissingSegments(ctx, dryRun, addAction); err != nil {
			return report, err
		}
	}
	if opts.ClearRefCountWarnings {
		if !addAction(RepairAction{Type: RepairClearRefCountWarnings, Target: "", Message: "clear refcount warnings"}) {
			return report, nil
		}
		if !dryRun {
			if err := s.ClearRefCountWarnings(ctx); err != nil {
				return report, err
			}
		}
	}
	return report, nil
}

func (s *Store) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *Store) pathAccessible(path string) bool {
	info, err := s.fs.Stat(path)
	return err == nil && info.IsDir()
}

func healthMessage(ok bool, okMessage, badMessage string) string {
	if ok {
		return okMessage
	}
	return badMessage
}

func (s *Store) referencedSegmentPathsLocked() map[string]bool {
	referenced := map[string]bool{}
	for _, seg := range s.meta.Segments {
		if seg == nil {
			continue
		}
		if seg.State != segmentStateDeleted {
			referenced[s.segmentPath(seg)] = true
		}
	}
	return referenced
}

func (s *Store) statSegment(seg segmentRecord) error {
	_, err := s.fs.Stat(s.segmentPath(&seg))
	return err
}

func (s *Store) walkFiles(ctx context.Context, root string, visit func(string) error) error {
	err := afero.Walk(s.fs, root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return err
		}
		if err := contextError(ctx); err != nil {
			return err
		}
		return visit(path)
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (s *Store) repairCompactingSegments(ctx context.Context, dryRun bool, addAction func(RepairAction) bool) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	ops := []metaOp{}
	for _, seg := range s.meta.Segments {
		if err := contextError(ctx); err != nil {
			return err
		}
		if seg == nil {
			continue
		}
		if seg.State != segmentStateCompacting {
			continue
		}
		if !addAction(RepairAction{Type: RepairResetCompacting, Target: seg.SegmentID, Message: "reset compacting segment to sealed"}) {
			break
		}
		if !dryRun {
			next := *seg
			next.State = segmentStateSealed
			ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
		}
	}
	if dryRun || len(ops) == 0 {
		return nil
	}
	return s.commitMetaLocked(ops)
}

func (s *Store) repairMissingSegments(ctx context.Context, dryRun bool, addAction func(RepairAction) bool) error {
	s.metaMu.RLock()
	segments := make([]segmentRecord, 0, len(s.meta.Segments))
	for _, seg := range s.meta.Segments {
		if seg == nil {
			continue
		}
		if seg.State != segmentStateDeleted {
			segments = append(segments, *seg)
		}
	}
	s.metaMu.RUnlock()
	missing := map[string]bool{}
	for _, seg := range segments {
		if err := contextError(ctx); err != nil {
			return err
		}
		if err := s.statSegment(seg); errors.Is(err, fs.ErrNotExist) {
			if !addAction(RepairAction{Type: RepairDegradeMissing, Target: seg.SegmentID, Message: "degrade objects referenced by missing segment"}) {
				break
			}
			missing[seg.SegmentID] = true
		} else if err != nil {
			return err
		}
	}
	if dryRun || len(missing) == 0 {
		return nil
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	ops := []metaOp{}
	issues := make([]CheckIssue, 0, len(missing))
	for segmentID := range missing {
		issues = append(issues, CheckIssue{Kind: string(IssueMissingSegment), SegmentID: segmentID, Reason: "segment file is missing"})
	}
	s.degradeIssuesLocked(issues, nowUnix(), &ops)
	return s.commitMetaLocked(ops)
}
