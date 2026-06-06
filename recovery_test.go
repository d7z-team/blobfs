package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func TestStatsAndHealthReportsMetadata(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/observed", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := bytes.Repeat([]byte("observed"), 32)
	putTestBytes(t, store, "tenant-a", "observed/blob", data)
	stats, err := store.Stats(testContext(t))
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Tenants != 1 || stats.Objects != 1 || stats.Directories < 2 {
		t.Fatalf("bad object stats: %+v", stats)
	}
	if stats.Manifests.Active != 1 || stats.Chunks.Active == 0 || stats.Segments.Sealed == 0 {
		t.Fatalf("bad storage stats: %+v", stats)
	}
	if stats.Bytes.LogicalObjectBytes != int64(len(data)) || stats.Bytes.RawChunkBytes == 0 || stats.Bytes.StoredChunkBytes == 0 {
		t.Fatalf("bad byte stats: %+v", stats.Bytes)
	}
	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthOK || !health.Readable || !health.Writable {
		t.Fatalf("bad health: %+v", health)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	health, err = store.Health(testContext(t))
	if err != nil {
		t.Fatalf("closed health: %v", err)
	}
	if health.State != HealthClosed || health.Readable || health.Writable {
		t.Fatalf("bad closed health: %+v", health)
	}
}

func TestHealthAndStatsExposeBackgroundGCFailure(t *testing.T) {
	store := openTestStore(t)
	gcErr := errors.New("background gc failed")
	store.backgroundMu.Lock()
	store.lastBackgroundGCAt = time.Now()
	store.lastBackgroundGC = &GCResult{Epoch: 7}
	store.lastBackgroundGCErr = gcErr
	store.backgroundMu.Unlock()

	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthDegraded || !hasHealthCheck(health, "background_gc", false) {
		t.Fatalf("background gc failure should degrade health: %+v", health)
	}
	stats, err := store.Stats(testContext(t))
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.GC.LastBackgroundEpoch != 7 || stats.GC.LastBackgroundError != gcErr.Error() || stats.GC.LastBackgroundAt.IsZero() {
		t.Fatalf("background gc stats missing: %+v", stats.GC)
	}
}

func TestRecoveryAPIsRejectNilContext(t *testing.T) {
	store := openTestStore(t)
	var nilContext context.Context
	if _, err := store.Health(nilContext); err == nil {
		t.Fatal("Health should reject nil context")
	}
	if _, err := store.Stats(nilContext); err == nil {
		t.Fatal("Stats should reject nil context")
	}
	if _, err := store.Diagnose(nilContext, DiagnoseOptions{}); err == nil {
		t.Fatal("Diagnose should reject nil context")
	}
	if _, err := store.Repair(nilContext, RepairOptions{}); err == nil {
		t.Fatal("Repair should reject nil context")
	}
}

func TestRemoveStaleLockAllowsExplicitReopen(t *testing.T) {
	fsys := afero.NewMemMapFs()
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/lock", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := []byte("lock recovery")
	putTestBytes(t, store, "tenant-a", "lock/blob", data)
	if store.metaLog != nil {
		_ = store.metaLog.Close()
	}
	if store.lockFile != nil {
		_ = store.lockFile.Close()
	}
	if _, err := OpenFS(fsys, "/blobfs", testConfig()); err == nil {
		t.Fatal("stale lock should prevent reopen")
	}
	if err := RemoveFSStaleLock(nil, "/blobfs"); err == nil {
		t.Fatal("nil filesystem should fail")
	}
	if err := RemoveFSStaleLock(fsys, "/blobfs"); err != nil {
		t.Fatalf("remove stale lock: %v", err)
	}
	if err := RemoveFSStaleLock(fsys, "/blobfs"); err != nil {
		t.Fatalf("remove missing stale lock should be idempotent: %v", err)
	}
	reopened, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("reopen after stale lock removal: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "lock/blob"); !bytes.Equal(got, data) {
		t.Fatalf("recovered data = %q", got)
	}
}

func TestRemoveStaleLockLocalFilesystem(t *testing.T) {
	dir := t.TempDir()
	lockDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	lockPath := filepath.Join(lockDir, "LOCK")
	if err := os.WriteFile(lockPath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write lock: %v", err)
	}
	if err := RemoveStaleLock(dir); err != nil {
		t.Fatalf("remove stale lock: %v", err)
	}
	if _, err := os.Stat(lockPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lock still exists: %v", err)
	}
	if err := RemoveStaleLock(dir); err != nil {
		t.Fatalf("remove missing stale lock should be idempotent: %v", err)
	}
}

func TestDiagnoseAndRepairStagingOrphanAndCompacting(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/repair", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "repair/blob", bytes.Repeat([]byte("r"), 128))
	stagingPath := filepath.Join(store.stagingDir, "leftover.tmp")
	if err := afero.WriteFile(store.fs, stagingPath, []byte("staging"), 0o600); err != nil {
		t.Fatalf("write staging: %v", err)
	}
	orphanPath := filepath.Join(store.segmentsDir, "orphan.blob")
	if err := afero.WriteFile(store.fs, orphanPath, []byte("orphan"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	segmentID := markFirstSegmentCompacting(t, store)
	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthDegraded {
		t.Fatalf("compacting segment should degrade health: %+v", health)
	}
	diagnose, err := store.Diagnose(testContext(t), DiagnoseOptions{CheckStaging: true, CheckOrphans: true, MaxIssues: 10})
	if err != nil {
		t.Fatalf("diagnose: %v", err)
	}
	if diagnose.Healthy || !hasIssue(diagnose, IssueStagingLeftover) || !hasIssue(diagnose, IssueOrphanSegment) || !hasIssue(diagnose, IssueCompactingSegment) {
		t.Fatalf("missing diagnose issues: %+v", diagnose)
	}
	preview, err := store.Repair(testContext(t), RepairOptions{DryRun: true, CleanStaging: true, CleanOrphans: true, ResetCompacting: true})
	if err != nil {
		t.Fatalf("repair dry-run: %v", err)
	}
	if len(preview.Actions) != 3 {
		t.Fatalf("dry-run actions = %+v", preview.Actions)
	}
	for _, action := range preview.Actions {
		if action.Applied {
			t.Fatalf("dry-run applied action: %+v", action)
		}
	}
	if _, err := store.fs.Stat(stagingPath); err != nil {
		t.Fatalf("dry-run removed staging: %v", err)
	}
	defaultPreview, err := store.Repair(testContext(t), RepairOptions{CleanStaging: true})
	if err != nil {
		t.Fatalf("repair default preview: %v", err)
	}
	if !defaultPreview.DryRun || len(defaultPreview.Actions) == 0 || defaultPreview.Actions[0].Applied {
		t.Fatalf("repair should default to dry-run: %+v", defaultPreview)
	}
	applied, err := store.Repair(testContext(t), RepairOptions{Apply: true, CleanStaging: true, CleanOrphans: true, ResetCompacting: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if len(applied.Actions) != 3 {
		t.Fatalf("applied actions = %+v", applied.Actions)
	}
	if _, err := store.fs.Stat(stagingPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("staging still exists: %v", err)
	}
	if _, err := store.fs.Stat(orphanPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("orphan still exists: %v", err)
	}
	store.metaMu.RLock()
	seg := store.meta.Segments[segmentID]
	store.metaMu.RUnlock()
	if seg == nil || seg.State != segmentStateSealed {
		t.Fatalf("segment was not reset: %+v", seg)
	}
}

func TestDiagnoseAndRepairMissingSegmentMarksCorrupt(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/missing", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "missing/blob", bytes.Repeat([]byte("m"), 128))
	segmentID, segmentPath := firstSegmentPath(t, store)
	if err := store.fs.Remove(segmentPath); err != nil {
		t.Fatalf("remove segment: %v", err)
	}
	diagnose, err := store.Diagnose(testContext(t), DiagnoseOptions{CheckFiles: true})
	if err != nil {
		t.Fatalf("diagnose missing: %v", err)
	}
	if diagnose.Healthy || !hasIssue(diagnose, IssueMissingSegment) {
		t.Fatalf("missing segment was not diagnosed: %+v", diagnose)
	}
	preview, err := store.Repair(testContext(t), RepairOptions{DryRun: true, MarkMissingCorrupt: true})
	if err != nil {
		t.Fatalf("repair missing dry-run: %v", err)
	}
	if len(preview.Actions) != 1 || preview.Actions[0].Applied {
		t.Fatalf("bad missing preview: %+v", preview)
	}
	applied, err := store.Repair(testContext(t), RepairOptions{Apply: true, MarkMissingCorrupt: true})
	if err != nil {
		t.Fatalf("repair missing apply: %v", err)
	}
	if len(applied.Actions) != 1 || !applied.Actions[0].Applied {
		t.Fatalf("bad missing apply: %+v", applied)
	}
	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health after corrupt mark: %v", err)
	}
	if health.State != HealthCorrupt {
		t.Fatalf("missing segment repair should mark corrupt health: %+v", health)
	}
	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	if store.meta.Segments[segmentID].State != segmentStateCorrupt {
		t.Fatalf("segment not marked corrupt")
	}
	for _, chunk := range store.meta.Chunks {
		if chunk.SegmentID == segmentID && chunk.State != chunkStateCorrupt {
			t.Fatalf("chunk not marked corrupt: %+v", chunk)
		}
	}
}

func TestHealthAndDiagnoseReportTornMetadataLogTail(t *testing.T) {
	fsys := afero.NewMemMapFs()
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/replay", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "replay/blob", []byte("replay"))
	logPath := filepath.Join("/blobfs/meta/txlog", store.metaLogName)
	simulateCrashWithoutCheckpoint(t, store)
	data, err := afero.ReadFile(fsys, logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	data = append(data, 0x01, 0x02, 0x03)
	if err := afero.WriteFile(fsys, logPath, data, 0o600); err != nil {
		t.Fatalf("append torn tail: %v", err)
	}
	reopened, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer reopened.Close()
	health, err := reopened.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthDegraded || !hasHealthCheck(health, "metadata_log_replay", false) {
		t.Fatalf("torn tail was not reported in health: %+v", health)
	}
	diagnose, err := reopened.Diagnose(testContext(t), DiagnoseOptions{})
	if err != nil {
		t.Fatalf("diagnose: %v", err)
	}
	if diagnose.Healthy || !hasIssue(diagnose, IssueMetadataLogTornTail) {
		t.Fatalf("torn tail was not diagnosed: %+v", diagnose)
	}
}

func markFirstSegmentCompacting(t *testing.T, store *Store) string {
	t.Helper()
	store.metaMu.Lock()
	defer store.metaMu.Unlock()
	for _, seg := range store.meta.Segments {
		next := *seg
		next.State = segmentStateCompacting
		if err := store.commitMetaLocked([]metaOp{{Type: "put_segment", Segment: &next}}); err != nil {
			t.Fatalf("mark compacting: %v", err)
		}
		return seg.SegmentID
	}
	t.Fatal("no segment found")
	return ""
}

func firstSegmentPath(t *testing.T, store *Store) (string, string) {
	t.Helper()
	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	for _, seg := range store.meta.Segments {
		return seg.SegmentID, store.segmentPath(seg)
	}
	t.Fatal("no segment found")
	return "", ""
}

func hasHealthCheck(report *HealthReport, name string, ok bool) bool {
	for _, check := range report.Checks {
		if check.Name == name && check.OK == ok {
			return true
		}
	}
	return false
}

func hasIssue(report *DiagnoseReport, kind IssueKind) bool {
	for _, issue := range report.Issues {
		if issue.Kind == kind {
			return true
		}
	}
	return false
}
