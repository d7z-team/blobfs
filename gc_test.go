package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestStartBackgroundRejectsInvalidContext(t *testing.T) {
	store := openTestStore(t)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.StartBackground(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("start background with canceled context should fail, got %v", err)
	}
	if err := store.StartBackground(nil); err == nil {
		t.Fatalf("start background should reject nil context")
	}

	if err := store.StartBackground(context.Background()); err != nil {
		t.Fatalf("start background worker: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := store.StartBackground(context.Background()); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("start background after close should fail, got %v", err)
	}
}

func TestRunGCRequiresTwoCyclesBeforeCompaction(t *testing.T) {
	store := openTestStore(t)
	live := []byte("live-file-data")
	deadA := []byte("dead-file-a!")
	deadB := []byte("dead-file-b!")
	putBytes(t, store, "tenant-a", "live", live)
	putBytes(t, store, "tenant-a", "dead-a", deadA)
	putBytes(t, store, "tenant-a", "dead-b", deadB)

	if err := store.DeleteObject(context.Background(), "tenant-a", "dead-a"); err != nil {
		t.Fatalf("delete dead-a: %v", err)
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", "dead-b"); err != nil {
		t.Fatalf("delete dead-b: %v", err)
	}

	first, err := store.RunGC(context.Background(), GCOptions{CandidateConfirmCycles: 2, Compact: true})
	if err != nil {
		t.Fatalf("first gc: %v", err)
	}
	if first.CandidatesMarked != 2 || first.ChunksDeleting != 0 {
		t.Fatalf("first gc should only mark candidates: %+v", first)
	}
	if got := readBytes(t, store, "tenant-a", "live"); !bytes.Equal(got, live) {
		t.Fatalf("live file changed after first gc")
	}

	second, err := store.RunGC(context.Background(), GCOptions{CandidateConfirmCycles: 2, Compact: true})
	if err != nil {
		t.Fatalf("second gc: %v", err)
	}
	if second.ChunksDeleting == 0 || second.SegmentsCompacted == 0 || second.SegmentsDeleted == 0 {
		t.Fatalf("second gc should delete through compaction: %+v", second)
	}
	if got := readBytes(t, store, "tenant-a", "live"); !bytes.Equal(got, live) {
		t.Fatalf("live file changed after compaction")
	}
	if _, err := store.OpenObject(context.Background(), "tenant-a", "dead-a"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted file should stay unreadable, got %v", err)
	}
}

func TestRunGCSafetyWindowKeepsNewlyDeletedChunksActive(t *testing.T) {
	cfg := testConfig()
	cfg.GC.SafetyWindow = time.Hour
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	result := putBytes(t, store, "tenant-a", "newly-deleted", []byte("newly-deleted"))
	if err := store.DeleteObject(context.Background(), "tenant-a", "newly-deleted"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	gc, err := store.RunGC(context.Background(), GCOptions{Compact: true})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if gc.CandidatesMarked != 0 || gc.ChunksDeleting != 0 || gc.SegmentsCompacted != 0 {
		t.Fatalf("safety window should keep new chunks untouched: %+v", gc)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	manifest := store.meta.Manifests[result.ManifestID]
	for _, ref := range store.manifestRefs(manifest) {
		if chunk := store.meta.Chunks[ref.ChunkID]; chunk == nil || chunk.State != chunkStateActive {
			t.Fatalf("newly deleted chunk should remain active during safety window: %+v", chunk)
		}
	}
}

func TestRunGCDeletesOrphanWritingChunksAndReclaimsUnreferencedManifest(t *testing.T) {
	store := openTestStore(t)
	now := nowUnix()

	store.mu.Lock()
	store.meta.Chunks["orphan-writing"] = &chunkRecord{
		ChunkID:    "orphan-writing",
		State:      chunkStateWriting,
		CreatedAt:  now - int64(time.Hour),
		LastSeenAt: now - int64(time.Hour),
	}
	store.meta.Manifests["orphan-manifest"] = &manifestRecord{
		ManifestID: "orphan-manifest",
		State:      manifestStateActive,
		CreatedAt:  now - int64(time.Hour),
	}
	store.mu.Unlock()

	result, err := store.RunGC(context.Background(), GCOptions{SafetyWindow: -1})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if result.ChunksDeleted != 1 || result.ManifestsReclaimed != 1 {
		t.Fatalf("orphan cleanup result mismatch: %+v", result)
	}
	store.mu.Lock()
	if store.meta.Chunks["orphan-writing"].State != chunkStateDeleted {
		t.Fatalf("orphan writing chunk should be deleted")
	}
	if store.meta.Manifests["orphan-manifest"].State != manifestStateDeleted {
		t.Fatalf("unreferenced active manifest should be deleted")
	}
	store.mu.Unlock()
}

func TestRunGCRetainsCompactedSegmentDuringDeleteDelay(t *testing.T) {
	cfg := testConfig()
	cfg.GC.SegmentDeleteDelay = time.Hour
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	live := []byte("reader-live!!")
	putBytes(t, store, "tenant-a", "live", live)
	putBytes(t, store, "tenant-a", "dead-a", []byte("reader-dead-a"))
	putBytes(t, store, "tenant-a", "dead-b", []byte("reader-dead-b"))

	reader, err := store.OpenObject(context.Background(), "tenant-a", "live")
	if err != nil {
		t.Fatalf("open reader before gc: %v", err)
	}
	defer reader.Close()

	if err := store.DeleteObject(context.Background(), "tenant-a", "dead-a"); err != nil {
		t.Fatalf("delete dead-a: %v", err)
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", "dead-b"); err != nil {
		t.Fatalf("delete dead-b: %v", err)
	}
	if _, err := store.RunGC(context.Background(), GCOptions{CandidateConfirmCycles: 2, Compact: true}); err != nil {
		t.Fatalf("first gc: %v", err)
	}
	result, err := store.RunGC(context.Background(), GCOptions{CandidateConfirmCycles: 2, Compact: true})
	if err != nil {
		t.Fatalf("second gc: %v", err)
	}
	if result.SegmentsCompacted == 0 {
		t.Fatalf("expected compaction: %+v", result)
	}
	if result.SegmentsDeleted != 0 {
		t.Fatalf("compacted segment should be retained during delete delay: %+v", result)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reader should still use pinned segment: %v", err)
	}
	if !bytes.Equal(got, live) {
		t.Fatalf("opened reader data mismatch")
	}
}

func TestScrubHonorsCanceledContext(t *testing.T) {
	store := openTestStore(t)
	putBytes(t, store, "tenant-a", "file", []byte("scrub-cancel"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Scrub(ctx, ScrubOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("scrub should return canceled context, got %v", err)
	}
}

func TestScrubDetectsChecksumCorruption(t *testing.T) {
	store := openTestStore(t)
	putBytes(t, store, "tenant-a", "file", []byte("checksum-data"))

	chunk, seg := firstStoredChunk(t, store)

	file, err := store.fs.OpenFile(store.segmentPath(&seg), os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err = file.WriteAt([]byte{0xff}, chunk.SegmentOffset+recordHeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt segment: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close corrupted segment: %v", err)
	}
	result, err := store.Scrub(context.Background(), ScrubOptions{})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("scrub should detect checksum corruption")
	}
	if result == nil || result.Healthy || len(result.CorruptChunks) == 0 || len(result.CorruptSegments) == 0 {
		t.Fatalf("scrub corruption result mismatch: %+v", result)
	}
	store.mu.Lock()
	if store.meta.Chunks[chunk.ChunkID].State != chunkStateCorrupt || store.meta.Segments[seg.SegmentID].State != segmentStateCorrupt {
		t.Fatalf("scrub should mark corrupt metadata")
	}
	store.mu.Unlock()
}
