package blobfs

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"testing"
)

func TestSmallSegmentAndChunkConfigSurvivesReopen(t *testing.T) {
	cfg := testConfig()
	cfg.SegmentSize = 160
	cfg.Chunking = ChunkingConfig{Algorithm: "FastCDC", MinSize: 8, AvgSize: 16, MaxSize: 24}
	dir := t.TempDir()
	store, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/reliable", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i*37 + i/11)
	}
	put := putTestBytes(t, store, "tenant-a", "reliable/blob", data)
	store.metaMu.RLock()
	segmentCount := len(store.meta.Segments)
	chunkCount := store.meta.Manifests[put.ManifestID].ChunkCount
	store.metaMu.RUnlock()
	if segmentCount < 2 || chunkCount < 2 {
		t.Fatalf("expected many chunks and segments, chunks=%d segments=%d", chunkCount, segmentCount)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "reliable/blob"); !bytes.Equal(got, data) {
		t.Fatalf("reopened data mismatch")
	}
	check, err := reopened.CheckObject(testContext(t), "tenant-a", "reliable/blob")
	if err != nil {
		t.Fatalf("check reopened object: %v", err)
	}
	if !check.Healthy || check.CheckedChunks != chunkCount {
		t.Fatalf("bad check after reopen: %+v", check)
	}
}

func TestGlobalDedupScopeSurvivesTenantDeleteAndGC(t *testing.T) {
	cfg := testConfig()
	cfg.DedupScope = DedupScopeGlobal
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/shared", 0o755); err != nil {
		t.Fatalf("mkdir tenant a: %v", err)
	}
	if err := store.MkdirAll("tenant-b/shared", 0o755); err != nil {
		t.Fatalf("mkdir tenant b: %v", err)
	}
	data := bytes.Repeat([]byte("global-dedup-"), 96)
	first := putTestBytes(t, store, "tenant-a", "shared/blob", data)
	second := putTestBytes(t, store, "tenant-b", "shared/blob", data)
	if first.ManifestID != second.ManifestID {
		t.Fatalf("global dedup should reuse manifest: %s != %s", first.ManifestID, second.ManifestID)
	}
	if err := store.DeleteObject(testContext(t), "tenant-a", "shared/blob"); err != nil {
		t.Fatalf("delete tenant a: %v", err)
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if got := readTestBytes(t, store, "tenant-b", "shared/blob"); !bytes.Equal(got, data) {
		t.Fatalf("tenant b data corrupted after tenant a delete")
	}
	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	for _, chunk := range store.meta.Chunks {
		if chunk.RefCount != 1 || chunk.State != chunkStateActive {
			t.Fatalf("unexpected chunk after global dedup gc: %+v", chunk)
		}
	}
}

func TestPutReusedLiveChunkSurvivesInterleavedGC(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/race", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := bytes.Repeat([]byte("reuse-me"), 64)
	putTestBytes(t, store, "tenant-a", "race/old", data)
	prepared, err := store.prepareObject(testContext(t), "tenant-a", "race/new", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("prepare reused object: %v", err)
	}
	defer store.releasePreparedPins(prepared)
	if len(prepared.pinned) == 0 {
		t.Fatal("expected reused chunk to pin its source segment")
	}
	gc, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true})
	if err != nil {
		t.Fatalf("interleaved gc: %v", err)
	}
	if gc.ChunksDeleted != 0 {
		t.Fatalf("gc deleted live chunk: %+v", gc)
	}
	if gc.SegmentsDeleted != 0 {
		t.Fatalf("gc deleted a pinned segment: %+v", gc)
	}
	if _, err := store.commitPreparedObject(testContext(t), prepared, putCommitOptions{}); err != nil {
		t.Fatalf("commit reused object after gc: %v", err)
	}
	if got := readTestBytes(t, store, "tenant-a", "race/new"); !bytes.Equal(got, data) {
		t.Fatal("reused object became unreadable after interleaved gc")
	}
	store.releasePreparedPins(prepared)
	if err := store.DeleteObject(testContext(t), "tenant-a", "race/old"); err != nil {
		t.Fatalf("delete old: %v", err)
	}
	if gc, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("post-commit gc: %v", err)
	} else if gc.SegmentsDeleted != 0 {
		t.Fatalf("post-commit gc deleted live segment: %+v", gc)
	}
}

func TestRepairObjectRestoresDegradedObject(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/revive", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := bytes.Repeat([]byte("revive-corrupt-"), 32)
	putTestBytes(t, store, "tenant-a", "revive/original", data)
	store.metaMu.Lock()
	var chunkID string
	for id, chunk := range store.meta.Chunks {
		next := *chunk
		next.State = chunkStateMissing
		next.MissingAt = nowUnix()
		next.MissingReason = "test missing chunk"
		if err := store.commitMetaLocked([]metaOp{{Type: "put_chunk", Chunk: &next}}); err != nil {
			store.metaMu.Unlock()
			t.Fatalf("mark missing: %v", err)
		}
		chunkID = id
		break
	}
	store.metaMu.Unlock()
	if chunkID == "" {
		t.Fatal("expected chunk")
	}
	if err := store.applyDegradation(testContext(t), []CheckIssue{{Kind: string(IssueMissingChunk), ChunkID: chunkID, Reason: "test missing chunk"}}); err != nil {
		t.Fatalf("apply degradation: %v", err)
	}
	if _, err := store.Put(testContext(t), "tenant-a", "revive/original", bytes.NewReader(data), nil); !errors.Is(err, ErrObjectDegraded) {
		t.Fatalf("put degraded object err = %v, want ErrObjectDegraded", err)
	}
	if _, err := store.RepairObject(testContext(t), "tenant-a", "revive/original", bytes.NewReader(data), RepairObjectOptions{}); err != nil {
		t.Fatalf("repair degraded object: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "revive/second", data)

	store.metaMu.RLock()
	chunk := store.meta.Chunks[chunkID]
	store.metaMu.RUnlock()
	if chunk == nil || chunk.State != chunkStateActive || chunk.RefCount != 2 {
		t.Fatalf("repaired chunk = %+v, want active refcount 2", chunk)
	}
	if got := readTestBytes(t, store, "tenant-a", "revive/original"); !bytes.Equal(got, data) {
		t.Fatalf("original content mismatch")
	}
	if got := readTestBytes(t, store, "tenant-a", "revive/second"); !bytes.Equal(got, data) {
		t.Fatalf("second content mismatch")
	}
}

func TestRepairObjectRejectsHealthyOrMissingTarget(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/repair", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := []byte("repair-target")
	putTestBytes(t, store, "tenant-a", "repair/healthy", data)

	if _, err := store.RepairObject(testContext(t), "tenant-a", "repair/healthy", bytes.NewReader(data), RepairObjectOptions{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("repair healthy err = %v, want ErrConflict", err)
	}
	if _, err := store.RepairObject(testContext(t), "tenant-a", "repair/missing", bytes.NewReader(data), RepairObjectOptions{}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("repair missing err = %v, want fs.ErrNotExist", err)
	}
}

func TestGCDeletesMissingUnreferencedChunks(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/gc", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "gc/corrupt", []byte("corrupt"))
	if err := store.DeleteObject(testContext(t), "tenant-a", "gc/corrupt"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	store.metaMu.Lock()
	var ops []metaOp
	for _, chunk := range store.meta.Chunks {
		next := *chunk
		next.State = chunkStateMissing
		next.MissingAt = nowUnix()
		next.MissingReason = "test missing chunk"
		ops = append(ops, metaOp{Type: "put_chunk", Chunk: &next})
		break
	}
	if len(ops) != 1 {
		store.metaMu.Unlock()
		t.Fatal("expected one chunk")
	}
	if err := store.commitMetaLocked(ops); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("mark missing: %v", err)
	}
	before := store.meta.TxID
	store.metaMu.Unlock()

	result, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	store.metaMu.RLock()
	txDelta := store.meta.TxID - before
	var state string
	for _, chunk := range store.meta.Chunks {
		state = chunk.State
		break
	}
	store.metaMu.RUnlock()
	if result.ChunksDeleted != 1 || result.SegmentsDeleted != 1 || txDelta != 4 || state != chunkStateDeleted {
		t.Fatalf("gc failed to delete missing chunk: result=%+v txDelta=%d state=%s", result, txDelta, state)
	}
	stats, err := store.Stats(testContext(t))
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.GC.LastRunState != "DONE" {
		t.Fatalf("gc run state = %q, want DONE", stats.GC.LastRunState)
	}
}

func TestDeleteDegradedObjectReclaimsMissingResources(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/reclaim", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "reclaim/blob", bytes.Repeat([]byte("r"), 64))
	segmentID, segmentPath := firstSegmentPath(t, store)
	if err := store.fs.Remove(segmentPath); err != nil {
		t.Fatalf("remove segment: %v", err)
	}
	if _, err := store.Repair(testContext(t), RepairOptions{Apply: true, DegradeMissing: true}); err != nil {
		t.Fatalf("degrade missing: %v", err)
	}
	if err := store.DeleteObject(testContext(t), "tenant-a", "reclaim/blob"); err != nil {
		t.Fatalf("delete degraded object: %v", err)
	}
	result, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true})
	if err != nil {
		t.Fatalf("gc after delete: %v", err)
	}
	if result.ChunksDeleted == 0 || result.SegmentsDeleted == 0 {
		t.Fatalf("expected missing resources to be reclaimed: %+v", result)
	}
	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	if seg := store.meta.Segments[segmentID]; seg == nil || seg.State != segmentStateDeleted {
		t.Fatalf("segment not deleted: %+v", seg)
	}
	for _, inode := range store.meta.Inodes {
		if inode != nil && inode.Name == "blob" && inode.State != fileStateDeleted {
			t.Fatalf("inode should stay deleted: %+v", inode)
		}
	}
}

func TestRepairObjectHonorsGenerationOnDegradedObject(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/generation", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	put := putTestBytes(t, store, "tenant-a", "generation/blob", []byte("old"))
	_, segmentPath := firstSegmentPath(t, store)
	if err := store.fs.Remove(segmentPath); err != nil {
		t.Fatalf("remove segment: %v", err)
	}
	if _, err := store.Repair(testContext(t), RepairOptions{Apply: true, DegradeMissing: true}); err != nil {
		t.Fatalf("degrade missing: %v", err)
	}

	if _, err := store.RepairObject(testContext(t), "tenant-a", "generation/blob", bytes.NewReader([]byte("new")), RepairObjectOptions{BaseGeneration: put.Generation + 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("repair with wrong generation err = %v, want ErrConflict", err)
	}
	result, err := store.RepairObject(testContext(t), "tenant-a", "generation/blob", bytes.NewReader([]byte("new")), RepairObjectOptions{BaseGeneration: put.Generation})
	if err != nil {
		t.Fatalf("repair with correct generation: %v", err)
	}
	if result.Generation <= put.Generation {
		t.Fatalf("generation did not advance: before=%d after=%d", put.Generation, result.Generation)
	}
}

func TestMaxFileSizeFailureLeavesNoVisibleObjectOrSegments(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFileSize = 32
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/limits", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	_, err = store.Put(testContext(t), "tenant-a", "limits/too-large", bytes.NewReader(bytes.Repeat([]byte("x"), 256)), nil)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("put too large = %v, want ErrTooLarge", err)
	}
	if _, err := store.OpenObject(testContext(t), "tenant-a", "limits/too-large"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("too-large object became visible: %v", err)
	}
	store.metaMu.RLock()
	segments := len(store.meta.Segments)
	chunks := len(store.meta.Chunks)
	store.metaMu.RUnlock()
	if segments != 0 || chunks != 0 {
		t.Fatalf("failed put left metadata behind: segments=%d chunks=%d", segments, chunks)
	}
}

func TestMaxOpenWriteSessionsIsEnforced(t *testing.T) {
	cfg := testConfig()
	cfg.MaxOpenWriteSessions = 1
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/sessions", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	first, err := store.OpenFile("tenant-a/sessions/first", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open first writer: %v", err)
	}
	if _, err := store.OpenFile("tenant-a/sessions/second", os.O_CREATE|os.O_RDWR, 0o644); !errors.Is(err, ErrTooManyOpenWriteSessions) {
		_ = first.Close()
		t.Fatalf("second writer = %v, want ErrTooManyOpenWriteSessions", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first writer: %v", err)
	}
	second, err := store.OpenFile("tenant-a/sessions/second", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open second after release: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second writer: %v", err)
	}
}

func TestGarbageCandidateChunkReusedOnReupload(t *testing.T) {
	cfg := testConfig()
	cfg.GC.CandidateConfirmCycles = 2
	cfg.GC.SegmentDeleteDelay = -1
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/reuse", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}

	data := []byte("dedup")
	first := putTestBytes(t, store, "tenant-a", "reuse/old", data)

	store.metaMu.RLock()
	oldManifest := store.meta.Manifests[first.ManifestID]
	if oldManifest == nil || len(oldManifest.Chunks) != 1 {
		store.metaMu.RUnlock()
		t.Fatalf("expected single chunk manifest")
	}
	oldChunkID := oldManifest.Chunks[0].ChunkID
	oldChunk := store.meta.Chunks[oldChunkID]
	if oldChunk == nil {
		store.metaMu.RUnlock()
		t.Fatal("old chunk missing")
	}
	oldSegmentID := oldChunk.SegmentID
	store.metaMu.RUnlock()

	if err := store.DeleteObject(testContext(t), "tenant-a", "reuse/old"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	result, err := store.RunGC(testContext(t), GCOptions{Compact: false})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}

	store.metaMu.RLock()
	candidate := store.meta.Chunks[oldChunkID]
	store.metaMu.RUnlock()
	if candidate == nil {
		t.Fatal("chunk was deleted, expected GARBAGE_CANDIDATE")
	}
	if candidate.State != chunkStateGarbageCandidate {
		t.Fatalf("chunk state = %s, want GARBAGE_CANDIDATE", candidate.State)
	}
	if result.CandidatesMarked < 1 {
		t.Fatalf("no candidates marked: %+v", result)
	}

	second := putTestBytes(t, store, "tenant-a", "reuse/new", data)
	if first.ManifestID != second.ManifestID {
		t.Fatalf("manifest changed: %s != %s", first.ManifestID, second.ManifestID)
	}

	store.metaMu.RLock()
	reusedChunk := store.meta.Chunks[oldChunkID]
	store.metaMu.RUnlock()
	if reusedChunk == nil {
		t.Fatal("reused chunk missing from metadata")
	}
	if reusedChunk.SegmentID != oldSegmentID {
		t.Fatalf("chunk reused different segment: %s != %s", reusedChunk.SegmentID, oldSegmentID)
	}
	if reusedChunk.State != chunkStateActive {
		t.Fatalf("chunk state = %s after reuse, want ACTIVE", reusedChunk.State)
	}
	if reusedChunk.RefCount != 1 {
		t.Fatalf("chunk refcount = %d, want 1", reusedChunk.RefCount)
	}
	if reusedChunk.GarbageSeenCount != 0 {
		t.Fatalf("chunk GarbageSeenCount = %d, want 0 after reuse", reusedChunk.GarbageSeenCount)
	}
}

func TestWriteSessionLimitReleasedAfterOpenFailure(t *testing.T) {
	cfg := testConfig()
	cfg.MaxOpenWriteSessions = 1
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/sessions", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	put := putTestBytes(t, store, "tenant-a", "sessions/blob", []byte("old"))
	store.metaMu.Lock()
	manifest := store.meta.Manifests[put.ManifestID]
	chunk := store.meta.Chunks[manifest.Chunks[0].ChunkID]
	next := *chunk
	next.State = chunkStateMissing
	if err := store.commitMetaLocked([]metaOp{{Type: "put_chunk", Chunk: &next}}); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("mark missing: %v", err)
	}
	store.metaMu.Unlock()
	if file, err := store.OpenFile("tenant-a/sessions/blob", os.O_RDWR, 0o644); err == nil {
		_ = file.Close()
		t.Fatal("opening corrupt file for copy should fail")
	}
	recovered, err := store.OpenFile("tenant-a/sessions/recovered", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("write session was not released after open failure: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("close recovered writer: %v", err)
	}
}
