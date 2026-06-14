package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.SegmentSize = 4096
	cfg.MaxFileSize = 8 << 20
	cfg.Chunking = ChunkingConfig{
		Algorithm: "FastCDC",
		MinSize:   8,
		AvgSize:   16,
		MaxSize:   24,
	}
	cfg.GC.SafetyWindow = -1
	cfg.GC.SegmentDeleteDelay = -1
	cfg.GC.CandidateConfirmCycles = 1
	cfg.GC.CompactGarbageRatio = 0.25
	cfg.GC.BackgroundGCInterval = time.Hour
	return cfg
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func putTestBytes(t *testing.T, store *Store, tenantID, path string, data []byte) *PutResult {
	t.Helper()
	result, err := store.Put(testContext(t), tenantID, path, bytes.NewReader(data), map[string]string{"kind": "test"})
	if err != nil {
		t.Fatalf("put %s: %v", path, err)
	}
	return result
}

func readTestBytes(t *testing.T, store *Store, tenantID, path string) []byte {
	t.Helper()
	reader, err := store.OpenObject(testContext(t), tenantID, path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func firstChunkPayload(t *testing.T, store *Store, tenantID, path string) ([]byte, string) {
	t.Helper()
	chunk, segment := firstChunkSnapshot(t, store, tenantID, path)
	payload, err := store.readChunkPayloadAt(segment, chunk)
	if err != nil {
		t.Fatalf("read first chunk payload: %v", err)
	}
	return payload, segment.SegmentID
}

func firstChunkSnapshot(t *testing.T, store *Store, tenantID, path string) (chunkRecord, segmentRecord) {
	t.Helper()
	store.metaMu.RLock()
	inode, err := store.resolvePathLocked(tenantID, path)
	if err != nil {
		store.metaMu.RUnlock()
		t.Fatalf("resolve %s: %v", path, err)
	}
	manifest := store.meta.Manifests[inode.ManifestID]
	if manifest == nil || len(manifest.Chunks) == 0 {
		store.metaMu.RUnlock()
		t.Fatalf("manifest has no chunks for %s", path)
	}
	chunk := store.meta.Chunks[manifest.Chunks[0].ChunkID]
	if chunk == nil {
		store.metaMu.RUnlock()
		t.Fatalf("chunk missing for %s", path)
	}
	segment := store.meta.Segments[chunk.SegmentID]
	if segment == nil {
		store.metaMu.RUnlock()
		t.Fatalf("segment missing for %s", path)
	}
	chunkCopy := *chunk
	segmentCopy := *segment
	store.metaMu.RUnlock()
	return chunkCopy, segmentCopy
}

func corruptFirstChunkPayloadByte(t *testing.T, store *Store, tenantID, path string) (chunkRecord, segmentRecord) {
	t.Helper()
	chunk, segment := firstChunkSnapshot(t, store, tenantID, path)
	file, err := store.fs.OpenFile(store.segmentPath(&segment), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := file.Seek(chunk.SegmentOffset+recordHeaderSize, io.SeekStart); err != nil {
		_ = file.Close()
		t.Fatalf("seek segment: %v", err)
	}
	original := []byte{0}
	if _, err := io.ReadFull(file, original); err != nil {
		_ = file.Close()
		t.Fatalf("read segment byte: %v", err)
	}
	if _, err := file.Seek(chunk.SegmentOffset+recordHeaderSize, io.SeekStart); err != nil {
		_ = file.Close()
		t.Fatalf("seek segment again: %v", err)
	}
	if _, err := file.Write([]byte{original[0] ^ 0xff}); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt segment: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupted segment: %v", err)
	}
	return chunk, segment
}

func markCompactionCandidatesForTest(t *testing.T, store *Store) ([]compactCandidate, *GCResult) {
	t.Helper()
	now := nowUnix()
	store.metaMu.Lock()
	ops := []metaOp{}
	result := &GCResult{}
	store.markUnreferencedChunksLocked(now, now+int64(time.Second), 1, result, &ops)
	if err := store.commitMetaLocked(ops); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("mark garbage: %v", err)
	}
	candidates, _ := store.collectSegmentWorkLocked(now, true)
	ops = ops[:0]
	for _, candidate := range candidates {
		next := candidate.Source
		next.State = segmentStateCompacting
		ops = append(ops, metaOp{Type: "put_segment", Segment: &next})
	}
	if err := store.commitMetaLocked(ops); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("mark compacting: %v", err)
	}
	store.metaMu.Unlock()
	if len(candidates) == 0 {
		t.Fatal("expected compaction candidate")
	}
	return candidates, result
}

func TestInodeRenamePersistsSubtreeWithoutRewritingChildren(t *testing.T) {
	ctx := testContext(t)
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/src/nested", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	for i := 0; i < 500; i++ {
		putTestBytes(t, store, "tenant-a", "src/nested/file-"+strconv.Itoa(i), []byte("payload"))
	}
	start := time.Now()
	if err := store.Rename("tenant-a/src", "tenant-a/dst"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("rename took too long; subtree was likely scanned")
	}
	if got := readTestBytes(t, store, "tenant-a", "dst/nested/file-42"); string(got) != "payload" {
		t.Fatalf("renamed content = %q", got)
	}
	if _, err := store.OpenObject(ctx, "tenant-a", "src/nested/file-42"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old path should not exist, got %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "dst/nested/file-42"); string(got) != "payload" {
		t.Fatalf("reopened content = %q", got)
	}
}

func TestSlowPutDoesNotBlockExistingReader(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "docs/blob", []byte("old"))
	reader := &blockingReader{started: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(reader.release)
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := store.Put(context.Background(), "tenant-a", "docs/blob", reader, nil)
		done <- err
	}()
	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("slow reader did not start")
	}
	readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	obj, err := store.OpenObject(readCtx, "tenant-a", "docs/blob")
	if err != nil {
		t.Fatalf("open while slow put is reading: %v", err)
	}
	data, err := io.ReadAll(obj)
	_ = obj.Close()
	if err != nil {
		t.Fatalf("read while slow put is reading: %v", err)
	}
	if string(data) != "old" {
		t.Fatalf("existing reader saw %q", data)
	}
	close(reader.release)
	released = true
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("slow put: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow put did not complete")
	}
}

func TestGCDeletesUnreferencedSegmentsWithoutBlockingReads(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "docs/a", bytes.Repeat([]byte("a"), 512))
	reader, err := store.OpenObject(testContext(t), "tenant-a", "docs/a")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.DeleteObject(testContext(t), "tenant-a", "docs/a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1}); err != nil {
		t.Fatalf("gc with pinned reader: %v", err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("pinned read after gc: %v", err)
	}
	if len(data) != 512 {
		t.Fatalf("pinned read size = %d", len(data))
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1}); err != nil {
		t.Fatalf("gc after reader close: %v", err)
	}
}

func TestRunGCCompactsPartiallyDeadSegment(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/compact", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := make([]byte, 768)
	for i := range data {
		data[i] = byte(i*31 + i/7)
	}
	putTestBytes(t, store, "tenant-a", "compact/source", data)
	livePayload, oldSegmentID := firstChunkPayload(t, store, "tenant-a", "compact/source")
	putTestBytes(t, store, "tenant-a", "compact/live", livePayload)
	if err := store.DeleteObject(testContext(t), "tenant-a", "compact/source"); err != nil {
		t.Fatalf("delete source: %v", err)
	}
	result, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true})
	if err != nil {
		t.Fatalf("compact gc: %v", err)
	}
	if result.SegmentsCompacted == 0 {
		t.Fatalf("expected compaction, got %+v", result)
	}
	if got := readTestBytes(t, store, "tenant-a", "compact/live"); !bytes.Equal(got, livePayload) {
		t.Fatalf("live payload corrupted after compaction")
	}
	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	inode, err := store.resolvePathLocked("tenant-a", "compact/live")
	if err != nil {
		t.Fatalf("resolve live: %v", err)
	}
	manifest := store.meta.Manifests[inode.ManifestID]
	chunk := store.meta.Chunks[manifest.Chunks[0].ChunkID]
	if chunk == nil || chunk.SegmentID == oldSegmentID {
		t.Fatalf("live chunk was not moved off compacted segment: %+v", chunk)
	}
	if segment := store.meta.Segments[oldSegmentID]; segment == nil || segment.State != segmentStateDeleted {
		t.Fatalf("old segment state = %+v", segment)
	}
}

func TestRunGCHonorsSegmentDeleteDelay(t *testing.T) {
	cfg := testConfig()
	cfg.GC.SegmentDeleteDelay = time.Hour
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/gc", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "gc/blob", bytes.Repeat([]byte("z"), 256))
	if err := store.DeleteObject(testContext(t), "tenant-a", "gc/blob"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	result, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1})
	if err != nil {
		t.Fatalf("gc with delay: %v", err)
	}
	if result.SegmentsDeleted != 0 {
		t.Fatalf("segment deleted before delay: %+v", result)
	}
	store.cfg.GC.SegmentDeleteDelay = -1
	result, err = store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1})
	if err != nil {
		t.Fatalf("gc without delay: %v", err)
	}
	if result.SegmentsDeleted == 0 {
		t.Fatalf("segment was not deleted after delay disabled: %+v", result)
	}
}

func TestRunGCCompactTrueDeletesFullyDeadSegments(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/gc", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "gc/blob", bytes.Repeat([]byte("dead"), 64))
	if err := store.DeleteObject(testContext(t), "tenant-a", "gc/blob"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	result, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true})
	if err != nil {
		t.Fatalf("compact gc: %v", err)
	}
	if result.ChunksDeleted == 0 {
		t.Fatalf("chunk was not deleted: %+v", result)
	}
	if result.SegmentsDeleted == 0 {
		t.Fatalf("dead segment was not deleted with Compact=true: %+v", result)
	}
}

func TestGCCollectSegmentWorkFindsCompactionAndDeadSegments(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/gcwork", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := make([]byte, 768)
	for i := range data {
		data[i] = byte(i*11 + i/5)
	}
	putTestBytes(t, store, "tenant-a", "gcwork/source", data)
	livePayload, _ := firstChunkPayload(t, store, "tenant-a", "gcwork/source")
	putTestBytes(t, store, "tenant-a", "gcwork/live", livePayload)
	putTestBytes(t, store, "tenant-a", "gcwork/dead", bytes.Repeat([]byte("dead"), 64))
	if err := store.DeleteObject(testContext(t), "tenant-a", "gcwork/source"); err != nil {
		t.Fatalf("delete source: %v", err)
	}
	if err := store.DeleteObject(testContext(t), "tenant-a", "gcwork/dead"); err != nil {
		t.Fatalf("delete dead: %v", err)
	}

	now := nowUnix()
	store.metaMu.Lock()
	ops := []metaOp{}
	result := &GCResult{}
	store.markUnreferencedChunksLocked(now, now+int64(time.Second), 1, result, &ops)
	if err := store.commitMetaLocked(ops); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("mark garbage: %v", err)
	}
	candidates, dead := store.collectSegmentWorkLocked(now, true)
	store.metaMu.Unlock()
	if len(candidates) == 0 {
		t.Fatal("expected compaction candidate from partially dead segment")
	}
	if len(dead) == 0 {
		t.Fatal("expected fully dead segment")
	}
}

func TestCompactionKeepsPinnedSourceSegmentUntilNextGC(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/pinned", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := make([]byte, 768)
	for i := range data {
		data[i] = byte(i*17 + i/5)
	}
	putTestBytes(t, store, "tenant-a", "pinned/source", data)
	livePayload, oldSegmentID := firstChunkPayload(t, store, "tenant-a", "pinned/source")
	putTestBytes(t, store, "tenant-a", "pinned/live", livePayload)
	if err := store.DeleteObject(testContext(t), "tenant-a", "pinned/source"); err != nil {
		t.Fatalf("delete source: %v", err)
	}
	candidates, gcResult := markCompactionCandidatesForTest(t, store)
	compacted, err := store.compactCandidates(testContext(t), candidates)
	if err != nil {
		t.Fatalf("compact candidates: %v", err)
	}
	store.pinSegment(oldSegmentID)
	_, err = store.commitCompactionResults(compacted, gcResult, nowUnix(), nowUnix())
	store.unpinSegment(oldSegmentID)
	if err != nil {
		t.Fatalf("commit compaction: %v", err)
	}
	if got := readTestBytes(t, store, "tenant-a", "pinned/live"); !bytes.Equal(got, livePayload) {
		t.Fatalf("live payload corrupted after pinned compaction")
	}
	store.metaMu.RLock()
	source := store.meta.Segments[oldSegmentID]
	if source == nil || source.State == segmentStateDeleted {
		store.metaMu.RUnlock()
		t.Fatalf("pinned source segment should remain until later gc: %+v", source)
	}
	store.metaMu.RUnlock()
	result, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1})
	if err != nil {
		t.Fatalf("gc after unpin: %v", err)
	}
	if result.SegmentsDeleted == 0 {
		t.Fatalf("source segment was not deleted after unpin: %+v", result)
	}
}

func TestCommitCompactionResultsDeletesOutputWhenSourceIsStale(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/stale", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := make([]byte, 768)
	for i := range data {
		data[i] = byte(i*13 + i/3)
	}
	putTestBytes(t, store, "tenant-a", "stale/source", data)
	livePayload, oldSegmentID := firstChunkPayload(t, store, "tenant-a", "stale/source")
	putTestBytes(t, store, "tenant-a", "stale/live", livePayload)
	if err := store.DeleteObject(testContext(t), "tenant-a", "stale/source"); err != nil {
		t.Fatalf("delete source: %v", err)
	}

	candidates, gcResult := markCompactionCandidatesForTest(t, store)
	compacted, err := store.compactCandidates(testContext(t), candidates)
	if err != nil {
		t.Fatalf("compact candidates: %v", err)
	}
	var newSegments []string
	for _, item := range compacted {
		for _, seg := range item.Segments {
			newSegments = append(newSegments, store.segmentPath(seg))
		}
	}
	store.metaMu.Lock()
	source := store.meta.Segments[oldSegmentID]
	next := *source
	next.State = segmentStateSealed
	if err := store.commitMetaLocked([]metaOp{{Type: "put_segment", Segment: &next}}); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("stale source: %v", err)
	}
	store.metaMu.Unlock()

	deleted, err := store.commitCompactionResults(compacted, gcResult, nowUnix(), nowUnix())
	if err != nil {
		t.Fatalf("commit stale compaction: %v", err)
	}
	if len(deleted) == 0 {
		t.Fatal("stale compaction output was not scheduled for deletion")
	}
	if err := store.removeCompactedSegments(compacted); err != nil {
		t.Fatalf("remove compacted stale output: %v", err)
	}
	if _, err := store.removeSegmentFiles(testContext(t), deleted); err != nil {
		t.Fatalf("remove stale compaction output: %v", err)
	}
	for _, path := range newSegments {
		if _, err := store.fs.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stale compaction segment still exists %s: %v", path, err)
		}
	}
	if got := readTestBytes(t, store, "tenant-a", "stale/live"); !bytes.Equal(got, livePayload) {
		t.Fatalf("live payload corrupted after stale compaction")
	}
}

func TestRollbackCompactionResetsCompactingSegments(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/rollback", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := make([]byte, 768)
	for i := range data {
		data[i] = byte(i*19 + i/7)
	}
	putTestBytes(t, store, "tenant-a", "rollback/source", data)
	livePayload, oldSegmentID := firstChunkPayload(t, store, "tenant-a", "rollback/source")
	putTestBytes(t, store, "tenant-a", "rollback/live", livePayload)
	if err := store.DeleteObject(testContext(t), "tenant-a", "rollback/source"); err != nil {
		t.Fatalf("delete source: %v", err)
	}

	candidates, _ := markCompactionCandidatesForTest(t, store)
	if err := store.rollbackCompaction(candidates); err != nil {
		t.Fatalf("rollback compaction: %v", err)
	}
	store.metaMu.RLock()
	source := store.meta.Segments[oldSegmentID]
	store.metaMu.RUnlock()
	if source == nil || source.State != segmentStateSealed {
		t.Fatalf("source was not rolled back to sealed: %+v", source)
	}
}

func TestAferoWriteConflictAndDirectoryOperations(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/work", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	first, err := store.OpenFile("tenant-a/work/log.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open first: %v", err)
	}
	second, err := store.OpenFile("tenant-a/work/log.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open second: %v", err)
	}
	if _, err := first.Write([]byte("first")); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	if _, err := second.Write([]byte("second")); err != nil {
		t.Fatalf("write second: %v", err)
	}
	if err := second.Close(); !errors.Is(err, ErrConflict) {
		t.Fatalf("second close = %v, want ErrConflict", err)
	}
	entries, err := afero.ReadDir(store, "tenant-a/work")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "log.txt" {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestCheckAndScrubUseSnapshots(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "docs/blob", []byte("healthy"))
	check, err := store.CheckObject(testContext(t), "tenant-a", "docs/blob")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !check.Healthy || check.CheckedChunks == 0 {
		t.Fatalf("bad check result: %+v", check)
	}
	scrub, err := store.Scrub(testContext(t), ScrubOptions{CheckFiles: true})
	if err != nil {
		t.Fatalf("scrub: %v", err)
	}
	if !scrub.Healthy || scrub.CheckedFiles != 1 {
		t.Fatalf("bad scrub result: %+v", scrub)
	}
}

func TestPublicAPIVFSSmokeAndBoundaries(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/api", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := []byte("0123456789abcdef")
	put := putTestBytes(t, store, "tenant-a", "api/blob", data)
	info, err := store.StatObject(testContext(t), "tenant-a", "api/blob")
	if err != nil {
		t.Fatalf("stat object: %v", err)
	}
	if info.Size != int64(len(data)) || info.FileHash != put.FileHash {
		t.Fatalf("bad stat info: %+v", info)
	}
	updated, err := store.UpdateMetadata(testContext(t), "tenant-a", "api/blob", map[string]string{"content-type": "text/plain"})
	if err != nil {
		t.Fatalf("update metadata: %v", err)
	}
	if updated.Options["content-type"] != "text/plain" || updated.ManifestID != put.ManifestID {
		t.Fatalf("bad metadata update: %+v", updated)
	}
	ranged, err := store.OpenRange(testContext(t), "tenant-a", "api/blob", 4, 5)
	if err != nil {
		t.Fatalf("open range: %v", err)
	}
	rangeData, err := io.ReadAll(ranged)
	_ = ranged.Close()
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if string(rangeData) != "45678" {
		t.Fatalf("range data = %q", rangeData)
	}
	if _, err := store.OpenRange(testContext(t), "tenant-a", "api/blob", int64(len(data)+1), 1); !errors.Is(err, io.EOF) {
		t.Fatalf("open range past eof = %v", err)
	}
	reader, err := store.OpenObject(testContext(t), "tenant-a", "api/blob")
	if err != nil {
		t.Fatalf("open object: %v", err)
	}
	if reader.Info().ManifestID != put.ManifestID || reader.ETag() != put.FileHash {
		t.Fatalf("bad reader metadata")
	}
	if _, err := reader.Seek(10, io.SeekStart); err != nil {
		t.Fatalf("reader seek: %v", err)
	}
	buf := make([]byte, 3)
	if _, err := io.ReadFull(reader, buf); err != nil {
		t.Fatalf("reader read after seek: %v", err)
	}
	_ = reader.Close()
	if string(buf) != "abc" {
		t.Fatalf("reader seek data = %q", buf)
	}
	tenantFS := store.TenantFS("tenant-a")
	if _, ok := tenantFS.(fs.StatFS); !ok {
		t.Fatal("tenant fs should implement fs.StatFS")
	}
	if _, ok := tenantFS.(fs.ReadDirFS); !ok {
		t.Fatal("tenant fs should implement fs.ReadDirFS")
	}
	if tenantData, err := fs.ReadFile(tenantFS, "api/blob"); err != nil || !bytes.Equal(tenantData, data) {
		t.Fatalf("tenant fs read = %q, %v", tenantData, err)
	}
	if stat, err := fs.Stat(tenantFS, "api/blob"); err != nil || stat.Size() != int64(len(data)) {
		t.Fatalf("tenant fs stat = %+v, %v", stat, err)
	}
	if entries, err := fs.ReadDir(tenantFS, "api"); err != nil || len(entries) != 1 || entries[0].Name() != "blob" {
		t.Fatalf("tenant fs readdir = %+v, %v", entries, err)
	}

	file, err := store.OpenFile("tenant-a/api/vfs.txt", os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	if _, err := file.WriteString("abcdef"); err != nil {
		t.Fatalf("write string: %v", err)
	}
	if _, err := file.Seek(3, io.SeekStart); err != nil {
		t.Fatalf("seek vfs: %v", err)
	}
	if _, err := file.Write([]byte("XYZ")); err != nil {
		t.Fatalf("write vfs: %v", err)
	}
	if err := file.Truncate(5); err != nil {
		t.Fatalf("truncate vfs: %v", err)
	}
	if stat, err := file.Stat(); err != nil || stat.Size() != 5 {
		t.Fatalf("file stat = %+v, %v", stat, err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close vfs: %v", err)
	}
	if got := readTestBytes(t, store, "tenant-a", "api/vfs.txt"); string(got) != "abcXY" {
		t.Fatalf("vfs content = %q", got)
	}
	mtime := time.Unix(123, 0)
	if err := store.Chmod("tenant-a/api/vfs.txt", 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := store.Chown("tenant-a/api/vfs.txt", 1000, 1001); err != nil {
		t.Fatalf("chown: %v", err)
	}
	if err := store.Chtimes("tenant-a/api/vfs.txt", mtime, mtime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	stat, err := store.Stat("tenant-a/api/vfs.txt")
	if err != nil {
		t.Fatalf("stat vfs: %v", err)
	}
	if stat.Mode().Perm()&0o111 != 0 || !stat.ModTime().Equal(mtime) {
		t.Fatalf("bad vfs metadata: mode=%v mtime=%v", stat.Mode(), stat.ModTime())
	}
	dir, err := store.Open("tenant-a/api")
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	names, err := dir.Readdirnames(-1)
	_ = dir.Close()
	if err != nil {
		t.Fatalf("readdirnames: %v", err)
	}
	if len(names) != 2 || names[0] != "blob" || names[1] != "vfs.txt" {
		t.Fatalf("dir names = %v", names)
	}
	if err := store.Remove("tenant-a/api/vfs.txt"); err != nil {
		t.Fatalf("remove vfs: %v", err)
	}
	if _, err := store.Stat("tenant-a/api/vfs.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed stat = %v", err)
	}
	if store.Name() != "blobfs" {
		t.Fatalf("store name = %q", store.Name())
	}
	if err := store.Mkdir("tenant-a/api/empty", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	created, err := store.Create("tenant-a/api/created.txt")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Name() != "tenant-a/api/created.txt" {
		t.Fatalf("created name = %q", created.Name())
	}
	if _, err := created.Write([]byte("created")); err != nil {
		_ = created.Close()
		t.Fatalf("write created: %v", err)
	}
	if err := created.Close(); err != nil {
		t.Fatalf("close created: %v", err)
	}
	dir, err = store.Open("tenant-a/api")
	if err != nil {
		t.Fatalf("open dir for readdir: %v", err)
	}
	dirEntries, err := dir.(*blobVFSFile).ReadDir(-1)
	_ = dir.Close()
	if err != nil || len(dirEntries) != 3 {
		t.Fatalf("read dir entries = %v, %v", dirEntries, err)
	}
	if _, err := store.Put(testContext(t), "bad/tenant", "x", bytes.NewReader(nil), nil); err == nil {
		t.Fatal("invalid tenant should fail")
	}
	if _, err := store.Put(testContext(t), "tenant-a", "../x", bytes.NewReader(nil), nil); err == nil {
		t.Fatal("invalid path should fail")
	}
}

func TestCheckObjectMarksCorruptSegment(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/corrupt", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "corrupt/blob", bytes.Repeat([]byte("c"), 128))
	chunk, segment := corruptFirstChunkPayloadByte(t, store, "tenant-a", "corrupt/blob")
	result, err := store.CheckObject(testContext(t), "tenant-a", "corrupt/blob")
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("check corrupt err = %v", err)
	}
	if result == nil || result.Healthy || len(result.Issues) == 0 {
		t.Fatalf("bad corrupt result: %+v", result)
	}
	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	if store.meta.Chunks[chunk.ChunkID].State != chunkStateCorrupt {
		t.Fatalf("chunk not marked corrupt")
	}
	if store.meta.Segments[segment.SegmentID].State != segmentStateCorrupt {
		t.Fatalf("segment not marked corrupt")
	}
}

func TestCheckpointCompactsMetadataLogAndRecovers(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/checkpoints", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	for i := 0; i < metaCheckpointInterval+10; i++ {
		putTestBytes(t, store, "tenant-a", "checkpoints/file-"+strconv.Itoa(i), []byte("payload-"+strconv.Itoa(i)))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", metaCheckpointFile)); err != nil {
		t.Fatalf("checkpoint missing: %v", err)
	}
	super, err := loadMetaSuperBlock(afero.NewOsFs(), filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatalf("load superblock: %v", err)
	}
	if super.LogFile == metaLogFile {
		t.Fatalf("active txlog should rotate after checkpoint, still %q", super.LogFile)
	}
	if stat, err := os.Stat(filepath.Join(dir, "meta", "txlog", super.LogFile)); err != nil {
		t.Fatalf("active txlog missing: %v", err)
	} else if stat.Size() != 0 {
		t.Fatalf("active txlog should be empty after close checkpoint, size=%d", stat.Size())
	}
	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "checkpoints/file-129"); string(got) != "payload-129" {
		t.Fatalf("recovered content = %q", got)
	}
}

func TestCloseRecreatesMissingTxLogDirDuringCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/checkpoints", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "checkpoints/blob", []byte("payload"))
	if err := os.RemoveAll(filepath.Join(dir, "meta", "txlog")); err != nil {
		t.Fatalf("remove txlog dir: %v", err)
	}
	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthReadOnly || !hasHealthCheck(health, "txlog_dir_available", false) {
		t.Fatalf("missing txlog dir should make health read-only: %+v", health)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close after missing txlog dir: %v", err)
	}
	super, err := loadMetaSuperBlock(afero.NewOsFs(), filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatalf("load superblock: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "txlog", super.LogFile)); err != nil {
		t.Fatalf("active txlog was not recreated: %v", err)
	}
	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "checkpoints/blob"); string(got) != "payload" {
		t.Fatalf("recovered content = %q", got)
	}
}

func TestRemoveAllDetachesNamespaceAndGCReleasesRefs(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/tree/sub", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	removed := putTestBytes(t, store, "tenant-a", "tree/sub/blob", bytes.Repeat([]byte("x"), 512))
	store.metaMu.RLock()
	removedManifest := store.meta.Manifests[removed.ManifestID]
	removedChunks := map[string]bool{}
	for _, ref := range removedManifest.Chunks {
		removedChunks[ref.ChunkID] = true
	}
	store.metaMu.RUnlock()
	if err := store.RemoveAll("tenant-a/tree"); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	if _, err := store.OpenObject(testContext(t), "tenant-a", "tree/sub/blob"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("detached path should not exist, got %v", err)
	}
	if err := store.MkdirAll("tenant-a/tree/sub", 0o755); err != nil {
		t.Fatalf("recreate removed tree: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "tree/sub/new", []byte("new"))
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	for _, chunk := range store.meta.Chunks {
		if removedChunks[chunk.ChunkID] && chunk.RefCount != 0 {
			t.Fatalf("removed chunk refcount not released: %+v", chunk)
		}
	}
}

func TestSharedContentSurvivesDeleteAndGC(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/shared", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := bytes.Repeat([]byte("shared-content-"), 64)
	putTestBytes(t, store, "tenant-a", "shared/a", data)
	putTestBytes(t, store, "tenant-a", "shared/b", data)
	if err := store.DeleteObject(testContext(t), "tenant-a", "shared/a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if got := readTestBytes(t, store, "tenant-a", "shared/b"); !bytes.Equal(got, data) {
		t.Fatalf("shared content corrupted after gc")
	}
}

func TestRenameSamePathIsNoopAndCannotMoveIntoSelf(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/a/b", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "a/b/file", []byte("data"))
	if err := store.Rename("tenant-a/a/b/file", "tenant-a/a/b/file"); err != nil {
		t.Fatalf("same-path file rename: %v", err)
	}
	if got := readTestBytes(t, store, "tenant-a", "a/b/file"); string(got) != "data" {
		t.Fatalf("content after same-path rename = %q", got)
	}
	if err := store.Rename("tenant-a/a", "tenant-a/a/b/moved"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("move into self = %v, want invalid", err)
	}
}

type blockingReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	done    bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	if !r.done {
		<-r.release
		copy(p, "new")
		r.done = true
		return 3, nil
	}
	return 0, io.EOF
}

func TestDeleteTenantBasic(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := []byte("delete-tenant-test")
	putTestBytes(t, store, "tenant-a", "dir/file", data)

	if err := store.DeleteTenant(testContext(t), "tenant-a"); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}

	_, err := store.OpenObject(testContext(t), "tenant-a", "dir/file")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open after delete: %v, want ErrNotExist", err)
	}
}

func TestDeleteTenantIdempotent(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/sub", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "sub/x", []byte("idempotent"))
	if err := store.DeleteTenant(testContext(t), "tenant-a"); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := store.DeleteTenant(testContext(t), "tenant-a"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("second delete: %v, want ErrNotExist", err)
	}
}

func TestDeleteTenantNonexistent(t *testing.T) {
	store := openTestStore(t)
	if err := store.DeleteTenant(testContext(t), "nonexistent"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("delete nonexistent: %v, want ErrNotExist", err)
	}
}

func TestDeleteTenantGCleanup(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/data", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "data/blob", bytes.Repeat([]byte("X"), 512))
	putTestBytes(t, store, "tenant-a", "data/other", bytes.Repeat([]byte("Y"), 512))

	store.metaMu.RLock()
	preInodes := len(store.meta.Inodes)
	preChunks := len(store.meta.Chunks)
	preSegments := len(store.meta.Segments)
	store.metaMu.RUnlock()

	if err := store.DeleteTenant(testContext(t), "tenant-a"); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}

	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}

	store.metaMu.RLock()
	defer store.metaMu.RUnlock()
	postChunks := len(store.meta.Chunks)
	postSegments := len(store.meta.Segments)
	activeInodes := 0
	for _, inode := range store.meta.Inodes {
		if inode.State == fileStateActive {
			activeInodes++
		}
	}

	if activeInodes > 0 {
		t.Errorf("%d active inodes remain after GC, want 0", activeInodes)
	}
	t.Logf("pre: inodes=%d chunks=%d segs=%d | post: chunks=%d segs=%d", preInodes, preChunks, preSegments, postChunks, postSegments)
}

func TestDeleteTenantRecreatable(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/sub", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "sub/f", []byte("old"))
	if err := store.DeleteTenant(testContext(t), "tenant-a"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Recreate the same tenant
	if err := store.MkdirAll("tenant-a/newtree", 0o755); err != nil {
		t.Fatalf("mkdirall after delete: %v", err)
	}
	newData := []byte("new-tenant-data")
	putTestBytes(t, store, "tenant-a", "newtree/g", newData)

	got := readTestBytes(t, store, "tenant-a", "newtree/g")
	if !bytes.Equal(got, newData) {
		t.Fatalf("read after recreate = %q, want %q", got, newData)
	}
}

func TestDeleteTenantVFSError(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "dir/f", []byte("vfs-test"))
	if err := store.DeleteTenant(testContext(t), "tenant-a"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err := store.Open("tenant-a/dir/f")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("VFS open after delete: %v, want ErrNotExist", err)
	}
}

func TestDeleteTenantConcurrentRead(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := bytes.Repeat([]byte("concurrent"), 128)
	putTestBytes(t, store, "tenant-a", "dir/f", data)

	reader, err := store.OpenObject(testContext(t), "tenant-a", "dir/f")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer reader.Close()

	if err := store.DeleteTenant(testContext(t), "tenant-a"); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}

	// Existing reader should still work (pinned chunks)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read after delete: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch after concurrent delete")
	}

	// New open should fail
	_, err = store.OpenObject(testContext(t), "tenant-a", "dir/f")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("new open after delete: %v, want ErrNotExist", err)
	}
}

func TestDeleteTenantOnlyAffectsTargetTenant(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdirall a: %v", err)
	}
	if err := store.MkdirAll("tenant-b/dir", 0o755); err != nil {
		t.Fatalf("mkdirall b: %v", err)
	}
	dataA := []byte("tenant-a-data")
	dataB := []byte("tenant-b-data")
	putTestBytes(t, store, "tenant-a", "dir/f", dataA)
	putTestBytes(t, store, "tenant-b", "dir/f", dataB)

	if err := store.DeleteTenant(testContext(t), "tenant-a"); err != nil {
		t.Fatalf("delete tenant-a: %v", err)
	}

	// tenant-a should be gone
	_, err := store.OpenObject(testContext(t), "tenant-a", "dir/f")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("tenant-a open after delete: %v, want ErrNotExist", err)
	}

	// tenant-b should be intact
	got := readTestBytes(t, store, "tenant-b", "dir/f")
	if !bytes.Equal(got, dataB) {
		t.Fatalf("tenant-b data = %q, want %q", got, dataB)
	}

	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthOK {
		t.Fatalf("health state = %s, want OK", health.State)
	}
}
