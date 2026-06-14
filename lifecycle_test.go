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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func TestCheckpointFailureKeepsMetadataLogUsable(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/checkpoint", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}

	store.metaMu.Lock()
	store.commitsSinceCheckpoint = metaCheckpointInterval - 1
	nextLog := nextMetaLogName(store.metaLogName)
	store.metaMu.Unlock()
	fsys.failSyncsTo(filepath.Join("meta", "txlog", nextLog), 1)

	if _, err := store.Put(testContext(t), "tenant-a", "checkpoint/first", bytes.NewReader([]byte("first")), nil); !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("put through checkpoint failure = %v, want injected fault", err)
	}
	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthDegraded {
		t.Fatalf("health state = %s, want DEGRADED", health.State)
	}
	if got := readTestBytes(t, store, "tenant-a", "checkpoint/first"); string(got) != "first" {
		t.Fatalf("first content after checkpoint failure = %q", got)
	}

	if _, err := store.Put(testContext(t), "tenant-a", "checkpoint/second", bytes.NewReader([]byte("second")), nil); err != nil {
		t.Fatalf("put after failed checkpoint should keep using old log: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "checkpoint/first"); string(got) != "first" {
		t.Fatalf("first content = %q", got)
	}
	if got := readTestBytes(t, reopened, "tenant-a", "checkpoint/second"); string(got) != "second" {
		t.Fatalf("second content = %q", got)
	}
}

func TestHealthConcurrentWithCheckpointRotation(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/health", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stop := make(chan struct{})
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := store.Health(ctx); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()
	for i := 0; i < metaCheckpointInterval*2+20; i++ {
		if _, err := store.Put(ctx, "tenant-a", "health/file-"+strconv.Itoa(i), bytes.NewReader([]byte("payload")), nil); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("put %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("health loop: %v", err)
	default:
	}
}

func TestObjectReaderConcurrentCloseKeepsOtherReaderPinned(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/readers", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := bytes.Repeat([]byte("reader-pin-"), 64)
	putTestBytes(t, store, "tenant-a", "readers/blob", data)
	first, err := store.OpenObject(testContext(t), "tenant-a", "readers/blob")
	if err != nil {
		t.Fatalf("open first: %v", err)
	}
	second, err := store.OpenObject(testContext(t), "tenant-a", "readers/blob")
	if err != nil {
		t.Fatalf("open second: %v", err)
	}
	defer second.Close()

	var wg sync.WaitGroup
	closeErrs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			closeErrs <- first.Close()
		}()
	}
	wg.Wait()
	close(closeErrs)
	for err := range closeErrs {
		if err != nil {
			t.Fatalf("close first: %v", err)
		}
	}

	if err := store.DeleteObject(testContext(t), "tenant-a", "readers/blob"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	got, err := io.ReadAll(second)
	if err != nil {
		t.Fatalf("read pinned second after gc: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("second reader content mismatch")
	}
}

func TestStoreCloseClosesOpenHandlesAndRejectsNewWork(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/close", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "close/blob", []byte("payload"))
	reader, err := store.OpenObject(testContext(t), "tenant-a", "close/blob")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	file, err := store.OpenFile("tenant-a/close/blob", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	dir, err := store.Open("tenant-a/close")
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store close: %v", err)
	}
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("reader after store close = %v, want ErrReaderClosed", err)
	}
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("vfs reader after store close = %v, want file closed", err)
	}
	if _, err := dir.Readdir(1); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("dir after store close = %v, want file closed", err)
	}
	if _, err := store.OpenObject(testContext(t), "tenant-a", "close/blob"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("open after close = %v, want os.ErrClosed", err)
	}
	if _, err := store.StatObject(testContext(t), "tenant-a", "close/blob"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("stat after close = %v, want os.ErrClosed", err)
	}
	if _, err := store.Stats(testContext(t)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("stats after close = %v, want os.ErrClosed", err)
	}
	if report, err := store.Health(testContext(t)); err != nil || report.State != HealthClosed {
		t.Fatalf("health after close = %+v, %v; want CLOSED", report, err)
	}
}

func TestStoreCloseCleansDirtyVFSWriteSession(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/close", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	file, err := store.OpenFile("tenant-a/close/dirty.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open dirty file: %v", err)
	}
	if _, err := file.Write([]byte("dirty")); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	if sessions := openWriteSessionCount(store); sessions != 1 {
		t.Fatalf("open write sessions before close = %d, want 1", sessions)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store close: %v", err)
	}
	if sessions := openWriteSessionCount(store); sessions != 0 {
		t.Fatalf("open write sessions after close = %d, want 0", sessions)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file close after store close: %v", err)
	}
}

func TestPutRewritesUnreferencedChunkInsteadOfReusingIt(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/reuse", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := []byte("same content")
	first := putTestBytes(t, store, "tenant-a", "reuse/old", data)
	store.metaMu.RLock()
	oldManifest := store.meta.Manifests[first.ManifestID]
	oldSegmentID := ""
	if oldManifest != nil && len(oldManifest.Chunks) == 1 {
		if oldChunk := store.meta.Chunks[oldManifest.Chunks[0].ChunkID]; oldChunk != nil {
			oldSegmentID = oldChunk.SegmentID
		}
	}
	store.metaMu.RUnlock()
	if oldSegmentID == "" {
		t.Fatal("old chunk segment is empty")
	}
	if err := store.DeleteObject(testContext(t), "tenant-a", "reuse/old"); err != nil {
		t.Fatalf("delete old: %v", err)
	}
	second := putTestBytes(t, store, "tenant-a", "reuse/new", data)
	if first.ManifestID != second.ManifestID {
		t.Fatalf("manifest changed for identical content: %s != %s", first.ManifestID, second.ManifestID)
	}
	store.metaMu.RLock()
	manifest := store.meta.Manifests[second.ManifestID]
	if manifest == nil || len(manifest.Chunks) != 1 {
		store.metaMu.RUnlock()
		t.Fatalf("manifest = %+v", manifest)
	}
	chunk := store.meta.Chunks[manifest.Chunks[0].ChunkID]
	segmentID := ""
	refCount := 0
	if chunk != nil {
		segmentID = chunk.SegmentID
		refCount = chunk.RefCount
	}
	store.metaMu.RUnlock()
	if refCount != 1 {
		t.Fatalf("chunk refcount = %d, want 1", refCount)
	}
	if oldSegmentID == segmentID {
		t.Fatalf("unreferenced chunk reused old segment %s", segmentID)
	}
}

func TestVFSFileConcurrentCloseCleansSessionOnce(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/vfs", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	file, err := store.OpenFile("tenant-a/vfs/concurrent.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	if _, err := file.Write([]byte("concurrent close")); err != nil {
		t.Fatalf("write: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- file.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	if sessions := openWriteSessionCount(store); sessions != 0 {
		t.Fatalf("open write sessions = %d, want 0", sessions)
	}
	if got := readTestBytes(t, store, "tenant-a", "vfs/concurrent.txt"); string(got) != "concurrent close" {
		t.Fatalf("content = %q", got)
	}
}

func TestVFSOpenFileContextCancellationCleansSession(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/vfs", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	file, err := store.OpenFileContext(ctx, "tenant-a/vfs/canceled.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open file context: %v", err)
	}
	if _, err := file.Write([]byte("not committed")); err != nil {
		t.Fatalf("write: %v", err)
	}
	cancel()
	if err := file.(*blobVFSFile).Sync(); !errors.Is(err, context.Canceled) {
		t.Fatalf("sync canceled = %v, want context.Canceled", err)
	}
	if err := file.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("close canceled = %v, want context.Canceled", err)
	}
	if sessions := openWriteSessionCount(store); sessions != 0 {
		t.Fatalf("open write sessions = %d, want 0", sessions)
	}
	if _, err := store.OpenObject(testContext(t), "tenant-a", "vfs/canceled.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("canceled VFS write became visible: %v", err)
	}
}

func TestCheckpointPrunesDeletedMetadataRecords(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/tombstones", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	for i := 0; i < 32; i++ {
		name := "tombstones/blob-" + strconv.Itoa(i)
		putTestBytes(t, store, "tenant-a", name, []byte("payload-"+strconv.Itoa(i)))
		if err := store.DeleteObject(testContext(t), "tenant-a", name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	reopened.metaMu.RLock()
	defer reopened.metaMu.RUnlock()
	for _, inode := range reopened.meta.Inodes {
		if inode.State == fileStateDeleted {
			t.Fatalf("deleted inode survived checkpoint compaction: %+v", inode)
		}
	}
	for _, manifest := range reopened.meta.Manifests {
		if manifest.State == manifestStateDeleted {
			t.Fatalf("deleted manifest survived checkpoint compaction: %+v", manifest)
		}
	}
	for _, chunk := range reopened.meta.Chunks {
		if chunk.State == chunkStateDeleted {
			t.Fatalf("deleted chunk survived checkpoint compaction: %+v", chunk)
		}
	}
	for _, segment := range reopened.meta.Segments {
		if segment.State == segmentStateDeleted {
			t.Fatalf("deleted segment survived checkpoint compaction: %+v", segment)
		}
	}
}

func TestScrubCheckFilesCompletesAfterCloseStarts(t *testing.T) {
	fsys := &blockingOpenFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/scrub", 0o755); err != nil {
		_ = store.Close()
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "scrub/blob", bytes.Repeat([]byte("scrub"), 32))

	fsys.blockOpensTo(".blob")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scrubDone := make(chan error, 1)
	go func() {
		_, err := store.Scrub(ctx, ScrubOptions{CheckFiles: true})
		scrubDone <- err
	}()
	select {
	case <-fsys.entered:
	case <-ctx.Done():
		_ = store.Close()
		t.Fatalf("scrub did not reach blocked segment read: %v", ctx.Err())
	}
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()
	fsys.releaseBlocked()
	if err := <-scrubDone; err != nil {
		t.Fatalf("scrub after close start: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestScrubCheckFilesKeepsOpenedSnapshotAfterDelete(t *testing.T) {
	fsys := &blockingOpenFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/scrub", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "scrub/blob", bytes.Repeat([]byte("snapshot"), 32))
	_, segmentPath := firstSegmentPath(t, store)
	fsys.blockOpensTo(filepath.Base(segmentPath))
	defer fsys.releaseBlocked()

	type scrubOutcome struct {
		result *ScrubResult
		err    error
	}
	done := make(chan scrubOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := store.Scrub(ctx, ScrubOptions{CheckFiles: true})
		done <- scrubOutcome{result: result, err: err}
	}()
	select {
	case <-fsys.entered:
	case <-time.After(time.Second):
		t.Fatal("scrub did not start segment read")
	}
	if err := store.DeleteObject(testContext(t), "tenant-a", "scrub/blob"); err != nil {
		t.Fatalf("delete during scrub: %v", err)
	}
	fsys.releaseBlocked()
	select {
	case outcome := <-done:
		if outcome.err != nil {
			t.Fatalf("scrub after delete: %v", outcome.err)
		}
		if outcome.result == nil || !outcome.result.Healthy || outcome.result.CheckedFiles != 1 {
			t.Fatalf("scrub did not use opened file snapshot: %+v", outcome.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scrub did not finish")
	}
}

func openWriteSessionCount(store *Store) int {
	store.writeSessionMu.Lock()
	defer store.writeSessionMu.Unlock()
	return store.openWriteSessions
}

type blockingOpenFS struct {
	afero.Fs
	mu      sync.Mutex
	suffix  string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingOpenFS) blockOpensTo(suffix string) {
	f.mu.Lock()
	f.suffix = suffix
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
	f.once = sync.Once{}
	f.mu.Unlock()
}

func (f *blockingOpenFS) releaseBlocked() {
	f.mu.Lock()
	release := f.release
	f.release = nil
	f.mu.Unlock()
	if release != nil {
		close(release)
	}
}

func (f *blockingOpenFS) Open(name string) (afero.File, error) {
	f.mu.Lock()
	block := f.suffix != "" && strings.HasSuffix(filepath.ToSlash(name), f.suffix) && f.release != nil
	entered := f.entered
	release := f.release
	f.mu.Unlock()
	if block {
		f.once.Do(func() { close(entered) })
		<-release
	}
	return f.Fs.Open(name)
}
