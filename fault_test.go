package blobfs

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/afero"
)

var (
	errInjectedFSFault    = errors.New("injected filesystem fault")
	errInjectedCloseFault = errors.New("injected close fault")
)

type faultFS struct {
	afero.Fs
	mu             sync.Mutex
	writeSuffix    string
	writeFailures  int
	syncSuffix     string
	syncFailures   int
	renameContains string
	renameSkips    int
	renameFailures int
	removeContains string
	removeFailures int
	closeFaults    map[string]int
}

func (f *faultFS) failWritesTo(suffix string, count int) {
	f.mu.Lock()
	f.writeSuffix = filepath.ToSlash(suffix)
	f.writeFailures = count
	f.mu.Unlock()
}

func (f *faultFS) failSyncsTo(suffix string, count int) {
	f.mu.Lock()
	f.syncSuffix = filepath.ToSlash(suffix)
	f.syncFailures = count
	f.mu.Unlock()
}

func (f *faultFS) failRenamesContaining(fragment string, count int) {
	f.failRenamesContainingAfter(fragment, 0, count)
}

func (f *faultFS) failRenamesContainingAfter(fragment string, skip, count int) {
	f.mu.Lock()
	f.renameContains = filepath.ToSlash(fragment)
	f.renameSkips = skip
	f.renameFailures = count
	f.mu.Unlock()
}

func (f *faultFS) failRemovesContaining(fragment string, count int) {
	f.mu.Lock()
	f.removeContains = filepath.ToSlash(fragment)
	f.removeFailures = count
	f.mu.Unlock()
}

func (f *faultFS) failClosesTo(suffix string, count int) {
	f.mu.Lock()
	if f.closeFaults == nil {
		f.closeFaults = map[string]int{}
	}
	f.closeFaults[filepath.ToSlash(suffix)] = count
	f.mu.Unlock()
}

func (f *faultFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f, name: filepath.ToSlash(name)}, nil
}

func (f *faultFS) Rename(oldname, newname string) error {
	f.mu.Lock()
	matches := strings.Contains(filepath.ToSlash(oldname), f.renameContains) || strings.Contains(filepath.ToSlash(newname), f.renameContains)
	fail := false
	if matches {
		if f.renameSkips > 0 {
			f.renameSkips--
		} else if f.renameFailures > 0 {
			f.renameFailures--
			fail = true
		}
	}
	f.mu.Unlock()
	if fail {
		return errInjectedFSFault
	}
	return f.Fs.Rename(oldname, newname)
}

func (f *faultFS) Remove(name string) error {
	f.mu.Lock()
	fail := f.removeFailures > 0 && strings.Contains(filepath.ToSlash(name), f.removeContains)
	if fail {
		f.removeFailures--
	}
	f.mu.Unlock()
	if fail {
		return errInjectedFSFault
	}
	return f.Fs.Remove(name)
}

func (f *faultFS) consumeWriteFault(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeFailures > 0 && strings.HasSuffix(name, f.writeSuffix) {
		f.writeFailures--
		return true
	}
	return false
}

func (f *faultFS) consumeSyncFault(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.syncFailures > 0 && strings.HasSuffix(name, f.syncSuffix) {
		f.syncFailures--
		return true
	}
	return false
}

func (f *faultFS) consumeCloseFault(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for suffix, failures := range f.closeFaults {
		if failures > 0 && strings.HasSuffix(name, suffix) {
			f.closeFaults[suffix] = failures - 1
			return true
		}
	}
	return false
}

type faultFile struct {
	afero.File
	fs   *faultFS
	name string
}

func (f *faultFile) Write(p []byte) (int, error) {
	if f.fs.consumeWriteFault(f.name) {
		return 0, errInjectedFSFault
	}
	return f.File.Write(p)
}

func (f *faultFile) Sync() error {
	if f.fs.consumeSyncFault(f.name) {
		return errInjectedFSFault
	}
	return f.File.Sync()
}

func (f *faultFile) Close() error {
	if f.fs.consumeCloseFault(f.name) {
		return fmt.Errorf("close %s: %w", f.name, errInjectedCloseFault)
	}
	return f.File.Close()
}

func countRegularFiles(t *testing.T, fsys afero.Fs, root string) int {
	t.Helper()
	count := 0
	err := afero.Walk(fsys, root, func(_ string, info fs.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return err
		}
		count++
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return count
}

func simulateCrashWithoutCheckpoint(t *testing.T, store *Store) {
	t.Helper()
	if store.metaLog != nil {
		_ = store.metaLog.Close()
	}
	if store.lockFile != nil {
		_ = store.lockFile.Close()
	}
	if err := store.fs.Remove(store.lockPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("remove lock: %v", err)
	}
}

func TestStoreCloseJoinsMetadataLogLockAndRemoveErrors(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/close", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	nextLog := nextMetaLogName(store.metaLogName)
	fsys.failClosesTo(filepath.Join("meta", "txlog", nextLog), 1)
	fsys.failClosesTo(filepath.Join("meta", "LOCK"), 1)
	fsys.failRemovesContaining(filepath.Join("meta", "LOCK"), 1)

	err = store.Close()
	if !errors.Is(err, errInjectedCloseFault) {
		t.Fatalf("close error missing close fault: %v", err)
	}
	if !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("close error missing remove fault: %v", err)
	}
	errText := err.Error()
	if !strings.Contains(errText, nextLog) || !strings.Contains(errText, "LOCK") {
		t.Fatalf("close error did not retain individual paths: %v", err)
	}
	if _, statErr := fsys.Stat(filepath.Join("/blobfs", "meta", "LOCK")); statErr != nil {
		t.Fatalf("failed lock remove should leave lock for diagnosis: %v", statErr)
	}
}

func TestSystemFaultMetadataWriteFailureIsNotPublishedAfterReopen(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/faults", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	fsys.failWritesTo(filepath.Join("meta", "txlog", metaLogFile), 1)
	_, err = store.Put(testContext(t), "tenant-a", "faults/blob", bytes.NewReader([]byte("data")), nil)
	if !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("put with metadata write fault = %v, want injected fault", err)
	}
	if _, err := store.OpenObject(testContext(t), "tenant-a", "faults/blob"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("failed put became visible before reopen: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.OpenObject(testContext(t), "tenant-a", "faults/blob"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("failed put became visible after reopen: %v", err)
	}
	if files := countRegularFiles(t, fsys, "/blobfs/data/segments"); files != 0 {
		t.Fatalf("orphan segment files were not cleaned, count=%d", files)
	}
}

func TestSystemFaultMetadataSyncFailureCrashReplayKeepsDataReadable(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.MkdirAll("tenant-a/faults", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := []byte("ambiguous metadata sync")
	fsys.failSyncsTo(filepath.Join("meta", "txlog", metaLogFile), 1)
	_, err = store.Put(testContext(t), "tenant-a", "faults/blob", bytes.NewReader(data), nil)
	if !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("put with metadata sync fault = %v, want injected fault", err)
	}
	simulateCrashWithoutCheckpoint(t, store)
	reopened, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("reopen after simulated crash: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "faults/blob"); !bytes.Equal(got, data) {
		t.Fatalf("recovered data = %q", got)
	}
}

func TestSystemFaultSegmentRenameFailureCleansStaging(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/faults", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	fsys.failRenamesContaining(filepath.Join("data", "segments"), 1)
	_, err = store.Put(testContext(t), "tenant-a", "faults/blob", bytes.NewReader(bytes.Repeat([]byte("x"), 128)), nil)
	if !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("put with segment rename fault = %v, want injected fault", err)
	}
	if _, err := store.OpenObject(testContext(t), "tenant-a", "faults/blob"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("rename-failed put became visible: %v", err)
	}
	if files := countRegularFiles(t, fsys, "/blobfs/data/staging"); files != 0 {
		t.Fatalf("staging files were not cleaned, count=%d", files)
	}
	if files := countRegularFiles(t, fsys, "/blobfs/data/segments"); files != 0 {
		t.Fatalf("segment files were published despite rename failure, count=%d", files)
	}
}

func TestSystemFaultSegmentHeaderWriteFailureCleansStaging(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/faults", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	fsys.failWritesTo(".blob", 1)
	_, err = store.Put(testContext(t), "tenant-a", "faults/blob", bytes.NewReader([]byte("payload")), nil)
	if !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("put with segment header fault = %v, want injected fault", err)
	}
	if files := countRegularFiles(t, fsys, "/blobfs/data/staging"); files != 0 {
		t.Fatalf("staging files were not cleaned, count=%d", files)
	}
	if files := countRegularFiles(t, fsys, "/blobfs/data/segments"); files != 0 {
		t.Fatalf("segment files were published despite header failure, count=%d", files)
	}
}

func TestSystemFaultLaterSegmentRenameFailureRollsBackPublishedSegments(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	cfg := testConfig()
	cfg.SegmentSize = 160
	store, err := OpenFS(fsys, "/blobfs", cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/faults", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	fsys.failRenamesContainingAfter(filepath.Join("data", "segments"), 1, 1)
	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i*31 + i/7)
	}
	_, err = store.Put(testContext(t), "tenant-a", "faults/blob", bytes.NewReader(data), nil)
	if !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("put with later segment rename fault = %v, want injected fault", err)
	}
	if _, err := store.OpenObject(testContext(t), "tenant-a", "faults/blob"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("rename-failed put became visible: %v", err)
	}
	if files := countRegularFiles(t, fsys, "/blobfs/data/staging"); files != 0 {
		t.Fatalf("staging files were not cleaned, count=%d", files)
	}
	if files := countRegularFiles(t, fsys, "/blobfs/data/segments"); files != 0 {
		t.Fatalf("published segment files were not rolled back, count=%d", files)
	}
}

func TestSystemFaultPreparedSegmentCleanupErrorIsReturned(t *testing.T) {
	fsys := &faultFS{Fs: afero.NewMemMapFs()}
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	fsys.failRemovesContaining(filepath.Join("data", "segments"), 1)
	_, err = store.Put(testContext(t), "tenant-a", "missing-parent/blob", bytes.NewReader(bytes.Repeat([]byte("x"), 128)), nil)
	if err == nil {
		t.Fatal("put into missing parent should fail")
	}
	if !errors.Is(err, errInjectedFSFault) {
		t.Fatalf("cleanup remove fault was not returned: %v", err)
	}
	if _, openErr := store.OpenObject(testContext(t), "tenant-a", "missing-parent/blob"); !errors.Is(openErr, fs.ErrNotExist) {
		t.Fatalf("failed put became visible: %v", openErr)
	}
}
