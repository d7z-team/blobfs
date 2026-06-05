package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func TestPublicAPINilContextAndErrorCases(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/api", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "api/blob", []byte("payload"))

	var nilCtx context.Context
	assertNilContext := func(name string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "context is nil") {
			t.Fatalf("%s error = %v, want context is nil", name, err)
		}
	}
	_, err := store.Put(nilCtx, "tenant-a", "api/other", bytes.NewReader(nil), nil)
	assertNilContext("Put", err)
	_, err = store.OpenObject(nilCtx, "tenant-a", "api/blob")
	assertNilContext("OpenObject", err)
	_, err = store.OpenRange(nilCtx, "tenant-a", "api/blob", 0, 1)
	assertNilContext("OpenRange", err)
	_, err = store.StatObject(nilCtx, "tenant-a", "api/blob")
	assertNilContext("StatObject", err)
	_, err = store.UpdateMetadata(nilCtx, "tenant-a", "api/blob", nil)
	assertNilContext("UpdateMetadata", err)
	assertNilContext("DeleteObject", store.DeleteObject(nilCtx, "tenant-a", "api/blob"))
	_, err = store.CheckObject(nilCtx, "tenant-a", "api/blob")
	assertNilContext("CheckObject", err)
	_, err = store.Scrub(nilCtx, ScrubOptions{})
	assertNilContext("Scrub", err)
	_, err = store.RunGC(nilCtx, GCOptions{})
	assertNilContext("RunGC", err)
	assertNilContext("StartBackground", store.StartBackground(nilCtx))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.OpenObject(ctx, "tenant-a", "api/blob"); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenObject canceled context = %v", err)
	}
	if _, err := store.Put(testContext(t), "tenant-a", "missing-parent/blob", bytes.NewReader(nil), nil); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Put missing parent = %v, want not exist", err)
	}
	if _, err := store.OpenRange(testContext(t), "tenant-a", "api/blob", -1, 1); err == nil {
		t.Fatal("OpenRange negative offset should fail")
	}
	if _, err := store.StatObject(testContext(t), "tenant-a", "api"); !errors.Is(err, ErrIsDir) {
		t.Fatalf("StatObject directory = %v, want ErrIsDir", err)
	}
	if _, err := store.UpdateMetadata(testContext(t), "tenant-a", "api", nil); !errors.Is(err, ErrIsDir) {
		t.Fatalf("UpdateMetadata directory = %v, want ErrIsDir", err)
	}
	if err := store.DeleteObject(testContext(t), "tenant-a", "api"); !errors.Is(err, ErrIsDir) {
		t.Fatalf("DeleteObject directory = %v, want ErrIsDir", err)
	}
	if _, err := OpenFS(nil, "/blobfs", testConfig()); err == nil {
		t.Fatal("OpenFS nil filesystem should fail")
	}
}

func TestHotObjectReaderAndVFSFileBoundaries(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/hot", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), 8)
	putTestBytes(t, store, "tenant-a", "hot/blob", data)

	reader, err := store.OpenObject(testContext(t), "tenant-a", "hot/blob")
	if err != nil {
		t.Fatalf("open object: %v", err)
	}
	if n, err := reader.Read(nil); n != 0 || err != nil {
		t.Fatalf("zero read = %d, %v", n, err)
	}
	if pos, err := reader.Seek(-4, io.SeekEnd); err != nil || pos != int64(len(data)-4) {
		t.Fatalf("seek from end = %d, %v", pos, err)
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read tail: %v", err)
	}
	if string(tail) != "cdef" {
		t.Fatalf("tail = %q", tail)
	}
	if pos, err := reader.Seek(int64(len(data)+100), io.SeekStart); err != nil || pos != int64(len(data)) {
		t.Fatalf("seek past end = %d, %v", pos, err)
	}
	if _, err := reader.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative reader seek should fail")
	}
	if _, err := reader.Seek(0, 99); err == nil {
		t.Fatal("invalid reader seek whence should fail")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	if _, err := reader.Read(make([]byte, 1)); err == nil {
		t.Fatal("closed reader read should fail")
	}
	if _, err := reader.Seek(0, io.SeekStart); err == nil {
		t.Fatal("closed reader seek should fail")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second reader close: %v", err)
	}

	rangeReader, err := store.OpenRange(testContext(t), "tenant-a", "hot/blob", int64(len(data)-3), 99)
	if err != nil {
		t.Fatalf("open clamped range: %v", err)
	}
	rangeTail, err := io.ReadAll(rangeReader)
	_ = rangeReader.Close()
	if err != nil {
		t.Fatalf("read clamped range: %v", err)
	}
	if string(rangeTail) != "def" {
		t.Fatalf("clamped range = %q", rangeTail)
	}

	file, err := store.OpenFile("tenant-a/hot/vfs.txt", os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	vfsFile := file.(*blobVFSFile)
	if _, err := vfsFile.WriteString("abc"); err != nil {
		t.Fatalf("write string: %v", err)
	}
	if _, err := vfsFile.WriteAt([]byte("Z"), 1); err != nil {
		t.Fatalf("write at: %v", err)
	}
	if _, err := vfsFile.ReadAt(make([]byte, 1), -1); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("negative read at = %v", err)
	}
	if _, err := vfsFile.WriteAt([]byte("x"), -1); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("negative write at = %v", err)
	}
	if _, err := vfsFile.Seek(-1, io.SeekStart); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("negative file seek = %v", err)
	}
	if _, err := vfsFile.Seek(0, 99); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("invalid file seek = %v", err)
	}
	if err := vfsFile.Truncate(store.cfg.MaxFileSize + 1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize truncate = %v", err)
	}
	if err := vfsFile.Truncate(2); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := vfsFile.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := vfsFile.Close(); err != nil {
		t.Fatalf("close vfs file: %v", err)
	}
	if _, err := vfsFile.Read(make([]byte, 1)); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("closed read = %v", err)
	}
	if _, err := vfsFile.Write([]byte("x")); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("closed write = %v", err)
	}
	if _, err := vfsFile.Seek(0, io.SeekStart); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("closed seek = %v", err)
	}
	if _, err := vfsFile.Stat(); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("closed stat = %v", err)
	}

	appendFile, err := store.OpenFile("tenant-a/hot/vfs.txt", os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := appendFile.Write([]byte("!")); err != nil {
		_ = appendFile.Close()
		t.Fatalf("append write: %v", err)
	}
	if _, err := appendFile.Seek(0, io.SeekStart); err != nil {
		_ = appendFile.Close()
		t.Fatalf("append seek: %v", err)
	}
	if _, err := appendFile.Write([]byte("?")); err != nil {
		_ = appendFile.Close()
		t.Fatalf("append write after seek: %v", err)
	}
	if err := appendFile.Close(); err != nil {
		t.Fatalf("close append: %v", err)
	}
	if got := readTestBytes(t, store, "tenant-a", "hot/vfs.txt"); string(got) != "aZ!?" {
		t.Fatalf("append content = %q", got)
	}
	readFile, err := store.OpenFile("tenant-a/hot/vfs.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open read file: %v", err)
	}
	if n, err := readFile.Read(nil); n != 0 || err != nil {
		_ = readFile.Close()
		t.Fatalf("zero vfs read = %d, %v", n, err)
	}
	if n, err := readFile.ReadAt(nil, 99); n != 0 || err != nil {
		_ = readFile.Close()
		t.Fatalf("zero vfs readat past eof = %d, %v", n, err)
	}
	_ = readFile.Close()

	dir, err := store.Open("tenant-a/hot")
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	if stat, err := dir.Stat(); err != nil || !stat.IsDir() {
		_ = dir.Close()
		t.Fatalf("dir stat = %+v, %v", stat, err)
	}
	if entries, err := dir.Readdir(1); err != nil || len(entries) != 1 {
		_ = dir.Close()
		t.Fatalf("readdir first = %+v, %v", entries, err)
	}
	if entries, err := dir.Readdir(10); err != nil || len(entries) != 1 {
		_ = dir.Close()
		t.Fatalf("readdir rest = %+v, %v", entries, err)
	}
	if _, err := dir.Readdir(1); !errors.Is(err, io.EOF) {
		_ = dir.Close()
		t.Fatalf("readdir eof = %v", err)
	}
	if err := dir.Close(); err != nil {
		t.Fatalf("close dir: %v", err)
	}
	if _, err := dir.Readdir(1); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("closed dir readdir = %v", err)
	}
}

func TestVFSTenantRootExistsAndKeepsMetadata(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.Open("tenant-missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open missing tenant root = %v, want not exist", err)
	}
	if _, err := store.Stat("tenant-missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat missing tenant root = %v, want not exist", err)
	}
	if _, err := store.TenantFS("tenant-missing").Open("."); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("tenant fs missing root = %v, want not exist", err)
	}
	if err := store.Mkdir("tenant-root", 0o750); err != nil {
		t.Fatalf("mkdir tenant root: %v", err)
	}
	if err := store.Mkdir("tenant-root", 0o750); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("mkdir existing tenant root = %v, want exist", err)
	}
	mtime := time.Unix(456, 0)
	atime := time.Unix(123, 0)
	if err := store.Chmod("tenant-root", 0o700); err != nil {
		t.Fatalf("chmod tenant root: %v", err)
	}
	if err := store.Chtimes("tenant-root", atime, mtime); err != nil {
		t.Fatalf("chtimes tenant root: %v", err)
	}
	stat, err := store.Stat("tenant-root")
	if err != nil {
		t.Fatalf("stat tenant root: %v", err)
	}
	if stat.Name() != "tenant-root" || !stat.IsDir() || stat.Mode().Perm() != 0o700 || !stat.ModTime().Equal(mtime) {
		t.Fatalf("bad tenant root stat: name=%q dir=%v mode=%v mtime=%v", stat.Name(), stat.IsDir(), stat.Mode(), stat.ModTime())
	}
	store.metaMu.RLock()
	root := store.meta.Inodes[store.meta.Tenants["tenant-root"]]
	store.metaMu.RUnlock()
	if root == nil || root.ATime != atime.UnixNano() {
		t.Fatalf("tenant root atime not stored: %+v", root)
	}
}

func TestPublicAPIOpenFSReopenSmoke(t *testing.T) {
	fsys := afero.NewMemMapFs()
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open fs: %v", err)
	}
	if err := store.MkdirAll("tenant-a/smoke/nested", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	input := bytes.Repeat([]byte("smoke-"), 32)
	options := map[string]string{"kind": "smoke"}
	put, err := store.Put(testContext(t), "tenant-a", "smoke/nested/blob", bytes.NewReader(input), options)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	options["kind"] = "mutated"
	info, err := store.StatObject(testContext(t), "tenant-a", "smoke/nested/blob")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.ManifestID != put.ManifestID || info.Options["kind"] != "smoke" {
		t.Fatalf("stat info = %+v", info)
	}
	if err := afero.WriteFile(store, "tenant-a/smoke/vfs.txt", []byte("via-vfs"), 0o644); err != nil {
		t.Fatalf("afero write file: %v", err)
	}
	health, err := store.Health(testContext(t))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.State != HealthOK || !health.Readable || !health.Writable {
		t.Fatalf("bad health: %+v", health)
	}
	stats, err := store.Stats(testContext(t))
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Objects != 2 || stats.Tenants != 1 {
		t.Fatalf("bad stats: %+v", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("reopen fs: %v", err)
	}
	defer reopened.Close()
	if got := readTestBytes(t, reopened, "tenant-a", "smoke/nested/blob"); !bytes.Equal(got, input) {
		t.Fatalf("reopened object mismatch")
	}
	if got, err := afero.ReadFile(reopened, "tenant-a/smoke/vfs.txt"); err != nil || string(got) != "via-vfs" {
		t.Fatalf("reopened vfs read = %q, %v", got, err)
	}
	diagnose, err := reopened.Diagnose(testContext(t), DiagnoseOptions{CheckFiles: true, CheckOrphans: true, CheckStaging: true})
	if err != nil {
		t.Fatalf("diagnose: %v", err)
	}
	if !diagnose.Healthy {
		t.Fatalf("diagnose issues: %+v", diagnose.Issues)
	}
}
