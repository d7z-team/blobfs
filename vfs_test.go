package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/afero"
)

func TestAferoFilesystemUsesExplicitDirsAndMutatingOperations(t *testing.T) {
	store := openTestStore(t)
	objectData := []byte("object-created-without-vfs-dir")
	if _, err := store.Put(context.Background(), "tenant-a", "docs/object.txt", bytes.NewReader(objectData), nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("put below missing explicit dir should fail, got %v", err)
	}
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	putBytes(t, store, "tenant-a", "docs/object.txt", objectData)

	tenantEntries, err := afero.ReadDir(store, "tenant-a")
	if err != nil {
		t.Fatalf("read tenant root: %v", err)
	}
	if len(tenantEntries) != 1 || tenantEntries[0].Name() != "docs" || !tenantEntries[0].IsDir() {
		t.Fatalf("tenant entries should expose docs dir: %+v", tenantEntries)
	}
	if got, err := afero.ReadFile(store, "tenant-a/docs/object.txt"); err != nil || !bytes.Equal(got, objectData) {
		t.Fatalf("read object from explicit dir = %q, %v", got, err)
	}
	if err := afero.WriteFile(store, "tenant-a/docs/new.txt", []byte("new-through-vfs"), 0o600); err != nil {
		t.Fatalf("write under explicit dir: %v", err)
	}
	if err := store.Remove("tenant-a/docs"); err == nil {
		t.Fatalf("remove non-empty explicit dir should fail")
	}

	if err := store.Rename("tenant-a/docs", "tenant-a/archive"); err != nil {
		t.Fatalf("rename explicit dir: %v", err)
	}
	if _, err := store.Stat("tenant-a/docs"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source dir should be inactive, got %v", err)
	}
	if got := readBytes(t, store, "tenant-a", "archive/object.txt"); !bytes.Equal(got, objectData) {
		t.Fatalf("renamed object data mismatch")
	}
	if got, err := afero.ReadFile(store, "tenant-a/archive/new.txt"); err != nil || !bytes.Equal(got, []byte("new-through-vfs")) {
		t.Fatalf("renamed vfs-created file = %q, %v", got, err)
	}

	if err := store.RemoveAll("tenant-a/archive"); err != nil {
		t.Fatalf("remove all archive: %v", err)
	}
	if _, err := store.OpenObject(context.Background(), "tenant-a", "archive/object.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed object should be inactive, got %v", err)
	}
	if _, err := store.Stat("tenant-a/archive"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed dir should be inactive, got %v", err)
	}
}

func TestAferoFilesystemStoresDirectoryMetadataAndRejectsWritingFileOverDirectory(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o750); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	if err := store.Chmod("tenant-a/dir", 0o700); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	info, err := store.Stat("tenant-a/dir")
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("dir metadata = mode:%v isDir:%t", info.Mode(), info.IsDir())
	}
	dir, err := store.Open("tenant-a/dir")
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	openedInfo, err := dir.Stat()
	if closeErr := dir.Close(); closeErr != nil {
		t.Fatalf("close dir: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("stat opened dir: %v", err)
	}
	if !openedInfo.IsDir() || openedInfo.Mode().Perm() != 0o700 {
		t.Fatalf("opened dir metadata = mode:%v isDir:%t", openedInfo.Mode(), openedInfo.IsDir())
	}
	if _, err := store.Put(context.Background(), "tenant-a", "dir", bytes.NewReader([]byte("file")), nil); !errors.Is(err, ErrIsDir) {
		t.Fatalf("object put should not overwrite active dir, got %v", err)
	}
}

func TestAferoFilesystemRejectsFileDirectoryPathCollisions(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	putBytes(t, store, "tenant-a", "docs/object.txt", []byte("object"))
	if _, err := store.Put(context.Background(), "tenant-a", "docs", bytes.NewReader([]byte("directory-collision")), nil); !errors.Is(err, ErrIsDir) {
		t.Fatalf("object put should not overwrite explicit dir, got %v", err)
	}

	putBytes(t, store, "tenant-a", "plain", []byte("file"))
	if _, err := store.Put(context.Background(), "tenant-a", "plain/child.txt", bytes.NewReader([]byte("child")), nil); !errors.Is(err, ErrNotDir) {
		t.Fatalf("object put should not create child below active file, got %v", err)
	}
	if err := afero.WriteFile(store, "tenant-a/plain/child.txt", []byte("child"), 0o644); !errors.Is(err, ErrNotDir) {
		t.Fatalf("vfs write should not create child below active file, got %v", err)
	}

	if err := store.MkdirAll("tenant-a/archive", 0o755); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	putBytes(t, store, "tenant-a", "archive/existing.txt", []byte("existing"))
	if err := store.Rename("tenant-a/docs", "tenant-a/archive"); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("rename to non-empty target dir should fail, got %v", err)
	}
}

func TestAferoFileHandleOperationsAndDirectoryIteration(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/work", 0o755); err != nil {
		t.Fatalf("mkdir work: %v", err)
	}
	file, err := store.OpenFile("tenant-a/work/log.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o755)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err = file.WriteString("abcdef"); err != nil {
		t.Fatalf("write string: %v", err)
	}
	if _, err = file.Seek(3, io.SeekStart); err != nil {
		t.Fatalf("seek log: %v", err)
	}
	buf := make([]byte, 3)
	if n, err := file.Read(buf); n != 3 || err != nil || string(buf) != "def" {
		t.Fatalf("read after seek = n:%d data:%q err:%v", n, buf, err)
	}
	if _, err = file.ReadAt(buf, 1); err != nil || string(buf) != "bcd" {
		t.Fatalf("readat log = %q, %v", buf, err)
	}
	if _, err = file.Seek(-1, io.SeekStart); err == nil {
		t.Fatalf("negative vfs seek should fail")
	}
	if err = file.Truncate(3); err != nil {
		t.Fatalf("truncate log: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}
	info, err := store.Stat("tenant-a/work/log.txt")
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if info.Mode().Perm()&0o111 != 0 {
		t.Fatalf("regular vfs file should not keep executable bits: %v", info.Mode())
	}
	if _, err = file.Read(buf); !errors.Is(err, afero.ErrFileClosed) {
		t.Fatalf("read after close should fail with closed error, got %v", err)
	}

	if _, err = store.OpenFile("tenant-a/work/log.txt", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640); !errors.Is(err, os.ErrExist) {
		t.Fatalf("exclusive create should fail for existing file, got %v", err)
	}
	appender, err := store.OpenFile("tenant-a/work/log.txt", os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err = appender.Write([]byte("XYZ")); err != nil {
		t.Fatalf("append log: %v", err)
	}
	if err = appender.Close(); err != nil {
		t.Fatalf("close appender: %v", err)
	}
	if got, err := afero.ReadFile(store, "tenant-a/work/log.txt"); err != nil || string(got) != "abcXYZ" {
		t.Fatalf("appended log = %q, %v", got, err)
	}

	readOnly, err := store.Open("tenant-a/work/log.txt")
	if err != nil {
		t.Fatalf("open read-only log: %v", err)
	}
	if _, err = readOnly.Write([]byte("!")); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("write on read-only handle should fail with permission error, got %v", err)
	}
	if _, err = readOnly.ReadAt(buf, 99); !errors.Is(err, io.EOF) {
		t.Fatalf("readat past eof should return eof, got %v", err)
	}
	if err = readOnly.Close(); err != nil {
		t.Fatalf("close read-only log: %v", err)
	}

	if err = afero.WriteFile(store, "tenant-a/work/notes.txt", []byte("notes"), 0o644); err != nil {
		t.Fatalf("write notes: %v", err)
	}
	dir, err := store.Open("tenant-a/work")
	if err != nil {
		t.Fatalf("open work dir: %v", err)
	}
	first, err := dir.Readdir(1)
	if err != nil || len(first) != 1 {
		t.Fatalf("readdir first = %+v, %v", first, err)
	}
	restNames, err := dir.Readdirnames(-1)
	if err != nil {
		t.Fatalf("readdirnames rest: %v", err)
	}
	allNames := append([]string{first[0].Name()}, restNames...)
	sort.Strings(allNames)
	if strings.Join(allNames, ",") != "log.txt,notes.txt" {
		t.Fatalf("directory entries = %v", allNames)
	}
	if _, err = dir.Read(buf); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("reading directory as file should fail, got %v", err)
	}
	if err = dir.Close(); err != nil {
		t.Fatalf("close dir: %v", err)
	}
}

func TestAferoFileHandleConflictsWhenGenerationChanges(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/work", 0o755); err != nil {
		t.Fatalf("mkdir work: %v", err)
	}
	if err := afero.WriteFile(store, "tenant-a/work/conflict.txt", []byte("base"), 0o644); err != nil {
		t.Fatalf("write base file: %v", err)
	}

	first, err := store.OpenFile("tenant-a/work/conflict.txt", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open first handle: %v", err)
	}
	second, err := store.OpenFile("tenant-a/work/conflict.txt", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open second handle: %v", err)
	}
	if _, err = first.WriteAt([]byte("one"), 0); err != nil {
		t.Fatalf("write first handle: %v", err)
	}
	if err = first.Close(); err != nil {
		t.Fatalf("close first handle: %v", err)
	}
	if _, err = second.WriteAt([]byte("two"), 0); err != nil {
		t.Fatalf("write second handle: %v", err)
	}
	if err = second.Close(); !errors.Is(err, ErrConflict) {
		t.Fatalf("second close should detect generation conflict, got %v", err)
	}
	if got, err := afero.ReadFile(store, "tenant-a/work/conflict.txt"); err != nil || string(got) != "onee" {
		t.Fatalf("conflict should preserve first write, got %q err=%v", got, err)
	}
}

func TestAferoFileHandleRejectsOversizedWrites(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFileSize = 4
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/work", 0o755); err != nil {
		t.Fatalf("mkdir work: %v", err)
	}
	file, err := store.OpenFile("tenant-a/work/limited.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open limited file: %v", err)
	}
	if n, err := file.Write([]byte("12345")); n != 0 || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized write = n:%d err:%v", n, err)
	}
	if err := file.Truncate(5); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized truncate should fail, got %v", err)
	}
	if _, err := file.Write([]byte("1234")); err != nil {
		t.Fatalf("write max-sized file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close max-sized file: %v", err)
	}
	if got, err := afero.ReadFile(store, "tenant-a/work/limited.txt"); err != nil || string(got) != "1234" {
		t.Fatalf("limited file = %q, %v", got, err)
	}
}

func TestAferoFilesystemRenameOverwritesFilesAndRejectsCrossTenantMove(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := afero.WriteFile(store, "tenant-a/docs/source.txt", []byte("new"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := afero.WriteFile(store, "tenant-a/docs/target.txt", []byte("old"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := store.Rename("tenant-a/docs/source.txt", "tenant-a/docs/target.txt"); err != nil {
		t.Fatalf("rename over file: %v", err)
	}
	if _, err := store.Stat("tenant-a/docs/source.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source should be tombstoned after rename, got %v", err)
	}
	if got, err := afero.ReadFile(store, "tenant-a/docs/target.txt"); err != nil || string(got) != "new" {
		t.Fatalf("target should contain renamed data, got %q err=%v", got, err)
	}
	if err := store.Rename("tenant-a/docs/target.txt", "tenant-b/target.txt"); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("cross-tenant rename should be invalid, got %v", err)
	}
}

func TestTenantFSSupportsStandardIOFS(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := afero.WriteFile(store, "tenant-a/docs/readme.txt", []byte("hello fs"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}

	tenant := store.TenantFS("tenant-a")
	got, err := fs.ReadFile(tenant, "docs/readme.txt")
	if err != nil || string(got) != "hello fs" {
		t.Fatalf("fs.ReadFile = %q, %v", got, err)
	}
	info, err := fs.Stat(tenant, "docs/readme.txt")
	if err != nil || info.IsDir() || info.Size() != int64(len("hello fs")) {
		t.Fatalf("fs.Stat = %+v, %v", info, err)
	}
	entries, err := fs.ReadDir(tenant, "docs")
	if err != nil || len(entries) != 1 || entries[0].Name() != "readme.txt" || entries[0].IsDir() {
		t.Fatalf("fs.ReadDir = %+v, %v", entries, err)
	}
	if _, err := tenant.Open("../bad"); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("tenant fs should reject traversal, got %v", err)
	}
}

func TestAferoFilesystemCanAllowExecutableFilesWhenConfigured(t *testing.T) {
	cfg := testConfig()
	cfg.AllowExecutableFiles = true
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/bin", 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	if err := afero.WriteFile(store, "tenant-a/bin/tool", []byte("script"), 0o755); err != nil {
		t.Fatalf("write executable file: %v", err)
	}
	info, err := store.Stat("tenant-a/bin/tool")
	if err != nil {
		t.Fatalf("stat executable file: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("configured executable mode should be preserved: %v", info.Mode())
	}
}

func TestAferoFilesystemPathValidationAndEmptyDirectoryRemoval(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.Open("../bad"); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("traversal open should be invalid, got %v", err)
	}
	if _, err := store.Open("C:/bad"); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("drive path open should be invalid, got %v", err)
	}
	if err := store.Mkdir("tenant-a/missing/child", 0o755); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mkdir below missing parent should fail, got %v", err)
	}
	if err := store.Mkdir("tenant-a/empty", 0o750); err != nil {
		t.Fatalf("mkdir empty: %v", err)
	}
	store.mu.Lock()
	firstDirID := store.meta.Files[fileKey("tenant-a", "empty")].FileID
	store.mu.Unlock()
	if err := store.Mkdir("tenant-a/empty", 0o750); !errors.Is(err, os.ErrExist) {
		t.Fatalf("mkdir existing dir should fail, got %v", err)
	}
	if err := store.Remove("tenant-a/empty"); err != nil {
		t.Fatalf("remove empty dir: %v", err)
	}
	if _, err := store.Stat("tenant-a/empty"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed dir should be missing, got %v", err)
	}
	if err := store.Mkdir("tenant-a/empty", 0o750); err != nil {
		t.Fatalf("recreate empty dir: %v", err)
	}
	store.mu.Lock()
	secondDirID := store.meta.Files[fileKey("tenant-a", "empty")].FileID
	store.mu.Unlock()
	if firstDirID == secondDirID {
		t.Fatalf("recreated directory should get a fresh file id, reused %q", firstDirID)
	}
	if err := store.Remove("/"); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("remove root should be invalid, got %v", err)
	}
}

func TestAferoFilesystemRejectsRenamingDirectoryIntoItself(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs/sub", 0o755); err != nil {
		t.Fatalf("mkdir docs subtree: %v", err)
	}
	if err := store.Rename("tenant-a/docs", "tenant-a/docs/sub/moved"); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("rename directory into its own subtree should be invalid, got %v", err)
	}
	if _, err := store.Stat("tenant-a/docs/sub"); err != nil {
		t.Fatalf("original subtree should remain after failed rename: %v", err)
	}
}
