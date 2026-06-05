package blobfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"testing"
	"time"
)

func TestScrubReportsCyclicInodeParent(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/cycle/a/b", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "cycle/a/b/blob", bytes.Repeat([]byte("cycle"), 32))

	store.metaMu.Lock()
	a, err := store.resolvePathLocked("tenant-a", "cycle/a")
	if err != nil {
		store.metaMu.Unlock()
		t.Fatalf("resolve a: %v", err)
	}
	b, err := store.resolvePathLocked("tenant-a", "cycle/a/b")
	if err != nil {
		store.metaMu.Unlock()
		t.Fatalf("resolve b: %v", err)
	}
	nextA := cloneInode(a)
	nextA.ParentInode = b.InodeID
	nextA.Generation++
	if err := store.commitMetaLocked([]metaOp{{Type: "put_inode", Inode: nextA}}); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("commit cycle: %v", err)
	}
	store.metaMu.Unlock()

	result, err := store.Scrub(testContext(t), ScrubOptions{CheckFiles: true})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("scrub cyclic parent error = %v, result=%+v", err, result)
	}
	if !hasCheckIssue(result, "inode_parent_invalid") {
		t.Fatalf("scrub did not report parent cycle: %+v", result)
	}
}

func TestObjectReaderSeekAcrossManyChunks(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/read", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i*17 + i/3)
	}
	putTestBytes(t, store, "tenant-a", "read/blob", data)
	reader, err := store.OpenObject(testContext(t), "tenant-a", "read/blob")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer reader.Close()

	for _, offset := range []int64{0, 17, 255, 1024, int64(len(data) - 11)} {
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			t.Fatalf("seek %d: %v", offset, err)
		}
		buf := make([]byte, 11)
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("read at %d: %v", offset, err)
		}
		if !bytes.Equal(buf[:n], data[offset:int(offset)+n]) {
			t.Fatalf("read at %d returned wrong bytes", offset)
		}
	}
}

func TestScrubReportsAffectedFilesForCorruptChunk(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/corrupt", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "corrupt/blob", bytes.Repeat([]byte("x"), 128))
	corruptFirstChunkPayloadByte(t, store, "tenant-a", "corrupt/blob")

	result, err := store.Scrub(testContext(t), ScrubOptions{})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("scrub corrupt chunk err = %v, result=%+v", err, result)
	}
	if !hasCheckIssue(result, "chunk_read_failed") && !hasCheckIssue(result, "segment_read_failed") {
		t.Fatalf("scrub did not report chunk read issue: %+v", result)
	}
	if !hasAffectedFile(result, "tenant-a/corrupt/blob") {
		t.Fatalf("scrub did not report affected file: %+v", result)
	}
}

func TestRenameRejectsCyclicDestinationParent(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/cycle/a", 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := store.MkdirAll("tenant-a/cycle/b/c", 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	store.metaMu.Lock()
	b, err := store.resolvePathLocked("tenant-a", "cycle/b")
	if err != nil {
		store.metaMu.Unlock()
		t.Fatalf("resolve b: %v", err)
	}
	c, err := store.resolvePathLocked("tenant-a", "cycle/b/c")
	if err != nil {
		store.metaMu.Unlock()
		t.Fatalf("resolve c: %v", err)
	}
	nextB := cloneInode(b)
	nextB.ParentInode = c.InodeID
	nextB.Generation++
	if err := store.commitMetaLocked([]metaOp{{Type: "put_inode", Inode: nextB}}); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("commit cycle: %v", err)
	}
	store.metaMu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- store.Rename("tenant-a/cycle/a", "tenant-a/cycle/b/c/a")
	}()
	select {
	case err := <-done:
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("rename with cyclic destination parent = %v, want invalid", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rename hung on cyclic destination parent")
	}
}

func hasCheckIssue(result *ScrubResult, kind string) bool {
	if result == nil {
		return false
	}
	for _, issue := range result.Issues {
		if issue.Kind == kind {
			return true
		}
	}
	return false
}

func hasAffectedFile(result *ScrubResult, path string) bool {
	if result == nil {
		return false
	}
	for _, affected := range result.AffectedFiles {
		if affected == path {
			return true
		}
	}
	return false
}
