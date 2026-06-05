package blobfs

import (
	"context"
	"io"
	"testing"
)

func TestObjectReaderSeekInfoAndCloseBehavior(t *testing.T) {
	store := openTestStore(t)
	data := []byte("0123456789")
	result := putBytes(t, store, "tenant-a", "seek", data)

	reader, err := store.OpenObject(context.Background(), "tenant-a", "seek")
	if err != nil {
		t.Fatalf("open seek file: %v", err)
	}
	defer reader.Close()
	info := reader.Info()
	if info.CreatedAt.IsZero() || info.UpdatedAt.IsZero() || info.FileHash != result.FileHash {
		t.Fatalf("reader info was not populated: %+v", info)
	}
	if reader.ETag() != result.FileHash {
		t.Fatalf("etag = %q, want %q", reader.ETag(), result.FileHash)
	}
	if offset, err := reader.Seek(-3, io.SeekEnd); err != nil || offset != 7 {
		t.Fatalf("seek from end = %d, %v", offset, err)
	}
	buf := make([]byte, 2)
	n, err := reader.Read(buf)
	if n != 2 || err != nil || string(buf) != "78" {
		t.Fatalf("read after seek = n:%d data:%q err:%v", n, buf, err)
	}
	if _, err := reader.Seek(-1, io.SeekStart); err == nil {
		t.Fatalf("negative seek should fail")
	}
	if _, err := reader.Seek(0, 99); err == nil {
		t.Fatalf("invalid seek whence should fail")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	if _, err := reader.Read(buf); err == nil {
		t.Fatalf("read after close should fail")
	}
	if _, err := reader.Seek(0, io.SeekStart); err == nil {
		t.Fatalf("seek after close should fail")
	}
}

func TestObjectReaderRejectsMissingSegmentMetadata(t *testing.T) {
	store := openTestStore(t)
	putBytes(t, store, "tenant-a", "missing-segment", []byte("missing segment"))
	chunk, _ := firstStoredChunk(t, store)

	store.mu.Lock()
	delete(store.meta.Segments, chunk.SegmentID)
	store.mu.Unlock()

	if _, err := store.OpenObject(context.Background(), "tenant-a", "missing-segment"); err == nil {
		t.Fatalf("open should reject missing segment metadata, got %v", err)
	}
}
