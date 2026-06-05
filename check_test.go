package blobfs

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestCheckObjectVerifiesHealthyObject(t *testing.T) {
	store := openTestStore(t)
	data := []byte("healthy-check-object")
	putBytes(t, store, "tenant-a", "healthy", data)

	result, err := store.CheckObject(context.Background(), "tenant-a", "healthy")
	if err != nil {
		t.Fatalf("check healthy object: %v", err)
	}
	if !result.Healthy || result.CheckedChunks == 0 || result.CheckedBytes != int64(len(data)) || len(result.Issues) != 0 {
		t.Fatalf("healthy check result mismatch: %+v", result)
	}
}

func TestCheckObjectDetectsFileHashMismatch(t *testing.T) {
	store := openTestStore(t)
	putBytes(t, store, "tenant-a", "bad-file-hash", []byte("file-hash-data"))

	store.mu.Lock()
	store.meta.Files[fileKey("tenant-a", "bad-file-hash")].FileHash = "not-the-content-hash"
	store.mu.Unlock()

	result, err := store.CheckObject(context.Background(), "tenant-a", "bad-file-hash")
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("check should report corruption, got %v", err)
	}
	if result == nil || result.Healthy || len(result.Issues) != 1 || result.Issues[0].Kind != "file_hash_mismatch" {
		t.Fatalf("file hash mismatch result = %+v", result)
	}
}

func TestCheckObjectMarksCorruptChunkAndSegment(t *testing.T) {
	store := openTestStore(t)
	putBytes(t, store, "tenant-a", "corrupt", []byte("corrupt-object-data"))
	chunk, seg := firstStoredChunk(t, store)

	file, err := store.fs.OpenFile(store.segmentPath(&seg), os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err = file.WriteAt([]byte{0xff}, chunk.SegmentOffset+recordHeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt payload: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	result, err := store.CheckObject(context.Background(), "tenant-a", "corrupt")
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("check should report corruption, got %v", err)
	}
	if result == nil || result.Healthy || len(result.Issues) == 0 {
		t.Fatalf("corrupt check result mismatch: %+v", result)
	}
	store.mu.Lock()
	if store.meta.Chunks[chunk.ChunkID].State != chunkStateCorrupt || store.meta.Segments[seg.SegmentID].State != segmentStateCorrupt {
		t.Fatalf("check should mark corrupt chunk/segment")
	}
	store.mu.Unlock()
}
