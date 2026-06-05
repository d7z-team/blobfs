package blobfs

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.LargeFileThreshold = 64
	cfg.SegmentSize = 4096
	cfg.Chunking = ChunkingConfig{
		Algorithm: "FastCDC",
		MinSize:   8,
		AvgSize:   16,
		MaxSize:   24,
	}
	cfg.GC.SafetyWindow = 0
	cfg.GC.SegmentDeleteDelay = 0
	cfg.GC.CandidateConfirmCycles = 2
	cfg.GC.CompactGarbageRatio = 0.25
	return cfg
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

func putBytes(t *testing.T, store *Store, tenantID, path string, data []byte) *PutResult {
	t.Helper()
	result, err := store.Put(context.Background(), tenantID, path, bytes.NewReader(data), map[string]string{"kind": "test"})
	if err != nil {
		t.Fatalf("put %s: %v", path, err)
	}
	return result
}

func readBytes(t *testing.T, store *Store, tenantID, path string) []byte {
	t.Helper()
	reader, err := store.OpenObject(context.Background(), tenantID, path)
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

func firstStoredChunk(t *testing.T, store *Store) (chunkRecord, segmentRecord) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, chunk := range store.meta.Chunks {
		if chunk.SegmentID == "" {
			continue
		}
		seg := store.meta.Segments[chunk.SegmentID]
		if seg != nil {
			return *chunk, *seg
		}
	}
	t.Fatalf("store has no stored chunks")
	return chunkRecord{}, segmentRecord{}
}

func countSegments(store *Store) int {
	count := 0
	for _, seg := range store.meta.Segments {
		if seg.State != segmentStateDeleted {
			count++
		}
	}
	return count
}

type failingReader struct {
	err error
}

func (r failingReader) Read([]byte) (int, error) {
	return 0, r.err
}
