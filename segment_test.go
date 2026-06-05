package blobfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRecordHeaderRejectsInvalidRecords(t *testing.T) {
	if _, _, _, _, _, _, err := parseRecordHeader(make([]byte, recordHeaderSize-1)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short header should return unexpected EOF, got %v", err)
	}

	header := makeRecordHeader("chunk", 3, 2, 1)
	header[0] = 0
	if _, _, _, _, _, _, err := parseRecordHeader(header); err == nil {
		t.Fatalf("bad magic should fail")
	}

	header = makeRecordHeader("chunk", 3, 2, 1)
	binary.LittleEndian.PutUint16(header[4:6], recordVersion+1)
	if _, _, _, _, _, _, err := parseRecordHeader(header); err == nil {
		t.Fatalf("bad version should fail")
	}

	header = makeRecordHeader("chunk", 3, 2, 1)
	binary.LittleEndian.PutUint64(header[96:104], 1)
	if _, _, _, _, _, _, err := parseRecordHeader(header); err == nil {
		t.Fatalf("payload length mismatch should fail")
	}
}

func TestPutRotatesSegmentsWhenCurrentSegmentIsFull(t *testing.T) {
	cfg := testConfig()
	cfg.SegmentSize = 160
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	putBytes(t, store, "tenant-a", "rotate-a", []byte("rotate-a"))
	putBytes(t, store, "tenant-a", "rotate-b", []byte("rotate-b"))

	store.mu.Lock()
	defer store.mu.Unlock()
	openSegments := 0
	sealedSegments := 0
	for _, seg := range store.meta.Segments {
		switch seg.State {
		case segmentStateOpen:
			openSegments++
		case segmentStateSealed:
			sealedSegments++
		}
	}
	if openSegments != 1 || sealedSegments != 1 {
		t.Fatalf("expected one open and one sealed segment, got open=%d sealed=%d", openSegments, sealedSegments)
	}
}

func TestPutUsesTwoLevelSegmentPathWithoutSegFilenamePrefix(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	putBytes(t, store, "tenant-a", "small", []byte("small-data"))

	store.mu.Lock()
	segments := make([]segmentRecord, 0, len(store.meta.Segments))
	for _, seg := range store.meta.Segments {
		segments = append(segments, *seg)
	}
	store.mu.Unlock()
	if len(segments) != 1 {
		t.Fatalf("expected one segment, got %+v", segments)
	}
	for _, seg := range segments {
		name := filepath.Base(seg.RelativePath)
		if strings.Contains(name, "seg-") {
			t.Fatalf("segment filename should not include seg- prefix: %q", name)
		}
		if strings.Contains(seg.SegmentID, "seg-") {
			t.Fatalf("segment id should not include seg- prefix: %q", seg.SegmentID)
		}
		if filepath.Ext(name) != ".blob" {
			t.Fatalf("segment filename should use .blob extension: %q", name)
		}
		if name != seg.SegmentID+".blob" {
			t.Fatalf("segment filename should match segment id: id=%q name=%q", seg.SegmentID, name)
		}
		parts := strings.Split(filepath.ToSlash(seg.RelativePath), "/")
		if len(parts) != 3 {
			t.Fatalf("segment path should use fixed two-level fanout: %q", seg.RelativePath)
		}
		if parts[0] == "small" || parts[0] == "normal" || parts[0] == "compacted" {
			t.Fatalf("segment path should not include type directory: %q", seg.RelativePath)
		}
		stat, err := store.fs.Stat(store.segmentPath(&seg))
		if err != nil {
			t.Fatalf("stat segment %s: %v", seg.RelativePath, err)
		}
		if stat.Mode().Perm()&0o111 != 0 {
			t.Fatalf("physical segment file should not be executable: %v", stat.Mode())
		}
	}
}

func TestScrubDetectsSegmentRecordHeaderCorruption(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func([]byte, chunkRecord)
	}{
		{
			name: "chunk id mismatch",
			corrupt: func(header []byte, _ chunkRecord) {
				for i := 8; i < 72; i++ {
					header[i] = 0
				}
				copy(header[8:72], []byte("different-chunk"))
			},
		},
		{
			name: "unsupported compression",
			corrupt: func(header []byte, _ chunkRecord) {
				binary.LittleEndian.PutUint32(header[88:92], compressionZstdID+1)
			},
		},
		{
			name: "raw size mismatch",
			corrupt: func(header []byte, chunk chunkRecord) {
				binary.LittleEndian.PutUint64(header[72:80], uint64(chunk.RawSize+1))
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			putBytes(t, store, "tenant-a", "file", []byte("header-corruption"))
			chunk, seg := firstStoredChunk(t, store)

			file, err := store.fs.OpenFile(store.segmentPath(&seg), os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("open segment: %v", err)
			}
			header := make([]byte, recordHeaderSize)
			if _, err = file.ReadAt(header, chunk.SegmentOffset); err != nil {
				_ = file.Close()
				t.Fatalf("read header: %v", err)
			}
			tt.corrupt(header, chunk)
			if _, err = file.WriteAt(header, chunk.SegmentOffset); err != nil {
				_ = file.Close()
				t.Fatalf("write corrupt header: %v", err)
			}
			if err = file.Close(); err != nil {
				t.Fatalf("close segment: %v", err)
			}
			if _, err = store.Scrub(context.Background(), ScrubOptions{}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("scrub should detect %s", tt.name)
			}
		})
	}
}

func TestReadRejectsSegmentRecordLengthMismatch(t *testing.T) {
	store := openTestStore(t)
	putBytes(t, store, "tenant-a", "file", []byte("length-mismatch"))
	chunk, seg := firstStoredChunk(t, store)

	file, err := store.fs.OpenFile(store.segmentPath(&seg), os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	header := make([]byte, recordHeaderSize)
	if _, err = file.ReadAt(header, chunk.SegmentOffset); err != nil {
		_ = file.Close()
		t.Fatalf("read header: %v", err)
	}
	binary.LittleEndian.PutUint64(header[80:88], uint64(chunk.StoredSize+1))
	binary.LittleEndian.PutUint64(header[96:104], uint64(chunk.StoredSize+1))
	if _, err = file.WriteAt(header, chunk.SegmentOffset); err != nil {
		_ = file.Close()
		t.Fatalf("write corrupt header: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	reader, err := store.OpenObject(context.Background(), "tenant-a", "file")
	if err != nil {
		t.Fatalf("open object: %v", err)
	}
	defer reader.Close()
	if _, err = io.ReadAll(reader); err == nil {
		t.Fatalf("read should reject segment record length mismatch")
	}
}

func TestOpenTruncatesSegmentRecordWithPayloadLengthPastEOF(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	putBytes(t, store, "tenant-a", "file", []byte("recover-data"))

	store.mu.Lock()
	var seg segmentRecord
	for _, item := range store.meta.Segments {
		seg = *item
		break
	}
	validEnd := seg.WriteOffset
	segmentPath := store.segmentPath(&seg)
	store.mu.Unlock()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	file, err := store.fs.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open segment append: %v", err)
	}
	if _, err = file.Write(makeRecordHeader("ghost", 1, 1<<30, 0)); err != nil {
		_ = file.Close()
		t.Fatalf("append corrupt record header: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	stat, err := reopened.fs.Stat(segmentPath)
	if err != nil {
		t.Fatalf("stat recovered segment: %v", err)
	}
	if stat.Size() != validEnd {
		t.Fatalf("segment was not truncated to valid end: got %d want %d", stat.Size(), validEnd)
	}
}

func TestOpenTruncatesPartialSegmentRecord(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	putBytes(t, store, "tenant-a", "file", []byte("recover-data"))

	store.mu.Lock()
	var seg segmentRecord
	for _, item := range store.meta.Segments {
		seg = *item
		break
	}
	validEnd := seg.WriteOffset
	segmentPath := store.segmentPath(&seg)
	store.mu.Unlock()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	file, err := store.fs.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open segment append: %v", err)
	}
	if _, err = file.Write([]byte("partial-junk")); err != nil {
		_ = file.Close()
		t.Fatalf("append partial: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	stat, err := reopened.fs.Stat(segmentPath)
	if err != nil {
		t.Fatalf("stat recovered segment: %v", err)
	}
	if stat.Size() != validEnd {
		t.Fatalf("segment was not truncated to valid end: got %d want %d", stat.Size(), validEnd)
	}
	if got := readBytes(t, reopened, "tenant-a", "file"); !bytes.Equal(got, []byte("recover-data")) {
		t.Fatalf("recovered file mismatch")
	}
}

func TestOpenRejectsInvalidSegmentHeaderDuringRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	putBytes(t, store, "tenant-a", "file", []byte("bad-segment-header"))
	_, seg := firstStoredChunk(t, store)
	segmentPath := store.segmentPath(&seg)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	file, err := store.fs.OpenFile(segmentPath, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err = file.WriteAt([]byte("BAD"), 0); err != nil {
		_ = file.Close()
		t.Fatalf("write invalid segment header: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	if reopened, err := Open(dir, testConfig()); err == nil {
		_ = reopened.Close()
		t.Fatalf("open should reject invalid segment header during recovery")
	}
}

func TestOpenSkipsMissingSegmentFileDuringRecoveryButReadFails(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	putBytes(t, store, "tenant-a", "file", []byte("missing-segment-file"))
	_, seg := firstStoredChunk(t, store)
	segmentPath := store.segmentPath(&seg)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := store.fs.Remove(segmentPath); err != nil {
		t.Fatalf("remove segment file: %v", err)
	}

	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen should tolerate missing segment file: %v", err)
	}
	defer reopened.Close()
	reader, err := reopened.OpenObject(context.Background(), "tenant-a", "file")
	if err != nil {
		t.Fatalf("open object should still build metadata snapshot: %v", err)
	}
	defer reader.Close()
	if _, err = reader.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read should fail when recovered segment file is missing")
	}
}
