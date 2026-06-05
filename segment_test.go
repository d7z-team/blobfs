package blobfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
)

func TestParseRecordHeaderRejectsInvalidHeaders(t *testing.T) {
	header := makeRecordHeader("chunk-id", 10, 5, 123)
	chunkID, rawSize, storedSize, compression, checksum, payloadLen, err := parseRecordHeader(header)
	if err != nil {
		t.Fatalf("parse valid header: %v", err)
	}
	if chunkID != "chunk-id" || rawSize != 10 || storedSize != 5 || compression != compressionZstdID || checksum != 123 || payloadLen != 5 {
		t.Fatalf("bad parsed header: id=%q raw=%d stored=%d compression=%d checksum=%d payload=%d", chunkID, rawSize, storedSize, compression, checksum, payloadLen)
	}
	if _, _, _, _, _, _, err := parseRecordHeader(header[:recordHeaderSize-1]); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short header = %v, want unexpected eof", err)
	}
	badMagic := append([]byte(nil), header...)
	binary.LittleEndian.PutUint32(badMagic[0:4], 0)
	if _, _, _, _, _, _, err := parseRecordHeader(badMagic); err == nil {
		t.Fatal("bad magic should fail")
	}
	badVersion := append([]byte(nil), header...)
	binary.LittleEndian.PutUint16(badVersion[4:6], recordVersion+1)
	if _, _, _, _, _, _, err := parseRecordHeader(badVersion); err == nil {
		t.Fatal("bad version should fail")
	}
	badPayloadLen := append([]byte(nil), header...)
	binary.LittleEndian.PutUint64(badPayloadLen[96:104], 6)
	if _, _, _, _, _, _, err := parseRecordHeader(badPayloadLen); err == nil {
		t.Fatal("payload length mismatch should fail")
	}
}

func TestSegmentRelativePathFanoutBoundaries(t *testing.T) {
	cases := map[int64]string{
		1:                               "0000/0000/0000000000000001.blob",
		segmentFanout:                   "0000/0000/0000000000001024.blob",
		segmentFanout + 1:               "0000/0001/0000000000001025.blob",
		segmentFanout*segmentFanout + 1: "0001/0000/0000000001048577.blob",
	}
	for seq, want := range cases {
		if got := segmentRelativePath(seq); got != want {
			t.Fatalf("segment path for %d = %q, want %q", seq, got, want)
		}
	}
}

func TestReadPathRejectsChunkHashMismatch(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/hash", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "hash/blob", bytes.Repeat([]byte("A"), 128))
	store.metaMu.RLock()
	inode, err := store.resolvePathLocked("tenant-a", "hash/blob")
	if err != nil {
		store.metaMu.RUnlock()
		t.Fatalf("resolve: %v", err)
	}
	manifest := store.meta.Manifests[inode.ManifestID]
	chunk := *store.meta.Chunks[manifest.Chunks[0].ChunkID]
	segment := *store.meta.Segments[chunk.SegmentID]
	store.metaMu.RUnlock()

	badRaw := bytes.Repeat([]byte("B"), int(chunk.RawSize))
	payload, err := compressZstd(badRaw)
	if err != nil {
		t.Fatalf("compress bad raw: %v", err)
	}
	checksum := crc32.Checksum(payload, crc32cTable)
	header := makeRecordHeader(chunk.ChunkID, int64(len(badRaw)), int64(len(payload)), checksum)
	file, err := store.fs.OpenFile(store.segmentPath(&segment), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := file.WriteAt(header, chunk.SegmentOffset); err != nil {
		_ = file.Close()
		t.Fatalf("write bad header: %v", err)
	}
	if _, err := file.WriteAt(payload, chunk.SegmentOffset+recordHeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("write bad payload: %v", err)
	}
	_ = file.Close()
	store.metaMu.Lock()
	current := store.meta.Chunks[chunk.ChunkID]
	current.StoredSize = int64(len(payload))
	current.SegmentLength = int64(recordHeaderSize + len(payload))
	current.ChecksumCRC32C = checksum
	store.metaMu.Unlock()

	reader, err := store.OpenObject(testContext(t), "tenant-a", "hash/blob")
	if err != nil {
		t.Fatalf("open object: %v", err)
	}
	defer reader.Close()
	if _, err := io.ReadAll(reader); !errors.Is(err, errChunkHashMismatch) {
		t.Fatalf("hash-mismatched read = %v, want errChunkHashMismatch", err)
	}
}

func TestStagingSegmentDirectoriesArePrivate(t *testing.T) {
	fsys := afero.NewMemMapFs()
	store, err := OpenFS(fsys, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/perms", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "perms/blob", bytes.Repeat([]byte("p"), 128))
	info, err := store.fs.Stat(filepath.Join(store.stagingDir, "0000", "0000"))
	if err != nil {
		t.Fatalf("stat staging segment dir: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("staging segment dir mode = %#o", info.Mode().Perm())
	}
}
