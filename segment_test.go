package blobfs

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"
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
