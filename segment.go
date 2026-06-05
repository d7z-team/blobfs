package blobfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/afero"
)

const (
	segmentHeaderMagic = "BLOBFSSEG2\n"
	recordMagic        = uint32(0x32465342)
	recordVersion      = uint16(2)
	recordHeaderSize   = 104

	compressionZstdID = uint32(1)
	segmentFanout     = int64(1024)
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var errChunkHashMismatch = errors.New("chunk hash mismatch")

type segmentBatchWriter struct {
	store    *Store
	current  *preparedSegment
	segments []*segmentRecord
}

type preparedSegment struct {
	record      *segmentRecord
	file        afero.File
	stagingPath string
}

func (s *Store) segmentPath(seg *segmentRecord) string {
	return filepath.Join(s.segmentsDir, seg.RelativePath)
}

func (s *Store) stagingSegmentPath(seg *segmentRecord) string {
	return filepath.Join(s.stagingDir, seg.RelativePath)
}

func (s *Store) newSegmentRecord() *segmentRecord {
	s.metaMu.Lock()
	seq := s.meta.NextSegmentSeq
	s.meta.NextSegmentSeq++
	s.metaMu.Unlock()
	id := fmt.Sprintf("%016d", seq)
	return &segmentRecord{
		SegmentID:    id,
		RelativePath: segmentRelativePath(seq),
		WriteOffset:  int64(len(segmentHeaderMagic)),
		State:        segmentStateSealed,
		CreatedAt:    nowUnix(),
	}
}

func (w *segmentBatchWriter) appendChunk(scopeID, chunkID string, raw []byte) (chunkRecord, error) {
	payload, err := compressZstd(raw)
	if err != nil {
		return chunkRecord{}, err
	}
	recordLen := int64(recordHeaderSize + len(payload))
	if w.current == nil || (w.current.record.WriteOffset > int64(len(segmentHeaderMagic)) && w.current.record.WriteOffset+recordLen > w.store.cfg.SegmentSize) {
		if err := w.rotate(); err != nil {
			return chunkRecord{}, err
		}
	}
	seg := w.current.record
	offset := seg.WriteOffset
	checksum := crc32.Checksum(payload, crc32cTable)
	header := makeRecordHeader(chunkID, int64(len(raw)), int64(len(payload)), checksum)
	if _, err := w.current.file.Write(header); err != nil {
		return chunkRecord{}, err
	}
	if _, err := w.current.file.Write(payload); err != nil {
		return chunkRecord{}, err
	}
	seg.WriteOffset += recordLen
	seg.TotalBytes += recordLen
	now := nowUnix()
	return chunkRecord{
		ChunkID:        chunkID,
		TenantID:       scopeID,
		RawSize:        int64(len(raw)),
		StoredSize:     int64(len(payload)),
		State:          chunkStateActive,
		SegmentID:      seg.SegmentID,
		SegmentOffset:  offset,
		SegmentLength:  recordLen,
		ChecksumCRC32C: checksum,
		Compression:    string(CompressionZstd),
		CreatedAt:      now,
		LastSeenAt:     now,
	}, nil
}

func (w *segmentBatchWriter) rotate() error {
	if w.current != nil {
		if err := w.current.file.Sync(); err != nil {
			return err
		}
		if err := w.current.file.Close(); err != nil {
			return err
		}
		w.current = nil
	}
	seg := w.store.newSegmentRecord()
	stagingPath := w.store.stagingSegmentPath(seg)
	if err := w.store.fs.MkdirAll(filepath.Dir(stagingPath), 0o755); err != nil {
		return err
	}
	file, err := w.store.fs.OpenFile(stagingPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write([]byte(segmentHeaderMagic)); err != nil {
		_ = file.Close()
		_ = w.store.fs.Remove(stagingPath)
		return err
	}
	w.current = &preparedSegment{record: seg, file: file, stagingPath: stagingPath}
	w.segments = append(w.segments, seg)
	return nil
}

func (w *segmentBatchWriter) finish() error {
	if w.current != nil {
		if err := w.current.file.Sync(); err != nil {
			return err
		}
		if err := w.current.file.Close(); err != nil {
			return err
		}
		w.current = nil
	}
	sealedAt := nowUnix()
	published := make([]*segmentRecord, 0, len(w.segments))
	for _, seg := range w.segments {
		if seg.SealedAt == 0 {
			seg.SealedAt = sealedAt
		}
		staging := w.store.stagingSegmentPath(seg)
		final := w.store.segmentPath(seg)
		if err := w.store.fs.MkdirAll(filepath.Dir(final), 0o755); err != nil {
			w.removePublished(published)
			return err
		}
		if err := w.store.fs.Rename(staging, final); err != nil {
			w.removePublished(published)
			return err
		}
		published = append(published, seg)
	}
	return nil
}

func (w *segmentBatchWriter) removePublished(segments []*segmentRecord) {
	for _, seg := range segments {
		_ = w.store.fs.Remove(w.store.segmentPath(seg))
	}
}

func (w *segmentBatchWriter) cleanup() {
	if w.current != nil {
		_ = w.current.file.Close()
		w.current = nil
	}
	for _, seg := range w.segments {
		_ = w.store.fs.Remove(w.store.stagingSegmentPath(seg))
	}
}

func segmentRelativePath(seq int64) string {
	slot := seq - 1
	first := slot / segmentFanout / segmentFanout
	second := (slot / segmentFanout) % segmentFanout
	return filepath.Join(fmt.Sprintf("%04d", first), fmt.Sprintf("%04d", second), fmt.Sprintf("%016d.blob", seq))
}

func sscanfSegmentID(id string, seq *int64) (int, error) {
	return fmt.Sscanf(id, "%d", seq)
}

func makeRecordHeader(chunkID string, rawSize, storedSize int64, checksum uint32) []byte {
	header := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], recordMagic)
	binary.LittleEndian.PutUint16(header[4:6], recordVersion)
	binary.LittleEndian.PutUint16(header[6:8], 1)
	copy(header[8:72], []byte(chunkID))
	binary.LittleEndian.PutUint64(header[72:80], uint64(rawSize))
	binary.LittleEndian.PutUint64(header[80:88], uint64(storedSize))
	binary.LittleEndian.PutUint32(header[88:92], compressionZstdID)
	binary.LittleEndian.PutUint32(header[92:96], checksum)
	binary.LittleEndian.PutUint64(header[96:104], uint64(storedSize))
	return header
}

func parseRecordHeader(header []byte) (
	chunkID string,
	rawSize int64,
	storedSize int64,
	compression uint32,
	checksum uint32,
	payloadLen int64,
	err error,
) {
	if len(header) != recordHeaderSize {
		return "", 0, 0, 0, 0, 0, io.ErrUnexpectedEOF
	}
	if binary.LittleEndian.Uint32(header[0:4]) != recordMagic {
		return "", 0, 0, 0, 0, 0, errors.New("invalid segment record magic")
	}
	if binary.LittleEndian.Uint16(header[4:6]) != recordVersion {
		return "", 0, 0, 0, 0, 0, errors.New("unsupported segment record version")
	}
	chunkIDBytes := bytes.TrimRight(header[8:72], "\x00")
	chunkID = string(chunkIDBytes)
	rawSize = int64(binary.LittleEndian.Uint64(header[72:80]))
	storedSize = int64(binary.LittleEndian.Uint64(header[80:88]))
	compression = binary.LittleEndian.Uint32(header[88:92])
	checksum = binary.LittleEndian.Uint32(header[92:96])
	payloadLen = int64(binary.LittleEndian.Uint64(header[96:104]))
	if payloadLen != storedSize || payloadLen < 0 {
		return "", 0, 0, 0, 0, 0, errors.New("invalid segment payload length")
	}
	return chunkID, rawSize, storedSize, compression, checksum, payloadLen, nil
}

func (s *Store) readChunkPayloadAt(seg segmentRecord, chunk chunkRecord) ([]byte, error) {
	file, err := s.fs.Open(s.segmentPath(&seg))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	header := make([]byte, recordHeaderSize)
	if _, err = file.ReadAt(header, chunk.SegmentOffset); err != nil {
		return nil, err
	}
	recordChunkID, rawSize, storedSize, compression, checksum, payloadLen, err := parseRecordHeader(header)
	if err != nil {
		return nil, err
	}
	if recordChunkID != chunk.ChunkID {
		return nil, fmt.Errorf("segment record chunk mismatch: want %s got %s", chunk.ChunkID, recordChunkID)
	}
	expectedPayloadLen := chunk.SegmentLength - recordHeaderSize
	if expectedPayloadLen < 0 || payloadLen != expectedPayloadLen || storedSize != chunk.StoredSize {
		return nil, errors.New("segment record length mismatch")
	}
	payload := make([]byte, payloadLen)
	if _, err = file.ReadAt(payload, chunk.SegmentOffset+recordHeaderSize); err != nil {
		return nil, err
	}
	if crc32.Checksum(payload, crc32cTable) != checksum || checksum != chunk.ChecksumCRC32C {
		return nil, errors.New("segment record checksum mismatch")
	}
	if compression != compressionZstdID {
		return nil, errors.New("unsupported compression")
	}
	raw, err := decompressZstd(payload)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != rawSize || rawSize != chunk.RawSize {
		return nil, errors.New("segment record raw size mismatch")
	}
	gotChunkID := hashBytes(chunk.TenantID, chunk.TenantID != "", raw)
	if gotChunkID != chunk.ChunkID {
		return nil, fmt.Errorf("%w: want %s got %s", errChunkHashMismatch, chunk.ChunkID, gotChunkID)
	}
	return raw, nil
}

func compressZstd(raw []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, make([]byte, 0, len(raw))), nil
}

func decompressZstd(payload []byte) ([]byte, error) {
	dec, err := zstd.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return io.ReadAll(dec)
}
