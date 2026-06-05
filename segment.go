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
	segmentHeaderMagic = "BLOBFSSEG1\n"
	recordMagic        = uint32(0x31465342)
	recordVersion      = uint16(1)
	recordHeaderSize   = 104

	compressionZstdID = uint32(1)
	segmentFanout     = int64(1024)
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type chunkLocation struct {
	SegmentID     string
	SegmentOffset int64
	SegmentLength int64
	StoredSize    int64
	Checksum      uint32
	Compression   string
}

func (s *Store) segmentPath(seg *segmentRecord) string {
	return filepath.Join(s.segmentsDir, seg.RelativePath)
}

func (s *Store) createSegmentLocked() (*segmentRecord, error) {
	seq := s.meta.NextSegmentSeq
	s.meta.NextSegmentSeq++
	id := fmt.Sprintf("%016d", seq)
	seg := &segmentRecord{
		SegmentID:    id,
		RelativePath: segmentRelativePath(seq),
		WriteOffset:  int64(len(segmentHeaderMagic)),
		State:        segmentStateOpen,
		CreatedAt:    nowUnix(),
	}
	if err := s.fs.MkdirAll(filepath.Dir(s.segmentPath(seg)), 0o755); err != nil {
		return nil, err
	}
	file, err := s.fs.OpenFile(s.segmentPath(seg), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err = file.Write([]byte(segmentHeaderMagic)); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	s.meta.Segments[id] = seg
	return seg, nil
}

func (s *Store) appendChunkRecordLocked(chunkID string, raw []byte) (chunkLocation, error) {
	payload, err := compressZstd(raw)
	if err != nil {
		return chunkLocation{}, err
	}
	recordLen := int64(recordHeaderSize + len(payload))
	seg := s.openSegmentLocked()
	if seg == nil || (seg.WriteOffset > int64(len(segmentHeaderMagic)) && seg.WriteOffset+recordLen > s.cfg.SegmentSize) {
		if seg != nil && seg.State == segmentStateOpen {
			seg.State = segmentStateSealed
			seg.SealedAt = nowUnix()
		}
		seg, err = s.createSegmentLocked()
		if err != nil {
			return chunkLocation{}, err
		}
	}
	checksum := crc32.Checksum(payload, crc32cTable)
	offset := seg.WriteOffset
	header := makeRecordHeader(chunkID, int64(len(raw)), int64(len(payload)), checksum)
	file, err := s.fs.OpenFile(s.segmentPath(seg), os.O_WRONLY, 0o600)
	if err != nil {
		return chunkLocation{}, err
	}
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return chunkLocation{}, err
	}
	if _, err = file.Write(header); err != nil {
		_ = file.Close()
		return chunkLocation{}, err
	}
	if _, err = file.Write(payload); err != nil {
		_ = file.Close()
		return chunkLocation{}, err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return chunkLocation{}, err
	}
	if err = file.Close(); err != nil {
		return chunkLocation{}, err
	}
	seg.WriteOffset += recordLen
	seg.TotalBytes += recordLen
	seg.LiveBytesEstimate += int64(len(payload))
	return chunkLocation{
		SegmentID:     seg.SegmentID,
		SegmentOffset: offset,
		SegmentLength: recordLen,
		StoredSize:    int64(len(payload)),
		Checksum:      checksum,
		Compression:   string(CompressionZstd),
	}, nil
}

func (s *Store) openSegmentLocked() *segmentRecord {
	for _, seg := range s.meta.Segments {
		if seg.State == segmentStateOpen {
			return seg
		}
	}
	return nil
}

func segmentRelativePath(seq int64) string {
	slot := seq - 1
	first := slot / segmentFanout / segmentFanout
	second := (slot / segmentFanout) % segmentFanout
	return filepath.Join(fmt.Sprintf("%04d", first), fmt.Sprintf("%04d", second), fmt.Sprintf("%016d.blob", seq))
}

func makeRecordHeader(
	chunkID string,
	rawSize, storedSize int64,
	checksum uint32,
) []byte {
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

func parseRecordHeader(
	header []byte,
) (
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

func (s *Store) readChunkPayload(chunk chunkRecord) ([]byte, error) {
	s.pinSegment(chunk.SegmentID)
	defer s.unpinSegment(chunk.SegmentID)
	seg, ok := s.segmentSnapshot(chunk.SegmentID)
	if !ok {
		return nil, fmt.Errorf("segment %s not found", chunk.SegmentID)
	}
	return s.readChunkPayloadAt(seg, chunk)
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

func (s *Store) recoverSegmentsLocked() error {
	for _, seg := range s.meta.Segments {
		if seg.State == segmentStateDeleted {
			continue
		}
		path := s.segmentPath(seg)
		file, err := s.fs.OpenFile(path, os.O_RDWR, 0o600)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		validEnd, err := scanSegment(file)
		if err != nil {
			_ = file.Close()
			return err
		}
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return err
		}
		if stat.Size() != validEnd {
			if err = file.Truncate(validEnd); err != nil {
				_ = file.Close()
				return err
			}
		}
		_ = file.Close()
		if seg.WriteOffset > validEnd || seg.WriteOffset == 0 {
			seg.WriteOffset = validEnd
		}
	}
	return nil
}

func scanSegment(file afero.File) (int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	fileSize := stat.Size()
	header := make([]byte, len(segmentHeaderMagic))
	n, err := file.ReadAt(header, 0)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if n != len(segmentHeaderMagic) || string(header) != segmentHeaderMagic {
		return 0, errors.New("invalid segment header")
	}
	offset := int64(len(segmentHeaderMagic))
	for {
		recordHeader := make([]byte, recordHeaderSize)
		n, err = file.ReadAt(recordHeader, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return offset, nil
			}
			return 0, err
		}
		if n != recordHeaderSize {
			return offset, nil
		}
		_, _, _, _, checksum, payloadLen, err := parseRecordHeader(recordHeader)
		if err != nil {
			return offset, nil
		}
		if payloadLen > fileSize-offset-int64(recordHeaderSize) {
			return offset, nil
		}
		payload := make([]byte, payloadLen)
		n, err = file.ReadAt(payload, offset+recordHeaderSize)
		if err != nil || int64(n) != payloadLen {
			return offset, nil
		}
		if crc32.Checksum(payload, crc32cTable) != checksum {
			return offset, nil
		}
		offset += int64(recordHeaderSize) + payloadLen
	}
}
