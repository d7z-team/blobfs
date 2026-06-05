package blobfs

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
)

var gearTable = makeGearTable()

func scopedHasher(tenantID string, scoped bool) hash.Hash {
	h := sha256.New()
	if scoped && tenantID != "" {
		h.Write([]byte(tenantID))
		h.Write([]byte{0})
	}
	return h
}

func hashBytes(tenantID string, scoped bool, data []byte) string {
	h := scopedHasher(tenantID, scoped)
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func splitFastCDC(data []byte, cfg ChunkingConfig) []chunkSlice {
	if len(data) == 0 {
		return []chunkSlice{{Offset: 0, Size: 0}}
	}
	if cfg.MinSize <= 0 || cfg.AvgSize <= 0 || cfg.MaxSize <= 0 || cfg.MinSize > cfg.AvgSize || cfg.AvgSize > cfg.MaxSize {
		cfg = DefaultConfig().Chunking
	}
	mask := uint64(nextPowerOfTwo(cfg.AvgSize) - 1)
	var result []chunkSlice
	for start := 0; start < len(data); {
		if len(data)-start <= cfg.MaxSize {
			result = append(result, chunkSlice{Offset: int64(start), Size: int64(len(data) - start)})
			break
		}
		minEnd := start + cfg.MinSize
		maxEnd := start + cfg.MaxSize
		if maxEnd > len(data) {
			maxEnd = len(data)
		}
		fp := uint64(0)
		cut := maxEnd
		for i := start; i < maxEnd; i++ {
			fp = (fp << 1) + gearTable[data[i]]
			if i+1 >= minEnd && (fp&mask) == 0 {
				cut = i + 1
				break
			}
		}
		result = append(result, chunkSlice{Offset: int64(start), Size: int64(cut - start)})
		start = cut
	}
	return result
}

func nextPowerOfTwo(v int) int {
	p := 1
	for p < v {
		p <<= 1
	}
	return p
}

func makeGearTable() [256]uint64 {
	var table [256]uint64
	x := uint64(0x9e3779b97f4a7c15)
	for i := range table {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		table[i] = z ^ (z >> 31)
	}
	return table
}

func manifestID(tenantID, fileHash string, fileSize int64, chunkingType string, refs []manifestChunk) string {
	h := sha256.New()
	h.Write([]byte("manifest"))
	h.Write([]byte{0})
	h.Write([]byte(tenantID))
	h.Write([]byte{0})
	h.Write([]byte(fileHash))
	h.Write([]byte{0})
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(fileSize))
	h.Write(buf[:8])
	h.Write([]byte(chunkingType))
	h.Write([]byte{0})
	for _, ref := range refs {
		h.Write([]byte(ref.ChunkID))
		binary.LittleEndian.PutUint64(buf[:8], uint64(ref.FileOffset))
		binary.LittleEndian.PutUint64(buf[8:], uint64(ref.ChunkSize))
		h.Write(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

type chunkSlice struct {
	Offset int64
	Size   int64
}
