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
