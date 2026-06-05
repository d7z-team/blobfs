package blobfs

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// CompressionType identifies the segment payload compression algorithm.
type CompressionType string

// ChecksumType identifies the segment payload checksum algorithm.
type ChecksumType string

// DedupScope controls whether hashes are tenant-scoped or global.
type DedupScope string

const (
	// CompressionZstd stores segment payloads with zstd compression.
	CompressionZstd CompressionType = "zstd"

	// ChecksumCRC32C validates compressed segment payloads with CRC32C.
	ChecksumCRC32C ChecksumType = "crc32c"

	// DedupScopeTenant deduplicates only within the same tenant.
	DedupScopeTenant DedupScope = "tenant"

	// DedupScopeGlobal deduplicates across all tenants.
	DedupScopeGlobal DedupScope = "global"
)

// Config controls chunking, segment layout, VFS write sessions, and GC behavior.
type Config struct {
	SegmentSize          int64
	MaxFileSize          int64
	MaxTenantLength      int
	MaxPathLength        int
	MaxComponentLength   int
	MaxOpenWriteSessions int
	AllowExecutableFiles bool
	Compression          CompressionType
	Checksum             ChecksumType
	DedupScope           DedupScope
	Chunking             ChunkingConfig
	GC                   GCConfig
}

// ChunkingConfig controls FastCDC-style content-defined chunking for large files.
type ChunkingConfig struct {
	Algorithm string
	MinSize   int
	AvgSize   int
	MaxSize   int
}

// GCConfig controls asynchronous mark/sweep and compaction behavior.
type GCConfig struct {
	SafetyWindow           time.Duration
	CandidateConfirmCycles int
	SegmentDeleteDelay     time.Duration
	CompactGarbageRatio    float64
}

// GCOptions overrides selected GC settings for a single run.
type GCOptions struct {
	SafetyWindow           time.Duration
	CandidateConfirmCycles int
	Compact                bool
}

// GCResult reports work completed by a GC run.
type GCResult struct {
	Epoch             int64
	LiveChunks        int
	CandidatesMarked  int
	ChunksDeleted     int
	SegmentsCompacted int
	SegmentsDeleted   int
	BytesRewritten    int64
	BytesMadeGarbage  int64
}

// ScrubOptions controls full-store corruption checks.
type ScrubOptions struct {
	CheckFiles bool
}

// CheckIssue describes one consistency or corruption problem found by CheckObject or Scrub.
type CheckIssue struct {
	Kind      string
	ID        string
	Path      string
	Reason    string
	TenantID  string
	ChunkID   string
	SegmentID string
}

// CheckResult reports object-level integrity verification.
type CheckResult struct {
	TenantID        string
	Path            string
	Healthy         bool
	CheckedChunks   int
	CheckedSegments int
	CheckedBytes    int64
	Issues          []CheckIssue
}

// ScrubResult reports full-store integrity verification.
type ScrubResult struct {
	Healthy         bool
	CheckedChunks   int
	CheckedSegments int
	CheckedFiles    int
	CheckedBytes    int64
	CorruptChunks   []string
	CorruptSegments []string
	AffectedFiles   []string
	Issues          []CheckIssue
}

// DefaultConfig returns production-oriented defaults for CAS chunk storage.
func DefaultConfig() Config {
	return Config{
		SegmentSize:          256 << 20,
		MaxFileSize:          1 << 40,
		MaxTenantLength:      128,
		MaxPathLength:        4096,
		MaxComponentLength:   255,
		MaxOpenWriteSessions: 1024,
		Compression:          CompressionZstd,
		Checksum:             ChecksumCRC32C,
		DedupScope:           DedupScopeTenant,
		Chunking: ChunkingConfig{
			Algorithm: "FastCDC",
			MinSize:   512 << 10,
			AvgSize:   4 << 20,
			MaxSize:   16 << 20,
		},
		GC: GCConfig{
			SafetyWindow:           24 * time.Hour,
			CandidateConfirmCycles: 2,
			SegmentDeleteDelay:     24 * time.Hour,
			CompactGarbageRatio:    0.6,
		},
	}
}

func normalizeConfig(cfg Config) Config {
	def := DefaultConfig()
	emptyGC := cfg.GC == GCConfig{}
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = def.SegmentSize
	}
	if cfg.MaxFileSize == 0 {
		cfg.MaxFileSize = def.MaxFileSize
	}
	if cfg.MaxTenantLength == 0 {
		cfg.MaxTenantLength = def.MaxTenantLength
	}
	if cfg.MaxPathLength == 0 {
		cfg.MaxPathLength = def.MaxPathLength
	}
	if cfg.MaxComponentLength == 0 {
		cfg.MaxComponentLength = def.MaxComponentLength
	}
	if cfg.MaxOpenWriteSessions == 0 {
		cfg.MaxOpenWriteSessions = def.MaxOpenWriteSessions
	}
	if cfg.Compression == "" {
		cfg.Compression = def.Compression
	}
	if cfg.Checksum == "" {
		cfg.Checksum = def.Checksum
	}
	if cfg.DedupScope == "" {
		cfg.DedupScope = def.DedupScope
	}
	if cfg.Chunking.Algorithm == "" {
		cfg.Chunking.Algorithm = def.Chunking.Algorithm
	}
	if cfg.Chunking.MinSize == 0 {
		cfg.Chunking.MinSize = def.Chunking.MinSize
	}
	if cfg.Chunking.AvgSize == 0 {
		cfg.Chunking.AvgSize = def.Chunking.AvgSize
	}
	if cfg.Chunking.MaxSize == 0 {
		cfg.Chunking.MaxSize = def.Chunking.MaxSize
	}
	if emptyGC {
		cfg.GC = def.GC
	} else {
		if cfg.GC.CandidateConfirmCycles == 0 {
			cfg.GC.CandidateConfirmCycles = def.GC.CandidateConfirmCycles
		}
		if cfg.GC.CompactGarbageRatio == 0 {
			cfg.GC.CompactGarbageRatio = def.GC.CompactGarbageRatio
		}
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.Compression != CompressionZstd {
		return fmt.Errorf("unsupported compression %q", cfg.Compression)
	}
	if cfg.Checksum != ChecksumCRC32C {
		return fmt.Errorf("unsupported checksum %q", cfg.Checksum)
	}
	if cfg.DedupScope != DedupScopeTenant && cfg.DedupScope != DedupScopeGlobal {
		return fmt.Errorf("unsupported dedup scope %q", cfg.DedupScope)
	}
	if !strings.EqualFold(cfg.Chunking.Algorithm, "FastCDC") {
		return fmt.Errorf("unsupported chunking algorithm %q", cfg.Chunking.Algorithm)
	}
	if cfg.SegmentSize <= 0 {
		return errors.New("segment size must be positive")
	}
	if cfg.MaxFileSize <= 0 {
		return errors.New("max file size must be positive")
	}
	if cfg.MaxTenantLength <= 0 || cfg.MaxPathLength <= 0 || cfg.MaxComponentLength <= 0 {
		return errors.New("path limits must be positive")
	}
	if cfg.MaxOpenWriteSessions <= 0 {
		return errors.New("max open write sessions must be positive")
	}
	if cfg.Chunking.MinSize <= 0 || cfg.Chunking.AvgSize <= 0 || cfg.Chunking.MaxSize <= 0 {
		return errors.New("chunk sizes must be positive")
	}
	if cfg.Chunking.MinSize > cfg.Chunking.AvgSize || cfg.Chunking.AvgSize > cfg.Chunking.MaxSize {
		return errors.New("chunk sizes must satisfy min <= avg <= max")
	}
	if cfg.GC.CandidateConfirmCycles <= 0 {
		return errors.New("candidate confirm cycles must be positive")
	}
	if cfg.GC.SafetyWindow < 0 || cfg.GC.SegmentDeleteDelay < 0 {
		return errors.New("gc durations must be non-negative")
	}
	if cfg.GC.CompactGarbageRatio <= 0 || cfg.GC.CompactGarbageRatio > 1 {
		return errors.New("compact garbage ratio must be within (0, 1]")
	}
	return nil
}
