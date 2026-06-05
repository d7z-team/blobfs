package blobfs

import "testing"

func TestOpenNormalizesZeroConfig(t *testing.T) {
	store, err := Open(t.TempDir(), Config{})
	if err != nil {
		t.Fatalf("open with zero config: %v", err)
	}
	defer store.Close()
	if store.cfg.LargeFileThreshold != DefaultConfig().LargeFileThreshold {
		t.Fatalf("zero config should apply defaults: %+v", store.cfg)
	}
}

func TestOpenRejectsUnsupportedConfig(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Config)
	}{
		{name: "compression", edit: func(cfg *Config) { cfg.Compression = "gzip" }},
		{name: "checksum", edit: func(cfg *Config) { cfg.Checksum = "sha256" }},
		{name: "dedup scope", edit: func(cfg *Config) { cfg.DedupScope = "all" }},
		{name: "chunking", edit: func(cfg *Config) { cfg.Chunking.Algorithm = "rabin" }},
		{name: "file threshold", edit: func(cfg *Config) { cfg.LargeFileThreshold = -1 }},
		{name: "segment size", edit: func(cfg *Config) { cfg.SegmentSize = -1 }},
		{name: "chunk sizes", edit: func(cfg *Config) { cfg.Chunking.MinSize = cfg.Chunking.MaxSize + 1 }},
		{name: "gc cycles", edit: func(cfg *Config) { cfg.GC.CandidateConfirmCycles = -1 }},
		{name: "gc duration", edit: func(cfg *Config) { cfg.GC.SafetyWindow = -1 }},
		{name: "compact ratio", edit: func(cfg *Config) { cfg.GC.CompactGarbageRatio = 2 }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.edit(&cfg)
			if _, err := Open(t.TempDir(), cfg); err == nil {
				t.Fatalf("open should reject invalid config")
			}
		})
	}
}
