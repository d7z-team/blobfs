package blobfs

import (
	"testing"
	"time"
)

func TestOpenNormalizesZeroConfig(t *testing.T) {
	store, err := Open(t.TempDir(), Config{})
	if err != nil {
		t.Fatalf("open with zero config: %v", err)
	}
	defer store.Close()
	if store.cfg.SegmentSize != DefaultConfig().SegmentSize {
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
		{name: "segment size", edit: func(cfg *Config) { cfg.SegmentSize = -1 }},
		{name: "chunk sizes", edit: func(cfg *Config) { cfg.Chunking.MinSize = cfg.Chunking.MaxSize + 1 }},
		{name: "gc cycles", edit: func(cfg *Config) { cfg.GC.CandidateConfirmCycles = -1 }},
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

func TestOpenNormalizesPartialGCConfig(t *testing.T) {
	cfg := Config{GC: GCConfig{CandidateConfirmCycles: 3}}
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open with partial gc config: %v", err)
	}
	defer store.Close()
	if store.cfg.GC.CandidateConfirmCycles != 3 {
		t.Fatalf("candidate cycles = %d", store.cfg.GC.CandidateConfirmCycles)
	}
	if store.cfg.GC.SafetyWindow != DefaultConfig().GC.SafetyWindow {
		t.Fatalf("safety window = %s", store.cfg.GC.SafetyWindow)
	}
	if store.cfg.GC.SegmentDeleteDelay != DefaultConfig().GC.SegmentDeleteDelay {
		t.Fatalf("segment delete delay = %s", store.cfg.GC.SegmentDeleteDelay)
	}
}

func TestOpenAllowsNegativeGCDurationsToDisableDelays(t *testing.T) {
	cfg := testConfig()
	cfg.GC.SafetyWindow = -time.Nanosecond
	cfg.GC.SegmentDeleteDelay = -time.Nanosecond
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open with disabled gc delays: %v", err)
	}
	defer store.Close()
	if store.cfg.GC.SafetyWindow != 0 || store.cfg.GC.SegmentDeleteDelay != 0 {
		t.Fatalf("negative gc durations should disable delays: %+v", store.cfg.GC)
	}
}
