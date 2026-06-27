package blobfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
)

func TestMigrateV1ToV2_RoundTrip(t *testing.T) {
	fsys := afero.NewMemMapFs()
	metaDir := "/meta"
	if err := fsys.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	now := nowUnix()

	// Build a v1 checkpoint.json with entries in all maps.
	v1 := metadataV1{
		Version:        1,
		TxID:           42,
		NextInodeID:    100,
		NextSegmentSeq: 5,
		NextGCEpoch:    3,
		Tenants:        map[string]uint64{"tenant-a": 1, "tenant-b": 5},
		Inodes: map[uint64]*inodeRecord{
			1: {InodeID: 1, TenantID: "tenant-a", Kind: fileKindDir, State: fileStateActive, CreatedAt: now, UpdatedAt: now},
			5: {InodeID: 5, TenantID: "tenant-b", Kind: fileKindDir, State: fileStateActive, CreatedAt: now, UpdatedAt: now},
			6: {InodeID: 6, TenantID: "tenant-a", Kind: fileKindFile, State: fileStateActive, ParentInode: 1, Name: "readme.md", ManifestID: "M1", Size: 100, CreatedAt: now, UpdatedAt: now},
		},
		DirEntries: map[uint64]map[string]uint64{1: {"readme.md": 6}},
		Manifests: map[string]*manifestRecord{
			"M1": {ManifestID: "M1", State: manifestStateActive, RefCount: 1, Chunks: []manifestChunk{{ChunkID: "C1", FileOffset: 0, ChunkSize: 100}}},
		},
		Chunks: map[string]*chunkRecord{
			"C1": {ChunkID: "C1", SegmentID: "S1", SegmentOffset: 0, SegmentLength: 100, State: chunkStateActive, RefCount: 1},
		},
		Segments: map[string]*segmentRecord{
			"S1": {SegmentID: "S1", State: segmentStateSealed},
		},
		GC: gcMetadata{TotalRuns: 5, LastEpoch: 5},
	}

	data, err := json.Marshal(v1)
	if err != nil {
		t.Fatalf("marshal v1: %v", err)
	}
	if err := afero.WriteFile(fsys, filepath.Join(metaDir, "checkpoint.json"), data, 0o600); err != nil {
		t.Fatalf("write checkpoint.json: %v", err)
	}

	// Migrate.
	if err := migrateV1ToV2(fsys, metaDir); err != nil {
		t.Fatalf("migrateV1ToV2: %v", err)
	}

	// Old file should be gone.
	if _, err := fsys.Stat(filepath.Join(metaDir, "checkpoint.json")); !os.IsNotExist(err) {
		t.Fatal("old checkpoint.json should be deleted after migration")
	}

	// New checkpoint directory should exist.
	if _, err := fsys.Stat(filepath.Join(metaDir, metaCheckpointDir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json missing: %v", err)
	}

	// Load via loadCheckpoint and verify data integrity.
	dst := newMetadata()
	if err := loadCheckpoint(fsys, metaDir, dst); err != nil {
		t.Fatalf("loadCheckpoint after migration: %v", err)
	}
	recomputeMetaCounters(dst)

	if dst.TxID != 42 {
		t.Fatalf("TxID: got %d, want 42", dst.TxID)
	}
	if dst.NextInodeID != 100 {
		t.Fatalf("NextInodeID: got %d, want 100", dst.NextInodeID)
	}
	if len(dst.Tenants) != 2 || dst.Tenants["tenant-a"] != 1 || dst.Tenants["tenant-b"] != 5 {
		t.Fatalf("tenants mismatch: %+v", dst.Tenants)
	}
	if len(dst.Inodes) != 3 {
		t.Fatalf("inodes count: got %d, want 3", len(dst.Inodes))
	}
	if dst.Inodes[6].Name != "readme.md" || dst.Inodes[6].ManifestID != "M1" {
		t.Fatalf("inode 6 mismatch: %+v", dst.Inodes[6])
	}
	if len(dst.Manifests) != 1 || dst.Manifests["M1"].State != manifestStateActive {
		t.Fatalf("manifests mismatch: %+v", dst.Manifests)
	}
	if len(dst.Chunks) != 1 || dst.Chunks["C1"].RefCount != 1 {
		t.Fatalf("chunks mismatch: %+v", dst.Chunks)
	}
	if len(dst.Segments) != 1 || dst.Segments["S1"].State != segmentStateSealed {
		t.Fatalf("segments mismatch: %+v", dst.Segments)
	}
	if dst.GC.TotalRuns != 5 || dst.GC.LastEpoch != 5 {
		t.Fatalf("GC mismatch: %+v", dst.GC)
	}
}

func TestMigrateV1ToV2_Idempotent(t *testing.T) {
	fsys := afero.NewMemMapFs()
	metaDir := "/meta"
	fsys.MkdirAll(metaDir, 0o755)

	v1 := metadataV1{Version: 1, TxID: 1}
	data, _ := json.Marshal(v1)
	afero.WriteFile(fsys, filepath.Join(metaDir, "checkpoint.json"), data, 0o600)

	// First migration.
	if err := migrateV1ToV2(fsys, metaDir); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	// Second migration — idempotent.
	if err := migrateV1ToV2(fsys, metaDir); err != nil {
		t.Fatalf("second migration should be idempotent: %v", err)
	}
	// Old file already deleted, should still succeed.
	if _, err := fsys.Stat(filepath.Join(metaDir, "checkpoint.json")); !os.IsNotExist(err) {
		t.Fatal("checkpoint.json should be gone")
	}
	// New checkpoint still intact.
	if _, err := fsys.Stat(filepath.Join(metaDir, metaCheckpointDir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json should still exist: %v", err)
	}
}

func TestMigrateV1ToV2_NoCheckpoint(t *testing.T) {
	fsys := afero.NewMemMapFs()
	metaDir := "/meta"
	fsys.MkdirAll(metaDir, 0o755)

	// No checkpoint.json at all — should succeed silently.
	if err := migrateV1ToV2(fsys, metaDir); err != nil {
		t.Fatalf("migration with no v1 checkpoint: %v", err)
	}
}

func TestMigrateV1ToV2_NilMaps(t *testing.T) {
	fsys := afero.NewMemMapFs()
	metaDir := "/meta"
	fsys.MkdirAll(metaDir, 0o755)

	// v1 with nil maps (omitted in JSON).
	v1 := metadataV1{Version: 1, TxID: 7, NextInodeID: 5, NextSegmentSeq: 1, NextGCEpoch: 1}
	data, _ := json.Marshal(v1)
	afero.WriteFile(fsys, filepath.Join(metaDir, "checkpoint.json"), data, 0o600)

	if err := migrateV1ToV2(fsys, metaDir); err != nil {
		t.Fatalf("migrate with nil maps: %v", err)
	}

	dst := newMetadata()
	if err := loadCheckpoint(fsys, metaDir, dst); err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if dst.TxID != 7 {
		t.Fatalf("TxID: got %d, want 7", dst.TxID)
	}
	if dst.NextInodeID != 5 {
		t.Fatalf("NextInodeID: got %d, want 5", dst.NextInodeID)
	}
	// Nil maps should be initialized to empty.
	if dst.Tenants == nil || len(dst.Tenants) != 0 {
		t.Fatalf("tenants should be empty map, got %v", dst.Tenants)
	}
	if dst.Inodes == nil || len(dst.Inodes) != 0 {
		t.Fatalf("inodes should be empty map, got %v", dst.Inodes)
	}
}

func TestMigrateV1ToV2_CorruptJSON(t *testing.T) {
	fsys := afero.NewMemMapFs()
	metaDir := "/meta"
	fsys.MkdirAll(metaDir, 0o755)

	afero.WriteFile(fsys, filepath.Join(metaDir, "checkpoint.json"), []byte("not valid json"), 0o600)

	err := migrateV1ToV2(fsys, metaDir)
	if err == nil {
		t.Fatal("expected error for corrupt JSON")
	}
}

func TestMigrateV1ToV2_StaleNextDirCleanup(t *testing.T) {
	fsys := afero.NewMemMapFs()
	metaDir := "/meta"
	fsys.MkdirAll(metaDir, 0o755)

	// Simulate a crashed previous migration: create checkpoint_next/.
	if err := fsys.MkdirAll(filepath.Join(metaDir, metaCheckpointNextDir), 0o755); err != nil {
		t.Fatalf("mkdir nextDir: %v", err)
	}
	// Old checkpoint.json exists (migration not yet done).
	v1 := metadataV1{Version: 1, TxID: 3}
	data, _ := json.Marshal(v1)
	afero.WriteFile(fsys, filepath.Join(metaDir, "checkpoint.json"), data, 0o600)

	// Migration should handle the stale state.
	if err := migrateV1ToV2(fsys, metaDir); err != nil {
		t.Fatalf("migration with stale nextDir: %v", err)
	}

	// After migration, nextDir should be cleaned up (by loadCheckpoint)
	// and checkpoint should be valid.
	dst := newMetadata()
	if err := loadCheckpoint(fsys, metaDir, dst); err != nil {
		t.Fatalf("loadCheckpoint after migration: %v", err)
	}
	if dst.TxID != 3 {
		t.Fatalf("TxID: got %d, want 3", dst.TxID)
	}
}
