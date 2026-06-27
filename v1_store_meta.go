package blobfs

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

// ---------------------------------------------------------------------------
// Version 1 checkpoint format — single monolithic checkpoint.json
// This file is FROZEN. Do not modify metadataV1 struct fields.
// ---------------------------------------------------------------------------

// metadataV1 is the exact JSON shape of v1 checkpoint.json.
// It uses the current record types (inodeRecord, manifestRecord, etc.)
// which are backward-compatible via omitempty tags.
// If a future version requires breaking changes to record types,
// the frozen copy of those types should be added here at that time.
type metadataV1 struct {
	Version        int                          `json:"version"`
	TxID           uint64                       `json:"txid"`
	NextInodeID    uint64                       `json:"next_inode_id"`
	NextSegmentSeq int64                        `json:"next_segment_seq"`
	NextGCEpoch    int64                        `json:"next_gc_epoch"`
	Tenants        map[string]uint64            `json:"tenants"`
	Inodes         map[uint64]*inodeRecord      `json:"inodes"`
	DirEntries     map[uint64]map[string]uint64 `json:"dir_entries"`
	Manifests      map[string]*manifestRecord   `json:"manifests"`
	Chunks         map[string]*chunkRecord      `json:"chunks"`
	Segments       map[string]*segmentRecord    `json:"segments"`
	GC             gcMetadata                   `json:"gc"`
}

func migrateV1ToV2(fs afero.Fs, metaDir string) error {
	// Idempotent: if v2 checkpoint already exists, just clean up the old file.
	if _, err := fs.Stat(filepath.Join(metaDir, metaCheckpointDir, "manifest.json")); err == nil {
		_ = fs.Remove(filepath.Join(metaDir, "checkpoint.json"))
		return nil
	}

	oldPath := filepath.Join(metaDir, "checkpoint.json")
	if _, err := fs.Stat(oldPath); os.IsNotExist(err) {
		return nil // nothing to migrate
	}

	data, err := afero.ReadFile(fs, oldPath)
	if err != nil {
		return err
	}
	var v1 metadataV1
	if err := json.Unmarshal(data, &v1); err != nil {
		return errors.New("v1 checkpoint.json is corrupt: " + err.Error())
	}

	// Build v2 metadata from v1 fields.
	meta := newMetadata()
	meta.Version = metaFormatVersion
	meta.TxID = v1.TxID
	meta.NextInodeID = v1.NextInodeID
	meta.NextSegmentSeq = v1.NextSegmentSeq
	meta.NextGCEpoch = v1.NextGCEpoch
	meta.Tenants = v1.Tenants
	meta.Inodes = v1.Inodes
	meta.DirEntries = v1.DirEntries
	meta.Manifests = v1.Manifests
	meta.Chunks = v1.Chunks
	meta.Segments = v1.Segments
	meta.GC = v1.GC

	// Nil maps → empty maps (v1 may have omitted empty maps in JSON).
	if meta.Tenants == nil {
		meta.Tenants = map[string]uint64{}
	}
	if meta.Inodes == nil {
		meta.Inodes = map[uint64]*inodeRecord{}
	}
	if meta.DirEntries == nil {
		meta.DirEntries = map[uint64]map[string]uint64{}
	}
	if meta.Manifests == nil {
		meta.Manifests = map[string]*manifestRecord{}
	}
	if meta.Chunks == nil {
		meta.Chunks = map[string]*chunkRecord{}
	}
	if meta.Segments == nil {
		meta.Segments = map[string]*segmentRecord{}
	}

	// Full checkpoint.
	if err := saveCheckpoint(fs, metaDir, meta); err != nil {
		return err
	}

	// Atomically remove old checkpoint.json after successful save.
	_ = fs.Remove(oldPath)
	return nil
}
