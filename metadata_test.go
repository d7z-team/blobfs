package blobfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
)

func TestLoadMetadataRepairsSparseMetadata(t *testing.T) {
	fs := afero.NewMemMapFs()
	path := "/blobfs.json"
	if err := afero.WriteFile(fs, path, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write sparse metadata: %v", err)
	}

	meta, err := loadMetadata(fs, path)
	if err != nil {
		t.Fatalf("load sparse metadata: %v", err)
	}
	if meta.NextFileSeq != 1 || meta.NextSegmentSeq != 1 || meta.NextGCEpoch != 1 {
		t.Fatalf("sequences should be repaired: %+v", meta)
	}
	if meta.Files == nil || meta.Manifests == nil || meta.Chunks == nil || meta.Segments == nil {
		t.Fatalf("metadata maps should be initialized: %+v", meta)
	}
}

func TestOpenRejectsCorruptMetadata(t *testing.T) {
	fs := afero.NewMemMapFs()
	dir := "/blobfs"
	metaDir := filepath.Join(dir, "meta")
	if err := fs.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("create metadata dir: %v", err)
	}
	if err := afero.WriteFile(fs, filepath.Join(metaDir, "blobfs.json"), []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("write corrupt metadata: %v", err)
	}
	if _, err := OpenFS(fs, dir, testConfig()); err == nil {
		t.Fatalf("open should reject corrupt metadata")
	}
}

func TestSaveMetadataFailsWhenParentIsFile(t *testing.T) {
	fs := afero.NewOsFs()
	parent := filepath.Join(t.TempDir(), "meta")
	if err := os.WriteFile(parent, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}
	if err := saveMetadata(fs, filepath.Join(parent, "blobfs.json"), newMetadata()); err == nil {
		t.Fatalf("save metadata should fail when parent path is a file")
	}
}
