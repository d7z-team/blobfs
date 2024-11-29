package blobfs

import (
	"os"
	"path/filepath"
)

type FSBlob struct {
	baseDir string
	metaDir string

	blob       *blob
	metaLocker *RWLockGroup
}

func BlobFS(basedir string) (*FSBlob, error) {
	basedir, err := filepath.Abs(basedir)
	if err != nil {
		return nil, err
	}
	result := &FSBlob{
		baseDir:    basedir,
		metaDir:    filepath.Join(basedir, "meta"),
		blob:       newBlob(filepath.Join(basedir, "blob"), filepath.Join(basedir, "cache")),
		metaLocker: NewRWLockGroup(),
	}
	if err := os.MkdirAll(result.baseDir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(result.blob.blob, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(result.blob.cache, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(result.metaDir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	return result, nil
}
