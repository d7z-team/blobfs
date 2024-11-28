package blobfs

import (
	"os"
	"path/filepath"
)

type FSBlob struct {
	baseDir  string
	cacheDir string
	metaDir  string
	blobDir  string

	blob *blob
}

func BlobFS(basedir string) (*FSBlob, error) {
	basedir, err := filepath.Abs(basedir)
	if err != nil {
		return nil, err
	}
	result := &FSBlob{
		baseDir:  basedir,
		cacheDir: filepath.Join(basedir, "cache"),
		metaDir:  filepath.Join(basedir, "meta"),
		blobDir:  filepath.Join(basedir, "blob"),
	}
	if err := os.MkdirAll(result.baseDir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(result.cacheDir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(result.metaDir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	result.blob = newBlob(result.blobDir, result.cacheDir)
	return result, nil
}
