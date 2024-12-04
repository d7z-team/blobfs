package blobfs

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type FSBlob struct {
	baseDir string
	metaDir string

	blob       *blob
	metaLocker *rwLockGroup
}

func (b *FSBlob) Close() error {
	// 卸载所有的写入
	return nil
}

func BlobFS(basedir string) (*FSBlob, error) {
	basedir, err := filepath.Abs(basedir)
	if err != nil {
		return nil, err
	}
	b, err := newBlob(filepath.Join(basedir, "blob"), filepath.Join(basedir, "cache"))
	if err != nil {
		return nil, err
	}
	result := &FSBlob{
		baseDir:    basedir,
		metaDir:    filepath.Join(basedir, "meta"),
		blob:       b,
		metaLocker: newRWLockGroup(),
	}
	if err := os.MkdirAll(result.baseDir, 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(result.metaDir, 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}

	meta := &metaOptions{}
	err = filepath.Walk(result.metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != ".meta" {
			return nil
		}
		metadata, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(metadata, meta); err != nil {
			return err
		}
		return result.blob.Link(meta.Blob)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *FSBlob) BlobGC() error {
	return b.blob.blobGC()
}
