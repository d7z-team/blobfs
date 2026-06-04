package blobfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type FSBlob struct {
	baseDir string
	metaDir string

	blob       *blob
	metaLocker *rwLockGroup
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

	err = filepath.Walk(result.metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != ".meta" || info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		metadata, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		meta := &metaOptions{}
		if err = json.Unmarshal(metadata, meta); err != nil {
			return nil
		}
		if !validBlobToken(meta.Blob) || !result.blob.Exists(meta.Blob) {
			return nil
		}
		return result.blob.Link(meta.Blob)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *FSBlob) mLock(path string) *rwLocker {
	return b.metaLocker.Open(path)
}

func normalizePath(path string) (string, error) {
	path = strings.ReplaceAll(filepath.ToSlash(path), "\\", "/")
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || (len(path) > 1 && path[1] == ':') {
		return "", fmt.Errorf("invalid object path %q: absolute paths are not allowed", path)
	}
	parts := strings.Split(path, "/")
	clean := make([]string, 0, len(parts))
	for _, item := range parts {
		switch item {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("invalid object path %q: parent traversal is not allowed", path)
		case ".meta", ".blob":
			clean = append(clean, "@"+item)
		default:
			clean = append(clean, item)
		}
	}
	return strings.Join(clean, "/"), nil
}

func (b *FSBlob) BlobGC() error {
	return b.blob.blobGC()
}
