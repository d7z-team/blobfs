package blobfs

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type metaOptions struct {
	Blob     string            `json:"blob"`
	CreateAt time.Time         `json:"create_at"`
	Options  map[string]string `json:"extras"`
}

func pathClean(path string) string {
	fsPath := strings.Split(strings.Trim(filepath.ToSlash(filepath.Clean(path)), "/"), "/")
	for i, item := range fsPath {
		if item == ".meta" || item == ".blob" {
			fsPath[i] = "@" + item
		}
	}
	return strings.Join(fsPath, "/")
}

// Push 将文件推入到块中
func (b *FSBlob) Push(path string, input io.Reader, options map[string]string) error {
	if options == nil {
		options = make(map[string]string)
	}
	lock := b.metaLocker.Open(path).Lock(false)
	defer lock.Close()
	token, err := b.blob.create(input)
	if err != nil {
		return err
	}
	meta, err := json.Marshal(metaOptions{
		Blob:     token,
		CreateAt: time.Now(),
		Options:  options,
	})
	if err != nil {
		return err
	}
	path = filepath.Join(b.metaDir, pathClean(path))
	if err = os.MkdirAll(path, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	metaPath := filepath.Join(path, ".meta")
	if err = os.WriteFile(metaPath, meta, 0600); err != nil {
		return err
	}
	return nil
}

// Pull Push 从块中拉取文件
func (b *FSBlob) Pull(path string) (io.ReadSeekCloser, error) {
	lock := b.metaLocker.Open(path).Lock(true)
	defer lock.Close()
	path = filepath.Join(b.metaDir, pathClean(path))
	metaPath := filepath.Join(path, ".meta")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}
	meta := &metaOptions{}
	if err = json.Unmarshal(data, meta); err != nil {
		return nil, err
	}
	return b.blob.open(meta.Blob)
}

func (b *FSBlob) Remove(pattern string, ttl time.Duration) error {
	return filepath.Walk(filepath.Join(pattern, b.metaDir), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Name() != ".meta" {
			return nil
		}
		metadata, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		meta := &metaOptions{}
		if err = json.Unmarshal(metadata, meta); err != nil {
			return err
		}
		return nil
	})
}
