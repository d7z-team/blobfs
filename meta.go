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

type PullContent struct {
	io.ReadSeekCloser
	CreateAt time.Time
	ETag     string
	Options  map[string]string
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
	path = pathClean(path)
	if options == nil {
		options = make(map[string]string)
	}
	lock := b.metaLocker.Open(path).Lock(false)
	defer lock.Close()
	token, err := b.blob.create(input)
	if err != nil {
		return err
	}
	if lastMeta, err := b.metaLoad(path); err == nil {
		if err = b.blob.Unlink(lastMeta.Blob); err != nil {
			return err
		}
	}
	if err = b.blob.Link(token); err != nil {
		return err
	}
	return b.metaSave(path, &metaOptions{
		Blob:     token,
		CreateAt: time.Now(),
		Options:  options,
	})
}

func (b *FSBlob) PullOrNil(path string) *PullContent {
	pull, err := b.Pull(path)
	if err != nil {
		return nil
	}
	return pull
}

// Pull Push 从块中拉取文件
func (b *FSBlob) Pull(path string) (*PullContent, error) {
	path = pathClean(path)
	lock := b.metaLocker.Open(path).Lock(true)
	defer lock.Close()
	meta, err := b.metaLoad(pathClean(path))
	if err != nil {
		b.metaLocker.Del(path)
		return nil, err
	}
	open, err := b.blob.open(meta.Blob)
	if err != nil {
		b.metaLocker.Del(path)
		return nil, err
	}
	return &PullContent{
		ReadSeekCloser: open,
		CreateAt:       meta.CreateAt,
		ETag:           meta.Blob,
		Options:        meta.Options,
	}, nil
}

func (b *FSBlob) Remove(pattern string, ttl time.Duration) error {
	date := time.Now().Add(-ttl)
	return filepath.Walk(filepath.Join(b.metaDir, pathClean(pattern)), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != ".meta" {
			return nil
		}
		fixedPath := strings.TrimSuffix(strings.TrimPrefix(path, b.metaDir+string(filepath.Separator)), "/.meta")
		lock := b.metaLocker.Open(fixedPath).Lock(true)
		defer lock.Close()
		meta, err := b.metaLoad(fixedPath)
		if err != nil {
			return err
		}

		if meta.CreateAt.Before(date) {
			if err = b.blob.Unlink(meta.Blob); err != nil {
				return err
			}
			if err = os.Remove(path); err != nil {
				return err
			}
			b.metaLocker.Del(fixedPath)
		}
		return nil
	})
}

func (b *FSBlob) metaLoad(path string) (*metaOptions, error) {
	data, err := os.ReadFile(filepath.Join(b.metaDir, path, ".meta"))
	if err != nil {
		return nil, err
	}
	meta := &metaOptions{}
	if err = json.Unmarshal(data, meta); err != nil {
		return nil, err
	}
	return meta, nil
}
func (b *FSBlob) metaSave(path string, options *metaOptions) error {
	meta, err := json.Marshal(options)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Join(b.metaDir, path), 0755); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = os.WriteFile(filepath.Join(b.metaDir, path, ".meta"), meta, 0600); err != nil {
		return err
	}
	return nil
}
