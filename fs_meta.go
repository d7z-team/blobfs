package blobfs

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type metaOptions struct {
	Blob     string            `json:"blob"`
	CreateAt time.Time         `json:"create_at"`
	Options  map[string]string `json:"extras"`
}

func (b *FSBlob) Refresh(path string) error {
	path = pathClean(path)
	lock := b.metaLocker.Open(path).Lock(false)
	defer lock.Close()
	if lastMeta, err := b.metaLoad(path); err == nil {
		lastMeta.CreateAt = time.Now()
		return b.metaSave(path, lastMeta)
	} else {
		return err
	}
}

// Transparent 透传内容
//
//goland:noinspection GoUnhandledErrorResult
func (b *FSBlob) Transparent(path string, input io.ReadCloser, options map[string]string) io.ReadCloser {
	rBlob, w1 := io.Pipe()
	rSync, w2 := io.Pipe()
	go func() {
		defer w1.Close()
		defer w2.Close()
		defer input.Close()
		_, err := io.Copy(io.MultiWriter(w1, w2), input)
		if err != nil {
			_ = w1.CloseWithError(err)
			_ = w2.CloseWithError(err)
		}
	}()
	go func() {
		defer rBlob.Close()
		if err := b.Push(path, rBlob, options); err != nil {
			_ = rBlob.CloseWithError(err)
		}
	}()
	return rSync
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

func (b *FSBlob) Remove(base string, regex *regexp.Regexp, ttl time.Duration) error {
	date := time.Now().Add(-ttl)
	if err := filepath.Walk(filepath.Join(b.metaDir, pathClean(base)), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != ".meta" {
			return nil
		}
		fixedPath := strings.TrimSuffix(strings.TrimPrefix(path, b.metaDir+string(filepath.Separator)), "/.meta")
		if regex != nil && !regex.MatchString(fixedPath) {
			// ignore regex
			return nil
		}
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
	}); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b *FSBlob) Child(name string) Objects {
	return newChildObjects(b, name)
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
	if err = os.MkdirAll(filepath.Join(b.metaDir, path), 0o755); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = os.WriteFile(filepath.Join(b.metaDir, path, ".meta"), meta, 0o600); err != nil {
		return err
	}
	return nil
}
