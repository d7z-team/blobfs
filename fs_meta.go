package blobfs

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type metaOptions struct {
	Blob     string            `json:"blob"`
	CreateAt time.Time         `json:"create_at"`
	Options  map[string]string `json:"extras"`
}

func (b *FSBlob) Cleanup(path string) error {
	lock := b.mLock(path).Lock(false)
	defer lock.Close()
	path = b.safePath(path)
	if lastMeta, err := b.metaLoad(path); err == nil {
		lastMeta.CreateAt = time.Now()
		return b.metaSave(path, lastMeta)
	} else {
		return err
	}
}

// Transparent 透传内容
//
// 请不要直接使用
//
//goland:noinspection GoUnhandledErrorResult
func (b *FSBlob) Transparent(path string, input io.ReadCloser, options map[string]string) io.ReadCloser {
	lock := b.mLock(path).Lock(false)
	wait := sync.WaitGroup{}
	wait.Add(2)
	p1, w1 := io.Pipe()
	p2, w2 := io.Pipe()
	go func(reader *io.PipeReader) {
		defer wait.Done()
		if err := b.pushInternal(path, reader, options); err != nil {
			reader.CloseWithError(err)
		}
		reader.Close()
	}(p2)
	go func(i io.ReadCloser, w1 *io.PipeWriter, w2 *io.PipeWriter) {
		defer wait.Done()
		defer i.Close()
		if _, err := io.Copy(io.MultiWriter(w1, w2), i); err != nil {
			_ = w1.CloseWithError(err)
			_ = w2.CloseWithError(err)
		}
		_ = w1.Close()
		_ = w2.Close()
	}(input, w1, w2)
	return &customCloserReader{
		Reader: p1,
		closer: func() error {
			err := p1.Close()
			wait.Wait()
			lock.close()
			return err
		},
	}
}

type customCloserReader struct {
	io.Reader
	closed bool
	closer func() error
}

func (c *customCloserReader) Close() error {
	return c.closer()
}

// Push 将文件推入到块中
func (b *FSBlob) Push(path string, input io.Reader, options map[string]string) error {
	lock := b.mLock(path).Lock(false)
	defer lock.Close()
	return b.pushInternal(path, input, options)
}

func (b *FSBlob) pushInternal(path string, input io.Reader, options map[string]string) error {
	path = b.safePath(path)
	if options == nil {
		options = make(map[string]string)
	}
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
	lock := b.mLock(path).Lock(true)
	defer lock.Close()
	path = b.safePath(path)
	meta, err := b.metaLoad(path)
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
	if err := filepath.Walk(filepath.Join(b.metaDir, b.safePath(base)), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != ".meta" {
			return nil
		}
		prefix := strings.TrimPrefix(path, b.metaDir+string(filepath.Separator))
		cleanPath := strings.TrimSuffix(filepath.ToSlash(prefix), "/.meta")
		if regex != nil && !regex.MatchString(cleanPath) {
			// ignore regex
			return nil
		}
		lock := b.metaLocker.Open(cleanPath).Lock(true)
		defer lock.Close()
		meta, err := b.metaLoad(cleanPath)
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
			b.metaLocker.Del(cleanPath)
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
