package blobfs

import (
	"encoding/json"
	"errors"
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
	path, err := normalizePath(path)
	if err != nil {
		return err
	}
	lock := b.mLock(path).Lock(false)
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
// 请不要直接使用
//
//goland:noinspection GoUnhandledErrorResult
func (b *FSBlob) Transparent(path string, input io.ReadCloser, options map[string]string) (io.ReadCloser, error) {
	path, err := normalizePath(path)
	if err != nil {
		return nil, err
	}
	lock := b.mLock(path).Lock(false)
	wait := sync.WaitGroup{}
	errCh := make(chan error, 2)
	wait.Add(2)
	p1, w1 := io.Pipe()
	p2, w2 := io.Pipe()
	go func(reader *io.PipeReader) {
		defer wait.Done()
		if err := b.pushInternal(path, reader, options); err != nil {
			_ = reader.CloseWithError(err)
			errCh <- err
			return
		}
		_ = reader.Close()
		errCh <- nil
	}(p2)
	go func(i io.ReadCloser, w1 *io.PipeWriter, w2 *io.PipeWriter) {
		defer wait.Done()
		defer i.Close()
		if _, err := io.Copy(io.MultiWriter(w1, w2), i); err != nil {
			_ = w1.CloseWithError(err)
			_ = w2.CloseWithError(err)
			errCh <- err
			return
		}
		err := errors.Join(w1.Close(), w2.Close())
		errCh <- err
	}(input, w1, w2)
	return &customCloserReader{
		Reader: p1,
		closer: func() error {
			closeErr := p1.Close()
			wait.Wait()
			close(errCh)
			lock.Close()
			var err error
			if closeErr != nil && !errors.Is(closeErr, io.ErrClosedPipe) {
				err = closeErr
			}
			for item := range errCh {
				if item != nil && !errors.Is(item, io.ErrClosedPipe) {
					err = errors.Join(err, item)
				}
			}
			return err
		},
	}, nil
}

type customCloserReader struct {
	io.Reader
	once     sync.Once
	closeErr error
	closer   func() error
}

func (c *customCloserReader) Close() error {
	c.once.Do(func() {
		c.closeErr = c.closer()
	})
	return c.closeErr
}

// Push 将文件推入到块中
func (b *FSBlob) Push(path string, input io.Reader, options map[string]string) error {
	path, err := normalizePath(path)
	if err != nil {
		return err
	}
	lock := b.mLock(path).Lock(false)
	defer lock.Close()
	return b.pushInternal(path, input, options)
}

func (b *FSBlob) pushInternal(path string, input io.Reader, options map[string]string) error {
	if options == nil {
		options = make(map[string]string)
	} else {
		copied := make(map[string]string, len(options))
		for k, v := range options {
			copied[k] = v
		}
		options = copied
	}
	token, err := b.blob.create(input)
	if err != nil {
		return err
	}
	lastMeta, lastErr := b.metaLoad(path)
	blobChanged := lastErr != nil || lastMeta.Blob != token
	if blobChanged {
		if err = b.blob.Link(token); err != nil {
			return err
		}
	}
	if err = b.metaSave(path, &metaOptions{
		Blob:     token,
		CreateAt: time.Now(),
		Options:  options,
	}); err != nil {
		if blobChanged {
			_ = b.blob.Unlink(token)
		}
		return err
	}
	if blobChanged && lastErr == nil && validBlobToken(lastMeta.Blob) {
		if err = b.blob.Unlink(lastMeta.Blob); err != nil {
			return err
		}
	}
	return nil
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
	path, err := normalizePath(path)
	if err != nil {
		return nil, err
	}
	lock := b.mLock(path).Lock(true)
	defer lock.Close()
	meta, err := b.metaLoad(path)
	if err != nil {
		return nil, err
	}
	open, err := b.blob.open(meta.Blob)
	if err != nil {
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
	base, err := normalizePath(base)
	if err != nil {
		return err
	}
	date := time.Now().Add(-ttl)
	if err := filepath.Walk(filepath.Join(b.metaDir, base), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != ".meta" || info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		rel, err := filepath.Rel(b.metaDir, path)
		if err != nil {
			return err
		}
		cleanPath := filepath.ToSlash(rel)
		if cleanPath == ".meta" {
			cleanPath = ""
		} else {
			cleanPath = strings.TrimSuffix(cleanPath, "/.meta")
		}
		if regex != nil && !regex.MatchString(cleanPath) {
			// ignore regex
			return nil
		}
		return func() error {
			lock := b.metaLocker.Open(cleanPath).Lock(false)
			defer lock.Close()
			meta, err := b.metaLoad(cleanPath)
			if err != nil {
				return err
			}
			if meta.CreateAt.Before(date) {
				if validBlobToken(meta.Blob) {
					if err = b.blob.Unlink(meta.Blob); err != nil {
						return err
					}
				}
				if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			return nil
		}()
	}); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b *FSBlob) Child(name string) (Objects, error) {
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
	dir := filepath.Join(b.metaDir, path)
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".meta.tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	if _, err = temp.Write(meta); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempName)
		return err
	}
	if err = temp.Sync(); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempName)
		return err
	}
	if err = temp.Close(); err != nil {
		_ = os.Remove(tempName)
		return err
	}
	if err = os.Rename(tempName, filepath.Join(dir, ".meta")); err != nil {
		_ = os.Remove(tempName)
		return err
	}
	return nil
}
