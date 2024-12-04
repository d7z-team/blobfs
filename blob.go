package blobfs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type blob struct {
	blob  string
	cache string

	locker *rwLockGroup

	gcLocker sync.RWMutex

	*linker
}

func newBlob(blobDir string, cacheDir string) (*blob, error) {
	b := &blob{
		blob:     blobDir,
		cache:    cacheDir,
		locker:   newRWLockGroup(),
		linker:   newLinker(),
		gcLocker: sync.RWMutex{},
	}

	if err := os.MkdirAll(b.blob, 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(b.cache, 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	return b, filepath.Walk(blobDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Mode().IsRegular() {
			b.linker.store.Store(info.Name(), 0)
		}
		return nil
	})
}

func (b *blob) create(input io.Reader) (token string, err error) {
	b.gcLocker.RLock()
	defer b.gcLocker.RUnlock()
	temp, err := os.CreateTemp(b.cache, "cache.*")
	if err != nil {
		return "", err
	}
	sha256Hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(temp, sha256Hash), input)
	_ = temp.Close()
	if err != nil {
		_ = os.Remove(temp.Name())
		return "", err
	}
	token = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	open := b.locker.Open(token)
	lock := open.Lock(false)
	defer lock.Close()
	destDir := filepath.Join(b.blob, token[:2], token[2:4])
	if err := os.MkdirAll(destDir, 0o755); err != nil && os.IsExist(err) {
		_ = os.Remove(temp.Name())
		return "", err
	}
	dest := filepath.Join(destDir, token)
	stat, err := os.Stat(dest)
	if err != nil && !os.IsNotExist(err) {
		_ = os.Remove(temp.Name())
		return "", err
	}
	if stat != nil && stat.IsDir() {
		_ = os.Remove(temp.Name())
		return "", errors.New("blob already exists and is a directory")
	}
	err = os.Rename(temp.Name(), dest)
	if err != nil {
		_ = os.Remove(temp.Name())
		return "", err
	}
	b.Init(token)
	return token, nil
}

func (b *blob) open(token string) (io.ReadSeekCloser, error) {
	b.gcLocker.RLock()
	defer b.gcLocker.RUnlock()
	if len(token) < 5 {
		return nil, errors.New("token too short")
	}
	if !b.linker.Exists(token) {
		return nil, errors.Join(os.ErrNotExist, errors.New("token not exists"))
	}
	dest := filepath.Join(b.blob, token[:2], token[2:4], token)
	return os.OpenFile(dest, os.O_RDONLY, 0o666)
}

func (b *blob) delete(token string) error {
	b.gcLocker.RLock()
	defer b.gcLocker.RUnlock()
	if len(token) < 5 {
		return errors.New("token too short")
	}

	open := b.locker.Open(token)
	lock := open.Lock(false)
	defer lock.Close()
	dest := filepath.Join(b.blob, token[:2], token[2:4], token)
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		return err
	}
	b.locker.Del(token)
	b.linker.Delete(token)
	return nil
}

func (b *blob) blobGC() error {
	b.gcLocker.Lock()
	defer b.gcLocker.Unlock()
	return b.linker.Gc(func(token string) error {
		open := b.locker.Open(token)
		lock := open.Lock(false)
		defer lock.Close()
		dest := filepath.Join(b.blob, token[:2], token[2:4], token)
		if err := os.Remove(dest); err != nil {
			return err
		}
		b.locker.Del(token)
		return nil
	})
}
