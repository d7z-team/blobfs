package blobfs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type blob struct {
	blob  string
	cache string

	locker *RWLockGroup
}

func newBlob(blobDir string, cacheDir string) *blob {
	return &blob{
		blob:   blobDir,
		cache:  cacheDir,
		locker: NewRWLockGroup(),
	}
}

func (b *blob) create(input io.Reader) (token string, err error) {
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
	if err := os.MkdirAll(destDir, 0755); err != nil && os.IsExist(err) {
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
	return token, os.Rename(temp.Name(), dest)
}

func (b *blob) open(token string) (io.ReadSeekCloser, error) {
	if len(token) < 5 {
		return nil, errors.New("token too short")
	}
	dest := filepath.Join(b.blob, token[:2], token[2:4], token)
	return os.OpenFile(dest, os.O_RDONLY, 0666)
}

func (b *blob) delete(token string) error {
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
	return nil
}
