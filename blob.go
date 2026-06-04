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

const blobTokenLength = sha256.Size * 2

func validBlobToken(token string) bool {
	if len(token) != blobTokenLength {
		return false
	}
	for _, item := range token {
		if (item < '0' || item > '9') && (item < 'a' || item > 'f') {
			return false
		}
	}
	return true
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
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}
		token := info.Name()
		if !validBlobToken(token) {
			return nil
		}
		if filepath.Clean(path) == filepath.Join(blobDir, token[:2], token[2:4], token) {
			b.linker.store.Store(token, 0)
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
	if err := os.MkdirAll(destDir, 0o755); err != nil {
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
	if stat != nil {
		existing, err := os.OpenFile(dest, os.O_RDONLY, 0o666)
		if err != nil {
			_ = os.Remove(temp.Name())
			return "", err
		}
		existingHash := sha256.New()
		_, err = io.Copy(existingHash, existing)
		_ = existing.Close()
		if err != nil {
			_ = os.Remove(temp.Name())
			return "", err
		}
		if fmt.Sprintf("%x", existingHash.Sum(nil)) == token {
			_ = os.Remove(temp.Name())
			b.Init(token)
			return token, nil
		}
	}
	err = os.Rename(temp.Name(), dest)
	if err != nil {
		_ = os.Remove(temp.Name())
		return "", err
	}
	b.Init(token)
	return token, nil
}

type BlobStat struct {
	file *os.File
	lock *lockerContent
}

func (b *BlobStat) Read(data []byte) (int, error) {
	return b.file.Read(data)
}

func (b *BlobStat) Seek(offset int64, whence int) (int64, error) {
	return b.file.Seek(offset, whence)
}

func (b *BlobStat) Close() error {
	err := b.file.Close()
	b.lock.Close()
	return err
}

func (b *blob) open(token string) (*BlobStat, error) {
	b.gcLocker.RLock()
	defer b.gcLocker.RUnlock()
	if !validBlobToken(token) {
		return nil, errors.New("invalid blob token")
	}
	if !b.linker.Exists(token) {
		return nil, errors.Join(os.ErrNotExist, errors.New("token not exists"))
	}
	open := b.locker.Open(token)
	lock := open.Lock(true)
	dest := filepath.Join(b.blob, token[:2], token[2:4], token)
	file, err := os.OpenFile(dest, os.O_RDONLY, 0o666)
	if err != nil {
		lock.Close()
		return nil, err
	}
	return &BlobStat{
		file: file,
		lock: lock,
	}, nil
}

func (b *blob) delete(token string) error {
	// Use Write Lock (Lock) to ensure exclusivity against create() (Reader) and blobGC() (Writer).
	// This prevents race conditions where a file is deleted while being created or accessed.
	b.gcLocker.Lock()
	defer b.gcLocker.Unlock()
	if !validBlobToken(token) {
		return errors.New("invalid blob token")
	}

	open := b.locker.Open(token)
	lock := open.Lock(false)
	defer lock.Close()
	dest := filepath.Join(b.blob, token[:2], token[2:4], token)
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		return err
	}
	b.linker.Delete(token)
	return nil
}

func (b *blob) blobGC() error {
	// gcLocker (Write) ensures this runs exclusively.
	// Note: Internally this acquires linkerLock -> tokenLock.
	// While 'delete' acquires tokenLock -> linkerLock, no deadlock occurs
	// because gcLocker serializes 'blobGC' and 'delete'.
	b.gcLocker.Lock()
	defer b.gcLocker.Unlock()
	return b.linker.Gc(func(token string) error {
		if !validBlobToken(token) {
			return nil
		}
		open := b.locker.Open(token)
		lock := open.Lock(false)
		defer lock.Close()
		dest := filepath.Join(b.blob, token[:2], token[2:4], token)
		if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	})
}
