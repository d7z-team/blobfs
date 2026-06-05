package blobfs

import (
	"errors"
	"io/fs"
	"os"
)

var (
	ErrNotEmpty = errors.New("directory not empty")
	ErrIsDir    = errors.New("is a directory")
	ErrNotDir   = errors.New("not a directory")
	ErrConflict = errors.New("file changed while handle was open")
	ErrTooLarge = errors.New("file too large")
)

func pathError(op, path string, err error) error {
	return &os.PathError{Op: op, Path: path, Err: err}
}

func notExist(op, path string) error {
	return pathError(op, path, fs.ErrNotExist)
}

func exists(op, path string) error {
	return pathError(op, path, fs.ErrExist)
}

func invalidPath(op, path string) error {
	return pathError(op, path, fs.ErrInvalid)
}
