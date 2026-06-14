package blobfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
)

var (
	ErrNotEmpty                 = errors.New("directory not empty")
	ErrIsDir                    = errors.New("is a directory")
	ErrNotDir                   = errors.New("not a directory")
	ErrConflict                 = errors.New("file changed while handle was open")
	ErrTooLarge                 = errors.New("file too large")
	ErrTooManyOpenWriteSessions = errors.New("too many open write sessions")
	ErrCorrupt                  = errors.New("blobfs corruption detected")
	ErrNilContext               = errors.New("context is nil")
	ErrNilFilesystem            = errors.New("filesystem is nil")
	ErrNilReader                = errors.New("input reader is nil")
	ErrInvalidRange             = errors.New("range offset and length must be non-negative")
	ErrReaderClosed             = errors.New("reader is closed")
	ErrInvalidSeek              = errors.New("invalid seek")
)

var (
	errMetadataLogClosed = fs.ErrClosed
	errManifestNotFound  = fs.ErrNotExist
	errChunkNotReadable  = errors.New("chunk not readable")
	errChunkHashMismatch = errors.New("chunk hash mismatch")
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

func contextError(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
