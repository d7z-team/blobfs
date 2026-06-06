package blobfs

import (
	"errors"
	"io/fs"
	"path"
)

var (
	_ fs.FS        = tenantFS{}
	_ fs.StatFS    = tenantFS{}
	_ fs.ReadDirFS = tenantFS{}
)

type tenantFS struct {
	store    *Store
	tenantID string
}

// TenantFS returns an io/fs view rooted at tenantID.
func (s *Store) TenantFS(tenantID string) fs.FS {
	return tenantFS{store: s, tenantID: tenantID}
}

func (t tenantFS) Open(name string) (fs.File, error) {
	fullPath, err := t.fullPath("open", name)
	if err != nil {
		return nil, err
	}
	return t.store.Open(fullPath)
}

func (t tenantFS) Stat(name string) (fs.FileInfo, error) {
	fullPath, err := t.fullPath("stat", name)
	if err != nil {
		return nil, err
	}
	return t.store.Stat(fullPath)
}

func (t tenantFS) ReadDir(name string) ([]fs.DirEntry, error) {
	fullPath, err := t.fullPath("readdir", name)
	if err != nil {
		return nil, err
	}
	file, err := t.store.Open(fullPath)
	if err != nil {
		return nil, err
	}
	dir, ok := file.(fs.ReadDirFile)
	if !ok {
		closeErr := file.Close()
		return nil, errors.Join(fs.ErrInvalid, closeErr)
	}
	entries, readErr := dir.ReadDir(-1)
	return entries, errors.Join(readErr, file.Close())
}

func (t tenantFS) fullPath(op, name string) (string, error) {
	if err := validateTenantID(t.tenantID, t.store.cfg); err != nil {
		return "", pathError(op, t.tenantID, err)
	}
	if name == "." {
		return t.tenantID, nil
	}
	if !fs.ValidPath(name) {
		return "", invalidPath(op, name)
	}
	return path.Join(t.tenantID, name), nil
}
