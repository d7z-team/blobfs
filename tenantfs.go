package blobfs

import (
	"io/fs"
	"path"
)

var _ fs.FS = tenantFS{}

type tenantFS struct {
	store    *Store
	tenantID string
}

// TenantFS returns an io/fs view rooted at tenantID.
func (s *Store) TenantFS(tenantID string) fs.FS {
	return tenantFS{store: s, tenantID: tenantID}
}

func (t tenantFS) Open(name string) (fs.File, error) {
	if err := validateTenantID(t.tenantID, t.store.cfg); err != nil {
		return nil, pathError("open", t.tenantID, err)
	}
	if name == "." {
		return t.store.Open(t.tenantID)
	}
	if !fs.ValidPath(name) {
		return nil, invalidPath("open", name)
	}
	return t.store.Open(path.Join(t.tenantID, name))
}
