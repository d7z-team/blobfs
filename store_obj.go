package blobfs

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// StatObject returns metadata for an active file object without opening its content.
func (s *Store) StatObject(ctx context.Context, tenantID, path string) (*ObjectInfo, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("stat", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("stat", path, err)
	}
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return nil, pathError("stat", path, err)
	}
	if inode.Kind != fileKindFile {
		return nil, pathError("stat", path, ErrIsDir)
	}
	info := objectInfoFromInode(inode, path)
	return &info, nil
}

// UpdateMetadata replaces user options on an active file without changing its content.
func (s *Store) UpdateMetadata(ctx context.Context, tenantID, path string, options map[string]string) (*ObjectInfo, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return nil, pathError("update metadata", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return nil, pathError("update metadata", path, err)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return nil, pathError("update metadata", path, err)
	}
	if inode.Kind != fileKindFile {
		return nil, pathError("update metadata", path, ErrIsDir)
	}
	next := cloneInode(inode)
	now := nowUnix()
	next.Options = copyOptions(options)
	next.Generation++
	next.MetadataGeneration++
	next.UpdatedAt = now
	next.CTime = now
	if err := s.commitMetaLocked([]metaOp{{Type: "put_inode", Inode: next}}); err != nil {
		return nil, err
	}
	info := objectInfoFromInode(next, path)
	return &info, nil
}

// DeleteObject removes one active file from the namespace and releases its references.
func (s *Store) DeleteObject(ctx context.Context, tenantID, path string) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return pathError("delete", tenantID, err)
	}
	path, err := normalizePath(path, s.cfg)
	if err != nil {
		return pathError("delete", path, err)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return pathError("delete", path, err)
	}
	if inode.Kind != fileKindFile {
		return pathError("delete", path, ErrIsDir)
	}
	parentID, name, err := s.resolveParentLocked(tenantID, path)
	if err != nil {
		return pathError("delete", path, err)
	}
	now := nowUnix()
	next := cloneInode(inode)
	next.State = fileStateDeleted
	next.DeletedAt = now
	next.UpdatedAt = now
	next.CTime = now
	next.Generation++
	ops := []metaOp{
		{Type: "put_inode", Inode: next},
		{Type: "delete_dirent", ParentID: parentID, Name: name},
	}
	addDeletedManifestOpsLocked(s.meta, inode.ManifestID, &ops, now, &s.refCountWarnings)
	return s.commitMetaLocked(ops)
}

// DeleteTenant immediately detaches the tenant namespace. Child inodes, manifests,
// chunks, and segment files are reclaimed asynchronously by GC.
func (s *Store) DeleteTenant(ctx context.Context, tenantID string) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return err
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	rootID := s.meta.Tenants[tenantID]
	if rootID == 0 {
		return fs.ErrNotExist
	}
	root := s.meta.Inodes[rootID]
	if root == nil || root.State != fileStateActive {
		return fs.ErrNotExist
	}
	now := nowUnix()
	next := cloneInode(root)
	next.State = fileStateDeleted
	next.DeletedAt = now
	next.UpdatedAt = now
	next.Generation++
	return s.commitMetaLocked([]metaOp{
		{Type: "del_tenant", TenantID: tenantID},
		{Type: "put_inode", Inode: next},
	})
}

func (s *Store) ensureTenantRoot(tenantID string) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.meta.Tenants[tenantID] != 0 {
		return nil
	}
	now := nowUnix()
	root := &inodeRecord{
		InodeID:             s.nextInodeIDLocked(),
		TenantID:            tenantID,
		Kind:                fileKindDir,
		Name:                "",
		State:               fileStateActive,
		Mode:                uint32(os.ModeDir | 0o755),
		Generation:          1,
		MetadataGeneration:  1,
		NamespaceGeneration: 1,
		CreatedAt:           now,
		UpdatedAt:           now,
		CTime:               now,
		MTime:               now,
		ModTime:             now,
	}
	return s.commitMetaLocked([]metaOp{
		{Type: "put_tenant", TenantID: tenantID, ChildID: root.InodeID},
		{Type: "put_inode", Inode: root},
	})
}

func objectInfoFromInode(inode *inodeRecord, path string) ObjectInfo {
	return ObjectInfo{
		FileID:     inodeFileID(inode.InodeID),
		TenantID:   inode.TenantID,
		Path:       path,
		Size:       inode.Size,
		FileHash:   inode.FileHash,
		ManifestID: inode.ManifestID,
		State:      inode.State,
		Readable:   inode.State == fileStateActive,
		Writable:   inode.State == fileStateActive,
		Reason:     inode.DegradedReason,
		Generation: inode.Generation,
		CreatedAt:  time.Unix(0, inode.CreatedAt),
		UpdatedAt:  time.Unix(0, inode.UpdatedAt),
		Options:    copyOptions(inode.Options),
	}
}

func inodeFileID(id uint64) string {
	return fmt.Sprintf("inode-%016d", id)
}
