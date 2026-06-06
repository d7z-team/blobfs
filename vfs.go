package blobfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/afero"
)

const defaultVFSMode = 0o644

var _ afero.Fs = (*Store)(nil)

func (s *Store) Create(name string) (afero.File, error) {
	return s.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, defaultVFSMode)
}

func (s *Store) Open(name string) (afero.File, error) {
	return s.OpenFile(name, os.O_RDONLY, 0)
}

func (s *Store) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return s.OpenFileContext(s.ctx, name, flag, perm)
}

// OpenFileContext opens a VFS file and uses ctx for reads needed during open and for later Sync/Close commits.
func (s *Store) OpenFileContext(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return nil, pathError("open", name, err)
	}
	writable := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0
	if root {
		if writable {
			return nil, pathError("open", name, fs.ErrInvalid)
		}
		return s.openDirFile(name, tenantID, path, root), nil
	}
	if path == "" {
		if writable {
			return nil, pathError("open", name, fs.ErrInvalid)
		}
		s.metaMu.RLock()
		_, err := s.resolvePathLocked(tenantID, "")
		s.metaMu.RUnlock()
		if err != nil {
			return nil, pathError("open", name, err)
		}
		return s.openDirFile(name, tenantID, path, false), nil
	}
	info, err := s.vfsNodeInfo(tenantID, path)
	if err != nil {
		if flag&os.O_CREATE == 0 {
			return nil, pathError("open", name, err)
		}
		if err := s.ensureTenantRoot(tenantID); err != nil {
			return nil, err
		}
		s.metaMu.RLock()
		_, _, parentErr := s.resolveParentLocked(tenantID, path)
		s.metaMu.RUnlock()
		if parentErr != nil {
			return nil, pathError("open", name, parentErr)
		}
	} else {
		if info.isDir {
			if writable {
				return nil, pathError("open", name, ErrIsDir)
			}
			return s.openDirFile(name, tenantID, path, false), nil
		}
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			return nil, exists("open", name)
		}
	}
	if !writable {
		reader, err := s.OpenObject(ctx, tenantID, path)
		if err != nil {
			return nil, err
		}
		return &blobVFSFile{name: name, reader: reader, size: info.size, mode: info.mode, modTime: info.modTime}, nil
	}
	session, sessionName, err := s.createWriteSession()
	if err != nil {
		return nil, err
	}
	cleanupSession := func() {
		_ = session.Close()
		_ = s.fs.Remove(sessionName)
		s.writeSessionMu.Lock()
		s.openWriteSessions--
		s.writeSessionMu.Unlock()
	}
	baseGeneration := uint64(0)
	options := map[string]string{}
	mode := s.regularFileMode(perm)
	modTime := time.Now()
	size := int64(0)
	if info.exists {
		baseGeneration = info.generation
		options = copyOptions(info.options)
		mode = s.regularFileMode(info.mode)
		modTime = info.modTime
		if flag&os.O_TRUNC == 0 {
			reader, err := s.OpenObject(ctx, tenantID, path)
			if err != nil {
				cleanupSession()
				return nil, err
			}
			size, err = io.Copy(session, reader)
			_ = reader.Close()
			if err != nil {
				cleanupSession()
				return nil, err
			}
		}
	}
	offset := int64(0)
	if flag&os.O_APPEND != 0 {
		offset = size
	}
	return &blobVFSFile{
		store:          s,
		ctx:            ctx,
		name:           name,
		tenantID:       tenantID,
		path:           path,
		session:        session,
		sessionName:    sessionName,
		size:           size,
		offset:         offset,
		mode:           mode.Perm(),
		modTime:        modTime,
		writable:       true,
		append:         flag&os.O_APPEND != 0,
		dirty:          !info.exists || flag&(os.O_CREATE|os.O_TRUNC) != 0,
		options:        options,
		baseGeneration: baseGeneration,
	}, nil
}

func (s *Store) Mkdir(name string, perm os.FileMode) error {
	if err := s.beginOp(s.ctx); err != nil {
		return err
	}
	defer s.endOp()
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("mkdir", name, err)
	}
	if root {
		return exists("mkdir", name)
	}
	if path == "" {
		s.metaMu.RLock()
		tenantExists := s.meta.Tenants[tenantID] != 0 && s.activeInodeLocked(s.meta.Tenants[tenantID]) != nil
		s.metaMu.RUnlock()
		if tenantExists {
			return exists("mkdir", name)
		}
		return s.ensureTenantRoot(tenantID)
	}
	if err := s.ensureTenantRoot(tenantID); err != nil {
		return err
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	parentID, base, err := s.resolveParentLocked(tenantID, path)
	if err != nil {
		return pathError("mkdir", name, err)
	}
	if child := s.meta.DirEntries[parentID][base]; child != 0 && s.activeInodeLocked(child) != nil {
		return exists("mkdir", name)
	}
	now := nowUnix()
	inode := &inodeRecord{
		InodeID:             s.nextInodeIDLocked(),
		TenantID:            tenantID,
		Kind:                fileKindDir,
		ParentInode:         parentID,
		Name:                base,
		State:               fileStateActive,
		Mode:                uint32(os.ModeDir | directoryMode(perm)),
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
		{Type: "put_inode", Inode: inode},
		{Type: "put_dirent", ParentID: parentID, Name: base, ChildID: inode.InodeID},
	})
}

func (s *Store) MkdirAll(name string, perm os.FileMode) error {
	if err := s.beginOp(s.ctx); err != nil {
		return err
	}
	defer s.endOp()
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("mkdir", name, err)
	}
	if root {
		return nil
	}
	if err := s.ensureTenantRoot(tenantID); err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	currentID := s.meta.Tenants[tenantID]
	now := nowUnix()
	pendingDirs := map[uint64]*inodeRecord{}
	ops := []metaOp{}
	for _, part := range strings.Split(path, "/") {
		current := s.activeInodeLocked(currentID)
		if current == nil {
			current = pendingDirs[currentID]
		}
		if current == nil || current.Kind != fileKindDir {
			return pathError("mkdir", name, ErrNotDir)
		}
		childID := s.meta.DirEntries[currentID][part]
		if childID != 0 {
			child := s.activeInodeLocked(childID)
			if child == nil {
				return pathError("mkdir", name, fs.ErrNotExist)
			}
			if child.Kind != fileKindDir {
				return pathError("mkdir", name, ErrNotDir)
			}
			currentID = childID
			continue
		}
		inode := &inodeRecord{
			InodeID:             s.nextInodeIDLocked(),
			TenantID:            tenantID,
			Kind:                fileKindDir,
			ParentInode:         currentID,
			Name:                part,
			State:               fileStateActive,
			Mode:                uint32(os.ModeDir | directoryMode(perm)),
			Generation:          1,
			MetadataGeneration:  1,
			NamespaceGeneration: 1,
			CreatedAt:           now,
			UpdatedAt:           now,
			CTime:               now,
			MTime:               now,
			ModTime:             now,
		}
		pendingDirs[inode.InodeID] = inode
		ops = append(ops, metaOp{Type: "put_inode", Inode: inode}, metaOp{Type: "put_dirent", ParentID: currentID, Name: part, ChildID: inode.InodeID})
		currentID = inode.InodeID
	}
	return s.commitMetaLocked(ops)
}

func (s *Store) Remove(name string) error {
	if err := s.beginOp(s.ctx); err != nil {
		return err
	}
	defer s.endOp()
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("remove", name, err)
	}
	if root || path == "" {
		return pathError("remove", name, fs.ErrInvalid)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return pathError("remove", name, err)
	}
	if inode.Kind == fileKindDir && len(s.meta.DirEntries[inode.InodeID]) > 0 {
		return pathError("remove", name, ErrNotEmpty)
	}
	parentID, base, err := s.resolveParentLocked(tenantID, path)
	if err != nil {
		return pathError("remove", name, err)
	}
	now := nowUnix()
	next := cloneInode(inode)
	next.State = fileStateDeleted
	next.DeletedAt = now
	next.UpdatedAt = now
	next.CTime = now
	next.Generation++
	ops := []metaOp{{Type: "put_inode", Inode: next}, {Type: "delete_dirent", ParentID: parentID, Name: base}}
	if inode.Kind == fileKindFile {
		addDeletedManifestOpsLocked(s.meta, inode.ManifestID, &ops, now)
	}
	return s.commitMetaLocked(ops)
}

func (s *Store) RemoveAll(name string) error {
	if err := s.beginOp(s.ctx); err != nil {
		return err
	}
	defer s.endOp()
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("remove", name, err)
	}
	if root {
		return pathError("remove", name, fs.ErrInvalid)
	}
	if path == "" {
		return pathError("remove", name, fs.ErrInvalid)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return pathError("remove", name, err)
	}
	parentID, base, err := s.resolveParentLocked(tenantID, path)
	if err != nil {
		return pathError("remove", name, err)
	}
	now := nowUnix()
	ops := []metaOp{{Type: "delete_dirent", ParentID: parentID, Name: base}}
	next := cloneInode(inode)
	next.State = fileStateDeleted
	next.DeletedAt = now
	next.UpdatedAt = now
	next.Generation++
	ops = append(ops, metaOp{Type: "put_inode", Inode: next})
	if inode.Kind == fileKindFile {
		addDeletedManifestOpsLocked(s.meta, inode.ManifestID, &ops, now)
	}
	return s.commitMetaLocked(ops)
}

func (s *Store) Rename(oldname, newname string) error {
	if err := s.beginOp(s.ctx); err != nil {
		return err
	}
	defer s.endOp()
	oldTenant, oldPath, oldRoot, err := s.splitVFSPath(oldname)
	if err != nil {
		return pathError("rename", oldname, err)
	}
	newTenant, newPath, newRoot, err := s.splitVFSPath(newname)
	if err != nil {
		return pathError("rename", newname, err)
	}
	if oldRoot || newRoot || oldPath == "" || newPath == "" {
		return pathError("rename", oldname, fs.ErrInvalid)
	}
	if oldTenant != newTenant {
		return pathError("rename", newname, fs.ErrInvalid)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	source, err := s.resolvePathLocked(oldTenant, oldPath)
	if err != nil {
		return pathError("rename", oldname, err)
	}
	oldParentID, oldBase, err := s.resolveParentLocked(oldTenant, oldPath)
	if err != nil {
		return pathError("rename", oldname, err)
	}
	newParentID, newBase, err := s.resolveParentLocked(newTenant, newPath)
	if err != nil {
		return pathError("rename", newname, err)
	}
	if oldParentID == newParentID && oldBase == newBase {
		return nil
	}
	if source.Kind == fileKindDir && s.isDescendantLocked(newParentID, source.InodeID) {
		return pathError("rename", newname, fs.ErrInvalid)
	}
	targetID := s.meta.DirEntries[newParentID][newBase]
	target := s.activeInodeLocked(targetID)
	now := nowUnix()
	ops := []metaOp{{Type: "delete_dirent", ParentID: oldParentID, Name: oldBase}}
	if target != nil {
		if source.Kind == fileKindDir {
			if target.Kind != fileKindDir {
				return pathError("rename", newname, ErrNotDir)
			}
			if len(s.meta.DirEntries[target.InodeID]) > 0 {
				return pathError("rename", newname, ErrNotEmpty)
			}
		} else if target.Kind == fileKindDir {
			return pathError("rename", newname, ErrIsDir)
		}
		tombstone := cloneInode(target)
		tombstone.State = fileStateDeleted
		tombstone.DeletedAt = now
		tombstone.UpdatedAt = now
		tombstone.Generation++
		ops = append(ops, metaOp{Type: "put_inode", Inode: tombstone})
		if target.Kind == fileKindFile {
			addDeletedManifestOpsLocked(s.meta, target.ManifestID, &ops, now)
		}
	}
	next := cloneInode(source)
	next.ParentInode = newParentID
	next.Name = newBase
	next.Generation++
	next.MetadataGeneration++
	next.NamespaceGeneration++
	next.CTime = now
	next.MTime = now
	next.ModTime = now
	next.UpdatedAt = now
	ops = append(ops, metaOp{Type: "put_inode", Inode: next}, metaOp{Type: "put_dirent", ParentID: newParentID, Name: newBase, ChildID: source.InodeID})
	return s.commitMetaLocked(ops)
}

func (s *Store) Stat(name string) (os.FileInfo, error) {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return nil, pathError("stat", name, err)
	}
	if root {
		return blobFileInfo{name: "/", mode: os.ModeDir | 0o755, modTime: time.Now(), isDir: true}, nil
	}
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return nil, pathError("stat", name, err)
	}
	info := fileInfoFromInode(inode)
	if path == "" {
		info.name = tenantID
	}
	return info, nil
}

func (s *Store) Name() string {
	return "blobfs"
}

func (s *Store) Chmod(name string, mode os.FileMode) error {
	return s.updateVFSMetadata("chmod", name, func(inode *inodeRecord) {
		if inode.Kind == fileKindDir {
			inode.Mode = uint32(os.ModeDir | directoryMode(mode))
		} else {
			inode.Mode = uint32(s.regularFileMode(mode))
		}
	})
}

func (s *Store) Chown(name string, uid, gid int) error {
	return s.updateVFSMetadata("chown", name, func(inode *inodeRecord) {
		inode.UID = uid
		inode.GID = gid
	})
}

func (s *Store) Chtimes(name string, atime, mtime time.Time) error {
	return s.updateVFSMetadata("chtimes", name, func(inode *inodeRecord) {
		inode.ATime = atime.UnixNano()
		inode.ModTime = mtime.UnixNano()
		inode.MTime = inode.ModTime
	})
}

func (s *Store) updateVFSMetadata(op, name string, edit func(*inodeRecord)) error {
	if err := s.beginOp(s.ctx); err != nil {
		return err
	}
	defer s.endOp()
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError(op, name, err)
	}
	if root {
		return nil
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return pathError(op, name, err)
	}
	next := cloneInode(inode)
	edit(next)
	now := nowUnix()
	next.Generation++
	next.MetadataGeneration++
	next.CTime = now
	next.UpdatedAt = now
	return s.commitMetaLocked([]metaOp{{Type: "put_inode", Inode: next}})
}

func (s *Store) openDirFile(name, tenantID, path string, root bool) afero.File {
	entries := s.listDir(tenantID, path, root)
	info := blobFileInfo{name: filepath.Base(name), mode: os.ModeDir | 0o755, modTime: time.Now(), isDir: true}
	if root {
		info.name = "/"
	} else {
		s.metaMu.RLock()
		if inode, err := s.resolvePathLocked(tenantID, path); err == nil {
			info = fileInfoFromInode(inode)
			if path == "" {
				info.name = tenantID
			}
		}
		s.metaMu.RUnlock()
	}
	return &blobVFSFile{name: name, mode: info.mode, modTime: info.modTime, isDir: true, entries: entries}
}

func (s *Store) listDir(tenantID, path string, root bool) []os.FileInfo {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	var entries []os.FileInfo
	if root {
		names := make([]string, 0, len(s.meta.Tenants))
		for tenant := range s.meta.Tenants {
			names = append(names, tenant)
		}
		sort.Strings(names)
		for _, tenant := range names {
			entries = append(entries, blobFileInfo{name: tenant, mode: os.ModeDir | 0o755, modTime: time.Now(), isDir: true})
		}
		return entries
	}
	dir, err := s.resolvePathLocked(tenantID, path)
	if err != nil || dir.Kind != fileKindDir {
		return nil
	}
	for _, name := range sortedNames(s.meta.DirEntries[dir.InodeID]) {
		child := s.activeInodeLocked(s.meta.DirEntries[dir.InodeID][name])
		if child != nil {
			info := fileInfoFromInode(child)
			info.name = name
			entries = append(entries, info)
		}
	}
	return entries
}

func (s *Store) isDescendantLocked(nodeID, ancestorID uint64) bool {
	seen := map[uint64]bool{}
	for nodeID != 0 {
		if nodeID == ancestorID {
			return true
		}
		if seen[nodeID] {
			return true
		}
		seen[nodeID] = true
		inode := s.meta.Inodes[nodeID]
		if inode == nil {
			return false
		}
		nodeID = inode.ParentInode
	}
	return false
}

func (s *Store) createWriteSession() (afero.File, string, error) {
	s.writeSessionMu.Lock()
	if s.openWriteSessions >= s.cfg.MaxOpenWriteSessions {
		s.writeSessionMu.Unlock()
		return nil, "", ErrTooManyOpenWriteSessions
	}
	s.openWriteSessions++
	s.writeSessionMu.Unlock()
	created := false
	defer func() {
		if !created {
			s.writeSessionMu.Lock()
			s.openWriteSessions--
			s.writeSessionMu.Unlock()
		}
	}()
	for i := 0; i < 100; i++ {
		name := filepath.Join(s.stagingDir, "sessions", "session-"+fmtTime()+"-"+strconv.Itoa(i)+".tmp")
		if err := s.fs.MkdirAll(filepath.Dir(name), 0o700); err != nil {
			return nil, "", err
		}
		file, err := s.fs.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if os.IsExist(err) {
			continue
		}
		if err == nil {
			created = true
		}
		return file, name, err
	}
	return nil, "", errors.New("create write session: exhausted name attempts")
}

func fmtTime() string {
	return strconv.FormatInt(nowUnix(), 10)
}

func (s *Store) regularFileMode(mode os.FileMode) os.FileMode {
	if mode == 0 {
		mode = defaultVFSMode
	}
	mode = mode.Perm()
	if !s.cfg.AllowExecutableFiles {
		mode &^= 0o111
	}
	return mode
}

func directoryMode(mode os.FileMode) os.FileMode {
	if mode == 0 {
		mode = 0o755
	}
	return mode.Perm()
}

func (s *Store) splitVFSPath(name string) (tenantID, path string, root bool, err error) {
	name = strings.ReplaceAll(filepath.ToSlash(name), "\\", "/")
	if name == "" || name == "." || name == "/" {
		return "", "", true, nil
	}
	if len(name) > 1 && name[1] == ':' {
		return "", "", false, fs.ErrInvalid
	}
	parts := strings.Split(name, "/")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			return "", "", false, fs.ErrInvalid
		}
		clean = append(clean, part)
	}
	if len(clean) == 0 {
		return "", "", true, nil
	}
	tenantID = clean[0]
	if err := validateTenantID(tenantID, s.cfg); err != nil {
		return "", "", false, err
	}
	if len(clean) == 1 {
		return tenantID, "", false, nil
	}
	path, err = normalizePath(strings.Join(clean[1:], "/"), s.cfg)
	if err != nil {
		return "", "", false, err
	}
	return tenantID, path, false, nil
}

type vfsNodeInfo struct {
	exists     bool
	isDir      bool
	generation uint64
	size       int64
	mode       os.FileMode
	modTime    time.Time
	options    map[string]string
}

func (s *Store) vfsNodeInfo(tenantID, path string) (vfsNodeInfo, error) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	inode, err := s.resolvePathLocked(tenantID, path)
	if err != nil {
		return vfsNodeInfo{}, err
	}
	info := fileInfoFromInode(inode)
	return vfsNodeInfo{
		exists:     true,
		isDir:      inode.Kind == fileKindDir,
		generation: inode.Generation,
		size:       inode.Size,
		mode:       info.mode,
		modTime:    info.modTime,
		options:    copyOptions(inode.Options),
	}, nil
}
