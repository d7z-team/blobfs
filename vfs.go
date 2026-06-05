package blobfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/afero"
)

const defaultVFSMode = 0o644

var _ afero.Fs = (*Store)(nil)

// Create creates or truncates a BlobFS object through the afero filesystem API.
func (s *Store) Create(name string) (afero.File, error) {
	return s.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, defaultVFSMode)
}

// Open opens a BlobFS object or directory through the afero filesystem API.
func (s *Store) Open(name string) (afero.File, error) {
	return s.OpenFile(name, os.O_RDONLY, 0)
}

// OpenFile opens a BlobFS object or directory through the afero filesystem API.
func (s *Store) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return nil, pathError("open", name, err)
	}
	writable := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0
	if root || path == "" {
		if writable {
			return nil, pathError("open", name, fs.ErrInvalid)
		}
		return s.openDirFile(name, tenantID, path, root)
	}

	lock := s.pathLocks.Open(fileKey(tenantID, path)).Lock(!writable)
	s.mu.Lock()
	record := s.activeRecordLocked(tenantID, path)
	if record != nil && record.Kind == fileKindDir {
		s.mu.Unlock()
		lock.Close()
		if writable {
			return nil, pathError("open", name, ErrIsDir)
		}
		return s.openDirFile(name, tenantID, path, false)
	}
	if record == nil {
		if flag&os.O_CREATE == 0 {
			s.mu.Unlock()
			lock.Close()
			return nil, notExist("open", name)
		}
		if err := s.ensureParentDirLocked("open", tenantID, path); err != nil {
			s.mu.Unlock()
			lock.Close()
			return nil, err
		}
	} else if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
		s.mu.Unlock()
		lock.Close()
		return nil, exists("open", name)
	}
	mode := s.regularFileMode(perm)
	modTime := time.Now()
	baseGeneration := uint64(0)
	options := map[string]string{}
	if record != nil {
		baseGeneration = record.Generation
		mode = s.regularFileMode(os.FileMode(record.Mode))
		modTime = time.Unix(0, record.MTime)
		options = copyOptions(record.Options)
	}
	if writable {
		if s.openWriteSessions >= s.cfg.MaxOpenWriteSessions {
			s.mu.Unlock()
			lock.Close()
			return nil, pathError("open", name, errors.New("too many open write sessions"))
		}
		s.openWriteSessions++
	}
	s.mu.Unlock()
	lock.Close()

	if !writable {
		reader, err := s.OpenObject(context.Background(), tenantID, path)
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, err
		}
		return &blobVFSFile{name: name, data: data, mode: mode, modTime: modTime}, nil
	}

	session, sessionName, err := s.createWriteSession()
	if err != nil {
		s.releaseWriteSession()
		return nil, err
	}
	size := int64(0)
	if record != nil && flag&os.O_TRUNC == 0 {
		reader, err := s.OpenObject(context.Background(), tenantID, path)
		if err != nil {
			_ = session.Close()
			_ = s.fs.Remove(sessionName)
			s.releaseWriteSession()
			return nil, err
		}
		size, err = io.Copy(session, reader)
		_ = reader.Close()
		if err != nil {
			_ = session.Close()
			_ = s.fs.Remove(sessionName)
			s.releaseWriteSession()
			return nil, err
		}
	}
	offset := int64(0)
	if flag&os.O_APPEND != 0 {
		offset = size
	}
	return &blobVFSFile{
		store:          s,
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
		dirty:          record == nil || flag&(os.O_CREATE|os.O_TRUNC) != 0,
		options:        options,
		baseGeneration: baseGeneration,
	}, nil
}

// Mkdir creates a directory metadata record.
func (s *Store) Mkdir(name string, perm os.FileMode) error {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("mkdir", name, err)
	}
	if root || path == "" {
		return exists("mkdir", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeRecordLocked(tenantID, path) != nil {
		return exists("mkdir", name)
	}
	if err := s.ensureParentDirLocked("mkdir", tenantID, path); err != nil {
		return err
	}
	s.createDirLocked(tenantID, path, perm)
	return saveMetadata(s.fs, s.metaPath, s.meta)
}

// MkdirAll creates a directory and any missing parents.
func (s *Store) MkdirAll(name string, perm os.FileMode) error {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("mkdir", name, err)
	}
	if root || path == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := ""
	for _, part := range strings.Split(path, "/") {
		if current == "" {
			current = part
		} else {
			current += "/" + part
		}
		record := s.activeRecordLocked(tenantID, current)
		if record == nil {
			s.createDirLocked(tenantID, current, perm)
			continue
		}
		if record.Kind != fileKindDir {
			return pathError("mkdir", current, ErrNotDir)
		}
	}
	return saveMetadata(s.fs, s.metaPath, s.meta)
}

// Remove removes an empty directory or tombstones a file.
func (s *Store) Remove(name string) error {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("remove", name, err)
	}
	if root || path == "" {
		return pathError("remove", name, fs.ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.activeRecordLocked(tenantID, path)
	if record == nil {
		return notExist("remove", name)
	}
	if record.Kind == fileKindDir && len(s.activeChildrenLocked(tenantID, path)) > 0 {
		return pathError("remove", name, ErrNotEmpty)
	}
	s.tombstoneRecordLocked(record, nowUnix())
	return saveMetadata(s.fs, s.metaPath, s.meta)
}

// RemoveAll recursively removes files and directory metadata under name.
func (s *Store) RemoveAll(name string) error {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError("remove", name, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := nowUnix()
	for _, record := range s.meta.Files {
		if record.State != fileStateActive {
			continue
		}
		if !root && (record.TenantID != tenantID || !pathContains(path, record.Path)) {
			continue
		}
		s.tombstoneRecordLocked(record, now)
	}
	rebuildDirEntries(s.meta)
	return saveMetadata(s.fs, s.metaPath, s.meta)
}

// Rename moves a file or directory metadata subtree.
func (s *Store) Rename(oldname, newname string) error {
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
	if oldTenant == newTenant && oldPath != newPath && pathContains(oldPath, newPath) {
		return pathError("rename", newname, fs.ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	source := s.activeRecordLocked(oldTenant, oldPath)
	if source == nil {
		return notExist("rename", oldname)
	}
	if err := s.ensureParentDirLocked("rename", newTenant, newPath); err != nil {
		return err
	}
	target := s.activeRecordLocked(newTenant, newPath)
	now := nowUnix()
	if target != nil {
		if source.Kind == fileKindDir {
			if target.Kind != fileKindDir {
				return pathError("rename", newname, ErrNotDir)
			}
			if len(s.activeChildrenLocked(newTenant, newPath)) > 0 {
				return pathError("rename", newname, ErrNotEmpty)
			}
		} else if target.Kind == fileKindDir {
			return pathError("rename", newname, ErrIsDir)
		}
		s.tombstoneRecordLocked(target, now)
	}
	moved := map[string]*fileRecord{}
	for key, record := range s.meta.Files {
		if record.State != fileStateActive || record.TenantID != oldTenant || !pathContains(oldPath, record.Path) {
			continue
		}
		delete(s.meta.Files, key)
		nextPath := newPath + strings.TrimPrefix(record.Path, oldPath)
		record.TenantID = newTenant
		record.Path = strings.TrimPrefix(nextPath, "/")
		record.ParentPath = parentPath(record.Path)
		record.Name = pathBase(record.Path)
		record.Generation++
		record.MetadataGeneration++
		record.CTime = now
		record.MTime = now
		record.ModTime = now
		record.UpdatedAt = now
		moved[fileKey(record.TenantID, record.Path)] = record
	}
	for key, record := range moved {
		s.meta.Files[key] = record
	}
	rebuildDirEntries(s.meta)
	return saveMetadata(s.fs, s.metaPath, s.meta)
}

// Stat returns filesystem metadata for a BlobFS object or directory.
func (s *Store) Stat(name string) (os.FileInfo, error) {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return nil, pathError("stat", name, err)
	}
	if root {
		return blobFileInfo{name: "/", mode: os.ModeDir | 0o755, modTime: time.Now(), isDir: true}, nil
	}
	if path == "" {
		return blobFileInfo{name: tenantID, mode: os.ModeDir | 0o755, modTime: time.Now(), isDir: true}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.activeRecordLocked(tenantID, path)
	if record == nil {
		return nil, notExist("stat", name)
	}
	return fileInfoFromRecord(record), nil
}

// Name returns the afero filesystem name.
func (s *Store) Name() string {
	return "blobfs"
}

// Chmod updates the mode stored in file metadata.
func (s *Store) Chmod(name string, mode os.FileMode) error {
	return s.updateVFSMetadata("chmod", name, func(record *fileRecord) {
		if record.Kind == fileKindDir {
			record.Mode = uint32(os.ModeDir | directoryMode(mode))
		} else {
			record.Mode = uint32(s.regularFileMode(mode))
		}
	})
}

// Chown updates uid/gid extension metadata.
func (s *Store) Chown(name string, uid, gid int) error {
	return s.updateVFSMetadata("chown", name, func(record *fileRecord) {
		record.UID = uid
		record.GID = gid
	})
}

// Chtimes updates modification time extension metadata.
func (s *Store) Chtimes(name string, _, mtime time.Time) error {
	return s.updateVFSMetadata("chtimes", name, func(record *fileRecord) {
		record.ModTime = mtime.UnixNano()
		record.MTime = record.ModTime
		record.UpdatedAt = record.ModTime
	})
}

func (s *Store) updateVFSMetadata(op, name string, edit func(*fileRecord)) error {
	tenantID, path, root, err := s.splitVFSPath(name)
	if err != nil {
		return pathError(op, name, err)
	}
	if root || path == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.activeRecordLocked(tenantID, path)
	if record == nil {
		return notExist(op, name)
	}
	edit(record)
	now := nowUnix()
	record.Generation++
	record.MetadataGeneration++
	record.CTime = now
	record.UpdatedAt = now
	return saveMetadata(s.fs, s.metaPath, s.meta)
}

func (s *Store) openDirFile(name, tenantID, path string, root bool) (afero.File, error) {
	entries := s.listDir(tenantID, path, root)
	info := blobFileInfo{name: filepath.Base(name), mode: os.ModeDir | 0o755, modTime: time.Now(), isDir: true}
	if root {
		info.name = "/"
	} else if path == "" {
		info.name = tenantID
	} else {
		s.mu.Lock()
		if record := s.activeRecordLocked(tenantID, path); record != nil {
			info = fileInfoFromRecord(record)
		}
		s.mu.Unlock()
	}
	return &blobVFSFile{name: name, mode: info.mode, modTime: info.modTime, isDir: true, entries: entries}, nil
}

func (s *Store) listDir(tenantID, path string, root bool) []os.FileInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]os.FileInfo{}
	if root {
		for _, record := range s.meta.Files {
			if record.State == fileStateActive {
				seen[record.TenantID] = blobFileInfo{name: record.TenantID, mode: os.ModeDir | 0o755, modTime: time.Now(), isDir: true}
			}
		}
	} else {
		for name, key := range s.meta.DirEntries[dirKey(tenantID, path)] {
			record := s.meta.Files[key]
			if record != nil && record.State == fileStateActive {
				info := fileInfoFromRecord(record)
				info.name = name
				seen[name] = info
			}
		}
	}
	entries := make([]os.FileInfo, 0, len(seen))
	for _, info := range seen {
		entries = append(entries, info)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries
}

func (s *Store) activeRecordLocked(tenantID, path string) *fileRecord {
	record := s.meta.Files[fileKey(tenantID, path)]
	if record == nil || record.State != fileStateActive {
		return nil
	}
	return record
}

func (s *Store) createDirLocked(tenantID, path string, perm os.FileMode) {
	now := nowUnix()
	record := s.newFileRecordLocked(tenantID, path, fileKindDir, now)
	record.Mode = uint32(os.ModeDir | directoryMode(perm))
	record.ModTime = now
	record.Size = 0
	s.meta.Files[fileKey(tenantID, path)] = record
	s.addDirEntryLocked(record)
}

func (s *Store) tombstoneRecordLocked(record *fileRecord, now int64) {
	record.State = fileStateDeleted
	record.DeletedAt = now
	record.UpdatedAt = now
	record.CTime = now
	record.Generation++
	s.removeDirEntryLocked(record)
	if record.Kind == fileKindFile {
		s.markManifestDeletedIfUnreferencedLocked(record.ManifestID, now)
	}
}

func (s *Store) createWriteSession() (afero.File, string, error) {
	for i := 0; i < 100; i++ {
		name := filepath.Join(s.sessionsDir, fmt.Sprintf("session-%d-%d.tmp", nowUnix(), i))
		file, err := s.fs.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if os.IsExist(err) {
			continue
		}
		return file, name, err
	}
	return nil, "", errors.New("create write session: exhausted name attempts")
}

func (s *Store) releaseWriteSession() {
	s.mu.Lock()
	if s.openWriteSessions > 0 {
		s.openWriteSessions--
	}
	s.mu.Unlock()
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

func parentPath(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[:i]
	}
	return ""
}

func pathContains(parent, child string) bool {
	return parent == "" || child == parent || strings.HasPrefix(child, parent+"/")
}
