package blobfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/afero"
)

type blobVFSFile struct {
	mu             sync.Mutex
	store          *Store
	ctx            context.Context
	name           string
	tenantID       string
	path           string
	data           []byte
	session        afero.File
	sessionName    string
	size           int64
	offset         int64
	mode           os.FileMode
	modTime        time.Time
	writable       bool
	append         bool
	dirty          bool
	closed         bool
	isDir          bool
	entries        []os.FileInfo
	dirPos         int
	options        map[string]string
	baseGeneration uint64
}

func (f *blobVFSFile) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	err := f.syncLocked()
	f.closed = true
	session := f.session
	store := f.store
	sessionName := f.sessionName
	f.session = nil
	f.mu.Unlock()

	if session != nil {
		if closeErr := session.Close(); err == nil {
			err = closeErr
		}
		if store != nil && sessionName != "" {
			_ = store.fs.Remove(sessionName)
			store.writeSessionMu.Lock()
			store.openWriteSessions--
			store.writeSessionMu.Unlock()
		}
	}
	return err
}

func (f *blobVFSFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, afero.ErrFileClosed
	}
	if f.isDir {
		return 0, os.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err := f.readAtLocked(p, f.offset)
	f.offset += int64(n)
	if errors.Is(err, io.EOF) && n > 0 {
		return n, nil
	}
	return n, err
}

func (f *blobVFSFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readAtLocked(p, off)
}

func (f *blobVFSFile) readAtLocked(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, afero.ErrFileClosed
	}
	if f.isDir {
		return 0, os.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, os.ErrInvalid
	}
	if off >= f.fileSizeLocked() {
		return 0, io.EOF
	}
	if f.session != nil {
		n, err := f.session.ReadAt(p, off)
		if int64(n)+off >= f.size && err == nil && n < len(p) {
			err = io.EOF
		}
		return n, err
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *blobVFSFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.append {
		f.offset = f.size
	}
	n, err := f.writeAtLocked(p, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *blobVFSFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeAtLocked(p, off)
}

func (f *blobVFSFile) writeAtLocked(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, afero.ErrFileClosed
	}
	if f.isDir || !f.writable || f.session == nil {
		return 0, os.ErrPermission
	}
	if off < 0 {
		return 0, os.ErrInvalid
	}
	end := off + int64(len(p))
	if end < off {
		return 0, ErrTooLarge
	}
	if end > f.store.cfg.MaxFileSize {
		return 0, ErrTooLarge
	}
	n, err := f.session.WriteAt(p, off)
	if writtenEnd := off + int64(n); writtenEnd > f.size {
		f.size = writtenEnd
	}
	if n > 0 {
		f.dirty = true
		f.modTime = time.Now()
	}
	return n, err
}

func (f *blobVFSFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, afero.ErrFileClosed
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = f.offset + offset
	case io.SeekEnd:
		next = f.fileSizeLocked() + offset
	default:
		return 0, os.ErrInvalid
	}
	if next < 0 {
		return 0, os.ErrInvalid
	}
	f.offset = next
	return next, nil
}

func (f *blobVFSFile) Name() string {
	return f.name
}

func (f *blobVFSFile) Readdir(count int) ([]os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, afero.ErrFileClosed
	}
	if !f.isDir {
		return nil, os.ErrInvalid
	}
	if f.dirPos >= len(f.entries) && count > 0 {
		return nil, io.EOF
	}
	if count <= 0 {
		entries := f.entries[f.dirPos:]
		f.dirPos = len(f.entries)
		return entries, nil
	}
	end := f.dirPos + count
	if end > len(f.entries) {
		end = len(f.entries)
	}
	entries := f.entries[f.dirPos:end]
	f.dirPos = end
	return entries, nil
}

func (f *blobVFSFile) Readdirnames(n int) ([]string, error) {
	entries, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names, nil
}

func (f *blobVFSFile) ReadDir(n int) ([]fs.DirEntry, error) {
	infos, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}
	entries := make([]fs.DirEntry, 0, len(infos))
	for _, info := range infos {
		entries = append(entries, fs.FileInfoToDirEntry(info))
	}
	return entries, nil
}

func (f *blobVFSFile) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, afero.ErrFileClosed
	}
	if f.isDir {
		return blobFileInfo{name: filepath.Base(f.name), mode: f.mode | os.ModeDir, modTime: f.modTime, isDir: true}, nil
	}
	return blobFileInfo{name: filepath.Base(f.name), size: f.fileSizeLocked(), mode: f.mode.Perm(), modTime: f.modTime}, nil
}

func (f *blobVFSFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncLocked()
}

func (f *blobVFSFile) syncLocked() error {
	if f.closed {
		return afero.ErrFileClosed
	}
	if f.store == nil || !f.writable || !f.dirty || f.session == nil {
		return nil
	}
	if _, err := f.session.Seek(0, io.SeekStart); err != nil {
		return err
	}
	ctx := f.ctx
	if ctx == nil {
		ctx = f.store.ctx
	}
	limit := io.LimitReader(f.session, f.size)
	result, err := f.store.putObject(ctx, f.tenantID, f.path, limit, putCommitOptions{
		baseGeneration:  f.baseGeneration,
		checkGeneration: true,
		mode:            f.mode,
		modTime:         f.modTime.UnixNano(),
		options:         copyOptions(f.options),
	})
	if err != nil {
		return err
	}
	f.baseGeneration = result.Generation
	f.size = result.Size
	f.dirty = false
	return nil
}

func (f *blobVFSFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return afero.ErrFileClosed
	}
	if f.isDir || !f.writable || f.session == nil || size < 0 {
		return os.ErrInvalid
	}
	if size > f.store.cfg.MaxFileSize {
		return ErrTooLarge
	}
	if err := f.session.Truncate(size); err != nil {
		return err
	}
	f.size = size
	if f.offset > size {
		f.offset = size
	}
	f.dirty = true
	f.modTime = time.Now()
	return nil
}

func (f *blobVFSFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

func (f *blobVFSFile) fileSizeLocked() int64 {
	if f.session != nil {
		return f.size
	}
	return int64(len(f.data))
}

type blobFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func fileInfoFromInode(inode *inodeRecord) blobFileInfo {
	mode := os.FileMode(inode.Mode)
	if mode == 0 {
		if inode.Kind == fileKindDir {
			mode = os.ModeDir | 0o755
		} else {
			mode = defaultVFSMode
		}
	}
	isDir := inode.Kind == fileKindDir
	if isDir {
		mode |= os.ModeDir
	}
	modTime := time.Unix(0, inode.MTime)
	if inode.MTime == 0 {
		modTime = time.Unix(0, inode.ModTime)
	}
	if inode.ModTime == 0 && inode.MTime == 0 {
		modTime = time.Unix(0, inode.UpdatedAt)
	}
	return blobFileInfo{
		name:    inode.Name,
		size:    inode.Size,
		mode:    mode,
		modTime: modTime,
		isDir:   isDir,
	}
}

func (i blobFileInfo) Name() string       { return i.name }
func (i blobFileInfo) Size() int64        { return i.size }
func (i blobFileInfo) Mode() os.FileMode  { return i.mode }
func (i blobFileInfo) ModTime() time.Time { return i.modTime }
func (i blobFileInfo) IsDir() bool        { return i.isDir }
func (i blobFileInfo) Sys() any           { return nil }
