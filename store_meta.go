package blobfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"
)

func (s *Store) commitMetaLocked(ops []metaOp) error {
	if len(ops) == 0 {
		return nil
	}
	txid := s.meta.TxID + 1
	tx := metaTx{TxID: txid, Ops: ops}
	if err := writeMetaTx(s.metaLog, tx); err != nil {
		return err
	}
	for _, op := range tx.Ops {
		applyMetaOp(s.meta, op)
	}
	if tx.TxID > s.meta.TxID {
		s.meta.TxID = tx.TxID
	}
	s.commitsSinceCheckpoint++
	if err := saveSuperBlock(s.fs, s.metaDir, s.meta.TxID, s.metaLogName); err != nil {
		s.lastCheckpointErr = err
		return err
	}
	s.lastCheckpointErr = nil
	if s.commitsSinceCheckpoint >= metaCheckpointInterval ||
		(time.Since(s.lastCheckpointTime) >= metaCheckpointTimeMin && s.commitsSinceCheckpoint > 0) {
		if err := s.checkpointMetaLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) checkpointMetaLocked() error {
	if s.metaLog == nil {
		return errMetadataLogClosed
	}
	compactMetadata(s.meta)
	if err := saveCheckpoint(s.fs, s.metaDir, s.meta); err != nil {
		s.lastCheckpointErr = err
		return err
	}
	newLog, newName, err := s.createMetaLogGenerationLocked(nextMetaLogName(s.metaLogName))
	if err != nil {
		s.lastCheckpointErr = err
		return err
	}
	if err := newLog.Sync(); err != nil {
		_ = newLog.Close()
		_ = s.fs.Remove(filepath.Join(metaTxLogDir(s.metaDir), newName))
		s.lastCheckpointErr = err
		return err
	}
	if err := saveSuperBlock(s.fs, s.metaDir, s.meta.TxID, newName); err != nil {
		_ = newLog.Close()
		_ = s.fs.Remove(filepath.Join(metaTxLogDir(s.metaDir), newName))
		s.lastCheckpointErr = err
		return err
	}
	oldLog := s.metaLog
	oldName := s.metaLogName
	s.metaLog = newLog
	s.metaLogName = newName
	s.commitsSinceCheckpoint = 0
	s.lastCheckpointTime = time.Now()
	s.lastCheckpointErr = nil
	s.diagMu.Lock()
	s.refCountWarnings = s.refCountWarnings[:0]
	s.diagMu.Unlock()
	var cleanupErr error
	if oldLog != nil {
		cleanupErr = errors.Join(cleanupErr, oldLog.Close())
	}
	if oldName != "" && oldName != newName {
		if err := s.fs.Remove(filepath.Join(metaTxLogDir(s.metaDir), oldName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if cleanupErr != nil {
		s.lastCheckpointErr = cleanupErr
	}
	return cleanupErr
}

func (s *Store) createMetaLogGenerationLocked(startName string) (afero.File, string, error) {
	txlogDir := metaTxLogDir(s.metaDir)
	if err := s.fs.MkdirAll(txlogDir, 0o755); err != nil {
		return nil, "", err
	}
	name := startName
	for i := 0; i < 1000; i++ {
		path := filepath.Join(txlogDir, name)
		file, err := s.fs.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0o600)
		if os.IsExist(err) {
			name = nextMetaLogName(name)
			continue
		}
		return file, name, err
	}
	return nil, "", errors.New("create metadata log generation: exhausted attempts")
}

func (s *Store) nextInodeIDLocked() uint64 {
	id := s.meta.NextInodeID
	s.meta.NextInodeID++
	return id
}

func (s *Store) visibleInodeLocked(id uint64) *inodeRecord {
	inode := s.meta.Inodes[id]
	if inode == nil || !inodeVisibleState(inode.State) {
		return nil
	}
	return inode
}

func (s *Store) resolvePathLocked(tenantID, path string) (*inodeRecord, error) {
	rootID := s.meta.Tenants[tenantID]
	if rootID == 0 {
		return nil, fs.ErrNotExist
	}
	current := s.visibleInodeLocked(rootID)
	if current == nil {
		return nil, fs.ErrNotExist
	}
	if path == "" {
		return current, nil
	}
	for _, name := range strings.Split(path, "/") {
		if current.Kind != fileKindDir {
			return nil, ErrNotDir
		}
		childID := s.meta.DirEntries[current.InodeID][name]
		if childID == 0 {
			return nil, fs.ErrNotExist
		}
		current = s.visibleInodeLocked(childID)
		if current == nil {
			return nil, fs.ErrNotExist
		}
	}
	return current, nil
}

func (s *Store) resolveParentLocked(tenantID, path string) (uint64, string, error) {
	parent := parentPath(path)
	name := pathBase(path)
	rootID := s.meta.Tenants[tenantID]
	if rootID == 0 {
		return 0, "", fs.ErrNotExist
	}
	if parent == "" {
		return rootID, name, nil
	}
	inode, err := s.resolvePathLocked(tenantID, parent)
	if err != nil {
		return 0, "", err
	}
	if inode.Kind != fileKindDir {
		return 0, "", ErrNotDir
	}
	return inode.InodeID, name, nil
}

func (s *Store) cleanupStagingAndOrphans() error {
	if err := afero.Walk(s.fs, s.stagingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return err
		}
		return s.fs.Remove(path)
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	referenced := map[string]bool{}
	for _, seg := range s.meta.Segments {
		if seg == nil {
			continue
		}
		if seg.State != segmentStateDeleted {
			referenced[s.segmentPath(seg)] = true
		}
	}
	return afero.Walk(s.fs, s.segmentsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return err
		}
		if !referenced[path] {
			return s.fs.Remove(path)
		}
		return nil
	})
}

func (s *Store) recordRefCountWarning(msg string) {
	s.diagMu.Lock()
	if len(s.refCountWarnings) >= maxRefCountWarnings {
		s.refCountWarnings = s.refCountWarnings[1:]
	}
	s.refCountWarnings = append(s.refCountWarnings, msg)
	s.diagMu.Unlock()
}

func (s *Store) refCountWarningCount() int {
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	return len(s.refCountWarnings)
}

// GetRefCountWarnings returns a snapshot of all recorded refcount warnings.
func (s *Store) GetRefCountWarnings(ctx context.Context) ([]string, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	warnings := make([]string, len(s.refCountWarnings))
	copy(warnings, s.refCountWarnings)
	return warnings, nil
}

// ClearRefCountWarnings clears all recorded refcount warnings.
func (s *Store) ClearRefCountWarnings(ctx context.Context) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()
	s.diagMu.Lock()
	s.refCountWarnings = s.refCountWarnings[:0]
	s.diagMu.Unlock()
	return nil
}
