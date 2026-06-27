package blobfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/afero"
)

// Store manages BlobFS metadata, content-addressed chunks, and segment files.
type Store struct {
	fs          afero.Fs
	baseDir     string
	metaDir     string
	segmentsDir string
	stagingDir  string
	lockPath    string
	lockFile    afero.File
	cfg         Config

	metaMu                 sync.RWMutex
	meta                   *metadata
	metaLog                afero.File
	metaLogName            string
	commitsSinceCheckpoint int
	lastCheckpointTime     time.Time
	lastCheckpointErr      error
	recoveryWarnings       []metadataReplayWarning

	pinMu sync.Mutex
	pins  map[string]int

	writeSessionMu    sync.Mutex
	openWriteSessions int

	backgroundMu        sync.Mutex
	lastBackgroundGCAt  time.Time
	lastBackgroundGC    *GCResult
	lastBackgroundGCErr error
	bgTicker            *time.Ticker

	handleMu sync.Mutex
	handles  map[storeHandle]struct{}

	leaseMu sync.Mutex
	leases  map[string]*segmentLease

	diagMu           sync.Mutex
	refCountWarnings []string

	lifeMu  sync.Mutex
	closing bool
	bgRuns  bool
	opWG    sync.WaitGroup
	bgWG    sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	closeOnce sync.Once
	closed    chan struct{}
}

// Open opens a store on the local operating system filesystem.
func Open(baseDir string, cfg Config) (*Store, error) {
	baseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, err
	}
	return OpenFS(afero.NewOsFs(), baseDir, cfg)
}

// OpenFS opens a store rooted at baseDir on the provided afero filesystem.
func OpenFS(fs afero.Fs, baseDir string, cfg Config) (*Store, error) {
	if fs == nil {
		return nil, ErrNilFilesystem
	}
	baseDir = filepath.Clean(baseDir)
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	storeCtx, cancel := context.WithCancel(context.Background())
	store := &Store{
		fs:          fs,
		baseDir:     baseDir,
		metaDir:     filepath.Join(baseDir, "meta"),
		segmentsDir: filepath.Join(baseDir, "data", "segments"),
		stagingDir:  filepath.Join(baseDir, "data", "staging"),
		lockPath:    filepath.Join(baseDir, "meta", "LOCK"),
		cfg:         cfg,
		pins:        map[string]int{},
		handles:     map[storeHandle]struct{}{},
		leases:      map[string]*segmentLease{},
		ctx:         storeCtx,
		cancel:      cancel,
		closed:      make(chan struct{}),
	}
	if err := fs.MkdirAll(store.metaDir, 0o755); err != nil {
		return nil, err
	}
	lockFile, err := fs.OpenFile(store.lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	store.lockFile = lockFile
	if err := fs.MkdirAll(store.segmentsDir, 0o755); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := fs.MkdirAll(store.stagingDir, 0o700); err != nil {
		_ = store.Close()
		return nil, err
	}
	var loadReport metadataLoadReport
	store.meta, store.metaLogName, loadReport, err = loadMetadata(fs, store.metaDir)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	store.recoveryWarnings = append([]metadataReplayWarning(nil), loadReport.ReplayWarnings...)
	if err := store.cleanupStagingAndOrphans(); err != nil {
		_ = store.Close()
		return nil, err
	}
	txlogDir := metaTxLogDir(store.metaDir)
	if err := fs.MkdirAll(txlogDir, 0o755); err != nil {
		_ = store.Close()
		return nil, err
	}
	logPath := filepath.Join(txlogDir, store.metaLogName)
	metaLog, err := fs.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	store.metaLog = metaLog
	store.lastCheckpointTime = time.Now()
	if err := saveSuperBlock(store.fs, store.metaDir, store.meta.TxID, store.metaLogName); err != nil {
		_ = store.Close()
		return nil, err
	}
	if store.cfg.GC.BackgroundGCInterval > 0 {
		store.startBackgroundGC()
	}
	return store, nil
}

func (s *Store) startBackgroundGC() {
	s.bgTicker = time.NewTicker(s.cfg.GC.BackgroundGCInterval)
	s.bgWG.Add(1)
	s.bgRuns = true

	go func() {
		defer func() {
			s.bgTicker.Stop()
			s.lifeMu.Lock()
			s.bgRuns = false
			s.bgTicker = nil
			s.lifeMu.Unlock()
			s.bgWG.Done()
		}()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-s.closed:
				return
			case <-s.bgTicker.C:
				result, err := s.RunGC(s.ctx, GCOptions{Compact: true})
				s.backgroundMu.Lock()
				s.lastBackgroundGCAt = time.Now()
				if result != nil {
					copyResult := *result
					s.lastBackgroundGC = &copyResult
				} else {
					s.lastBackgroundGC = nil
				}
				s.lastBackgroundGCErr = err
				s.backgroundMu.Unlock()
			}
		}
	}()
}

func (s *Store) beginOp(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.lifeMu.Lock()
	if s.closing {
		s.lifeMu.Unlock()
		return os.ErrClosed
	}
	s.opWG.Add(1)
	s.lifeMu.Unlock()
	if err := contextError(ctx); err != nil {
		s.endOp()
		return err
	}
	return nil
}

func (s *Store) endOp() {
	s.opWG.Done()
}

type storeHandle interface {
	forceCloseFromStore() error
}

func (s *Store) registerHandle(handle storeHandle) error {
	s.lifeMu.Lock()
	if s.closing {
		s.lifeMu.Unlock()
		return os.ErrClosed
	}
	s.handleMu.Lock()
	s.handles[handle] = struct{}{}
	s.handleMu.Unlock()
	s.lifeMu.Unlock()
	return nil
}

func (s *Store) unregisterHandle(handle storeHandle) {
	s.handleMu.Lock()
	delete(s.handles, handle)
	s.handleMu.Unlock()
}

func (s *Store) closeHandles() error {
	s.handleMu.Lock()
	handles := make([]storeHandle, 0, len(s.handles))
	for handle := range s.handles {
		handles = append(handles, handle)
	}
	s.handleMu.Unlock()

	var err error
	for _, handle := range handles {
		err = errors.Join(err, handle.forceCloseFromStore())
	}
	return err
}

// Close stops background work, waits for in-flight operations, checkpoints
// metadata, closes resources, and removes the process lock.
func (s *Store) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.lifeMu.Lock()
		s.closing = true
		if s.cancel != nil {
			s.cancel()
		}
		close(s.closed)
		s.lifeMu.Unlock()

		s.bgWG.Wait()
		s.opWG.Wait()
		closeErr = errors.Join(closeErr, s.closeHandles())

		s.metaMu.Lock()
		closeErr = errors.Join(closeErr, s.checkpointMetaLocked())
		if s.metaLog != nil {
			closeErr = errors.Join(closeErr, s.metaLog.Close())
			s.metaLog = nil
		}
		s.metaMu.Unlock()
		if s.lockFile != nil {
			closeErr = errors.Join(closeErr, s.lockFile.Close())
			if err := s.fs.Remove(s.lockPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				closeErr = errors.Join(closeErr, err)
			}
			s.lockFile = nil
		}
	})
	return closeErr
}
