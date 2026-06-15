package blobfs

import (
	"context"
	"runtime"
	"sync"
	"time"
)

func (s *Store) CleanupStore(ctx context.Context, opts *CleanupOptions) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()

	if opts == nil {
		opts = &CleanupOptions{}
	}
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 10000
	}

	filter := opts.Filter
	if filter == nil {
		filter = func(*CleanupInfo) bool { return false }
	}

	snapshot := s.takeWalkSnapshot()
	tenantIDs := make([]string, 0, len(snapshot.tenants))
	for tenantID := range snapshot.tenants {
		tenantIDs = append(tenantIDs, tenantID)
	}

	type cleanupTask struct {
		info CleanupInfo
	}
	tasks := make(chan cleanupTask, batchSize)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				select {
				case <-ctx.Done():
					return
				default:
					if !opts.DryRun && filter(&task.info) {
						if opts.ErrorHandler != nil {
							err := s.DeleteObject(ctx, task.info.TenantID, task.info.Path)
							if err != nil {
								opts.ErrorHandler(err, &task.info)
							}
						} else {
							_ = s.DeleteObject(ctx, task.info.TenantID, task.info.Path)
						}
					}
				}
			}
		}()
	}

	var progressMu sync.Mutex
	var processed int64
	total := int64(len(snapshot.inodes))

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tasks)

		var producers sync.WaitGroup
		for _, tenantID := range tenantIDs {
			producers.Add(1)
			go func(tid string) {
				defer producers.Done()
				rootID := snapshot.tenants[tid]
				if rootID == 0 {
					return
				}

				type queueItem struct {
					inodeID uint64
					path    string
					depth   int
				}
				queue := []queueItem{{inodeID: rootID, path: "", depth: 0}}
				visited := map[uint64]struct{}{}

				for len(queue) > 0 {
					select {
					case <-ctx.Done():
						return
					default:
					}

					item := queue[0]
					queue = queue[1:]
					if _, ok := visited[item.inodeID]; ok {
						continue
					}
					visited[item.inodeID] = struct{}{}

					inode, ok := snapshot.inodes[item.inodeID]
					if !ok || !inodeVisibleState(inode.State) {
						continue
					}

					info := CleanupInfo{
						TenantID:  tid,
						Path:      item.path,
						Inode:     inode,
						Depth:     item.depth,
						Size:      inode.Size,
						CreatedAt: time.Unix(0, inode.CreatedAt),
						UpdatedAt: time.Unix(0, inode.UpdatedAt),
						State:     inode.State,
					}

					progressMu.Lock()
					processed++
					currentProcessed := processed
					currentTenant := tid
					progressMu.Unlock()

					current := &TenantCount{TenantID: currentTenant, TotalInodes: currentProcessed}

					if opts.ProgressCallback != nil {
						if !opts.ProgressCallback(currentProcessed, total, current) {
							return
						}
					}

					select {
					case tasks <- cleanupTask{info: info}:
					case <-ctx.Done():
						return
					}

					if inode.Kind == fileKindDir {
						for _, childID := range snapshot.dirEntries[item.inodeID] {
							queue = append(queue, queueItem{inodeID: childID, path: "", depth: item.depth + 1})
						}
					}
				}
			}(tenantID)
		}
		producers.Wait()
	}()

	wg.Wait()
	return ctx.Err()
}

func (s *Store) CountStoreBeforeCleanup(ctx context.Context, filter func(*CleanupInfo) bool) (map[string]int64, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()

	if filter == nil {
		filter = func(*CleanupInfo) bool { return false }
	}

	counts := map[string]int64{"files": 0, "dirs": 0}
	snapshot := s.takeWalkSnapshot()

	for _, inode := range snapshot.inodes {
		if !inodeVisibleState(inode.State) {
			continue
		}
		info := &CleanupInfo{Inode: inode, State: inode.State}
		if filter(info) {
			if inode.Kind == fileKindFile {
				counts["files"]++
			} else {
				counts["dirs"]++
			}
		}
	}
	return counts, nil
}
