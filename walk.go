package blobfs

import (
	"context"
	"runtime"
	"sort"
	"sync"
)

// TenantCount 租户级别统计（用于进度估算和显示）
type TenantCount struct {
	TenantID       string // 租户ID
	TotalInodes    int64  // inode 总数
	TotalDirs      int64  // 目录数
	TotalFiles     int64  // 文件数
	TotalBytes     int64  // 文件逻辑大小
	EstimatedTotal int64  // 全局预估总数（用于进度百分比）
}

// walkResult 遍历结果项
type walkResult struct {
	TenantID string
	Inode    inodeRecord
	Depth    int
}

// WalkFunc 遍历回调
// 返回 false 表示尽快终止遍历。
type WalkFunc func(result walkResult) bool

// WalkOptions 遍历选项
type WalkOptions struct {
	Workers        int  // 并行 worker 数（默认 runtime.NumCPU）
	BatchSize      int  // channel 缓冲大小（默认 10000）
	IncludeDeleted bool // 是否包含 DELETED 状态 inode
}

type walkSnapshot struct {
	tenants    map[string]uint64
	inodes     map[uint64]inodeRecord
	dirEntries map[uint64][]uint64
}

type walkRoot struct {
	tenantID string
	inodeID  uint64
}

func (s *Store) takeWalkSnapshot() *walkSnapshot {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()

	snapshot := &walkSnapshot{
		tenants:    make(map[string]uint64, len(s.meta.Tenants)),
		inodes:     make(map[uint64]inodeRecord, len(s.meta.Inodes)),
		dirEntries: make(map[uint64][]uint64, len(s.meta.DirEntries)),
	}
	for tenantID, rootID := range s.meta.Tenants {
		snapshot.tenants[tenantID] = rootID
	}
	for inodeID, inode := range s.meta.Inodes {
		if inode == nil {
			continue
		}
		next := *inode
		next.Options = copyOptions(inode.Options)
		snapshot.inodes[inodeID] = next
	}
	for parentID, entries := range s.meta.DirEntries {
		if len(entries) == 0 {
			continue
		}
		children := make([]uint64, 0, len(entries))
		for _, name := range sortedNames(entries) {
			children = append(children, entries[name])
		}
		snapshot.dirEntries[parentID] = children
	}
	return snapshot
}

func normalizeWalkOptions(opts *WalkOptions) WalkOptions {
	if opts == nil {
		return WalkOptions{Workers: runtime.NumCPU(), BatchSize: 10000}
	}
	next := *opts
	if next.Workers <= 0 {
		next.Workers = runtime.NumCPU()
	}
	if next.BatchSize <= 0 {
		next.BatchSize = 10000
	}
	return next
}

func (snap *walkSnapshot) countTenant(tenantID string) *TenantCount {
	count := &TenantCount{TenantID: tenantID}
	rootID := snap.tenants[tenantID]
	if rootID == 0 {
		return count
	}

	type queueItem struct {
		inodeID uint64
	}
	queue := []queueItem{{inodeID: rootID}}
	visited := map[uint64]struct{}{}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		if _, ok := visited[item.inodeID]; ok {
			continue
		}
		visited[item.inodeID] = struct{}{}

		inode, ok := snap.inodes[item.inodeID]
		if !ok || !inodeVisibleState(inode.State) {
			continue
		}
		count.TotalInodes++
		if inode.Kind == fileKindDir {
			count.TotalDirs++
			for _, childID := range snap.dirEntries[item.inodeID] {
				queue = append(queue, queueItem{inodeID: childID})
			}
			continue
		}
		count.TotalFiles++
		count.TotalBytes += inode.Size
	}
	return count
}

// CountAll 统计所有租户的 inode 数量（用于进度估算）
func (s *Store) CountAll(ctx context.Context) (map[string]*TenantCount, error) {
	if err := s.beginOp(ctx); err != nil {
		return nil, err
	}
	defer s.endOp()

	snapshot := s.takeWalkSnapshot()
	tenantIDs := make([]string, 0, len(snapshot.tenants))
	for tenantID := range snapshot.tenants {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)

	counts := make(map[string]*TenantCount, len(tenantIDs))
	var total int64
	for _, tenantID := range tenantIDs {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		count := snapshot.countTenant(tenantID)
		total += count.TotalInodes
		counts[tenantID] = count
	}
	for _, count := range counts {
		count.EstimatedTotal = total
	}
	return counts, nil
}

// WalkAll 遍历所有租户下的所有 inode。
// 遍历基于只读快照，不保证顺序，结果为最终一致视图。
func (s *Store) WalkAll(ctx context.Context, fn WalkFunc, opts *WalkOptions) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()

	snapshot := s.takeWalkSnapshot()
	tenantIDs := make([]string, 0, len(snapshot.tenants))
	for tenantID := range snapshot.tenants {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)

	roots := make([]walkRoot, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		roots = append(roots, walkRoot{tenantID: tenantID, inodeID: snapshot.tenants[tenantID]})
	}
	return walkSnapshotRoots(ctx, snapshot, roots, fn, normalizeWalkOptions(opts))
}

// WalkTenant 遍历单个租户。
func (s *Store) WalkTenant(ctx context.Context, tenantID string, fn WalkFunc, opts *WalkOptions) error {
	if err := s.beginOp(ctx); err != nil {
		return err
	}
	defer s.endOp()

	snapshot := s.takeWalkSnapshot()
	rootID := snapshot.tenants[tenantID]
	if rootID == 0 {
		return nil
	}
	return walkSnapshotRoots(ctx, snapshot, []walkRoot{{tenantID: tenantID, inodeID: rootID}}, fn, normalizeWalkOptions(opts))
}

func walkSnapshotRoots(ctx context.Context, snap *walkSnapshot, roots []walkRoot, fn WalkFunc, opts WalkOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	work := make(chan walkResult, opts.BatchSize)
	var wg sync.WaitGroup

	for range opts.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				if !fn(item) {
					cancel()
					return
				}
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(work)

		var producers sync.WaitGroup
		for _, root := range roots {
			producers.Add(1)
			go func(root walkRoot) {
				defer producers.Done()
				walkDirBFS(ctx, snap, root, opts.IncludeDeleted, work)
			}(root)
		}
		producers.Wait()
	}()

	wg.Wait()
	return ctx.Err()
}

// walkDirBFS 非递归 BFS 遍历
func walkDirBFS(ctx context.Context, snap *walkSnapshot, root walkRoot, includeDeleted bool, work chan<- walkResult) {
	if root.inodeID == 0 {
		return
	}

	type queueItem struct {
		inodeID uint64
		depth   int
	}
	queue := []queueItem{{inodeID: root.inodeID}}
	visited := map[uint64]struct{}{}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return
		}

		item := queue[0]
		queue = queue[1:]
		if _, ok := visited[item.inodeID]; ok {
			continue
		}
		visited[item.inodeID] = struct{}{}

		inode, ok := snap.inodes[item.inodeID]
		if !ok {
			continue
		}
		if !includeDeleted && !inodeVisibleState(inode.State) {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case work <- walkResult{TenantID: root.tenantID, Inode: inode, Depth: item.depth}:
		}

		if inode.Kind != fileKindDir {
			continue
		}
		for _, childID := range snap.dirEntries[item.inodeID] {
			queue = append(queue, queueItem{inodeID: childID, depth: item.depth + 1})
		}
	}
}

// walkDirBFSCollect 非递归 BFS 遍历，收集可达 inode 到 map（用于 GC）
// caller must hold metaMu
func walkDirBFSCollectLocked(meta *metadata, inodeID uint64, reachable map[uint64]bool) {
	if inodeID == 0 || reachable[inodeID] {
		return
	}

	queue := []uint64{inodeID}
	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]
		if reachable[currID] {
			continue
		}

		inode := meta.Inodes[currID]
		if inode == nil || !inodeVisibleState(inode.State) {
			continue
		}
		reachable[currID] = true

		if inode.Kind != fileKindDir {
			continue
		}
		for _, childID := range meta.DirEntries[currID] {
			queue = append(queue, childID)
		}
	}
}
