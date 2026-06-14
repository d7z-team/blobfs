package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func walkTimeoutContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func collectWalkAll(t *testing.T, store *Store, opts *WalkOptions) []walkResult {
	t.Helper()
	results := make([]walkResult, 0)
	var mu sync.Mutex
	err := store.WalkAll(testContext(t), func(result walkResult) bool {
		mu.Lock()
		results = append(results, result)
		mu.Unlock()
		return true
	}, opts)
	if err != nil {
		t.Fatalf("walk all: %v", err)
	}
	return results
}

func collectWalkTenant(t *testing.T, store *Store, tenantID string, opts *WalkOptions) []walkResult {
	t.Helper()
	results := make([]walkResult, 0)
	var mu sync.Mutex
	err := store.WalkTenant(testContext(t), tenantID, func(result walkResult) bool {
		mu.Lock()
		results = append(results, result)
		mu.Unlock()
		return true
	}, opts)
	if err != nil {
		t.Fatalf("walk tenant %s: %v", tenantID, err)
	}
	return results
}

func TestWalkAllAndCountAllSmoke(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/docs/specs", 0o755); err != nil {
		t.Fatalf("mkdir tenant-a: %v", err)
	}
	if err := store.MkdirAll("tenant-b/assets", 0o755); err != nil {
		t.Fatalf("mkdir tenant-b: %v", err)
	}
	if err := store.MkdirAll("tenant-c", 0o755); err != nil {
		t.Fatalf("mkdir tenant-c: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "docs/specs/a.txt", []byte("A"))
	putTestBytes(t, store, "tenant-a", "docs/specs/b.txt", []byte("BB"))
	putTestBytes(t, store, "tenant-b", "assets/logo.svg", []byte("CCC"))

	counts, err := store.CountAll(testContext(t))
	if err != nil {
		t.Fatalf("count all: %v", err)
	}
	if len(counts) != 3 {
		t.Fatalf("expected 3 tenants, got %d", len(counts))
	}
	if counts["tenant-a"].TotalInodes != 5 || counts["tenant-a"].TotalFiles != 2 || counts["tenant-a"].TotalDirs != 3 {
		t.Fatalf("unexpected tenant-a counts: %+v", *counts["tenant-a"])
	}
	if counts["tenant-b"].TotalInodes != 3 || counts["tenant-b"].TotalFiles != 1 || counts["tenant-b"].TotalDirs != 2 {
		t.Fatalf("unexpected tenant-b counts: %+v", *counts["tenant-b"])
	}
	if counts["tenant-c"].TotalInodes != 1 || counts["tenant-c"].TotalFiles != 0 || counts["tenant-c"].TotalDirs != 1 {
		t.Fatalf("unexpected tenant-c counts: %+v", *counts["tenant-c"])
	}
	if counts["tenant-a"].EstimatedTotal != 9 || counts["tenant-b"].EstimatedTotal != 9 || counts["tenant-c"].EstimatedTotal != 9 {
		t.Fatalf("unexpected estimated total: a=%d b=%d c=%d", counts["tenant-a"].EstimatedTotal, counts["tenant-b"].EstimatedTotal, counts["tenant-c"].EstimatedTotal)
	}

	results := collectWalkAll(t, store, &WalkOptions{Workers: 4, BatchSize: 8})
	if len(results) != 9 {
		t.Fatalf("expected 9 walk results, got %d", len(results))
	}

	maxDepth := map[string]int{}
	seen := map[string]int{}
	for _, result := range results {
		seen[result.TenantID]++
		if result.Depth > maxDepth[result.TenantID] {
			maxDepth[result.TenantID] = result.Depth
		}
	}
	if seen["tenant-a"] != 5 || seen["tenant-b"] != 3 || seen["tenant-c"] != 1 {
		t.Fatalf("unexpected per-tenant walk counts: %+v", seen)
	}
	if maxDepth["tenant-a"] != 3 || maxDepth["tenant-b"] != 2 || maxDepth["tenant-c"] != 0 {
		t.Fatalf("unexpected max depth: %+v", maxDepth)
	}
}

func TestWalkTenantScopesSingleTenant(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdir tenant-a: %v", err)
	}
	if err := store.MkdirAll("tenant-b/dir", 0o755); err != nil {
		t.Fatalf("mkdir tenant-b: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "dir/file-a", []byte("A"))
	putTestBytes(t, store, "tenant-b", "dir/file-b", []byte("B"))

	results := collectWalkTenant(t, store, "tenant-a", nil)
	if len(results) != 3 {
		t.Fatalf("expected 3 tenant-a results, got %d", len(results))
	}
	for _, result := range results {
		if result.TenantID != "tenant-a" {
			t.Fatalf("walk tenant leaked %s", result.TenantID)
		}
	}

	empty := collectWalkTenant(t, store, "tenant-missing", nil)
	if len(empty) != 0 {
		t.Fatalf("expected no results for missing tenant, got %d", len(empty))
	}
}

func TestWalkAllDeletedSemantics(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))
	putTestBytes(t, store, "tenant-a", "d1/file2", []byte("content2"))
	if err := store.DeleteObject(testContext(t), "tenant-a", "d1/file1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	store.metaMu.Lock()
	dir, err := store.resolvePathLocked("tenant-a", "d1")
	if err != nil {
		store.metaMu.Unlock()
		t.Fatalf("resolve dir: %v", err)
	}
	var deletedID uint64
	for inodeID, inode := range store.meta.Inodes {
		if inode != nil && inode.Name == "file1" && inode.State == fileStateDeleted {
			deletedID = inodeID
			break
		}
	}
	if deletedID == 0 {
		store.metaMu.Unlock()
		t.Fatal("deleted inode not found")
	}
	store.meta.DirEntries[dir.InodeID]["ghost-file1"] = deletedID
	store.metaMu.Unlock()

	activeOnly := collectWalkAll(t, store, nil)
	if len(activeOnly) != 3 {
		t.Fatalf("expected 3 active inodes, got %d", len(activeOnly))
	}
	for _, result := range activeOnly {
		if result.Inode.State != fileStateActive {
			t.Fatalf("expected active inode only, got %s", result.Inode.State)
		}
	}

	withDeleted := collectWalkAll(t, store, &WalkOptions{IncludeDeleted: true})
	if len(withDeleted) != 4 {
		t.Fatalf("expected 4 inodes including deleted, got %d", len(withDeleted))
	}
	deletedSeen := 0
	for _, result := range withDeleted {
		if result.Inode.State == fileStateDeleted {
			deletedSeen++
			if result.Inode.Name != "file1" {
				t.Fatalf("unexpected deleted inode %q", result.Inode.Name)
			}
		}
	}
	if deletedSeen != 1 {
		t.Fatalf("expected 1 deleted inode, got %d", deletedSeen)
	}
}

func TestWalkAndCountIncludeDegradedObjects(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/degraded", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "degraded/blob", []byte("payload"))
	_, segmentPath := firstSegmentPath(t, store)
	if err := store.fs.Remove(segmentPath); err != nil {
		t.Fatalf("remove segment: %v", err)
	}
	if _, err := store.Repair(testContext(t), RepairOptions{Apply: true, DegradeMissing: true}); err != nil {
		t.Fatalf("degrade missing: %v", err)
	}

	results := collectWalkAll(t, store, nil)
	degradedSeen := 0
	for _, result := range results {
		if result.Inode.State == fileStateDegraded {
			degradedSeen++
			if result.Inode.Name != "blob" {
				t.Fatalf("unexpected degraded inode: %+v", result.Inode)
			}
		}
	}
	if degradedSeen != 1 {
		t.Fatalf("expected 1 degraded inode, got %d", degradedSeen)
	}

	counts, err := store.CountAll(testContext(t))
	if err != nil {
		t.Fatalf("count all: %v", err)
	}
	if counts["tenant-a"].TotalFiles != 1 || counts["tenant-a"].TotalInodes != 3 {
		t.Fatalf("unexpected degraded counts: %+v", *counts["tenant-a"])
	}
}

func TestWalkAllCanceledContext(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "dir/file", []byte("content"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.WalkAll(ctx, func(result walkResult) bool { return true }, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled error, got %v", err)
	}
}

func TestWalkAllStopsEarlySingleWorker(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 32; i++ {
		putTestBytes(t, store, "tenant-a", "dir/file-"+strconv.Itoa(i), []byte("content"))
	}

	seen := 0
	err := store.WalkAll(testContext(t), func(result walkResult) bool {
		seen++
		return seen < 5
	}, &WalkOptions{Workers: 1, BatchSize: 4})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled error, got %v", err)
	}
	if seen != 5 {
		t.Fatalf("expected exactly 5 results, got %d", seen)
	}
}

func TestWalkAllDeadlineExceeded(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 8; i++ {
		putTestBytes(t, store, "tenant-a", "dir/file-"+strconv.Itoa(i), []byte("content"))
	}

	ctx := walkTimeoutContext(t, 40*time.Millisecond)
	err := store.WalkAll(ctx, func(result walkResult) bool {
		time.Sleep(15 * time.Millisecond)
		return true
	}, &WalkOptions{Workers: 1, BatchSize: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestWalkAndCountHandleCycles(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/dir", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "dir/file", []byte("content"))

	store.metaMu.Lock()
	dir, err := store.resolvePathLocked("tenant-a", "dir")
	if err != nil {
		store.metaMu.Unlock()
		t.Fatalf("resolve dir: %v", err)
	}
	store.meta.DirEntries[dir.InodeID]["self"] = dir.InodeID
	store.metaMu.Unlock()

	walkCtx := walkTimeoutContext(t, 200*time.Millisecond)
	seen := map[uint64]int{}
	var seenMu sync.Mutex
	err = store.WalkAll(walkCtx, func(result walkResult) bool {
		seenMu.Lock()
		seen[result.Inode.InodeID]++
		seenMu.Unlock()
		return true
	}, &WalkOptions{Workers: 2, BatchSize: 2})
	if err != nil {
		t.Fatalf("walk cycle: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 unique inodes, got %d", len(seen))
	}
	for inodeID, count := range seen {
		if count != 1 {
			t.Fatalf("inode %d visited %d times", inodeID, count)
		}
	}

	counts, err := store.CountAll(walkCtx)
	if err != nil {
		t.Fatalf("count cycle: %v", err)
	}
	if counts["tenant-a"].TotalInodes != 3 || counts["tenant-a"].TotalDirs != 2 || counts["tenant-a"].TotalFiles != 1 {
		t.Fatalf("unexpected cycle counts: %+v", *counts["tenant-a"])
	}
}

func TestWalkAllConcurrentWithWrites(t *testing.T) {
	store := openTestStore(t)
	if err := store.MkdirAll("tenant-a/hot", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	ctx := walkTimeoutContext(t, 800*time.Millisecond)

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			err := store.WalkAll(ctx, func(result walkResult) bool { return true }, &WalkOptions{Workers: 4, BatchSize: 16})
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				errCh <- err
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			name := "hot/file-" + strconv.Itoa(i%8)
			tmp := name + ".tmp"
			payload := []byte(strings.Repeat(strconv.Itoa(i%10), 8))
			switch i % 4 {
			case 0:
				_, err := store.Put(ctx, "tenant-a", name, bytes.NewReader(payload), nil)
				if err != nil && ctx.Err() == nil {
					errCh <- err
					return
				}
			case 1:
				err := store.DeleteObject(ctx, "tenant-a", name)
				if err != nil && !errors.Is(err, fs.ErrNotExist) && ctx.Err() == nil {
					errCh <- err
					return
				}
			case 2:
				err := store.Rename("tenant-a/"+name, "tenant-a/"+tmp)
				if err != nil && !errors.Is(err, fs.ErrNotExist) && ctx.Err() == nil {
					errCh <- err
					return
				}
			case 3:
				err := store.Rename("tenant-a/"+tmp, "tenant-a/"+name)
				if err != nil && !errors.Is(err, fs.ErrNotExist) && ctx.Err() == nil {
					errCh <- err
					return
				}
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent walk/write: %v", err)
	}
}

func TestWalkTenantConcurrentWithDeleteTenant(t *testing.T) {
	store := openTestStore(t)
	putTestBytes(t, store, "tenant-a", "file", []byte("content"))
	ctx := walkTimeoutContext(t, 800*time.Millisecond)

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			err := store.WalkTenant(ctx, "tenant-a", func(result walkResult) bool { return true }, &WalkOptions{Workers: 2, BatchSize: 8})
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				errCh <- err
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			err := store.DeleteTenant(ctx, "tenant-a")
			if err != nil && !errors.Is(err, fs.ErrNotExist) && ctx.Err() == nil {
				errCh <- err
				return
			}
			_, err = store.Put(ctx, "tenant-a", "file-"+strconv.Itoa(i%4), bytes.NewReader([]byte("reborn")), nil)
			if err != nil && ctx.Err() == nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent walk/delete-tenant: %v", err)
	}
}
