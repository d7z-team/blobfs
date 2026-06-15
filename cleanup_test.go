package blobfs

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCleanupStoreBasic(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))
	putTestBytes(t, store, "tenant-a", "d1/file2", []byte("content2"))

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	deletedCount := 0
	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		Filter: func(info *CleanupInfo) bool {
			return info.Inode.Kind == fileKindFile
		},
		ErrorHandler: func(err error, info *CleanupInfo) {
			mu.Lock()
			deletedCount++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	mu.Lock()
	if deletedCount != 2 {
		t.Fatalf("expected 2 deletions, got %d", deletedCount)
	}
	mu.Unlock()
}

func TestCleanupStoreDryRun(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))
	putTestBytes(t, store, "tenant-a", "d1/file2", []byte("content2"))

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		DryRun:  true,
		Filter: func(info *CleanupInfo) bool {
			return info.Inode.Kind == fileKindFile
		},
	})
	if err != nil {
		t.Fatalf("cleanup dry run: %v", err)
	}

	counts, err := store.CountAll(ctx)
	if err != nil {
		t.Fatalf("countall: %v", err)
	}
	if counts["tenant-a"].TotalFiles != 2 {
		t.Fatalf("expected 2 files still exist after dry run, got %d", counts["tenant-a"].TotalFiles)
	}
}

func TestCleanupStoreProgressCallback(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 10; i++ {
		putTestBytes(t, store, "tenant-a", "file"+string(rune('0'+i)), []byte("content"))
	}

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	var lastProgress int64
	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		Filter: func(info *CleanupInfo) bool {
			return false
		},
		ProgressCallback: func(processed, total int64, current *TenantCount) bool {
			mu.Lock()
			lastProgress = processed
			mu.Unlock()
			return true
		},
	})
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if lastProgress == 0 {
		t.Fatal("expected progress callback to be called")
	}
}

func TestCleanupStoreCancellation(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 100; i++ {
		putTestBytes(t, store, "tenant-a", "file-"+string(rune('a'+i%26)), []byte("content"))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		Filter: func(info *CleanupInfo) bool {
			return true
		},
	})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestCleanupStoreMultipleTenants(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	for i := 0; i < 5; i++ {
		if err := store.MkdirAll("tenant-"+string(rune('a'+i))+"/d1", 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		putTestBytes(t, store, "tenant-"+string(rune('a'+i)), "d1/file", []byte("content"))
	}

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	tenantsSeen := make(map[string]bool)
	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		Filter: func(info *CleanupInfo) bool {
			return false
		},
		ProgressCallback: func(processed, total int64, current *TenantCount) bool {
			mu.Lock()
			tenantsSeen[current.TenantID] = true
			mu.Unlock()
			return true
		},
	})
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if len(tenantsSeen) != 5 {
		t.Fatalf("expected 5 tenants, got %d", len(tenantsSeen))
	}
}

func TestCleanupStoreAgeFilter(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/oldfile", []byte("old content"))

	time.Sleep(10 * time.Millisecond)
	cutoff := time.Now()

	putTestBytes(t, store, "tenant-a", "d1/newfile", []byte("new content"))

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	var deletedPaths []string
	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		Filter: func(info *CleanupInfo) bool {
			return info.Inode.Kind == fileKindFile && info.UpdatedAt.Before(cutoff)
		},
		ErrorHandler: func(err error, info *CleanupInfo) {
			mu.Lock()
			deletedPaths = append(deletedPaths, info.Path)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if len(deletedPaths) != 1 {
		t.Fatalf("expected 1 deletion, got %d", len(deletedPaths))
	}
}

func TestCleanupStoreErrorHandler(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	errors := 0
	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		Filter: func(info *CleanupInfo) bool {
			return info.Inode.Kind == fileKindFile
		},
		ErrorHandler: func(err error, info *CleanupInfo) {
			mu.Lock()
			errors++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	mu.Lock()
	if errors != 1 {
		t.Fatalf("expected 1 error, got %d", errors)
	}
	mu.Unlock()
}

func TestCleanupStoreWorkerScaling(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 50; i++ {
		putTestBytes(t, store, "tenant-a", "file"+string(rune('0'+i%10)), []byte("content"))
	}

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	for _, workers := range []int{1, 2, 4, 8} {
		count := 0
		err := store.CleanupStore(ctx, &CleanupOptions{
			Workers: workers,
			Filter: func(info *CleanupInfo) bool {
				return false
			},
			ProgressCallback: func(processed, total int64, current *TenantCount) bool {
				count++
				return true
			},
		})
		if err != nil {
			t.Fatalf("cleanup with %d workers: %v", workers, err)
		}
		if count == 0 {
			t.Fatalf("expected progress callback with %d workers", workers)
		}
	}
}

func TestCountStoreBeforeCleanup(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))
	putTestBytes(t, store, "tenant-a", "d1/file2", []byte("content2"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	counts, err := store.CountStoreBeforeCleanup(ctx, func(info *CleanupInfo) bool {
		return info.Inode.Kind == fileKindFile
	})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if counts["files"] != 2 {
		t.Fatalf("expected 2 files, got %d", counts["files"])
	}
}

func TestCleanupStoreNilFilter(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	err := store.CleanupStore(ctx, &CleanupOptions{
		Workers: 2,
		Filter:  nil,
	})
	if err != nil {
		t.Fatalf("cleanup with nil filter: %v", err)
	}
}
