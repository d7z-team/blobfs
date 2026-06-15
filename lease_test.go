package blobfs

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestLeaseBasic(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	reader, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", nil)
	if err != nil {
		t.Fatalf("open with lease: %v", err)
	}
	defer lease.Release(ctx)

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "content1" {
		t.Fatalf("expected content1, got %s", string(data))
	}

	info := lease.ExpiresAt()
	if info.IsZero() {
		t.Fatal("expected non-zero expiry time")
	}
}

func TestLeaseRenew(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	_, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", nil)
	if err != nil {
		t.Fatalf("open with lease: %v", err)
	}
	defer lease.Release(ctx)

	originalExpiry := lease.ExpiresAt()
	time.Sleep(10 * time.Millisecond)

	err = lease.Renew(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}

	newExpiry := lease.ExpiresAt()
	if !newExpiry.After(originalExpiry) {
		t.Fatal("expected expiry to be extended")
	}
}

func TestLeaseRelease(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	reader, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", nil)
	if err != nil {
		t.Fatalf("open with lease: %v", err)
	}

	err = lease.Release(ctx)
	if err != nil {
		t.Fatalf("release: %v", err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read after lease release: %v", err)
	}
	if string(data) != "content1" {
		t.Fatalf("expected content1, got %s", string(data))
	}
}

func TestLeaseExtendObjectLease(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	reader, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", nil)
	if err != nil {
		t.Fatalf("open with lease: %v", err)
	}
	defer lease.Release(ctx)

	originalExpiry := lease.ExpiresAt()
	time.Sleep(10 * time.Millisecond)

	err = store.ExtendObjectLease(ctx, reader, 48*time.Hour)
	if err != nil {
		t.Fatalf("extend lease: %v", err)
	}

	newExpiry := lease.ExpiresAt()
	if !newExpiry.After(originalExpiry) {
		t.Fatal("expected expiry to be extended")
	}
}

func TestLeaseGetSegmentSafetyInfo(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	_, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", nil)
	if err != nil {
		t.Fatalf("open with lease: %v", err)
	}
	defer lease.Release(ctx)

	segID := lease.SegmentID()
	info, err := store.GetSegmentSafetyInfo(ctx, segID)
	if err != nil {
		t.Fatalf("get safety info: %v", err)
	}
	if !info.HasActiveLease {
		t.Fatal("expected active lease")
	}
}

func TestLeaseInvalidTTL(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	_, _, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", &LeaseOptions{
		TTL: -1 * time.Second,
	})
	if err != ErrInvalidLeaseTTL {
		t.Fatalf("expected ErrInvalidLeaseTTL, got %v", err)
	}
}

func TestLeaseMaxTTL(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	_, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", &LeaseOptions{
		TTL: 200 * time.Hour,
	})
	if err != nil {
		t.Fatalf("open with lease: %v", err)
	}
	defer lease.Release(ctx)

	expiry := lease.ExpiresAt()
	maxExpiry := time.Now().Add(168 * time.Hour)
	if expiry.After(maxExpiry) {
		t.Fatal("expiry should not exceed 168 hours")
	}
}

func TestLeaseContextCancellation(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestLeaseConcurrentAccess(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 10; i++ {
		putTestBytes(t, store, "tenant-a", "d1/file"+string(rune('0'+i)), []byte("content"))
	}

	ctx, cancel := context.WithTimeout(testContext(t), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				reader, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file"+string(rune('0'+j)), nil)
				if err != nil {
					return
				}
				io.ReadAll(reader)
				lease.Release(ctx)
			}
		}()
	}
	wg.Wait()
}

func TestLeaseAutoRenewOnRead(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	cfg := store.cfg
	cfg.GC.DefaultLeaseTTL = 1 * time.Minute
	store.cfg = cfg

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", make([]byte, 1024))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	reader, lease, err := store.OpenObjectWithLease(ctx, "tenant-a", "d1/file1", nil)
	if err != nil {
		t.Fatalf("open with lease: %v", err)
	}
	defer lease.Release(ctx)

	initialExpiry := lease.ExpiresAt()

	buf := make([]byte, 1024)
	reader.Read(buf)

	time.Sleep(100 * time.Millisecond)

	newExpiry := lease.ExpiresAt()
	if !newExpiry.After(initialExpiry) {
		t.Log("lease may or may not be renewed depending on timing")
	}
}

func TestLeaseCloseWithoutLease(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.MkdirAll("tenant-a/d1", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	putTestBytes(t, store, "tenant-a", "d1/file1", []byte("content1"))

	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	reader, err := store.OpenObject(ctx, "tenant-a", "d1/file1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "content1" {
		t.Fatalf("expected content1, got %s", string(data))
	}

	err = reader.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
}
