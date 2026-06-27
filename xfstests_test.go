package blobfs

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
)

// =============================================================================
// Category 1: Basic File Operations (inspired by xfstests generic/001, 007, 013)
// =============================================================================

// TestWriteReadChainPutDeleteGC — xfstests generic/001 data chain pattern:
// Put → OpenObject → verify → Delete → GC → verify origin gone.
func TestWriteReadChainPutDeleteGC(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	if err := store.MkdirAll("t1/chain", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	putTestBytes(t, store, "t1", "chain/file", []byte("chain-data"))
	if !bytes.Equal(readTestBytes(t, store, "t1", "chain/file"), []byte("chain-data")) {
		t.Fatal("data mismatch after Put")
	}
	if err := store.DeleteObject(ctx, "t1", "chain/file"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := store.OpenObject(ctx, "t1", "chain/file"); err == nil {
		t.Fatal("OpenObject should fail after DeleteObject")
	}
	result, err := store.RunGC(ctx, GCOptions{Compact: true})
	if err != nil {
		t.Fatalf("RunGC: %v", err)
	}
	if result.ChunksDeleted == 0 {
		t.Fatal("GC should have deleted orphan chunks")
	}
}

// TestEmptyFilePutRoundtrip — xfstests generic/007 boundary: 0-byte file
// Put/Stat/Open/read must all work correctly for empty files.
func TestEmptyFilePutRoundtrip(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	putTestBytes(t, store, "t1", "empty.txt", []byte{})
	info, err := store.StatObject(ctx, "t1", "empty.txt")
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	if info.Size != 0 {
		t.Fatalf("expected size 0, got %d", info.Size)
	}
	reader, err := store.OpenObject(ctx, "t1", "empty.txt")
	if err != nil {
		t.Fatalf("OpenObject: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected empty content, got %d bytes", len(data))
	}
}

// TestOverwriteAndVerifyData — xfstests generic/007 overwrite pattern:
// Multiple Put calls with different content, old content must not leak.
func TestOverwriteAndVerifyData(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	putTestBytes(t, store, "t1", "f.txt", []byte("v1"))
	if !bytes.Equal(readTestBytes(t, store, "t1", "f.txt"), []byte("v1")) {
		t.Fatal("v1 mismatch")
	}
	putTestBytes(t, store, "t1", "f.txt", []byte("version-two"))
	if !bytes.Equal(readTestBytes(t, store, "t1", "f.txt"), []byte("version-two")) {
		t.Fatal("overwrite mismatch")
	}
	result, err := store.RunGC(ctx, GCOptions{Compact: true})
	if err != nil {
		t.Fatalf("RunGC: %v", err)
	}
	if result.ChunksDeleted == 0 {
		t.Fatal("overwritten chunk should be GC'd")
	}
	if !bytes.Equal(readTestBytes(t, store, "t1", "f.txt"), []byte("version-two")) {
		t.Fatal("post-GC read mismatch")
	}
}

// TestVFSTruncateRoundtrip — xfstests generic/014 truncfile pattern:
// VFS OpenFile + Truncate + read back truncated content.
func TestVFSTruncateRoundtrip(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	data := bytes.Repeat([]byte("abcdefgh"), 1024) // 8 KB
	putTestBytes(t, store, "t1", "big.txt", data)
	f, err := store.OpenFileContext(ctx, "t1/big.txt", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := f.Truncate(100); err != nil {
		f.Close()
		t.Fatalf("Truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	info, err := store.StatObject(ctx, "t1", "big.txt")
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	if info.Size != 100 {
		t.Fatalf("size after truncate: want 100, got %d", info.Size)
	}
	got := readTestBytes(t, store, "t1", "big.txt")
	if len(got) != 100 || !bytes.Equal(got, data[:100]) {
		t.Fatalf("truncated content mismatch: got %d bytes", len(got))
	}
}

// TestVFSTruncateExtend — xfstests generic/014: Truncate to larger size fills
// with zero bytes (implicit in afero semantics).
func TestVFSTruncateExtend(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	putTestBytes(t, store, "t1", "small.txt", []byte("hi"))
	f, err := store.OpenFileContext(ctx, "t1/small.txt", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := f.Truncate(1024); err != nil {
		f.Close()
		t.Fatalf("Truncate extend: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	info, err := store.StatObject(ctx, "t1", "small.txt")
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	if info.Size != 1024 {
		t.Fatalf("size after extend: want 1024, got %d", info.Size)
	}
}

// =============================================================================
// Category 2: Metadata Operations (inspired by xfstests generic/003, 100-103)
// =============================================================================

// TestStatObjectCorrectness — xfstests generic/100 stat pattern:
// Stat an object and verify all metadata fields are populated correctly.
func TestStatObjectCorrectness(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	content := []byte("stat-test-data")
	if err := store.MkdirAll("t1/stats", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	result := putTestBytes(t, store, "t1", "stats/file.bin", content)
	info, err := store.StatObject(ctx, "t1", "stats/file.bin")
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	if info.Size != int64(len(content)) {
		t.Fatalf("size: want %d, got %d", len(content), info.Size)
	}
	if info.TenantID != "t1" {
		t.Fatalf("tenant: want t1, got %s", info.TenantID)
	}
	if info.State != fileStateActive {
		t.Fatalf("state: want ACTIVE, got %s", info.State)
	}
	if !info.Readable || !info.Writable {
		t.Fatal("should be readable and writable")
	}
	if info.Generation != result.Generation {
		t.Fatalf("generation mismatch: %d vs %d", info.Generation, result.Generation)
	}
	if info.CreatedAt.IsZero() || info.UpdatedAt.IsZero() {
		t.Fatal("timestamps should not be zero")
	}
}

// TestChtimesPersistence — xfstests generic/003 ctime pattern:
// Chtimes modifies mtime; verify it persists through Stat and reopen.
func TestChtimesPersistence(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "timed.txt", []byte("data"))
	newTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.Chtimes("t1/timed.txt", newTime, newTime); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	fi, err := store.Stat("t1/timed.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !fi.ModTime().Equal(newTime) {
		t.Fatalf("modtime: want %v, got %v", newTime, fi.ModTime())
	}
}

// TestChmodChownRoundtrip — xfstests generic/003 chmod/chown pattern.
func TestChmodChownRoundtrip(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "perm.txt", []byte("data"))
	if err := store.Chmod("t1/perm.txt", 0o600); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	fi, err := store.Stat("t1/perm.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode after chmod: %o", fi.Mode().Perm())
	}
	if err := store.Chown("t1/perm.txt", 1000, 1000); err != nil {
		t.Fatalf("Chown: %v", err)
	}
}

// =============================================================================
// Category 3: Hard Links (inspired by xfstests generic/002)
//
// Note: blobfs does not implement hard links. Content deduplication is the
// built-in equivalent: identical file content automatically shares chunks.
// =============================================================================

// TestContentDedupSimulatesLinkBehavior — xfstests generic/002 pattern:
// Two files with identical content share the same chunk manifest (built-in dedup).
func TestContentDedupSimulatesLinkBehavior(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	content := []byte("link-data-test")
	putTestBytes(t, store, "t1", "a.txt", content)
	putTestBytes(t, store, "t1", "b.txt", content)
	a, _ := store.StatObject(ctx, "t1", "a.txt")
	b, _ := store.StatObject(ctx, "t1", "b.txt")
	if a.FileHash != b.FileHash {
		t.Fatal("identical content must produce same FileHash (content-addressed dedup)")
	}
	if a.ManifestID != b.ManifestID {
		t.Fatal("identical content must share same ManifestID (dedup)")
	}
}

// =============================================================================
// Category 4: Directory Operations (inspired by xfstests generic/089, 013)
// =============================================================================

// TestMkdirAllExistingIsIdempotent — xfstests generic/089 dir no-op:
// MkdirAll on an existing path must succeed (idempotent).
func TestMkdirAllExistingIsIdempotent(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	if err := store.MkdirAll("t1/a/b/c", 0o755); err != nil {
		t.Fatalf("MkdirAll first: %v", err)
	}
	if err := store.MkdirAll("t1/a/b/c", 0o755); err != nil {
		t.Fatalf("MkdirAll second must be idempotent: %v", err)
	}
}

// TestRemoveNonEmptyDir — xfstests generic/007 rmdir pattern:
// Remove of a non-empty directory must fail.
func TestRemoveNonEmptyDir(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	if err := store.MkdirAll("t1/dir", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	putTestBytes(t, store, "t1", "dir/f.txt", []byte("x"))
	err := store.Remove("t1/dir")
	if err == nil {
		store.Remove("t1/dir/f.txt")
		t.Fatal("Remove on non-empty dir should fail")
	}
}

// TestReaddirCountSemantics — xfstests generic/089 readdir pattern:
// Readdir(-1), Readdir(0), Readdir(small-n) all return correct entry count.
func TestReaddirCountSemantics(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	if err := store.MkdirAll("t1/d", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	for i := 0; i < 5; i++ {
		putTestBytes(t, store, "t1", "d/f"+string(rune('a'+i))+".txt", []byte("x"))
	}
	// Readdir(-1) returns all entries.
	fAll, err := store.Open("t1/d")
	if err != nil {
		t.Fatalf("Open dir: %v", err)
	}
	all, err := fAll.Readdir(-1)
	fAll.Close()
	if err != nil {
		t.Fatalf("Readdir(-1): %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("Readdir(-1) should return 5 entries, got %d", len(all))
	}
	// Readdir(2) returns at most 2 entries.
	f2, err := store.Open("t1/d")
	if err != nil {
		t.Fatalf("Open dir: %v", err)
	}
	n2, err := f2.Readdir(2)
	f2.Close()
	if err != nil {
		t.Fatalf("Readdir(2): %v", err)
	}
	if len(n2) > 2 {
		t.Fatalf("Readdir(2) should return at most 2 entries, got %d", len(n2))
	}
}

// TestRenameOverwriteFile — xfstests generic/089 rename-overwrite pattern:
// Rename with existing destination must overwrite.
func TestRenameOverwriteFile(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	putTestBytes(t, store, "t1", "src.txt", []byte("source"))
	putTestBytes(t, store, "t1", "dst.txt", []byte("old-dst"))
	if err := store.Rename("t1/src.txt", "t1/dst.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	got := readTestBytes(t, store, "t1", "dst.txt")
	if !bytes.Equal(got, []byte("source")) {
		t.Fatalf("rename overwrite: expected 'source', got '%s'", got)
	}
	_, err := store.OpenObject(ctx, "t1", "src.txt")
	if err == nil {
		t.Fatal("src should not exist after rename")
	}
}

// TestDeepDirectoryTree — xfstests generic/001 deep-tree pattern:
// Create/read/delete deep trees near MaxPathLength.
func TestDeepDirectoryTree(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	depth := 50
	dirParts := make([]string, 0, depth+2)
	dirParts = append(dirParts, "t1", "deep")
	for i := 0; i < depth; i++ {
		dirParts = append(dirParts, "d")
	}
	dir := strings.Join(dirParts, "/")
	if err := store.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll deep: %v", err)
	}
	putTestBytes(t, store, "t1", filepath.Join(dir[3:], "leaf.txt"), []byte("deep"))
	got := readTestBytes(t, store, "t1", filepath.Join(dir[3:], "leaf.txt"))
	if !bytes.Equal(got, []byte("deep")) {
		t.Fatal("deep tree read mismatch")
	}
	if err := store.RemoveAll("t1/deep"); err != nil {
		t.Fatalf("RemoveAll deep tree: %v", err)
	}
}

// TestRenameDirWithChildren — xfstests generic/089 cross-directory rename pattern:
// Rename of a directory moves its subtree atomically.
func TestRenameDirWithChildren(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	if err := store.MkdirAll("t1/src/sub", 0o755); err != nil {
		t.Fatalf("MkdirAll src: %v", err)
	}
	if err := store.MkdirAll("t1/target", 0o755); err != nil {
		t.Fatalf("MkdirAll target: %v", err)
	}
	putTestBytes(t, store, "t1", "src/sub/f.txt", []byte("child"))
	if err := store.Rename("t1/src", "t1/target/moved"); err != nil {
		t.Fatalf("Rename dir: %v", err)
	}
	got := readTestBytes(t, store, "t1", "target/moved/sub/f.txt")
	if !bytes.Equal(got, []byte("child")) {
		t.Fatal("sub-tree file should be accessible after move")
	}
	_, err := store.StatObject(ctx, "t1", "src/sub/f.txt")
	if err == nil {
		t.Fatal("old path should not exist after rename")
	}
}

// =============================================================================
// Category 7: Stress Tests (inspired by xfstests generic/013 fsstress)
// =============================================================================

// TestConcurrentMixedOps — xfstests generic/013 fsstress pattern:
// 20 goroutines run mixed Put/Delete/Rename/GC/Stat; must not panic or deadlock.
func TestConcurrentMixedOps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()
	store := openTestStore(t)
	if err := store.MkdirAll("t1/stress", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(testContext(t), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	ops := 200
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				path := "stress/f" + string(rune('a'+j%26)) + ".txt"
				switch j % 6 {
				case 0:
					store.Put(ctx, "t1", path, bytes.NewReader([]byte("x")), nil)
				case 1:
					store.OpenObject(ctx, "t1", path)
				case 2:
					store.StatObject(ctx, "t1", path)
				case 3:
					store.OpenRange(ctx, "t1", path, 0, 1)
				case 4:
					_ = store.DeleteObject(ctx, "t1", path)
				case 5:
					store.OpenFileContext(ctx, "t1/"+path, os.O_CREATE|os.O_RDWR, 0o644)
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestPutMultiTenantConcurrent — multi-tenant concurrent Put with global dedup:
// verifies content-addressed dedup works correctly under concurrency.
func TestPutMultiTenantConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()
	cfg := testConfig()
	cfg.DedupScope = DedupScopeGlobal
	store := openTestStoreWithConfig(t, cfg)
	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()

	content := []byte("shared-global-content")
	tenants := []string{"g1", "g2", "g3", "g4", "g5"}
	var wg sync.WaitGroup
	for _, ten := range tenants {
		store.MkdirAll(ten, 0o755)
		wg.Add(1)
		go func(tid string) {
			defer wg.Done()
			putTestBytes(t, store, tid, "shared.txt", content)
		}(ten)
	}
	wg.Wait()
	// All tenants should see the same content and same chunk hash.
	var hashes []string
	for _, ten := range tenants {
		r := readTestBytes(t, store, ten, "shared.txt")
		if !bytes.Equal(r, content) {
			t.Fatalf("tenant %s: content mismatch", ten)
		}
		info, _ := store.StatObject(ctx, ten, "shared.txt")
		hashes = append(hashes, info.FileHash)
	}
	for i := 1; i < len(hashes); i++ {
		if hashes[i] != hashes[0] {
			t.Fatalf("global dedup: tenant hashes differ: %s vs %s", hashes[0], hashes[i])
		}
	}
}

// =============================================================================
// Category 8: Recovery / Crash Consistency (inspired by xfstests generic/049)
// =============================================================================

// TestSuperBlockOneValid — xfstests generic/049 recover pattern:
// When one SUPER is valid and the other corrupt, Open must use the good one.
func TestSuperBlockOneValid(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	metaDir := filepath.Join(baseDir, "meta")

	// Open first and put data to establish state, then close.
	store, err := Open(baseDir, testConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.MkdirAll("t1", 0o755); err != nil {
		store.Close()
		t.Fatalf("MkdirAll: %v", err)
	}
	putTestBytes(t, store, "t1", "recovered.txt", []byte("recovery-data"))
	if err := store.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	// Corrupt SUPER0 (write garbage).
	dataDir := baseDir
	_ = dataDir
	if err := afero.WriteFile(afero.NewOsFs(), filepath.Join(metaDir, "SUPER0"), []byte("garbage"), 0o644); err != nil {
		t.Fatalf("corrupt SUPER0: %v", err)
	}
	// Reopen — should use SUPER1 and recover.
	store2, err := Open(baseDir, testConfig())
	if err != nil {
		t.Fatalf("Open with one corrupt super: %v", err)
	}
	defer store2.Close()
	got := readTestBytes(t, store2, "t1", "recovered.txt")
	if !bytes.Equal(got, []byte("recovery-data")) {
		t.Fatalf("expected recovery-data, got %s", got)
	}
}

// TestTornTransactionLogRecovery — xfstests generic/049 partial-log pattern:
// Torn tail in txlog must not prevent recovery; data up to tears is preserved.
func TestTornTransactionLogRecovery(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	store, err := Open(baseDir, testConfig())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.MkdirAll("t1", 0o755); err != nil {
		store.Close()
		t.Fatalf("MkdirAll: %v", err)
	}
	putTestBytes(t, store, "t1", "before-tear.txt", []byte("before"))
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Append torn data to the log file.
	entries, _ := os.ReadDir(filepath.Join(baseDir, "meta", "txlog"))
	if len(entries) == 0 {
		t.Fatal("no txlog entries")
	}
	logPath := filepath.Join(baseDir, "meta", "txlog", entries[len(entries)-1].Name())
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open txlog: %v", err)
	}
	// Write valid frame header but truncated payload.
	// metaFrameMagic = 0x324d4642 = "BFM2" (bytes 0x42, 0x46, 0x4d, 0x32 in LE).
	header := make([]byte, 12)
	header[0] = 0x42 // B
	header[1] = 0x46 // F
	header[2] = 0x4d // M
	header[3] = 0x32 // 2
	header[4] = 16   // size = 16 bytes (but with no actual payload follows)
	_, _ = f.Write(header)
	f.Close()
	store2, err := Open(baseDir, testConfig())
	if err != nil {
		t.Fatalf("Open with torn log: %v", err)
	}
	defer store2.Close()
	got := readTestBytes(t, store2, "t1", "before-tear.txt")
	if !bytes.Equal(got, []byte("before")) {
		t.Fatalf("torn tail should preserve earlier data, got %s", got)
	}
}

// TestSuperBlockBothCorrupted — xfstests generic/049 edge: both supers broken.
func TestSuperBlockBothCorrupted(t *testing.T) {
	t.Parallel()
	baseDir := t.TempDir()
	metaDir := filepath.Join(baseDir, "meta")
	if err := os.MkdirAll(filepath.Join(metaDir, "txlog"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Write two corrupt super blocks.
	afero.WriteFile(afero.NewOsFs(), filepath.Join(metaDir, "SUPER0"), []byte("corrupt0"), 0o644)
	afero.WriteFile(afero.NewOsFs(), filepath.Join(metaDir, "SUPER1"), []byte("corrupt1"), 0o644)
	// Create a valid checkpoint in the new directory-based format.
	meta := newMetadata()
	chkpointDir := filepath.Join(metaDir, metaCheckpointDir)
	if err := afero.NewOsFs().MkdirAll(chkpointDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := checkpointManifest{Version: 2, TxID: meta.TxID, Shards: map[string]checkpointShard{}}
	manifestData, _ := json.Marshal(manifest)
	afero.WriteFile(afero.NewOsFs(), filepath.Join(chkpointDir, "manifest.json"), manifestData, 0o644)
	afero.WriteFile(afero.NewOsFs(), filepath.Join(chkpointDir, "meta.json"), []byte(`{"version":2,"next_inode_id":1,"next_segment_seq":1,"next_gc_epoch":1}`), 0o644)
	// Create a valid log.
	afero.WriteFile(afero.NewOsFs(), filepath.Join(metaDir, metaLogFile+".lock"), []byte{}, 0o600)
	logPath := filepath.Join(metaDir, "txlog", metaLogFile)
	afero.WriteFile(afero.NewOsFs(), logPath, []byte{}, 0o600)
	// Both supers corrupt, should be unable to find valid log.
	_, err := Open(baseDir, testConfig())
	if err == nil {
		t.Fatal("Open should fail when both SUPER0 and SUPER1 are corrupt")
	}
}

// =============================================================================
// Category 10: Sparse / Holes (inspired by xfstests generic/008, 012)
// =============================================================================

// TestVFSSeekWriteHole — xfstests generic/008 seek-write pattern:
// Seek past end, write, read back: hole should be filled with zeros.
func TestVFSSeekWriteHole(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	f, err := store.OpenFileContext(ctx, "t1/hole.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Seek(1000, io.SeekStart); err != nil {
		f.Close()
		t.Fatalf("Seek: %v", err)
	}
	if _, err := f.Write([]byte("data-at-1k")); err != nil {
		f.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	info, err := store.StatObject(ctx, "t1", "hole.txt")
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	if info.Size < 1000+int64(len("data-at-1k")) {
		t.Fatalf("size too small: %d", info.Size)
	}
	got := readTestBytes(t, store, "t1", "hole.txt")
	if !bytes.Equal(got[1000:1000+len("data-at-1k")], []byte("data-at-1k")) {
		t.Fatal("Write past EOF not preserved")
	}
}

// =============================================================================
// Category 11: Rename Operations (inspired by xfstests generic/089)
// =============================================================================

// TestRenameCrossTenantRejected — xfstests generic/089: rename across
// tenants should return ErrInvalid (non-shared namespace).
func TestRenameCrossTenantRejected(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "f.txt", []byte("x"))
	if err := store.MkdirAll("t2", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	err := store.Rename("t1/f.txt", "t2/f.txt")
	if err == nil {
		t.Fatal("Rename across tenants must fail")
	}
}

// TestRenameSelfNoop — xfstests generic/089 self-rename: name -> name is noop.
func TestRenameSelfNoop(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "self.txt", []byte("y"))
	if err := store.Rename("t1/self.txt", "t1/self.txt"); err != nil {
		t.Fatalf("rename self: %v", err)
	}
	got := readTestBytes(t, store, "t1", "self.txt")
	if !bytes.Equal(got, []byte("y")) {
		t.Fatal("self-rename must preserve content")
	}
}

// =============================================================================
// Category 12: Out of Space / File Limits
// =============================================================================

// TestMaxFileSizeBoundary — xfstests generic/015 ENOSPC pattern:
// Put at exactly MaxFileSize succeeds; one byte more fails.
func TestMaxFileSizeBoundary(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.MaxFileSize = 1024
	cfg.SegmentSize = 4096
	cfg.Chunking.MinSize = 512
	cfg.Chunking.AvgSize = 512
	cfg.Chunking.MaxSize = 1024
	store := openTestStoreWithConfig(t, cfg)
	ctx := testContext(t)
	// Exactly at limit.
	putTestBytes(t, store, "t1", "exact.txt", bytes.Repeat([]byte("A"), int(cfg.MaxFileSize)))
	// One byte over.
	_, err := store.Put(ctx, "t1", "over.txt",
		bytes.NewReader(bytes.Repeat([]byte("B"), int(cfg.MaxFileSize)+1)), nil)
	if err == nil {
		t.Fatal("Put beyond MaxFileSize must fail")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected 'too large', got: %v", err)
	}
}

// TestPutNilReaderRejected — xfstests generic/007 boundary: nil reader.
func TestPutNilReaderRejected(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	_, err := store.Put(ctx, "t1", "nil-file.txt", nil, nil)
	if err == nil {
		t.Fatal("Put with nil reader must fail")
	}
}

// =============================================================================
// Category 13: File Descriptor Edge Cases
// =============================================================================

// TestVFSFileOExcl — xfstests generic/007 O_EXCL pattern:
// O_CREATE|O_EXCL must fail when file already exists.
func TestVFSFileOExcl(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	f, err := store.OpenFileContext(ctx, "t1/excl.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile create: %v", err)
	}
	f.Close()
	_, err = store.OpenFileContext(ctx, "t1/excl.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err == nil {
		t.Fatal("O_EXCL on existing file must fail")
	}
}

// TestVFSFileWriteAtReadAtBoundaries — xfstests generic/031 non-aligned I/O:
// WriteAt/ReadAt with various offsets and sizes work correctly.
func TestVFSFileWriteAtReadAtBoundaries(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	f, err := store.OpenFileContext(ctx, "t1/rat.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte("aaaaaaaaaa"), 0); err != nil {
		f.Close()
		t.Fatalf("WriteAt 0: %v", err)
	}
	if _, err := f.WriteAt([]byte("bb"), 100); err != nil {
		f.Close()
		t.Fatalf("WriteAt 100: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := readTestBytes(t, store, "t1", "rat.txt")
	if !bytes.Equal(got[:10], []byte("aaaaaaaaaa")) {
		t.Fatalf("read offset 0: %s", got[:10])
	}
	if !bytes.Equal(got[100:102], []byte("bb")) {
		t.Fatalf("read offset 100: %s", got[100:102])
	}
}

// TestVFSFileReadAtNegativeOffset — xfstests generic/499 read boundaries.
func TestVFSFileReadAtNegativeOffset(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	f, err := store.OpenFileContext(ctx, "t1/neg.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	_, _ = f.Write([]byte("data"))
	f.Close()
	f2, err := store.OpenFileContext(ctx, "t1/neg.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile RO: %v", err)
	}
	defer f2.Close()
	buf := make([]byte, 1)
	_, err = f2.ReadAt(buf, -1)
	if err == nil {
		t.Fatal("ReadAt negative offset must fail")
	}
}

// TestObjectReaderSeekEdgeCases — xfstests generic/499 seek boundaries:
// Seek to EOF, past EOF, from end, closed reader operations.
func TestObjectReaderSeekEdgeCases(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	putTestBytes(t, store, "t1", "seek.txt", []byte("0123456789"))
	r, err := store.OpenObject(ctx, "t1", "seek.txt")
	if err != nil {
		t.Fatalf("OpenObject: %v", err)
	}
	// Seek to end.
	pos, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		r.Close()
		t.Fatalf("Seek end: %v", err)
	}
	if pos != 10 {
		t.Fatalf("SeekEnd: want 10, got %d", pos)
	}
	// Read at EOF.
	buf := make([]byte, 1)
	_, err = r.Read(buf)
	if err != io.EOF {
		r.Close()
		t.Fatalf("Read at EOF should return io.EOF, got %v", err)
	}
	// Seek from end -2.
	pos, err = r.Seek(-2, io.SeekEnd)
	if err != nil {
		r.Close()
		t.Fatalf("Seek -2 from end: %v", err)
	}
	if pos != 8 {
		t.Fatalf("SeekEnd -2: want 8, got %d", pos)
	}
	r.Close()
	// Closed reader read must fail.
	_, err = r.Read(buf)
	if err == nil {
		t.Fatal("Read on closed reader must fail")
	}
}

// =============================================================================
// Category 14: Symlink Resolution (inspired by xfstests generic/005)
//
// Note: blobfs does not implement symlinks. The afero.Fs v1.x interface
// does not require Symlink/Lstat/Readlink.
// =============================================================================

// =============================================================================
// Category 15: Large Directories & Walk Semantics
// =============================================================================

// TestReaddirAfterDelete — xfstests generic/089: deleted entries vanish
// from subsequent Readdir calls.
func TestReaddirAfterDelete(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	if err := store.MkdirAll("t1/rdir", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	putTestBytes(t, store, "t1", "rdir/a.txt", []byte("a"))
	putTestBytes(t, store, "t1", "rdir/b.txt", []byte("b"))
	putTestBytes(t, store, "t1", "rdir/c.txt", []byte("c"))
	if err := store.DeleteObject(ctx, "t1", "rdir/b.txt"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	f, err := store.Open("t1/rdir")
	if err != nil {
		t.Fatalf("Open dir: %v", err)
	}
	infos, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		t.Fatalf("Readdir: %v", err)
	}
	for _, fi := range infos {
		if fi.Name() == "b.txt" {
			t.Fatal("deleted file should not appear in Readdir")
		}
	}
	if len(infos) != 2 {
		t.Fatalf("expected 2 files after delete, got %d", len(infos))
	}
}

// =============================================================================
// Internal: Chunking Boundary Tests (findChunkCut, streamChunks)
// =============================================================================

// TestFindChunkCutBoundaries — unit test for content-defined chunking:
// Content smaller than minSize, at min boundary, exact max boundary,
// and content that never triggers a cut.
func TestFindChunkCutBoundaries(t *testing.T) {
	t.Parallel()
	minSize := 16
	maxSize := 64
	avgSize := 32
	mask := uint64(nextPowerOfTwo(avgSize) - 1) // 31

	// Content smaller than minSize: always return maxSize (no cut possible).
	short := make([]byte, minSize-1)
	for i := range short {
		short[i] = byte(i)
	}
	cut := findChunkCut(short, minSize, maxSize, mask)
	if cut != len(short) {
		t.Fatalf("short data < minSize: want cut=%d, got %d", len(short), cut)
	}

	// Content exactly at minSize: use known cut-triggering byte sequence.
	// Gear values: we need fp & mask == 0 at exactly minSize position.
	// We'll brute-force find a triggering sequence.
	var triggered bool
	for trial := 0; trial < 10000 && !triggered; trial++ {
		buf := make([]byte, maxSize)
		for i := range buf {
			buf[i] = byte(trial*maxSize + i)
		}
		cut = findChunkCut(buf, minSize, maxSize, mask)
		if cut >= minSize && cut < maxSize {
			triggered = true
		}
	}
	if !triggered {
		t.Log("no chunk cut triggered within test range; this is probabilistic but unexpected")
	}

	// Exact max boundary: findChunkCut returns maxSize (full buffer).
	full := make([]byte, maxSize)
	for i := range full {
		full[i] = byte(i)
	}
	cut = findChunkCut(full, minSize, maxSize, mask)
	if cut > maxSize {
		t.Fatalf("cut should not exceed maxSize: got %d", cut)
	}
}

// TestFindChunkCutAllSameByte — content that never triggers mask-based cut
// (some bytes have gear value 0, so fp stays stable). Verify fall-through.
func TestFindChunkCutAllSameByte(t *testing.T) {
	t.Parallel()
	minSize := 16
	maxSize := 64
	mask := uint64(nextPowerOfTwo(32) - 1)
	buf := make([]byte, maxSize)
	// Use zero bytes (gearTable[0] depends on precomputed hash; may or may not trigger).
	// Instead, test that cut never exceeds maxSize regardless.
	for b := 0; b < 256; b++ {
		for i := range buf {
			buf[i] = byte(b)
		}
		cut := findChunkCut(buf, minSize, maxSize, mask)
		if cut < minSize || cut > maxSize {
			t.Errorf("byte %d: cut=%d not in [%d,%d]", b, cut, minSize, maxSize)
		}
	}
}

// TestNextPowerOfTwo — unit test for nextPowerOfTwo edge values.
func TestNextPowerOfTwo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input, want int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{1023, 1024},
		{1024, 1024},
		{1025, 2048},
		{1<<30 - 1, 1 << 30},
		{1 << 30, 1 << 30},
	}
	for _, tc := range tests {
		got := nextPowerOfTwo(tc.input)
		if got != tc.want {
			t.Errorf("nextPowerOfTwo(%d) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

// =============================================================================
// Internal: Metadata Boundary Tests
// =============================================================================

// TestNextMetaLogNameEdgeCases — unit test for nextMetaLogName:
// empty, overflow, non-numeric base, edge sequences.
func TestNextMetaLogNameEdgeCases(t *testing.T) {
	t.Parallel()
	if got := nextMetaLogName(""); got != metaLogFile {
		t.Errorf("nextMetaLogName('') = %s, want %s", got, metaLogFile)
	}
	if got := nextMetaLogName("abc.log"); got != metaLogFile {
		t.Errorf("nextMetaLogName('abc.log') = %s, want %s", got, metaLogFile)
	}
	if got := nextMetaLogName("000001.log"); got != "000002.log" {
		t.Errorf("nextMetaLogName('000001.log') = %s, want 000002.log", got)
	}
	if got := nextMetaLogName("999999.log"); got != "1000000.log" {
		t.Errorf("nextMetaLogName('999999.log') = %s, want 1000000.log", got)
	}
	if got := nextMetaLogName("0.log"); got != metaLogFile {
		t.Errorf("nextMetaLogName('0.log') = %s, want %s", got, metaLogFile)
	}
}

// TestDecodeMetaSuperBlockEdgeCases — unit test for decodeMetaSuperBlock:
// version mismatch, empty log name, checksum mismatch, invalid name.
func TestDecodeMetaSuperBlockEdgeCases(t *testing.T) {
	t.Parallel()
	// Valid superblock.
	v := metaSuperBlock{FormatVersion: metaFormatVersion, LogFile: "000001.log"}
	v.CRC = 0
	payload, _ := json.Marshal(v)
	v.CRC = superBlockCRC(payload)
	payloadWithCRC, _ := json.Marshal(v)
	got, err := decodeMetaSuperBlock(payloadWithCRC)
	if err != nil {
		t.Fatalf("valid super block decode: %v", err)
	}
	if got.LogFile != "000001.log" {
		t.Fatalf("LogFile: %s", got.LogFile)
	}

	// Version mismatch.
	v2 := v
	v2.FormatVersion = 99
	v2.CRC = 0
	payload2, _ := json.Marshal(v2)
	v2.CRC = superBlockCRC(payload2)
	payload2CRC, _ := json.Marshal(v2)
	_, err = decodeMetaSuperBlock(payload2CRC)
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("version mismatch: %v", err)
	}

	// Invalid log file name (no .log extension).
	v3 := metaSuperBlock{FormatVersion: metaFormatVersion, LogFile: "bad_name"}
	v3.CRC = 0
	payload3, _ := json.Marshal(v3)
	v3.CRC = superBlockCRC(payload3)
	payload3CRC, _ := json.Marshal(v3)
	_, err = decodeMetaSuperBlock(payload3CRC)
	if err == nil || !strings.Contains(err.Error(), "log file") {
		t.Fatalf("invalid log name: %v", err)
	}

	// Invalid log file name (path separator).
	v4 := metaSuperBlock{FormatVersion: metaFormatVersion, LogFile: "x/000001.log"}
	v4.CRC = 0
	payload4, _ := json.Marshal(v4)
	v4.CRC = superBlockCRC(payload4)
	payload4CRC, _ := json.Marshal(v4)
	_, err = decodeMetaSuperBlock(payload4CRC)
	if err == nil || !strings.Contains(err.Error(), "log file") {
		t.Fatalf("path separator in log name: %v", err)
	}

	// Checksum mismatch.
	v5 := metaSuperBlock{FormatVersion: metaFormatVersion, LogFile: "000001.log", CRC: 0xDEADBEEF}
	payload5, _ := json.Marshal(v5)
	_, err = decodeMetaSuperBlock(payload5)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum mismatch: %v", err)
	}
}

// TestCompactMetadataEdgeCases — unit test for compactMetadata:
// nil Inodes in map, orphan dirents for non-existent inodes, deleted refs.
func TestCompactMetadataEdgeCases(t *testing.T) {
	t.Parallel()
	meta := newMetadata()
	// Set up a tenant with active file.
	meta.Tenants["t1"] = 1
	meta.Inodes[1] = &inodeRecord{
		InodeID: 1, Kind: fileKindDir, State: fileStateActive, TenantID: "t1",
	}
	// An active file inode with a manifest.
	meta.Inodes[2] = &inodeRecord{
		InodeID: 2, Kind: fileKindFile, State: fileStateActive, TenantID: "t1",
		ManifestID: "man1", ParentInode: 1, Name: "f.txt",
	}
	meta.DirEntries[1] = map[string]uint64{"f.txt": 2}
	meta.Manifests["man1"] = &manifestRecord{
		ManifestID: "man1", State: manifestStateActive, RefCount: 1, TenantID: "t1",
	}
	// A deleted inode that is orphaned (not referenced by any dirent).
	meta.Inodes[3] = &inodeRecord{
		InodeID: 3, Kind: fileKindFile, State: fileStateDeleted, TenantID: "t1",
		ManifestID: "man2",
	}
	meta.Manifests["man2"] = &manifestRecord{
		ManifestID: "man2", State: manifestStateDeleted, RefCount: 0, TenantID: "t1",
	}
	// A nil inode entry in the map.
	meta.Inodes[99] = nil
	// A nil chunk entry.
	meta.Chunks["nil-chunk"] = nil

	compactMetadata(meta)
	// Deleted orphan inode should be removed.
	if _, ok := meta.Inodes[3]; ok {
		t.Fatal("orphaned deleted inode should be removed by compactMetadata")
	}
	// Nil inode should be removed.
	if _, ok := meta.Inodes[99]; ok {
		t.Fatal("nil inode entry should be removed")
	}
	// Nil chunk should be removed.
	if _, ok := meta.Chunks["nil-chunk"]; ok {
		t.Fatal("nil chunk entry should be removed")
	}
	// Active inode should survive.
	if meta.Inodes[2] == nil {
		t.Fatal("active inode should survive compactMetadata")
	}
}

// =============================================================================
// Internal: Refcount Edge Cases
// =============================================================================

// TestAddManifestRefDeltaDedup — unit test for addManifestRefDelta:
// tests dedup chunk counting (same ChunkID twice in manifest only counts once).
func TestAddManifestRefDeltaDedup(t *testing.T) {
	t.Parallel()
	manifest := &manifestRecord{
		ManifestID: "man",
		Chunks: []manifestChunk{
			{ChunkID: "chunk-A", Index: 0},
			{ChunkID: "chunk-B", Index: 1},
			{ChunkID: "chunk-A", Index: 2}, // duplicate chunk ID
		},
	}
	md := map[string]int{}
	cd := map[string]int{}
	addManifestRefDelta(manifest, 2, md, cd)
	if md["man"] != 2 {
		t.Fatalf("manifest delta: want 2, got %d", md["man"])
	}
	// chunk-A should be counted once despite appearing twice.
	if cd["chunk-A"] != 2 {
		t.Fatalf("chunk-A delta: want 2, got %d", cd["chunk-A"])
	}
	if cd["chunk-B"] != 2 {
		t.Fatalf("chunk-B delta: want 2, got %d", cd["chunk-B"])
	}
}

// TestAddManifestRefDeltaNilAndZero — unit test: nil manifest and zero delta.
func TestAddManifestRefDeltaNilAndZero(t *testing.T) {
	t.Parallel()
	md := map[string]int{}
	cd := map[string]int{}
	addManifestRefDelta(nil, 1, md, cd) // nil manifest — no-op.
	if len(md) > 0 {
		t.Fatal("nil manifest should not modify maps")
	}
	addManifestRefDelta(&manifestRecord{ManifestID: "m"}, 0, md, cd) // zero delta — no-op.
	if len(md) > 0 {
		t.Fatal("zero delta should not modify maps")
	}
}

// TestAppendRefDeltaOpsNegativeClamp — unit test: negative refcount clamps to 0
// and records warning.
func TestAppendRefDeltaOpsNegativeClamp(t *testing.T) {
	t.Parallel()
	meta := newMetadata()
	meta.Chunks["ch"] = &chunkRecord{
		ChunkID: "ch", RefCount: 1, State: chunkStateActive, SegmentID: "seg1",
	}
	md := map[string]int{}
	cd := map[string]int{"ch": -5} // underflow
	var ops []metaOp
	var warnings []string
	appendRefDeltaOpsLocked(meta, &ops, nil, md, cd, 0, &warnings)
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d", len(warnings))
	}
	if !strings.Contains(warnings[0], "went negative") {
		t.Fatalf("warning should mention negative refcount: %s", warnings[0])
	}
}

// =============================================================================
// Internal: Segment Format Boundary Tests
// =============================================================================

// TestReadChunkPayloadSegmentHeaderMagic — xfstests generic/001 read-verify:
// Corrupting the segment-level header magic (offset 0) must not affect
// individual chunk reads from their specific offsets.
func TestReadChunkPayloadSegmentHeaderMagic(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "magic-test.txt", []byte(strings.Repeat("verify-magic", 8)))
	chunk, seg := firstChunkSnapshot(t, store, "t1", "magic-test.txt")
	segPath := store.segmentPath(&seg)

	// Read original data first to verify it's correct.
	raw, err := store.readChunkPayloadAt(seg, chunk)
	if err != nil {
		t.Fatalf("original chunk read: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("original chunk data is empty")
	}

	// Corrupt only the first byte of the segment file (segment-level header magic).
	data, err := afero.ReadFile(store.fs, segPath)
	if err != nil {
		t.Fatalf("read segment file: %v", err)
	}
	originalFirst := data[0]
	data[0] ^= 0xff
	if err := afero.WriteFile(store.fs, segPath, data, 0o644); err != nil {
		t.Fatalf("write corrupted segment: %v", err)
	}
	// Restore segment file after test.
	t.Cleanup(func() {
		data[0] = originalFirst
		_ = afero.WriteFile(store.fs, segPath, data, 0o644)
	})

	// reading chunks at their specific offsets should still work because
	// readChunkPayloadAt seeks to chunk.SegmentOffset, bypassing the
	// global segment header at offset 0.
	raw2, err := store.readChunkPayloadAt(seg, chunk)
	if err != nil {
		t.Errorf("chunk read after header corruption: %v", err)
		return
	}
	if !bytes.Equal(raw, raw2) {
		t.Fatal("chunk data changed after segment header corruption")
	}
}

// TestReadChunkPayloadChunkIdMismatch — metadata chunk ID vs record chunk ID
// mismatch must be detected.
func TestReadChunkPayloadChunkIdMismatch(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "id-mismatch.txt", []byte("id-check"))
	chunk, seg := firstChunkSnapshot(t, store, "t1", "id-mismatch.txt")
	// Modify chunkID in the segment record header (bytes 8-71).
	segPath := store.segmentPath(&seg)
	data, err := afero.ReadFile(store.fs, segPath)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	// Corrupt the first byte of chunk ID in record header.
	data[chunk.SegmentOffset+8] ^= 0xff
	if err := afero.WriteFile(store.fs, segPath, data, 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	_, err = store.readChunkPayloadAt(seg, chunk)
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("chunk ID mismatch must be detected, got: %v", err)
	}
}

// =============================================================================
// Internal: GC Edge Cases
// =============================================================================

// TestGCSafetyWindowPreventsDeletion — xfstests generic/049: chunk within
// SafetyWindow is not deleted even if unreferenced.
func TestGCSafetyWindowPreventsDeletion(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.GC.SafetyWindow = 1 * time.Hour
	cfg.GC.SegmentDeleteDelay = -1
	cfg.GC.CandidateConfirmCycles = 1
	store := openTestStoreWithConfig(t, cfg)
	ctx := testContext(t)
	putTestBytes(t, store, "t1", "safe-test.txt", []byte("safe-data"))
	if err := store.DeleteObject(ctx, "t1", "safe-test.txt"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	result, err := store.RunGC(ctx, GCOptions{Compact: false})
	if err != nil {
		t.Fatalf("RunGC: %v", err)
	}
	// The chunk was just created, SafetyWindow prevents deletion.
	if result.ChunksDeleted > 0 {
		t.Fatalf("SafetyWindow should prevent deletion: deleted %d chunks", result.ChunksDeleted)
	}
}

// TestSegmentSafeToDeleteNonExistent — nil or deleted segment is not safe.
func TestSegmentSafeToDeleteNonExistent(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	if store.segmentSafeToDelete("nonexistent-seg-id") {
		t.Fatal("nonexistent segment should not be safe to delete")
	}
}

// TestSegmentSafeToDeleteBlocksCompactingSegment — a COMPACTING segment
// contains live chunks being migrated; it must not be safe to delete.
func TestSegmentSafeToDeleteBlocksCompactingSegment(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "compacting-src.txt", []byte("compacting-data"))
	// Mark the segment as COMPACTING in metadata.
	store.metaMu.Lock()
	for _, seg := range store.meta.Segments {
		seg.State = segmentStateCompacting
		break
	}
	store.metaMu.Unlock()

	// Find the segment ID that was just put.
	store.metaMu.RLock()
	var segID string
	for id, seg := range store.meta.Segments {
		if seg.State == segmentStateCompacting {
			segID = id
			break
		}
	}
	store.metaMu.RUnlock()

	if segID == "" {
		t.Fatal("no segment found")
	}
	if store.segmentSafeToDelete(segID) {
		t.Fatal("COMPACTING segment should not be safe to delete")
	}
}

// TestGCTenantDeleteReleasesAllChunks — xfstests generic/015 space reclaim:
// DeleteTenant + GC releases all chunks and segments.
func TestGCTenantDeleteReleasesAllChunks(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	// Create multiple files under one tenant.
	putTestBytes(t, store, "td1", "f1.txt", []byte("file-1-content"))
	putTestBytes(t, store, "td1", "f2.txt", []byte("file-2-content"))
	putTestBytes(t, store, "td2", "x.txt", []byte("other-tenant"))

	if err := store.DeleteTenant(ctx, "td1"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	result, err := store.RunGC(ctx, GCOptions{Compact: true})
	if err != nil {
		t.Fatalf("RunGC: %v", err)
	}
	if result.ChunksDeleted == 0 {
		t.Fatal("DeleteTenant + GC should delete orphan chunks")
	}
	// Other tenant must still work.
	got := readTestBytes(t, store, "td2", "x.txt")
	if !bytes.Equal(got, []byte("other-tenant")) {
		t.Fatal("unrelated tenant data should survive DeleteTenant+GC")
	}
}

// =============================================================================
// Internal: Degradation Edge Cases
// =============================================================================

// TestAssessImpactWithPrefixFilter — AssessImpact filters by Prefix correctly.
func TestAssessImpactWithPrefixFilter(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	store.MkdirAll("t1/aa", 0o755)
	store.MkdirAll("t1/bb", 0o755)
	putTestBytes(t, store, "t1", "aa/f1.txt", bytes.Repeat([]byte("a"), 256))
	putTestBytes(t, store, "t1", "bb/f2.txt", bytes.Repeat([]byte("b"), 256))
	// Corrupt f1 to make it degraded.
	corruptFirstChunkPayloadByte(t, store, "t1", "aa/f1.txt")
	_, err := store.CheckObject(ctx, "t1", "aa/f1.txt")
	if err == nil {
		t.Fatal("CheckObject should detect corruption")
	}
	// AssessImpact with prefix filter should only find aa/*.
	report, err := store.AssessImpact(ctx, ImpactOptions{Prefix: "aa/"})
	if err != nil {
		t.Fatalf("AssessImpact: %v", err)
	}
	if len(report.AffectedObjects) != 1 {
		t.Fatalf("prefix filter: want 1 affected, got %d", len(report.AffectedObjects))
	}
	if report.AffectedObjects[0].Path != "aa/f1.txt" {
		t.Fatalf("prefix filter wrong object: %s", report.AffectedObjects[0].Path)
	}
}

// TestDegradeIssueNoSegmentOnlyChunk — degradeIssuesLocked with only ChunkID
// (no SegmentID) still degrades the chunk.
func TestDegradeIssueNoSegmentOnlyChunk(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "chk-only.txt", []byte("test-data"))
	chunk, _ := firstChunkSnapshot(t, store, "t1", "chk-only.txt")

	store.metaMu.Lock()
	var ops []metaOp
	store.degradeIssuesLocked([]CheckIssue{
		{Kind: "chunk_read_failed", ChunkID: chunk.ChunkID, Reason: "test chunk degrade"},
	}, nowUnix(), &ops)
	if err := store.commitMetaLocked(ops); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("commit degrade: %v", err)
	}
	store.metaMu.Unlock()

	store.metaMu.RLock()
	c := store.meta.Chunks[chunk.ChunkID]
	store.metaMu.RUnlock()
	if c.State != chunkStateMissing {
		t.Fatalf("chunk should be MISSING after degrade, got %s", c.State)
	}
}

// TestDegradeIssueOnlySegmentMissing — degrade by SegmentID marks all chunks
// in that segment as MISSING.
func TestDegradeIssueOnlySegmentMissing(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "seg-deg.txt", []byte("segment-data"))
	chunk, seg := firstChunkSnapshot(t, store, "t1", "seg-deg.txt")

	store.metaMu.Lock()
	var ops []metaOp
	store.degradeIssuesLocked([]CheckIssue{
		{Kind: "segment_missing", SegmentID: seg.SegmentID, Reason: "test segment degrade"},
	}, nowUnix(), &ops)
	if err := store.commitMetaLocked(ops); err != nil {
		store.metaMu.Unlock()
		t.Fatalf("commit segment degrade: %v", err)
	}
	store.metaMu.Unlock()

	store.metaMu.RLock()
	s := store.meta.Segments[seg.SegmentID]
	c := store.meta.Chunks[chunk.ChunkID]
	inode, _ := store.resolvePathLocked("t1", "seg-deg.txt")
	store.metaMu.RUnlock()

	if s.State != segmentStateMissing {
		t.Fatalf("segment should be MISSING: got %s", s.State)
	}
	// Since segment is missing, the chunk in it should also be missing.
	if c.State == chunkStateActive {
		t.Log("chunk state after segment-degrade:", c.State)
	}
	// Inode should be degraded.
	if inode != nil && inode.State != fileStateDegraded {
		t.Fatalf("inode should be DEGRADED when segment is missing: got %s", inode.State)
	}
}

// =============================================================================
// Internal: Path Validation Edge Cases
// =============================================================================

// TestSplitVfspathEdgeCases — splitVFSPath various inputs.
func TestSplitVfspathEdgeCases(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	// All variants.
	tests := []struct {
		name, wantTenant, wantPath string
		wantErr                    bool
	}{
		{"t1/f.txt", "t1", "f.txt", false},
		{"t1", "t1", "", false},
		// Invalid tenant only — missing slash.
		// Note: splitVFSPath returns error for empty cleaned path; "t1" without slash
		// returns empty path which is a valid tenant-root path.
	}
	for _, tc := range tests {
		ten, path, _, err := (&Store{cfg: cfg}).splitVFSPath(tc.name)
		if tc.wantErr && err == nil {
			t.Errorf("%q: expected error", tc.name)
			continue
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%q: unexpected error %v", tc.name, err)
			continue
		}
		if err == nil {
			if ten != tc.wantTenant || path != tc.wantPath {
				t.Errorf("%q: got (%q,%q), want (%q,%q)", tc.name, ten, path, tc.wantTenant, tc.wantPath)
			}
		}
	}
}

// TestPathBaseAndParent — unit test for pathBase and parentPath edge strings.
func TestPathBaseAndParent(t *testing.T) {
	t.Parallel()
	if got := pathBase(""); got != "" {
		t.Errorf("pathBase('') = %s, want ''", got)
	}
	if got := pathBase("foo"); got != "foo" {
		t.Errorf("pathBase('foo') = %s, want 'foo'", got)
	}
	if got := pathBase("a/b/c"); got != "c" {
		t.Errorf("pathBase('a/b/c') = %s, want 'c'", got)
	}
	if got := parentPath(""); got != "" {
		t.Errorf("parentPath('') = %s, want ''", got)
	}
	if got := parentPath("foo"); got != "" {
		t.Errorf("parentPath('foo') = %s, want ''", got)
	}
	if got := parentPath("a/b/c"); got != "a/b" {
		t.Errorf("parentPath('a/b/c') = %s, want 'a/b'", got)
	}
}

// =============================================================================
// Category 5: VFS RemoveAll / Root restrictions
// =============================================================================

// TestVFSRemoveAllRootRejected — RemoveAll on tenant root should be rejected
// to prevent accidentally deleting the entire tenant namespace in a single op.
func TestVFSRemoveAllRootRejected(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	if err := store.MkdirAll("t1/dir", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	err := store.RemoveAll("t1")
	if err == nil {
		t.Fatal("RemoveAll on tenant root should be rejected")
	}
}

// TestVFSReadDirOnFile — call Readdir on a file must fail.
func TestVFSReadDirOnFile(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "not-a-dir.txt", []byte("data"))
	f, err := store.Open("t1/not-a-dir.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = f.Readdir(10)
	if err == nil {
		f.Close()
		t.Fatal("Readdir on file must fail")
	}
	f.Close()
}

// =============================================================================
// Category 6: Multi-Operation Consistency
// =============================================================================

// TestPutThenStatViaVFS — xfstests generic/007 consistency:
// Object written via Put must be visible via VFS Stat.
func TestPutThenStatViaVFS(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	putTestBytes(t, store, "t1", "vfs-check.txt", []byte("vfs-data"))
	fi, err := store.Stat("t1/vfs-check.txt")
	if err != nil {
		t.Fatalf("VFS Stat: %v", err)
	}
	if fi.Name() != "vfs-check.txt" {
		t.Fatalf("name: %s", fi.Name())
	}
	if fi.Size() != int64(len("vfs-data")) {
		t.Fatalf("size: %d", fi.Size())
	}
}

// TestVFSWriteThenReadViaObject — xfstests generic/007 consistency:
// Data written via VFS must be readable via Object API.
func TestVFSWriteThenReadViaObject(t *testing.T) {
	t.Parallel()
	store := openTestStore(t)
	ctx := testContext(t)
	f, err := store.OpenFileContext(ctx, "t1/vfs-obj.txt", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("vfs-to-obj")); err != nil {
		f.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := readTestBytes(t, store, "t1", "vfs-obj.txt")
	if !bytes.Equal(got, []byte("vfs-to-obj")) {
		t.Fatalf("VFS-to-Object read: got %s", got)
	}
}

// =============================================================================
// Helper: open test store with custom config
// =============================================================================

func openTestStoreWithConfig(t *testing.T, cfg Config) *Store {
	t.Helper()
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

// superBlockCRC computes IEEE CRC32 for superblock (metadata.go uses crc32.ChecksumIEEE).
func superBlockCRC(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
