package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func TestPutStoresFileClassesAsExpected(t *testing.T) {
	store := openTestStore(t)

	empty := putBytes(t, store, "tenant-a", "empty", nil)
	if empty.ChunkingType != chunkingSingle || empty.Size != 0 || empty.ChunkCount != 1 {
		t.Fatalf("empty classification = %s/%d size=%d", empty.ChunkingType, empty.ChunkCount, empty.Size)
	}

	tiny := putBytes(t, store, "tenant-a", "tiny", []byte("1234567"))
	if tiny.ChunkingType != chunkingSingle || tiny.ChunkCount != 1 {
		t.Fatalf("tiny classification = %s/%d", tiny.ChunkingType, tiny.ChunkCount)
	}

	small := putBytes(t, store, "tenant-a", "small", []byte("123456789abcdef0"))
	if small.ChunkingType != chunkingSingle || small.ChunkCount != 1 {
		t.Fatalf("small classification = %s/%d", small.ChunkingType, small.ChunkCount)
	}

	mediumData := []byte(strings.Repeat("m", 64))
	medium := putBytes(t, store, "tenant-a", "medium", mediumData)
	if medium.ChunkingType != chunkingSingle || medium.ChunkCount != 1 {
		t.Fatalf("medium classification = %s/%d", medium.ChunkingType, medium.ChunkCount)
	}

	largeData := []byte(strings.Repeat("abcdefghij", 20))
	large := putBytes(t, store, "tenant-a", "large", largeData)
	if large.ChunkingType != chunkingFastCDC || large.ChunkCount < 2 {
		t.Fatalf("large classification = %s/%d", large.ChunkingType, large.ChunkCount)
	}

	if got := readBytes(t, store, "tenant-a", "tiny"); !bytes.Equal(got, []byte("1234567")) {
		t.Fatalf("tiny read mismatch: %q", got)
	}
	if got := readBytes(t, store, "tenant-a", "empty"); len(got) != 0 {
		t.Fatalf("empty read mismatch: %q", got)
	}
	if got := readBytes(t, store, "tenant-a", "medium"); !bytes.Equal(got, mediumData) {
		t.Fatalf("medium read mismatch")
	}
	if got := readBytes(t, store, "tenant-a", "large"); !bytes.Equal(got, largeData) {
		t.Fatalf("large read mismatch")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if countSegments(store) != 1 {
		t.Fatalf("expected all file classes to share one segment layer")
	}
}

func TestPutDeduplicatesWithinTenantAndDeleteKeepsSharedManifest(t *testing.T) {
	store := openTestStore(t)
	data := []byte("same-payload")
	first := putBytes(t, store, "tenant-a", "a", data)
	second := putBytes(t, store, "tenant-a", "b", data)
	if first.ManifestID != second.ManifestID {
		t.Fatalf("same tenant same content should reuse manifest")
	}

	otherTenant := putBytes(t, store, "tenant-b", "a", data)
	if otherTenant.ManifestID == first.ManifestID || otherTenant.FileHash == first.FileHash {
		t.Fatalf("tenant-scoped dedup should not share manifest/hash across tenants")
	}

	if err := store.DeleteObject(context.Background(), "tenant-a", "a"); err != nil {
		t.Fatalf("delete first shared file: %v", err)
	}
	if got := readBytes(t, store, "tenant-a", "b"); !bytes.Equal(got, data) {
		t.Fatalf("remaining shared file became unreadable")
	}
	store.mu.Lock()
	if store.meta.Manifests[first.ManifestID].State != manifestStateActive {
		t.Fatalf("manifest was deleted while another active file referenced it")
	}
	store.mu.Unlock()

	if err := store.DeleteObject(context.Background(), "tenant-a", "b"); err != nil {
		t.Fatalf("delete second shared file: %v", err)
	}
	store.mu.Lock()
	if store.meta.Manifests[first.ManifestID].State != manifestStateDeleted {
		t.Fatalf("manifest should be deleted after last active reference")
	}
	store.mu.Unlock()
}

func TestPutDeduplicatesGloballyWhenConfigured(t *testing.T) {
	cfg := testConfig()
	cfg.DedupScope = DedupScopeGlobal
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	data := []byte("global-payload")
	first := putBytes(t, store, "tenant-a", "file", data)
	second := putBytes(t, store, "tenant-b", "file", data)
	if first.FileHash != second.FileHash || first.ManifestID != second.ManifestID {
		t.Fatalf("global dedup should reuse file hash and manifest")
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", "file"); err != nil {
		t.Fatalf("delete first tenant file: %v", err)
	}
	if got := readBytes(t, store, "tenant-b", "file"); !bytes.Equal(got, data) {
		t.Fatalf("remaining global-dedup file became unreadable")
	}
	store.mu.Lock()
	if store.meta.Manifests[first.ManifestID].State != manifestStateActive {
		t.Fatalf("global manifest should remain active while referenced by another tenant")
	}
	store.mu.Unlock()
}

func TestOpenRangeReadsAcrossLargeFileChunks(t *testing.T) {
	store := openTestStore(t)
	data := []byte(strings.Repeat("0123456789abcdef", 16))
	result := putBytes(t, store, "tenant-a", "large", data)
	if result.ChunkCount < 2 {
		t.Fatalf("test needs multiple chunks, got %d", result.ChunkCount)
	}

	reader, err := store.OpenRange(context.Background(), "tenant-a", "large", 17, 101)
	if err != nil {
		t.Fatalf("open range: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close range: %v", err)
	}
	if !bytes.Equal(got, data[17:118]) {
		t.Fatalf("range mismatch")
	}

	empty, err := store.OpenRange(context.Background(), "tenant-a", "large", 17, 0)
	if err != nil {
		t.Fatalf("open empty range: %v", err)
	}
	got, err = io.ReadAll(empty)
	if err != nil {
		t.Fatalf("read empty range: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty range returned data: %q", got)
	}
	if err := empty.Close(); err != nil {
		t.Fatalf("close empty range: %v", err)
	}

	atEOF, err := store.OpenRange(context.Background(), "tenant-a", "large", int64(len(data)), 10)
	if err != nil {
		t.Fatalf("open eof range: %v", err)
	}
	if got, err = io.ReadAll(atEOF); err != nil || len(got) != 0 {
		t.Fatalf("eof range = %q, %v", got, err)
	}
	if err := atEOF.Close(); err != nil {
		t.Fatalf("close eof range: %v", err)
	}
	if _, err := store.OpenRange(context.Background(), "tenant-a", "large", int64(len(data))+1, 1); !errors.Is(err, io.EOF) {
		t.Fatalf("range past eof should return io.EOF, got %v", err)
	}
	hugeRange, err := store.OpenRange(context.Background(), "tenant-a", "large", 0, 1<<62)
	if err != nil {
		t.Fatalf("open huge range: %v", err)
	}
	if got, err = io.ReadAll(hugeRange); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("huge range should clamp to file size: len=%d err=%v", len(got), err)
	}
	if err := hugeRange.Close(); err != nil {
		t.Fatalf("close huge range: %v", err)
	}
	if _, err := store.OpenRange(context.Background(), "tenant-a", "large", -1, 1); err == nil {
		t.Fatalf("negative range should fail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.OpenRange(ctx, "tenant-a", "large", 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled range should return context error, got %v", err)
	}
}

func TestPutReusesDeletedManifestWhileChunksRemain(t *testing.T) {
	store := openTestStore(t)
	data := []byte("reactivate")
	first := putBytes(t, store, "tenant-a", "source", data)
	if err := store.DeleteObject(context.Background(), "tenant-a", "source"); err != nil {
		t.Fatalf("delete source: %v", err)
	}

	store.mu.Lock()
	if store.meta.Manifests[first.ManifestID].State != manifestStateDeleted {
		t.Fatalf("manifest should be deleted after deleting only file")
	}
	store.mu.Unlock()

	second := putBytes(t, store, "tenant-a", "new", data)
	if second.ManifestID != first.ManifestID {
		t.Fatalf("same file should reactivate existing manifest")
	}
	if got := readBytes(t, store, "tenant-a", "new"); !bytes.Equal(got, data) {
		t.Fatalf("reactivated file read mismatch")
	}
}

func TestPutRebuildsChunksForCollectedManifest(t *testing.T) {
	store := openTestStore(t)
	data := []byte("collected")
	first := putBytes(t, store, "tenant-a", "source", data)
	if err := store.DeleteObject(context.Background(), "tenant-a", "source"); err != nil {
		t.Fatalf("delete source: %v", err)
	}
	if _, err := store.RunGC(context.Background(), GCOptions{CandidateConfirmCycles: 2, Compact: false}); err != nil {
		t.Fatalf("first gc: %v", err)
	}
	if _, err := store.RunGC(context.Background(), GCOptions{CandidateConfirmCycles: 2, Compact: true}); err != nil {
		t.Fatalf("second gc: %v", err)
	}

	store.mu.Lock()
	for _, ref := range store.manifestRefs(store.meta.Manifests[first.ManifestID]) {
		if store.meta.Chunks[ref.ChunkID].State != chunkStateDeleted {
			t.Fatalf("expected original chunk to be collected")
		}
	}
	store.mu.Unlock()

	second := putBytes(t, store, "tenant-a", "new", data)
	if second.ManifestID != first.ManifestID {
		t.Fatalf("manifest id should remain content-addressed")
	}
	if got := readBytes(t, store, "tenant-a", "new"); !bytes.Equal(got, data) {
		t.Fatalf("rebuilt manifest read mismatch")
	}
}

func TestOpenReloadsMetadataAfterRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	want := []byte("persistent-data")
	result := putBytes(t, store, "tenant-a", "persist", want)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	if got := readBytes(t, reopened, "tenant-a", "persist"); !bytes.Equal(got, want) {
		t.Fatalf("persisted data mismatch")
	}
	info, err := reopened.StatObject(context.Background(), "tenant-a", "persist")
	if err != nil {
		t.Fatalf("stat persisted file: %v", err)
	}
	if info.ManifestID != result.ManifestID || info.FileHash != result.FileHash {
		t.Fatalf("persisted metadata mismatch: %+v", info)
	}
}

func TestStoreImplementsAferoFilesystemAndPersistsVFSMetadata(t *testing.T) {
	fs := afero.NewMemMapFs()
	store, err := OpenFS(fs, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open afero store: %v", err)
	}
	var _ afero.Fs = store
	if err := store.MkdirAll("tenant-a/docs", 0o750); err != nil {
		t.Fatalf("mkdir through vfs: %v", err)
	}

	want := []byte("afero-backed-data")
	if err := afero.WriteFile(store, "tenant-a/docs/file.txt", want, 0o640); err != nil {
		t.Fatalf("write through vfs: %v", err)
	}
	modTime := time.Unix(1700000000, 123)
	if err := store.Chtimes("tenant-a/docs/file.txt", modTime, modTime); err != nil {
		t.Fatalf("chtimes through vfs: %v", err)
	}
	if err := store.Chown("tenant-a/docs/file.txt", 1001, 1002); err != nil {
		t.Fatalf("chown through vfs: %v", err)
	}
	if _, err := fs.Stat("/blobfs/meta/blobfs.json"); err != nil {
		t.Fatalf("metadata should be written to provided filesystem: %v", err)
	}
	if got, err := afero.ReadFile(store, "tenant-a/docs/file.txt"); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("vfs read = %q, %v", got, err)
	}
	entries, err := afero.ReadDir(store, "tenant-a/docs")
	if err != nil {
		t.Fatalf("readdir through vfs: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "file.txt" {
		t.Fatalf("vfs entries = %+v", entries)
	}
	stat, err := store.Stat("tenant-a/docs/file.txt")
	if err != nil {
		t.Fatalf("stat through vfs: %v", err)
	}
	if stat.Mode().Perm() != 0o640 || !stat.ModTime().Equal(modTime) {
		t.Fatalf("vfs metadata mode=%v mod=%v", stat.Mode(), stat.ModTime())
	}
	store.mu.Lock()
	record := store.meta.Files[fileKey("tenant-a", "docs/file.txt")]
	if record == nil || record.UID != 1001 || record.GID != 1002 {
		t.Fatalf("vfs uid/gid metadata = %+v", record)
	}
	store.mu.Unlock()
	info, err := store.StatObject(context.Background(), "tenant-a", "docs/file.txt")
	if err != nil {
		t.Fatalf("stat object written through vfs: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close afero store: %v", err)
	}

	reopened, err := OpenFS(fs, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("reopen afero store: %v", err)
	}
	defer reopened.Close()
	reopenedInfo, err := reopened.StatObject(context.Background(), "tenant-a", "docs/file.txt")
	if err != nil {
		t.Fatalf("stat reopened afero object: %v", err)
	}
	if reopenedInfo.ManifestID != info.ManifestID {
		t.Fatalf("reopened afero metadata mismatch: %+v", info)
	}
	reopenedStat, err := reopened.Stat("tenant-a/docs/file.txt")
	if err != nil {
		t.Fatalf("stat reopened vfs file: %v", err)
	}
	if reopenedStat.Mode().Perm() != 0o640 || !reopenedStat.ModTime().Equal(modTime) {
		t.Fatalf("reopened vfs metadata mode=%v mod=%v", reopenedStat.Mode(), reopenedStat.ModTime())
	}
	if got, err := afero.ReadFile(reopened, "tenant-a/docs/file.txt"); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reopened afero read mismatch")
	}
}

func TestOpenFSRejectsNilFilesystem(t *testing.T) {
	if _, err := OpenFS(nil, "/blobfs", testConfig()); err == nil {
		t.Fatalf("open should reject nil filesystem")
	}
}

func TestUpdateMetadataReplacesOptionsWithoutRewritingContent(t *testing.T) {
	store := openTestStore(t)
	data := []byte("metadata-only-data")
	result := putBytes(t, store, "tenant-a", "meta", data)
	before, err := store.StatObject(context.Background(), "tenant-a", "meta")
	if err != nil {
		t.Fatalf("stat before metadata update: %v", err)
	}

	nextFileSeq := store.meta.NextFileSeq
	options := map[string]string{
		"content-type": "text/plain",
		"owner":        "alice",
	}
	updated, err := store.UpdateMetadata(context.Background(), "tenant-a", "meta", options)
	if err != nil {
		t.Fatalf("update metadata: %v", err)
	}
	if updated.FileID != before.FileID || updated.ManifestID != result.ManifestID ||
		updated.FileHash != result.FileHash || updated.Size != int64(len(data)) {
		t.Fatalf("metadata update changed content identity: before=%+v after=%+v result=%+v", before, updated, result)
	}
	if !updated.CreatedAt.Equal(before.CreatedAt) || updated.UpdatedAt.Before(before.UpdatedAt) {
		t.Fatalf("metadata update timestamps mismatch: before=%+v after=%+v", before, updated)
	}
	if !maps.Equal(updated.Options, options) {
		t.Fatalf("metadata options = %+v, want %+v", updated.Options, options)
	}
	options["owner"] = "mutated"
	stat, err := store.StatObject(context.Background(), "tenant-a", "meta")
	if err != nil {
		t.Fatalf("stat after metadata update: %v", err)
	}
	if stat.Options["owner"] != "alice" {
		t.Fatalf("metadata should be copied on update, got %+v", stat.Options)
	}
	if got := readBytes(t, store, "tenant-a", "meta"); !bytes.Equal(got, data) {
		t.Fatalf("metadata update rewrote content")
	}
	store.mu.Lock()
	if store.meta.NextFileSeq != nextFileSeq {
		t.Fatalf("metadata update should not allocate a new file id")
	}
	if store.meta.Files[fileKey("tenant-a", "meta")].ManifestID != result.ManifestID {
		t.Fatalf("metadata update should not change manifest")
	}
	store.mu.Unlock()

	cleared, err := store.UpdateMetadata(context.Background(), "tenant-a", "meta", nil)
	if err != nil {
		t.Fatalf("clear metadata: %v", err)
	}
	if len(cleared.Options) != 0 {
		t.Fatalf("nil metadata update should clear options, got %+v", cleared.Options)
	}
}

func TestUpdateMetadataRejectsInactiveAndInvalidFiles(t *testing.T) {
	store := openTestStore(t)
	putBytes(t, store, "tenant-a", "meta", []byte("x"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.UpdateMetadata(ctx, "tenant-a", "meta", map[string]string{"k": "v"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled metadata update should fail with context error, got %v", err)
	}
	if _, err := store.UpdateMetadata(context.Background(), "tenant-a", "../bad", map[string]string{"k": "v"}); err == nil {
		t.Fatalf("metadata update should reject unsafe path")
	}
	if _, err := store.UpdateMetadata(context.Background(), "tenant-a", "missing", map[string]string{"k": "v"}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("metadata update should reject missing file, got %v", err)
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", "meta"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	if _, err := store.UpdateMetadata(context.Background(), "tenant-a", "meta", map[string]string{"k": "v"}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("metadata update should reject deleted file, got %v", err)
	}
}

func TestStoreRejectsUnsafePaths(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.Put(context.Background(), "tenant-a", "", bytes.NewReader([]byte("x")), nil); err == nil {
		t.Fatalf("put should reject empty path")
	}
	if _, err := store.StatObject(context.Background(), "tenant-a", "."); err == nil {
		t.Fatalf("stat should reject empty normalized path")
	}
	if _, err := store.Put(context.Background(), "tenant-a", "../outside", bytes.NewReader([]byte("x")), nil); err == nil {
		t.Fatalf("put should reject traversal")
	}
	if _, err := store.OpenObject(context.Background(), "tenant-a", "/outside"); err == nil {
		t.Fatalf("open should reject absolute path")
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", "../outside"); err == nil {
		t.Fatalf("delete should reject traversal")
	}
	if _, err := store.Put(context.Background(), "tenant-a", "nil", nil, nil); err == nil {
		t.Fatalf("put should reject nil input")
	}
	if _, err := store.Put(context.Background(), "bad/tenant", "file", bytes.NewReader([]byte("x")), nil); err == nil {
		t.Fatalf("put should reject unsafe tenant id")
	}
	if _, err := store.Put(context.Background(), "tenant-a", "missing/file", bytes.NewReader([]byte("x")), nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("put below missing explicit parent should fail with not-exist, got %v", err)
	}
	if err := store.MkdirAll("tenant-a/missing", 0o755); err != nil {
		t.Fatalf("mkdir explicit parent: %v", err)
	}
	if _, err := store.Put(context.Background(), "tenant-a", "missing/file", bytes.NewReader([]byte("x")), nil); err != nil {
		t.Fatalf("put below explicit parent should succeed: %v", err)
	}
}

func TestStoreEnforcesConfiguredSizeAndPathLimits(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFileSize = 4
	cfg.MaxTenantLength = 4
	cfg.MaxPathLength = 16
	cfg.MaxComponentLength = 4
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	if _, err := store.Put(context.Background(), "tenant", "file", bytes.NewReader([]byte("x")), nil); err == nil {
		t.Fatalf("put should reject tenant over configured length")
	}
	if _, err := store.Put(context.Background(), "tent", "a/very-long-path", bytes.NewReader([]byte("x")), nil); err == nil {
		t.Fatalf("put should reject path over configured length")
	}
	if _, err := store.Put(context.Background(), "tent", "longer/x", bytes.NewReader([]byte("x")), nil); err == nil {
		t.Fatalf("put should reject component over configured length")
	}
	if _, err := store.Put(context.Background(), "tent", "file", bytes.NewReader([]byte("12345")), nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("put should reject oversized input, got %v", err)
	}
	if _, err := store.Put(context.Background(), "tent", "file", bytes.NewReader([]byte("1234")), nil); err != nil {
		t.Fatalf("put at size limit should succeed: %v", err)
	}
}

func TestObjectAPIReturnsPathErrors(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.StatObject(context.Background(), "tenant-a", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat missing should return not-exist path error, got %v", err)
	} else if pathErr := new(os.PathError); !errors.As(err, &pathErr) || pathErr.Op != "stat" || pathErr.Path != "missing" {
		t.Fatalf("stat missing should wrap os.PathError, got %#v", err)
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete missing should return not-exist path error, got %v", err)
	} else if pathErr := new(os.PathError); !errors.As(err, &pathErr) || pathErr.Op != "delete" || pathErr.Path != "missing" {
		t.Fatalf("delete missing should wrap os.PathError, got %#v", err)
	}
	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if _, err := store.OpenObject(context.Background(), "tenant-a", "docs"); !errors.Is(err, ErrIsDir) {
		t.Fatalf("open directory as object should return ErrIsDir, got %v", err)
	} else if pathErr := new(os.PathError); !errors.As(err, &pathErr) || pathErr.Op != "open" || pathErr.Path != "docs" {
		t.Fatalf("open directory should wrap os.PathError, got %#v", err)
	}
}

func TestPutPropagatesContextAndReaderErrors(t *testing.T) {
	store := openTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Put(ctx, "tenant-a", "canceled", bytes.NewReader([]byte("x")), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("put should return canceled context, got %v", err)
	}

	readErr := errors.New("read failed")
	if _, err := store.Put(context.Background(), "tenant-a", "read-error", failingReader{err: readErr}, nil); !errors.Is(err, readErr) {
		t.Fatalf("put should return reader error, got %v", err)
	}
}
