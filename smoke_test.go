package blobfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/afero"
)

func TestSmokeStoreVFSObjectAndGCWorkflow(t *testing.T) {
	ctx := context.Background()
	backend := afero.NewMemMapFs()
	store, err := OpenFS(backend, "/blobfs", testConfig())
	if err != nil {
		t.Fatalf("open smoke store: %v", err)
	}
	defer store.Close()

	var vfs afero.Fs = store
	if vfs.Name() != "blobfs" {
		t.Fatalf("vfs name = %q", vfs.Name())
	}
	if err := store.Mkdir("tenant-smoke/docs", 0o755); err != nil {
		t.Fatalf("mkdir smoke docs: %v", err)
	}

	file, err := store.Create("tenant-smoke/docs/report.txt")
	if err != nil {
		t.Fatalf("create report through vfs: %v", err)
	}
	if !strings.HasSuffix(file.Name(), "report.txt") {
		t.Fatalf("created file name = %q", file.Name())
	}
	if _, err = file.WriteString("hello blobfs"); err != nil {
		t.Fatalf("write report: %v", err)
	}
	if _, err = file.Seek(6, io.SeekStart); err != nil {
		t.Fatalf("seek report: %v", err)
	}
	tail := make([]byte, 6)
	if n, err := file.Read(tail); n != len(tail) || err != nil || string(tail) != "blobfs" {
		t.Fatalf("read after seek = n:%d data:%q err:%v", n, tail, err)
	}
	if _, err = file.ReadAt(tail, 6); err != nil || string(tail) != "blobfs" {
		t.Fatalf("readat report = %q, %v", tail, err)
	}
	if err = file.Truncate(5); err != nil {
		t.Fatalf("truncate report: %v", err)
	}
	if err = file.Sync(); err != nil {
		t.Fatalf("sync report: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close report: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("second close should be harmless: %v", err)
	}

	if got, err := afero.ReadFile(vfs, "tenant-smoke/docs/report.txt"); err != nil || string(got) != "hello" {
		t.Fatalf("read report through vfs = %q, %v", got, err)
	}
	result, err := store.Put(ctx, "tenant-smoke", "docs/blob.bin", bytes.NewReader([]byte("0123456789")), map[string]string{"kind": "initial"})
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	info, err := store.UpdateMetadata(ctx, "tenant-smoke", "docs/blob.bin", map[string]string{"kind": "smoke"})
	if err != nil {
		t.Fatalf("update object metadata: %v", err)
	}
	if info.ManifestID != result.ManifestID || info.Options["kind"] != "smoke" {
		t.Fatalf("metadata update changed object identity: %+v result=%+v", info, result)
	}

	rangeReader, err := store.OpenRange(ctx, "tenant-smoke", "docs/blob.bin", 2, 5)
	if err != nil {
		t.Fatalf("open object range: %v", err)
	}
	rangeData, err := io.ReadAll(rangeReader)
	if closeErr := rangeReader.Close(); closeErr != nil {
		t.Fatalf("close range reader: %v", closeErr)
	}
	if err != nil || string(rangeData) != "23456" {
		t.Fatalf("range data = %q, %v", rangeData, err)
	}

	dir, err := store.Open("tenant-smoke/docs")
	if err != nil {
		t.Fatalf("open docs dir: %v", err)
	}
	names, err := dir.Readdirnames(-1)
	if closeErr := dir.Close(); closeErr != nil {
		t.Fatalf("close docs dir: %v", closeErr)
	}
	sort.Strings(names)
	if err != nil || strings.Join(names, ",") != "blob.bin,report.txt" {
		t.Fatalf("docs dir names = %v, %v", names, err)
	}

	if err = store.DeleteObject(ctx, "tenant-smoke", "docs/blob.bin"); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if _, err = store.OpenObject(ctx, "tenant-smoke", "docs/blob.bin"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted object should be hidden immediately, got %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err = store.RunGC(ctx, GCOptions{CandidateConfirmCycles: 1, SafetyWindow: -1, Compact: true}); err != nil {
			t.Fatalf("gc cycle %d: %v", i+1, err)
		}
	}
	if _, err = store.Scrub(ctx, ScrubOptions{CheckFiles: true}); err != nil {
		t.Fatalf("scrub smoke store: %v", err)
	}
	if _, err = backend.Stat("/blobfs/meta/blobfs.json"); err != nil {
		t.Fatalf("smoke metadata should be persisted on backend fs: %v", err)
	}
}
