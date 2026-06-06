package blobfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestChaosDeterministicOperationsMaintainInvariants(t *testing.T) {
	cfg := testConfig()
	cfg.SegmentSize = 512
	cfg.GC.CompactGarbageRatio = 0.2
	dir := t.TempDir()
	store, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if store != nil {
			_ = store.Close()
		}
	})
	rng := rand.New(rand.NewSource(0xB10BF5))
	model := map[string][]byte{}
	for _, name := range []string{"chaos/a", "chaos/b", "chaos/c"} {
		if err := store.MkdirAll("tenant-a/"+name, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
	}
	for step := 0; step < 80; step++ {
		switch rng.Intn(8) {
		case 0, 1:
			p := chaosPath(rng)
			if err := store.MkdirAll("tenant-a/"+path.Dir(p), 0o755); err != nil {
				t.Fatalf("step %d mkdir parent: %v", step, err)
			}
			data := chaosBytes(rng)
			if _, err := store.Put(testContext(t), "tenant-a", p, bytes.NewReader(data), map[string]string{"step": strconv.Itoa(step)}); err != nil {
				t.Fatalf("step %d put %s: %v", step, p, err)
			}
			model[p] = data
		case 2:
			p := chaosPath(rng)
			err := store.DeleteObject(testContext(t), "tenant-a", p)
			if _, ok := model[p]; ok {
				if err != nil {
					t.Fatalf("step %d delete existing %s: %v", step, p, err)
				}
				delete(model, p)
			} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("step %d delete missing %s: %v", step, p, err)
			}
		case 3:
			oldPath := chaosPath(rng)
			newPath := chaosPath(rng)
			if err := store.MkdirAll("tenant-a/"+path.Dir(newPath), 0o755); err != nil {
				t.Fatalf("step %d mkdir rename parent: %v", step, err)
			}
			err := store.Rename("tenant-a/"+oldPath, "tenant-a/"+newPath)
			data, ok := model[oldPath]
			if ok {
				if err != nil {
					t.Fatalf("step %d rename %s -> %s: %v", step, oldPath, newPath, err)
				}
				if oldPath != newPath {
					model[newPath] = data
					delete(model, oldPath)
				}
			} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("step %d rename missing %s -> %s: %v", step, oldPath, newPath, err)
			}
		case 4:
			prefix := "chaos/" + string(rune('a'+rng.Intn(3)))
			err := store.RemoveAll("tenant-a/" + prefix)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("step %d removeall %s: %v", step, prefix, err)
			}
			for p := range model {
				if strings.HasPrefix(p, prefix+"/") {
					delete(model, p)
				}
			}
			if err := store.MkdirAll("tenant-a/"+prefix, 0o755); err != nil {
				t.Fatalf("step %d recreate %s: %v", step, prefix, err)
			}
		case 5:
			if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: rng.Intn(2) == 0}); err != nil {
				t.Fatalf("step %d gc: %v", step, err)
			}
		case 6:
			store = reopenChaosStore(t, store, dir, cfg)
		case 7:
			if p, ok := chooseChaosKey(rng, model); ok {
				check, err := store.CheckObject(testContext(t), "tenant-a", p)
				if err != nil {
					t.Fatalf("step %d check %s: %v", step, p, err)
				}
				if !check.Healthy {
					t.Fatalf("step %d unhealthy check %s: %+v", step, p, check)
				}
			}
		}
		if step%25 == 0 {
			verifyChaosModel(t, store, model)
		}
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("final gc: %v", err)
	}
	store = reopenChaosStore(t, store, dir, cfg)
	verifyChaosModel(t, store, model)
	scrub, err := store.Scrub(testContext(t), ScrubOptions{CheckFiles: true})
	if err != nil {
		t.Fatalf("final scrub: %v", err)
	}
	if !scrub.Healthy || scrub.CheckedFiles != len(model) {
		t.Fatalf("bad final scrub: %+v, model files=%d", scrub, len(model))
	}
}

func TestChaosMixedObjectAndVFSOperations(t *testing.T) {
	cfg := testConfig()
	cfg.SegmentSize = 768
	cfg.GC.CompactGarbageRatio = 0.2
	dir := t.TempDir()
	store, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if store != nil {
			_ = store.Close()
		}
	})
	for _, name := range []string{"mixed/a", "mixed/b", "mixed/c"} {
		if err := store.MkdirAll("tenant-a/"+name, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
	}
	rng := rand.New(rand.NewSource(0xC0FFEE))
	model := map[string][]byte{}
	for step := 0; step < 100; step++ {
		switch rng.Intn(10) {
		case 0, 1:
			p := mixedChaosPath(rng)
			if err := store.MkdirAll("tenant-a/"+path.Dir(p), 0o755); err != nil {
				t.Fatalf("step %d mkdir parent: %v", step, err)
			}
			data := chaosBytes(rng)
			if _, err := store.Put(testContext(t), "tenant-a", p, bytes.NewReader(data), map[string]string{"api": "object"}); err != nil {
				t.Fatalf("step %d put %s: %v", step, p, err)
			}
			model[p] = data
		case 2:
			p := mixedChaosPath(rng)
			if err := store.MkdirAll("tenant-a/"+path.Dir(p), 0o755); err != nil {
				t.Fatalf("step %d mkdir vfs parent: %v", step, err)
			}
			data := chaosBytes(rng)
			file, err := store.OpenFile("tenant-a/"+p, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
			if err != nil {
				t.Fatalf("step %d open vfs %s: %v", step, p, err)
			}
			if _, err := file.Write(data); err != nil {
				_ = file.Close()
				t.Fatalf("step %d write vfs %s: %v", step, p, err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("step %d close vfs %s: %v", step, p, err)
			}
			model[p] = data
		case 3:
			if p, ok := chooseChaosKey(rng, model); ok {
				info, err := store.UpdateMetadata(testContext(t), "tenant-a", p, map[string]string{"step": strconv.Itoa(step)})
				if err != nil {
					t.Fatalf("step %d update metadata %s: %v", step, p, err)
				}
				if info.Options["step"] != strconv.Itoa(step) {
					t.Fatalf("step %d metadata not updated: %+v", step, info)
				}
			}
		case 4:
			if p, ok := chooseChaosKey(rng, model); ok {
				want := model[p]
				offset := rng.Intn(len(want))
				length := rng.Intn(len(want) - offset + 8)
				reader, err := store.OpenRange(testContext(t), "tenant-a", p, int64(offset), int64(length))
				if err != nil {
					t.Fatalf("step %d open range %s: %v", step, p, err)
				}
				got, err := io.ReadAll(reader)
				_ = reader.Close()
				if err != nil {
					t.Fatalf("step %d read range %s: %v", step, p, err)
				}
				end := offset + length
				if end > len(want) {
					end = len(want)
				}
				if !bytes.Equal(got, want[offset:end]) {
					t.Fatalf("step %d range mismatch %s", step, p)
				}
			}
		case 5:
			oldPath := mixedChaosPath(rng)
			newPath := mixedChaosPath(rng)
			if err := store.MkdirAll("tenant-a/"+path.Dir(newPath), 0o755); err != nil {
				t.Fatalf("step %d mkdir rename parent: %v", step, err)
			}
			err := store.Rename("tenant-a/"+oldPath, "tenant-a/"+newPath)
			data, ok := model[oldPath]
			if ok {
				if err != nil {
					t.Fatalf("step %d rename %s -> %s: %v", step, oldPath, newPath, err)
				}
				if oldPath != newPath {
					delete(model, oldPath)
					model[newPath] = data
				}
			} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("step %d rename missing %s -> %s: %v", step, oldPath, newPath, err)
			}
		case 6:
			p := mixedChaosPath(rng)
			err := store.DeleteObject(testContext(t), "tenant-a", p)
			if _, ok := model[p]; ok {
				if err != nil {
					t.Fatalf("step %d delete %s: %v", step, p, err)
				}
				delete(model, p)
			} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("step %d delete missing %s: %v", step, p, err)
			}
		case 7:
			prefix := "mixed/" + string(rune('a'+rng.Intn(3)))
			err := store.RemoveAll("tenant-a/" + prefix)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("step %d removeall %s: %v", step, prefix, err)
			}
			for p := range model {
				if strings.HasPrefix(p, prefix+"/") {
					delete(model, p)
				}
			}
			if err := store.MkdirAll("tenant-a/"+prefix, 0o755); err != nil {
				t.Fatalf("step %d recreate %s: %v", step, prefix, err)
			}
		case 8:
			if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: rng.Intn(2) == 0}); err != nil {
				t.Fatalf("step %d gc: %v", step, err)
			}
		case 9:
			store = reopenChaosStore(t, store, dir, cfg)
		}
		if step%20 == 0 {
			verifyChaosModel(t, store, model)
		}
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("final gc: %v", err)
	}
	store = reopenChaosStore(t, store, dir, cfg)
	verifyChaosModel(t, store, model)
	stats, err := store.Stats(testContext(t))
	if err != nil {
		t.Fatalf("final stats: %v", err)
	}
	if stats.Objects != len(model) {
		t.Fatalf("final object count = %d, model=%d", stats.Objects, len(model))
	}
	scrub, err := store.Scrub(testContext(t), ScrubOptions{CheckFiles: true})
	if err != nil {
		t.Fatalf("final scrub: %v", err)
	}
	if !scrub.Healthy || scrub.CheckedFiles != len(model) {
		t.Fatalf("bad final scrub: %+v, model files=%d", scrub, len(model))
	}
}

func TestChaosPutReuseInterleavedWithAggressiveGC(t *testing.T) {
	cfg := testConfig()
	cfg.SegmentSize = 512
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/reuse", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	rng := rand.New(rand.NewSource(0x51A7E))
	for step := 0; step < 40; step++ {
		data := chaosBytes(rng)
		oldPath := fmt.Sprintf("reuse/old-%02d", step)
		newPath := fmt.Sprintf("reuse/new-%02d", step)
		putTestBytes(t, store, "tenant-a", oldPath, data)
		if err := store.DeleteObject(testContext(t), "tenant-a", oldPath); err != nil {
			t.Fatalf("step %d delete old: %v", step, err)
		}
		prepared, err := store.prepareObject(testContext(t), "tenant-a", newPath, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("step %d prepare: %v", step, err)
		}
		if len(prepared.pinned) == 0 {
			store.releasePreparedPins(prepared)
			t.Fatalf("step %d expected reused chunk pin", step)
		}
		if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
			store.releasePreparedPins(prepared)
			t.Fatalf("step %d gc: %v", step, err)
		}
		if _, err := store.commitPreparedObject(testContext(t), prepared, putCommitOptions{}); err != nil {
			store.releasePreparedPins(prepared)
			t.Fatalf("step %d commit: %v", step, err)
		}
		store.releasePreparedPins(prepared)
		if got := readTestBytes(t, store, "tenant-a", newPath); !bytes.Equal(got, data) {
			t.Fatalf("step %d committed data mismatch", step)
		}
		if step%5 == 0 {
			if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
				t.Fatalf("step %d follow-up gc: %v", step, err)
			}
		}
	}
	store.pinMu.Lock()
	pins := len(store.pins)
	store.pinMu.Unlock()
	if pins != 0 {
		t.Fatalf("prepared chunk pins leaked: %d", pins)
	}
	scrub, err := store.Scrub(testContext(t), ScrubOptions{CheckFiles: true})
	if err != nil {
		t.Fatalf("scrub: %v", err)
	}
	if !scrub.Healthy {
		t.Fatalf("scrub found issues: %+v", scrub)
	}
}

func TestChaosScrubRunsThroughConcurrentDeletesAndGC(t *testing.T) {
	cfg := testConfig()
	cfg.SegmentSize = 512
	cfg.GC.CompactGarbageRatio = 0.2
	store, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.MkdirAll("tenant-a/scrub", 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	for i := 0; i < 32; i++ {
		data := bytes.Repeat([]byte{byte(i), byte(i * 7)}, 128)
		putTestBytes(t, store, "tenant-a", fmt.Sprintf("scrub/file-%02d", i), data)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := store.Scrub(ctx, ScrubOptions{CheckFiles: true})
		if err != nil {
			done <- err
			return
		}
		if !result.Healthy {
			done <- fmt.Errorf("scrub reported issues: %+v", result.Issues)
			return
		}
		done <- nil
	}()
	for i := 0; i < 16; i++ {
		if err := store.DeleteObject(testContext(t), "tenant-a", fmt.Sprintf("scrub/file-%02d", i)); err != nil {
			t.Fatalf("delete during scrub: %v", err)
		}
		if i%4 == 0 {
			if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
				t.Fatalf("gc during scrub: %v", err)
			}
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("concurrent scrub: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent scrub did not finish")
	}
	if _, err := store.RunGC(testContext(t), GCOptions{CandidateConfirmCycles: 1, Compact: true}); err != nil {
		t.Fatalf("final gc: %v", err)
	}
	scrub, err := store.Scrub(testContext(t), ScrubOptions{CheckFiles: true})
	if err != nil {
		t.Fatalf("final scrub: %v", err)
	}
	if !scrub.Healthy || scrub.CheckedFiles != 16 {
		t.Fatalf("bad final scrub: %+v", scrub)
	}
}

func chaosPath(rng *rand.Rand) string {
	return fmt.Sprintf("chaos/%c/file-%02d", 'a'+rng.Intn(3), rng.Intn(16))
}

func chaosBytes(rng *rand.Rand) []byte {
	data := make([]byte, 1+rng.Intn(192))
	for i := range data {
		data[i] = byte(rng.Intn(256))
	}
	return data
}

func mixedChaosPath(rng *rand.Rand) string {
	return fmt.Sprintf("mixed/%c/file-%02d", 'a'+rng.Intn(3), rng.Intn(20))
}

func chooseChaosKey(rng *rand.Rand, model map[string][]byte) (string, bool) {
	if len(model) == 0 {
		return "", false
	}
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys[rng.Intn(len(keys))], true
}

func verifyChaosModel(t *testing.T, store *Store, model map[string][]byte) {
	t.Helper()
	for p, want := range model {
		if got := readTestBytes(t, store, "tenant-a", p); !bytes.Equal(got, want) {
			t.Fatalf("model mismatch for %s", p)
		}
	}
}

func reopenChaosStore(t *testing.T, store *Store, dir string, cfg Config) *Store {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	reopened, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return reopened
}
