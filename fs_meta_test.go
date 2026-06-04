package blobfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testReader(data string) io.ReadCloser {
	return io.NopCloser(bytes.NewBufferString(data))
}

func TestMetaPullPush(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	for _, path := range []string{"", "aaa/bbb", "aaa/bbb/.meta", "aaa/bbb/.blob"} {
		assert.NoError(t, fs.Push(path, testReader("hello world"), nil))
		pull, err := fs.Pull(path)
		assert.NoError(t, err)
		all, err := io.ReadAll(pull)
		_ = pull.Close()
		assert.NoError(t, err)
		assert.Equal(t, "hello world", string(all))
	}
}

func TestMetaClear(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("base/route", testReader("hello world"), nil))
	all, err := io.ReadAll(fs.PullOrNil("base/route"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
	assert.NoError(t, fs.Remove("base/", nil, -10*time.Millisecond))
	assert.Empty(t, fs.PullOrNil("base/route"))
}

func TestRepeatedPushSameContentDoesNotLeakReference(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("base/route", testReader("hello world"), nil))
	assert.NoError(t, fs.Push("base/route", testReader("hello world"), nil))
	pull, err := fs.Pull("base/route")
	assert.NoError(t, err)
	token := pull.ETag
	assert.NoError(t, pull.Close())

	assert.NoError(t, fs.Remove("base/route", nil, -time.Millisecond))
	assert.NoError(t, fs.BlobGC())
	_, err = fs.blob.open(token)
	assert.Error(t, err)
}

func TestTransparent(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	transparent, err := fs.Transparent("base/route", testReader("hello world"), nil)
	assert.NoError(t, err)
	data, err := io.ReadAll(transparent)
	assert.NoError(t, err)
	assert.NoError(t, transparent.Close())
	assert.Equal(t, "hello world", string(data))

	all, err := io.ReadAll(fs.PullOrNil("base/route"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
}

type failedReader struct {
	data io.Reader
}

func (f *failedReader) Close() error {
	if c, ok := f.data.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func newFailedReader(data io.Reader) *failedReader {
	return &failedReader{
		data: data,
	}
}

func (f *failedReader) Read(p []byte) (n int, err error) {
	c, _ := f.data.Read(p)
	return c, errors.New("some error")
}

func TestTransparentInternalError(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	cli1, err := fs.Transparent("base/route", newFailedReader(testReader("hello world")), nil)
	assert.NoError(t, err)
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		cli2, err := fs.Transparent("base/route", testReader("hello dragon"), nil)
		if err != nil {
			done <- err
			return
		}
		// cli2 处于阻塞状态
		cli2Data, err := io.ReadAll(cli2)
		if err != nil {
			done <- err
			return
		}
		if err = cli2.Close(); err != nil {
			done <- err
			return
		}
		if string(cli2Data) != "hello dragon" {
			done <- errors.New("unexpected cli2 data")
			return
		}
		done <- nil
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("second transparent completed before first released lock: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	_, err = io.ReadAll(cli1)
	assert.ErrorContains(t, err, "some error")
	assert.ErrorContains(t, cli1.Close(), "some error")
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second transparent did not complete")
	}
	cli3, err := fs.Pull("base/route")
	assert.NoError(t, err)
	Cli3Data, err := io.ReadAll(cli3)
	assert.NoError(t, err)
	assert.NoError(t, cli3.Close())
	assert.Equal(t, string(Cli3Data), "hello dragon")
}

func TestTransparentClientError(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	cli1, err := fs.Transparent("base/route", testReader("hello world"), nil)
	assert.NoError(t, err)
	assert.NoError(t, cli1.Close())
	assert.Nil(t, fs.PullOrNil("base/route"))
}

// 第一个任务失败退出应该释放锁
func TestTransparentClientErrorLocker(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	cli1, err := fs.Transparent("base/route", testReader("hello world"), nil)
	assert.NoError(t, err)
	acquired := make(chan error, 1)
	go func() {
		cli2, err := fs.Transparent("base/route", testReader("hello world"), nil)
		if err != nil {
			acquired <- err
			return
		}
		acquired <- cli2.Close()
	}()
	select {
	case err := <-acquired:
		t.Fatalf("second transparent acquired lock before first close: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	assert.NoError(t, cli1.Close())
	select {
	case err := <-acquired:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second transparent did not acquire lock")
	}
}

func TestNamespace(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	child, err := fs.Child("group")
	assert.NoError(t, err)
	assert.NoError(t, child.Push("base/route", testReader("hello world"), nil))
	all, err := io.ReadAll(child.PullOrNil("base/route"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
	all, err = io.ReadAll(fs.PullOrNil("group/base/route"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
}

func TestPathTraversalRejected(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)

	assert.Error(t, fs.Push("../outside", testReader("hello world"), nil))
	assert.Error(t, fs.Push("/outside", testReader("hello world"), nil))
	assert.Error(t, fs.Remove("../outside", nil, 0))
	_, err = fs.Pull("../outside")
	assert.Error(t, err)
	_, err = fs.Transparent("../outside", testReader("hello world"), nil)
	assert.Error(t, err)
	_, err = fs.Child("../outside")
	assert.Error(t, err)

	child, err := fs.Child("group")
	assert.NoError(t, err)
	assert.Error(t, child.Push("../outside", testReader("hello world"), nil))
	assert.Error(t, child.Push("/outside", testReader("hello world"), nil))
}

func TestRemoveRootObject(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("", testReader("root"), nil))
	assert.NoError(t, fs.Remove("", nil, -time.Millisecond))
	assert.Nil(t, fs.PullOrNil(""))
}

func TestBlobGCIgnoresMalformedBlobFile(t *testing.T) {
	base := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(base, "blob"), 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(base, "blob", "x"), []byte("x"), 0o600))

	fs, err := BlobFS(base)
	assert.NoError(t, err)
	assert.NoError(t, fs.BlobGC())
}

func TestPushMetadataSaveFailureKeepsOldContent(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("stable", testReader("old"), nil))

	metaDir := filepath.Join(fs.metaDir, "stable")
	assert.NoError(t, os.Chmod(metaDir, 0o500))
	err = fs.Push("stable", testReader("new"), nil)
	assert.NoError(t, os.Chmod(metaDir, 0o700))
	if err == nil {
		t.Skip("metadata directory remained writable")
	}

	pull, err := fs.Pull("stable")
	assert.NoError(t, err)
	data, err := io.ReadAll(pull)
	assert.NoError(t, err)
	assert.NoError(t, pull.Close())
	assert.Equal(t, "old", string(data))

	sum := sha256.Sum256([]byte("new"))
	assert.NoError(t, fs.BlobGC())
	_, err = fs.blob.open(hex.EncodeToString(sum[:]))
	assert.Error(t, err)
}

func TestTransparentCloseReturnsStorageError(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(fs.metaDir, "blocked"), []byte("x"), 0o600))

	transparent, err := fs.Transparent("blocked/child", testReader("hello world"), nil)
	assert.NoError(t, err)
	data, err := io.ReadAll(transparent)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
	assert.Error(t, transparent.Close())
	assert.Nil(t, fs.PullOrNil("blocked/child"))
}

func TestBlobGCWaitsForOpenReaders(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("route", testReader("old"), nil))
	pull, err := fs.Pull("route")
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("route", testReader("new"), nil))

	done := make(chan error, 1)
	go func() {
		done <- fs.BlobGC()
	}()
	select {
	case err := <-done:
		t.Fatalf("GC completed before open reader closed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	data, err := io.ReadAll(pull)
	assert.NoError(t, err)
	assert.Equal(t, "old", string(data))
	assert.NoError(t, pull.Close())
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("GC did not complete after reader closed")
	}
}
