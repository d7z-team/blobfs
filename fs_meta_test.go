package blobfs

import (
	"bytes"
	"errors"
	"io"
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

func TestTransparent(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	transparent := fs.Transparent("base/route", testReader("hello world"), nil)
	data, err := io.ReadAll(transparent)
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
	cli1 := fs.Transparent("base/route", newFailedReader(testReader("hello world")), nil)
	count := 0
	go func() {
		cli2 := fs.Transparent("base/route", testReader("hello dragon"), nil)
		// cli2 处于阻塞状态
		cli2Data, err := io.ReadAll(cli2)
		count = 1
		assert.NoError(t, err)
		assert.NoError(t, cli2.Close())
		assert.Equal(t, string(cli2Data), "hello dragon")
	}()
	assert.Equal(t, count, 0)
	_, err = io.ReadAll(cli1)
	assert.ErrorContains(t, err, "some error")
	assert.NoError(t, cli1.Close())
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, count) // 由下一个进行覆盖内容
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
	cli1 := fs.Transparent("base/route", testReader("hello world"), nil)
	assert.NoError(t, cli1.Close())
	assert.Nil(t, fs.PullOrNil("base/route"))
}

// 第一个任务失败退出应该释放锁
func TestTransparentClientErrorLocker(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	cli1 := fs.Transparent("base/route", testReader("hello world"), nil)
	step := 0
	go func() {
		cli2 := fs.Transparent("base/route", testReader("hello world"), nil)
		step = 1
		assert.NoError(t, cli2.Close())
	}()
	assert.Equal(t, 0, step)
	assert.NoError(t, cli1.Close())
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, step)
}

func TestNamespace(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	child := fs.Child("group")
	assert.NoError(t, child.Push("base/route", testReader("hello world"), nil))
	all, err := io.ReadAll(child.PullOrNil("base/route"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
	all, err = io.ReadAll(fs.PullOrNil("group/base/route"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
}
