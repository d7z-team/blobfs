package blobfs

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetaPullPush(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("", bytes.NewBufferString("hello world"), nil))
	pull, err := fs.Pull("")
	assert.NoError(t, err)
	all, err := io.ReadAll(pull)
	_ = pull.Close()
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
}

func TestMetaClear(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	assert.NoError(t, fs.Push("base/route", bytes.NewBufferString("hello world"), nil))
	all, err := io.ReadAll(fs.PullOrNil("base/route"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
	assert.NoError(t, fs.Remove("base/", nil, -10*time.Millisecond))
	assert.Empty(t, fs.PullOrNil("base/route"))
}
