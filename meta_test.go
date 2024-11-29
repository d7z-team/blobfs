package blobfs

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestMetaPullPush(t *testing.T) {
	fs, err := BlobFS(t.TempDir())
	assert.NoError(t, err)
	err = fs.Push("", bytes.NewBufferString("hello world"), nil)
	if err != nil {
		assert.NoError(t, err)
	}
	pull, err := fs.Pull("")
	assert.NoError(t, err)
	all, err := io.ReadAll(pull)
	_ = pull.Close()
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(all))
}
