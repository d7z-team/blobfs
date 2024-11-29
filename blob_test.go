package blobfs

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestBlobPullPush(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	bl := newBlob(dir1, dir2)

	token, err := bl.create(bytes.NewBufferString("hello world"))
	assert.NoError(t, err)
	open, err := bl.open(token)
	assert.NoError(t, err)
	all, err := io.ReadAll(open)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello world"), all)
	err = open.Close()
	assert.NoError(t, err)
	err = bl.delete(token)
	assert.NoError(t, err)
	err = open.Close()
	assert.Error(t, err)
}
