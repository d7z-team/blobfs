package blobfs

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlobPullPush(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	bl, err := newBlob(dir1, dir2)
	assert.NoError(t, err)
	token, err := bl.create(bytes.NewBufferString("hello world"))
	assert.NoError(t, err)
	token2, err := bl.create(bytes.NewBufferString("hello world"))
	assert.NoError(t, err)
	assert.Equal(t, token, token2)
	open, err := bl.open(token)
	assert.NoError(t, err)
	all, err := io.ReadAll(open)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello world"), all)
	err = open.Close()
	assert.NoError(t, err)
	err = bl.delete(token)
	assert.NoError(t, err)
}

func TestFSBlobGc(t *testing.T) {
	bl, err := newBlob(t.TempDir(), t.TempDir())
	assert.NoError(t, err)
	token1, _ := bl.create(bytes.NewBufferString("hello world 01"))
	token2, _ := bl.create(bytes.NewBufferString("hello world 02"))
	assert.NotEqual(t, token1, token2)
	assert.NoError(t, bl.Link(token1))
	assert.NoError(t, bl.blobGC())
	_, err = bl.open(token2)
	assert.Error(t, err)
	data, err := bl.open(token1)
	assert.NoError(t, err)
	assert.NoError(t, data.Close())
	assert.NoError(t, bl.Unlink(token1))
	assert.NoError(t, bl.blobGC())
}
