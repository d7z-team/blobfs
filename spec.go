package blobfs

import (
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type PullContent struct {
	io.ReadSeekCloser
	CreateAt time.Time
	ETag     string
	Options  map[string]string
}

// Objects 对象管理
type Objects interface {
	// Push 存入对象
	Push(path string, input io.Reader, options map[string]string) error
	// Pull 取出对象
	Pull(path string) (*PullContent, error)
	// PullOrNil 取出对象
	PullOrNil(path string) *PullContent
	// Cleanup 刷新对象过期时间
	Cleanup(path string) error
	// Remove 按照策略移除对象
	Remove(base string, regex *regexp.Regexp, ttl time.Duration) error
	// Child 创建新的命名空间
	Child(name string) Objects
	// Transparent 透传内容
	Transparent(path string, input io.ReadCloser, options map[string]string) io.ReadCloser
}

type ChildObjects struct {
	root  Objects
	group string
}

func newChildObjects(objects Objects, group string) *ChildObjects {
	group = strings.Trim(filepath.Clean(group), "/")
	return &ChildObjects{
		root:  objects,
		group: group,
	}
}

func (c *ChildObjects) Push(path string, input io.Reader, options map[string]string) error {
	return c.root.Push(c.group+"/"+path, input, options)
}

func (c *ChildObjects) PullOrNil(path string) *PullContent {
	return c.root.PullOrNil(c.group + "/" + path)
}

func (c *ChildObjects) Pull(path string) (*PullContent, error) {
	return c.root.Pull(c.group + "/" + path)
}

func (c *ChildObjects) Cleanup(path string) error {
	return c.root.Cleanup(c.group + "/" + path)
}

func (c *ChildObjects) Remove(base string, regex *regexp.Regexp, ttl time.Duration) error {
	return c.root.Remove(c.group+"/"+base, regex, ttl)
}

func (c *ChildObjects) Transparent(path string, input io.ReadCloser, options map[string]string) io.ReadCloser {
	return c.root.Transparent(c.group+"/"+path, input, options)
}

func (c *ChildObjects) Child(name string) Objects {
	return newChildObjects(c.root, strings.Trim(c.group+"/"+name, "/"))
}
