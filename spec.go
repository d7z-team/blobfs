package blobfs

import (
	"io"
	"regexp"
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
	Child(name string) (Objects, error)
	// Transparent 透传内容
	Transparent(path string, input io.ReadCloser, options map[string]string) (io.ReadCloser, error)
}

type ChildObjects struct {
	root  Objects
	group string
}

func newChildObjects(objects Objects, group string) (*ChildObjects, error) {
	group, err := normalizePath(group)
	if err != nil {
		return nil, err
	}
	return &ChildObjects{
		root:  objects,
		group: group,
	}, nil
}

func (c *ChildObjects) path(name string) (string, error) {
	name, err := normalizePath(name)
	if err != nil {
		return "", err
	}
	if c.group == "" {
		return name, nil
	}
	if name == "" {
		return c.group, nil
	}
	return c.group + "/" + name, nil
}

func (c *ChildObjects) Push(path string, input io.Reader, options map[string]string) error {
	path, err := c.path(path)
	if err != nil {
		return err
	}
	return c.root.Push(path, input, options)
}

func (c *ChildObjects) PullOrNil(path string) *PullContent {
	path, err := c.path(path)
	if err != nil {
		return nil
	}
	return c.root.PullOrNil(path)
}

func (c *ChildObjects) Pull(path string) (*PullContent, error) {
	path, err := c.path(path)
	if err != nil {
		return nil, err
	}
	return c.root.Pull(path)
}

func (c *ChildObjects) Cleanup(path string) error {
	path, err := c.path(path)
	if err != nil {
		return err
	}
	return c.root.Cleanup(path)
}

func (c *ChildObjects) Remove(base string, regex *regexp.Regexp, ttl time.Duration) error {
	base, err := c.path(base)
	if err != nil {
		return err
	}
	return c.root.Remove(base, regex, ttl)
}

func (c *ChildObjects) Transparent(path string, input io.ReadCloser, options map[string]string) (io.ReadCloser, error) {
	path, err := c.path(path)
	if err != nil {
		return nil, err
	}
	return c.root.Transparent(path, input, options)
}

func (c *ChildObjects) Child(name string) (Objects, error) {
	name, err := c.path(name)
	if err != nil {
		return nil, err
	}
	return newChildObjects(c.root, name)
}
