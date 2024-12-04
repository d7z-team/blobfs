package blobfs

import (
	"errors"
	"sync"
)

type linker struct {
	store sync.Map

	locker sync.RWMutex
}

func newLinker() *linker {
	return &linker{
		store:  sync.Map{},
		locker: sync.RWMutex{},
	}
}

func (p *linker) Init(blob string) {
	p.locker.RLock()
	defer p.locker.RUnlock()
	_, _ = p.store.LoadOrStore(blob, uint(0))
}

func (p *linker) Link(blob string) error {
	p.locker.RLock()
	defer p.locker.RUnlock()
	for {
		actual, find := p.store.Load(blob)
		if !find {
			return errors.New("blob not found")
		}
		if p.store.CompareAndSwap(blob, actual, actual.(uint)+1) {
			break
		}
	}
	return nil
}

func (p *linker) Unlink(blob string) error {
	p.locker.RLock()
	defer p.locker.RUnlock()
	for {
		actual, find := p.store.Load(blob)
		if !find {
			return errors.New("blob not found")
		}
		if actual == 0 {
			return errors.New("blob is empty")
		}
		if p.store.CompareAndSwap(blob, actual, actual.(uint)-1) {
			break
		}
	}
	return nil
}

func (p *linker) Delete(token string) {
	p.locker.RLock()
	defer p.locker.RUnlock()
	p.store.Delete(token)
}

func (p *linker) Gc(item func(key string) error) error {
	p.locker.Lock()
	defer p.locker.Unlock()
	data := make([]string, 0)
	p.store.Range(func(k, v interface{}) bool {
		if v.(uint) == 0 {
			data = append(data, k.(string))
		}
		return true
	})
	for _, datum := range data {
		if err := item(datum); err != nil {
			return err
		}
		p.store.Delete(datum)
	}
	return nil
}

func (p *linker) Exists(token string) bool {
	p.locker.RLock()
	defer p.locker.RUnlock()
	_, found := p.store.Load(token)
	return found
}
