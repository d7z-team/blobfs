package blobfs

import "sync"

type rwLockGroup struct {
	group sync.Map
}

func newRWLockGroup() *rwLockGroup {
	return &rwLockGroup{
		sync.Map{},
	}
}

func (g *rwLockGroup) Open(key string) *rwLocker {
	actual, _ := g.group.LoadOrStore(key, &rwLocker{
		locker:       sync.RWMutex{},
		switchLocker: sync.Mutex{},
	})
	locker := actual.(*rwLocker)
	return locker
}

func (g *rwLockGroup) Del(key string) {
	g.group.Delete(key)
}

type rwLocker struct {
	locker       sync.RWMutex
	switchLocker sync.Mutex
}

func (rw *rwLocker) Lock(read bool) *lockerContent {
	rw.switchLocker.Lock()
	defer rw.switchLocker.Unlock()
	if read {
		rw.locker.RLock()
		return &lockerContent{
			rw,
			true,
			func() {
				rw.locker.RUnlock()
			},
		}
	} else {
		rw.locker.Lock()
		return &lockerContent{
			rw,
			false,
			func() {
				rw.locker.Unlock()
			},
		}
	}
}

type lockerContent struct {
	locker *rwLocker
	rLock  bool
	close  func()
}

func (c *lockerContent) AsRLocker() {
	if !c.rLock {
		c.locker.switchLocker.Lock()
		defer c.locker.switchLocker.Unlock()
		if !c.rLock {
			c.close()
			c.rLock = true
			c.locker.locker.RLock()
			c.close = func() {
				c.locker.locker.RUnlock()
			}
		}
	}
}

func (c *lockerContent) AsLocker() {
	if c.rLock {
		c.locker.switchLocker.Lock()
		defer c.locker.switchLocker.Unlock()
		if c.rLock {
			c.close()
			c.rLock = false
			c.locker.locker.Lock()
			c.close = func() {
				c.locker.locker.Unlock()
			}
		}
	}
}

func (c *lockerContent) Close() {
	c.close()
}
