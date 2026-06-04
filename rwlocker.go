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
		locker: sync.RWMutex{},
	})
	locker := actual.(*rwLocker)
	return locker
}

type rwLocker struct {
	locker sync.RWMutex
}

func (rw *rwLocker) Lock(read bool) *lockerContent {
	if read {
		rw.locker.RLock()
		return &lockerContent{
			close: func() {
				rw.locker.RUnlock()
			},
		}
	} else {
		rw.locker.Lock()
		return &lockerContent{
			close: func() {
				rw.locker.Unlock()
			},
		}
	}
}

type lockerContent struct {
	once  sync.Once
	close func()
}

func (c *lockerContent) Close() {
	c.once.Do(c.close)
}
