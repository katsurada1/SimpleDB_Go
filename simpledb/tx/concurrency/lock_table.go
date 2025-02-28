package concurrency

import (
	"errors"
	"sync"
	"time"
	"simpledb_go/simpledb/file"
)

const MAX_TIME = 10 * time.Second
var ErrLockAbort = errors.New("lock request aborted due to timeout")

type LockTable struct {
	mu     sync.Mutex
	locks  map[file.BlockId]int
	notify chan struct{}
}

func NewLockTable() *LockTable {
	lt := &LockTable{
		locks:  make(map[file.BlockId]int),
		notify: make(chan struct{}),
	}
	return lt
}

func (lt *LockTable) SLock(blk file.BlockId) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	startTime := time.Now()
	for lt.hasXLock(blk) {
		waitTime := MAX_TIME - time.Since(startTime)
		if waitTime <= 0 {
			return ErrLockAbort
		}

		lt.mu.Unlock()
		select {
		case <-lt.notify:
		case <-time.After(waitTime):
			lt.mu.Lock()
			return ErrLockAbort
		}
		lt.mu.Lock()
	}
	lt.locks[blk] = lt.getLockVal(blk) + 1
	return nil
}

func (lt *LockTable) XLock(blk file.BlockId) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	startTime := time.Now()
	for lt.hasOtherSLocks(blk) {
		waitTime := MAX_TIME - time.Since(startTime)
		if waitTime <= 0 {
			return ErrLockAbort
		}
		lt.mu.Unlock()
		select {
		case <-lt.notify:
		case <-time.After(waitTime):
			lt.mu.Lock()
			return ErrLockAbort
		}
		lt.mu.Lock()
	}
	lt.locks[blk] = -1
	return nil
}

func (lt *LockTable) UpgradeLock(blk file.BlockId) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	startTime := time.Now()
	for lt.getLockVal(blk) != 1 {
		waitTime := MAX_TIME - time.Since(startTime)
		if waitTime <= 0 {
			return ErrLockAbort
		}
		lt.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		lt.mu.Lock()
	}
	lt.locks[blk] = -1
	return nil
}


func (lt *LockTable) Unlock(blk file.BlockId) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.getLockVal(blk)
	if val > 1 {
		lt.locks[blk] = val - 1
	} else {
		delete(lt.locks, blk)
		close(lt.notify)
		lt.notify = make(chan struct{})
	}
}

func (lt *LockTable) hasXLock(blk file.BlockId) bool {
	val := lt.getLockVal(blk)
	return val < 0
}

func (lt *LockTable) hasOtherSLocks(blk file.BlockId) bool {
	val := lt.getLockVal(blk)
	return val > 0
}

func (lt *LockTable) getLockVal(blk file.BlockId) int {
	if val, exists := lt.locks[blk]; exists {
		return val
	}
	return 0
}
