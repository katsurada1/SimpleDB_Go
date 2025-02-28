package concurrency

import (
	"sync"
	"simpledb_go/simpledb/file"
)

type ConcurrencyMgr struct {
	lockTable *LockTable         
	locks     map[file.BlockId]string 
	mu        sync.Mutex         
}


func NewConcurrencyMgr(lockTable *LockTable) *ConcurrencyMgr {
	return &ConcurrencyMgr{
		lockTable: lockTable,
		locks:     make(map[file.BlockId]string),
	}
}

func NewDefaultConcurrencyMgr() *ConcurrencyMgr {
	lt := NewLockTable()
	return NewConcurrencyMgr(lt)
}

func (cm *ConcurrencyMgr) sLockInternal(blk file.BlockId) error {
	if _, exists := cm.locks[blk]; !exists {
		if err := cm.lockTable.SLock(blk); err != nil {
			return err
		}
		cm.locks[blk] = "S"
	}
	return nil
}

func (cm *ConcurrencyMgr) SLock(blk file.BlockId) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.sLockInternal(blk)
}

func (cm *ConcurrencyMgr) XLock(blk file.BlockId) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.hasXLock(blk) {
		return nil
	}

	if lockType, exists := cm.locks[blk]; !exists || lockType != "S" {
		if err := cm.sLockInternal(blk); err != nil {
			return err
		}
	}

	if err := cm.lockTable.UpgradeLock(blk); err != nil {
		return err
	}
	cm.locks[blk] = "X"
	return nil
}

func (cm *ConcurrencyMgr) Release() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for blk := range cm.locks {
		cm.lockTable.Unlock(blk)
	}
	cm.locks = make(map[file.BlockId]string)
}

func (cm *ConcurrencyMgr) hasXLock(blk file.BlockId) bool {
	lockType, exists := cm.locks[blk]
	return exists && lockType == "X"
}
