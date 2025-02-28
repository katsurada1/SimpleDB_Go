package concurrency

import (
	"testing"
	"time"
	"simpledb_go/simpledb/file"
)

func TestConcurrencyMgr_SLock(t *testing.T) {
	lockTable := NewLockTable()
	cm := NewConcurrencyMgr(lockTable)
	blk := *file.NewBlockId("testfile", 1) 
	if err := cm.SLock(blk); err != nil {
		t.Fatalf("SLock failed: %v", err)
	}
	if err := cm.SLock(blk); err != nil {
		t.Fatalf("Duplicate SLock failed: %v", err)
	}
	cm.Release()
}

func TestConcurrencyMgr_XLock(t *testing.T) {
	lockTable := NewLockTable()
	cm := NewConcurrencyMgr(lockTable)
	blk := *file.NewBlockId("testfile", 2) 
	if err := cm.SLock(blk); err != nil {
		t.Fatalf("SLock failed: %v", err)
	}
	cm.Release()
	if err := cm.XLock(blk); err != nil {
		t.Fatalf("XLock failed: %v", err)
	}
	cm.Release()
}

func TestConcurrencyMgr_UpgradeSLockToXLock(t *testing.T) {
	lockTable := NewLockTable()
	cm := NewConcurrencyMgr(lockTable)
	blk := *file.NewBlockId("testfile", 3)
	if err := cm.SLock(blk); err != nil {
		t.Fatalf("SLock failed: %v", err)
	}
	if err := cm.XLock(blk); err != nil {
		t.Fatalf("Upgrade to XLock failed: %v", err)
	}
	cm.Release()
}

func TestConcurrencyMgr_ConcurrentLocks(t *testing.T) {
	lockTable := NewLockTable()
	cm1 := NewConcurrencyMgr(lockTable)
	cm2 := NewConcurrencyMgr(lockTable)
	blk := *file.NewBlockId("testfile", 4)
	if err := cm1.SLock(blk); err != nil {
		t.Fatalf("Transaction 1 SLock failed: %v", err)
	}
	if err := cm2.SLock(blk); err != nil {
		t.Fatalf("Transaction 2 SLock failed: %v", err)
	}
	cm1.Release()
	cm2.Release()
}

func TestConcurrencyMgr_Deadlock(t *testing.T) {
	lockTable := NewLockTable()
	cm1 := NewConcurrencyMgr(lockTable)
	cm2 := NewConcurrencyMgr(lockTable)
	blk := *file.NewBlockId("testfile", 5)
	if err := cm1.SLock(blk); err != nil {
		t.Fatalf("Transaction 1 SLock failed: %v", err)
	}

	ch := make(chan error, 1)
	go func() {
		ch <- cm2.XLock(blk)
	}()
	time.Sleep(100 * time.Millisecond)
	cm1.Release()
	if err := <-ch; err != nil {
		t.Fatalf("Transaction 2 XLock failed after release: %v", err)
	}

	cm2.Release()
}

func TestConcurrencyMgr_Timeout(t *testing.T) {
	lockTable := NewLockTable()
	cm1 := NewConcurrencyMgr(lockTable)
	cm2 := NewConcurrencyMgr(lockTable)
	blk := *file.NewBlockId("testfile", 6)
	if err := cm1.XLock(blk); err != nil {
		t.Fatalf("Transaction 1 XLock failed: %v", err)
	}
	ch := make(chan error, 1)
	go func() {
		ch <- cm2.XLock(blk)
	}()
	select {
	case err := <-ch:
		if err != ErrLockAbort {
			t.Fatalf("Expected timeout error, but got: %v", err)
		}
	case <-time.After(MAX_TIME + time.Second):
		t.Fatalf("Test did not timeout as expected")
	}
	cm1.Release()
}
