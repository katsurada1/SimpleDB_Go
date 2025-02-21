package buffer_test

import (
	"time"
	"simpledb_go/simpledb/buffer"
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
	"testing"
)

func TestBufferMgr(t *testing.T) {
	fm := file.NewFileMgr("testdir", 400)
	lm := log.NewLogMgr(fm, "testlog")
	bm := buffer.NewBufferMgr(fm, lm, 3)
	if bm.Available() != 3 {
		t.Errorf("Expected 3 available buffers, got %d", bm.Available())
	}
	blk := file.NewBlockId("testfile", 0)
	buf, err := bm.Pin(blk)
	if err != nil {
		t.Errorf("Failed to pin block: %v", err)
	}
	if bm.Available() != 2 {
		t.Errorf("Expected 2 available buffers after pinning, got %d", bm.Available())
	}
	bm.Unpin(buf)
	if bm.Available() != 3 {
		t.Errorf("Expected 3 available buffers after unpinning, got %d", bm.Available())
	}
	bm.FlushAll(1)
}

func TestBufferMgrNoAvailableBuffer(t *testing.T) {
	fm := file.NewFileMgr("testdir", 400)
	lm := log.NewLogMgr(fm, "testlog")
	bm := buffer.NewBufferMgr(fm, lm, 1)
	blk1 := file.NewBlockId("testfile", 0)
	blk2 := file.NewBlockId("testfile", 1)
	buf1, err := bm.Pin(blk1)
	if err != nil {
		t.Fatalf("Failed to pin first block: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := bm.Pin(blk2)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	bm.Unpin(buf1)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Failed to pin second block after unpinning first: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Test timed out: Pin operation did not complete")
	}
}
