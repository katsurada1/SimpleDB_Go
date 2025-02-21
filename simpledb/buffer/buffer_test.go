package buffer_test

import (
	"simpledb_go/simpledb/buffer"
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
	"testing"
)

func TestBuffer(t *testing.T) {
	fm := file.NewFileMgr("testdir", 400)
	lm := log.NewLogMgr(fm, "testlog")
	buf := buffer.NewBuffer(fm, lm)

	if buf.IsPinned() {
		t.Errorf("Expected buffer to be unpinned, but it was pinned")
	}

	buf.Pin()
	if !buf.IsPinned() {
		t.Errorf("Expected buffer to be pinned, but it was not")
	}

	buf.Unpin()
	if buf.IsPinned() {
		t.Errorf("Expected buffer to be unpinned after unpinning, but it was pinned")
	}

	buf.SetModified(1, 100)
	if buf.ModifyingTx() != 1 {
		t.Errorf("Expected modifying transaction to be 1, got %d", buf.ModifyingTx())
	}

	blk := file.NewBlockId("testfile", 0)
	buf.AssignToBlock(blk)
	if buf.Block() != blk {
		t.Errorf("Expected assigned block to match, but it did not")
	}
}