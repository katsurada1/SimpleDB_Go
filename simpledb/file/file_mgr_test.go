package file

import (
	"testing"
)

func TestFileMgr(t *testing.T) {
	fm := NewFileMgr("filetest", 400)
	blk := NewBlockId("testfile", 2)
	pos1 := 88
	p1 := NewPage(fm.BlockSize())
	p1.SetString(pos1, "abcdefghijklm")
	size := MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)
	err := fm.Write(blk, p1)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	p2 := NewPage(fm.BlockSize())
	err = fm.Read(blk, p2)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if got := p2.GetInt(pos2); got != 345 {
		t.Errorf("offset %d contains %d; want 345", pos2, got)
	}

	if got := p2.GetString(pos1); got != "abcdefghijklm" {
		t.Errorf("offset %d contains %s; want 'abcdefghijklm'", pos1, got)
	}
}
