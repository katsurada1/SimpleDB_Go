package log

import (
	"fmt"
	"simpledb_go/simpledb/file"
)

type LogIterator struct {
	fm         *file.FileMgr
	blk        *file.BlockId
	page       *file.Page
	currentPos int
	boundary   int
}

func NewLogIterator(fm *file.FileMgr, blk *file.BlockId) *LogIterator {
	page := file.NewPage(fm.BlockSize()) 
	iterator := &LogIterator{
		fm:   fm,
		blk:  blk,
		page: page,
	}
	iterator.moveToBlock(blk) 
	return iterator
}

func (it *LogIterator) HasNext() bool {
	hasNext := it.currentPos < it.fm.BlockSize() || it.blk.Number() > 0
	if !hasNext {
			fmt.Println("LogIterator.HasNext: No more records.")
	}
	return hasNext
}

func (it *LogIterator) Next() []byte {
	if it.currentPos == it.fm.BlockSize() {
		it.blk = file.NewBlockId(it.blk.FileName(), it.blk.Number()-1)
		it.moveToBlock(it.blk)
	}
	rec := it.page.GetBytes(it.currentPos) 
	it.currentPos += file.IntSize + len(rec) 
	return rec
}

func (it *LogIterator) moveToBlock(blk *file.BlockId) {
	it.fm.Read(blk, it.page) 
	it.boundary = it.page.GetInt(0) 
	it.currentPos = it.boundary 
}
