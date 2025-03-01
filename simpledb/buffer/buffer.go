package buffer

import (
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

type Buffer struct {
	fm       *file.FileMgr
	lm       *log.LogMgr
	contents *file.Page
	blk      *file.BlockId
	pins     int
	txnum    int
	lsn      int
}

func NewBuffer(fm *file.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPage(fm.BlockSize()),
		blk:      nil,
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() *file.BlockId {
	return b.blk
}

func (b *Buffer) SetModified(txnum, lsn int) {
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

func (b *Buffer) AssignToBlock(blk *file.BlockId) {
	if blk == nil {
		panic("Attempted to assign nil BlockId to buffer")
	}
	b.blk = blk    
	b.Flush()      
	b.fm.Read(blk, b.contents)
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.blk, b.contents)
		b.txnum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}
