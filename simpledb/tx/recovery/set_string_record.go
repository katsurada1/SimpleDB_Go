package recovery

import (
	"fmt"
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

type SetStringRecord struct {
	txnum  int
	offset int
	val    string
	blk    file.BlockId
}

func NewSetStringRecordFromPage(p *file.Page) *SetStringRecord {
	tpos := 4
	txnum := p.GetInt(tpos)
	fpos := tpos + 4
	filename := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(filename))
	blknum := p.GetInt(bpos)
	opos := bpos + 4
	offset := p.GetInt(opos)
	vpos := opos + 4
	val := p.GetString(vpos)
	return &SetStringRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    *file.NewBlockId(filename, blknum),
	}
}

func (sr *SetStringRecord) Op() int {
	return SETSTRING
}

func (sr *SetStringRecord) TxNumber() int {
	return sr.txnum
}

func (sr *SetStringRecord) Undo(tx Tx) {
	tx.Pin(sr.blk)
	tx.SetString(sr.blk, sr.offset, sr.val, false)
	tx.Unpin(sr.blk)
}

func (sr *SetStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s>", sr.txnum, sr.blk, sr.offset, sr.val)
}

func WriteSetStringRecord(lm *log.LogMgr, txnum int, blk file.BlockId, offset int, oldval string) int {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + 4
	vpos := opos + 4
	recLen := vpos + file.MaxLength(len(oldval))
	rec := make([]byte, recLen)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, SETSTRING)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, oldval)
	return lm.Append(rec)
}
