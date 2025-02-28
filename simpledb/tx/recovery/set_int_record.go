package recovery

import (
	"fmt"

	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

// SetIntRecord は SETINT ログレコードを表します。
type SetIntRecord struct {
	txnum  int
	offset int
	val    int
	blk    file.BlockId
}

func NewSetIntRecordFromPage(p *file.Page) *SetIntRecord {
	tpos := 4
	txnum := p.GetInt(tpos)
	fpos := tpos + 4
	filename := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(filename))
	blknum := p.GetInt(bpos)
	opos := bpos + 4
	offset := p.GetInt(opos)
	vpos := opos + 4
	val := p.GetInt(vpos)
	return &SetIntRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    *file.NewBlockId(filename, blknum), // dereference to get value type
	}
}

func (sr *SetIntRecord) Op() int {
	return SETINT
}

func (sr *SetIntRecord) TxNumber() int {
	return sr.txnum
}

func (sr *SetIntRecord) Undo(tx Tx) {
	tx.Pin(sr.blk)
	tx.SetInt(sr.blk, sr.offset, sr.val, false)
	tx.Unpin(sr.blk)
}

func (sr *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %v %d %d>", sr.txnum, sr.blk, sr.offset, sr.val)
}

func WriteSetIntRecord(lm *log.LogMgr, txnum int, blk file.BlockId, offset, oldval int) int {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + 4
	vpos := opos + 4
	recLen := vpos + 4
	rec := make([]byte, recLen)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, SETINT)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, oldval)
	return lm.Append(rec)
}
