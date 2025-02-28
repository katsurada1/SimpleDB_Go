package recovery

import (
	"fmt"

	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

type RollbackRecord struct {
	txnum int
}

func NewRollbackRecordFromPage(p *file.Page) *RollbackRecord {
	tpos := 4
	return &RollbackRecord{
		txnum: p.GetInt(tpos),
	}
}

func (rr *RollbackRecord) Op() int {
	return ROLLBACK
}

func (rr *RollbackRecord) TxNumber() int {
	return rr.txnum
}

func (rr *RollbackRecord) Undo(tx Tx) {

}

func (rr *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", rr.txnum)
}

func WriteRollbackRecord(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 2*4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, ROLLBACK)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
