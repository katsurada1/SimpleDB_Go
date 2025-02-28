package recovery

import (
	"fmt"

	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

type CommitRecord struct {
	txnum int
}

func NewCommitRecordFromPage(p *file.Page) *CommitRecord {
	tpos := 4
	return &CommitRecord{
		txnum: p.GetInt(tpos),
	}
}

func (cr *CommitRecord) Op() int {
	return COMMIT
}

func (cr *CommitRecord) TxNumber() int {
	return cr.txnum
}

func (cr *CommitRecord) Undo(tx Tx) {
}

func (cr *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", cr.txnum)
}

func WriteCommitRecord(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 2*4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
