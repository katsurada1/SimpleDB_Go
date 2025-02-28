package recovery

import (
	"fmt"

	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

// StartRecord は START ログレコードを表します。
type StartRecord struct {
	txnum int
}

func NewStartRecordFromPage(p *file.Page) *StartRecord {
	tpos := 4
	return &StartRecord{
		txnum: p.GetInt(tpos),
	}
}

func (sr *StartRecord) Op() int {
	return START
}

func (sr *StartRecord) TxNumber() int {
	return sr.txnum
}

func (sr *StartRecord) Undo(tx Tx) {
	// 何もしない
}

func (sr *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", sr.txnum)
}

func WriteStartRecord(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 2*4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, START)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
