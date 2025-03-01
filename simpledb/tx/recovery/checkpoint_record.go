package recovery

import (
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

type CheckpointRecord struct{}

func (cr *CheckpointRecord) Op() int {
	return CHECKPOINT
}

func (cr *CheckpointRecord) TxNumber() int {
	return -1
}

func (cr *CheckpointRecord) Undo(tx Tx) {
}

func (cr *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointRecord(lm *log.LogMgr) int {
	rec := make([]byte, 4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}
