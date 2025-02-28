package recovery

import (
	"simpledb_go/simpledb/buffer"
	"simpledb_go/simpledb/log"
)

type RecoveryMgr struct {
	lm    *log.LogMgr
	bm    *buffer.BufferMgr
	tx    Tx
	txnum int
}

func NewRecoveryMgr(tx Tx, txnum int, lm *log.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	WriteStartRecord(lm, txnum)
	return &RecoveryMgr{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txnum: txnum,
	}
}

func (rm *RecoveryMgr) Commit() {
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCommitRecord(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteRollbackRecord(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCheckpointRecord(rm.lm)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) SetInt(buff *buffer.Buffer, offset, newval int) int {
	oldval := buff.Contents().GetInt(offset)
	blkPtr := buff.Block() // 仮に *file.BlockId を返すとする
	return WriteSetIntRecord(rm.lm, rm.txnum, *blkPtr, offset, oldval)
}

func (rm *RecoveryMgr) SetString(buff *buffer.Buffer, offset int, newval string) int {
	oldval := buff.Contents().GetString(offset)
	blkPtr := buff.Block()
	return WriteSetStringRecord(rm.lm, rm.txnum, *blkPtr, offset, oldval)
}

func (rm *RecoveryMgr) doRollback() {
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.TxNumber() == rm.txnum {
			if rec.Op() == START {
				return
			}
			rec.Undo(rm.tx)
		}
	}
}

func (rm *RecoveryMgr) doRecover() {
	finishedTxs := make(map[int]bool)
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.Op() == CHECKPOINT {
			return
		}
		if rec.Op() == COMMIT || rec.Op() == ROLLBACK {
			finishedTxs[rec.TxNumber()] = true
		} else if !finishedTxs[rec.TxNumber()] {
			rec.Undo(rm.tx)
		}
	}
}
