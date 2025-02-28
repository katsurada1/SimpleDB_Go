package tx

import (
	"fmt"
	"sync"
	"simpledb_go/simpledb/buffer"
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
	"simpledb_go/simpledb/tx/concurrency"
	"simpledb_go/simpledb/tx/recovery"
)

const END_OF_FILE = -1

type Transaction struct {
	fm          *file.FileMgr
	bm          *buffer.BufferMgr
	lm          *log.LogMgr
	recoveryMgr *recovery.RecoveryMgr
	concurMgr   *concurrency.ConcurrencyMgr
	myBuffers   *BufferList
	txnum       int
}

var (
	nextTxNum   int
	nextTxMutex sync.Mutex
)

func nextTxNumber() int {
	nextTxMutex.Lock()
	defer nextTxMutex.Unlock()
	nextTxNum++
	return nextTxNum
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	txnum := nextTxNumber()
	tx := &Transaction{
		fm:        fm,
		lm:        lm,
		bm:        bm,
		txnum:     txnum,
		myBuffers: NewBufferList(bm),
	}
	tx.recoveryMgr = recovery.NewRecoveryMgr(tx, txnum, lm, bm)
	tx.concurMgr = concurrency.NewDefaultConcurrencyMgr()
	return tx
}

func (tx *Transaction) Commit() {
	tx.recoveryMgr.Commit()
	fmt.Printf("transaction %d committed\n", tx.txnum)
	tx.concurMgr.Release()
	tx.myBuffers.UnpinAll()
}

func (tx *Transaction) Rollback() {
	tx.recoveryMgr.Rollback()
	fmt.Printf("transaction %d rolled back\n", tx.txnum)
	tx.concurMgr.Release()
	tx.myBuffers.UnpinAll()
}

func (tx *Transaction) Recover() {
	tx.bm.FlushAll(tx.txnum)
	tx.recoveryMgr.Recover()
}

func (tx *Transaction) Pin(blk file.BlockId) {
	tx.myBuffers.Pin(blk)
}

func (tx *Transaction) Unpin(blk file.BlockId) {
	tx.myBuffers.Unpin(blk)
}
func (tx *Transaction) SetInt(blk file.BlockId, offset, val int, okToLog bool) {
	tx.concurMgr.XLock(blk)
	buff := tx.myBuffers.GetBuffer(blk)
	if buff == nil {
			tx.myBuffers.Pin(blk)
			buff = tx.myBuffers.GetBuffer(blk)
			if buff == nil {
					panic("SetInt: failed to pin block")
			}
	}
	lsn := -1
	if okToLog {
			lsn = tx.recoveryMgr.SetInt(buff, offset, val)
	}
	p := buff.Contents()
	p.SetInt(offset, val)
	buff.SetModified(tx.txnum, lsn)
}

func (tx *Transaction) SetString(blk file.BlockId, offset int, val string, okToLog bool) {
	tx.concurMgr.XLock(blk)
	buff := tx.myBuffers.GetBuffer(blk)
	if buff == nil {
			tx.myBuffers.Pin(blk)
			buff = tx.myBuffers.GetBuffer(blk)
			if buff == nil {
					panic("SetString: failed to pin block")
			}
	}
	lsn := -1
	if okToLog {
			lsn = tx.recoveryMgr.SetString(buff, offset, val)
	}
	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(tx.txnum, lsn)
}


func (tx *Transaction) GetInt(blk file.BlockId, offset int) int {
	if err := tx.concurMgr.SLock(blk); err != nil {
			panic(err)
	}
	 buff := tx.myBuffers.GetBuffer(blk)
	if buff == nil {
			tx.myBuffers.Pin(blk)
			buff = tx.myBuffers.GetBuffer(blk)
			if buff == nil {
					panic("GetInt: failed to pin block")
			}
	}
	return buff.Contents().GetInt(offset)
}

func (tx *Transaction) GetString(blk file.BlockId, offset int) string {
	if err := tx.concurMgr.SLock(blk); err != nil {
			panic(err)
	}
	 buff := tx.myBuffers.GetBuffer(blk)
	if buff == nil {
			tx.myBuffers.Pin(blk)
			buff = tx.myBuffers.GetBuffer(blk)
			if buff == nil {
					panic("GetString: failed to pin block")
			}
	}
	return buff.Contents().GetString(offset)
}


func (tx *Transaction) Size(filename string) int {
	dummyBlk := file.NewBlockId(filename, END_OF_FILE)
	if err := tx.concurMgr.SLock(*dummyBlk); err != nil {
		panic(err)
	}
	return tx.fm.Length(filename)
}

func (tx *Transaction) Append(filename string) file.BlockId {
	dummyBlk := file.NewBlockId(filename, END_OF_FILE)
	if err := tx.concurMgr.XLock(*dummyBlk); err != nil {
			panic(err)
	}
	blk, err := tx.fm.Append(filename)
	if err != nil {
			panic(err)
	}
	tx.myBuffers.Pin(*blk)
	return *blk
}



func (tx *Transaction) AvailableBuffs() int {
	return tx.bm.Available()
}
