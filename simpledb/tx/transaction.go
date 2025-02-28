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

// END_OF_FILE はファイル末尾を示す定数
const END_OF_FILE = -1

// Transaction はトランザクション管理を行います。
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

// NewTransaction は新しいトランザクションを生成します。
func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	txnum := nextTxNumber()
	tx := &Transaction{
		fm:        fm,
		lm:        lm,
		bm:        bm,
		txnum:     txnum,
		myBuffers: NewBufferList(bm),
	}
	// Transaction は recovery.Tx インターフェースを実装しているので self を渡す
	tx.recoveryMgr = recovery.NewRecoveryMgr(tx, txnum, lm, bm)
	tx.concurMgr = concurrency.NewDefaultConcurrencyMgr()
	return tx
}

// Commit はトランザクションをコミットします。
func (tx *Transaction) Commit() {
	tx.recoveryMgr.Commit()
	fmt.Printf("transaction %d committed\n", tx.txnum)
	tx.concurMgr.Release()
	tx.myBuffers.UnpinAll()
}

// Rollback はトランザクションをロールバックします。
func (tx *Transaction) Rollback() {
	tx.recoveryMgr.Rollback()
	fmt.Printf("transaction %d rolled back\n", tx.txnum)
	tx.concurMgr.Release()
	tx.myBuffers.UnpinAll()
}

// Recover はシステム起動時のリカバリを実行します。
func (tx *Transaction) Recover() {
	tx.bm.FlushAll(tx.txnum)
	tx.recoveryMgr.Recover()
}

// --- 以下、recovery.Tx インターフェースの実装 ---

func (tx *Transaction) Pin(blk file.BlockId) {
	tx.myBuffers.Pin(blk)
}

func (tx *Transaction) Unpin(blk file.BlockId) {
	tx.myBuffers.Unpin(blk)
}
func (tx *Transaction) SetInt(blk file.BlockId, offset, val int, okToLog bool) {
	tx.concurMgr.XLock(blk)
	buff := tx.myBuffers.GetBuffer(blk)
	// もしバッファがピンされていなければ、ここでピンする
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
	// 同様に、バッファがピンされていなければピンする
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
	// まず共有ロックを取得
	if err := tx.concurMgr.SLock(blk); err != nil {
			panic(err)
	}
	// トランザクションのバッファリストから対象ブロックを取得
	 buff := tx.myBuffers.GetBuffer(blk)
	// もしピンされていなければ、改めてピンする
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


// Size はファイルのブロック数を返します。
func (tx *Transaction) Size(filename string) int {
	// file.NewBlockId returns *file.BlockId; ここでは値型に変換
	dummyBlk := file.NewBlockId(filename, END_OF_FILE)
	// SLock は引数が値型なので、*dummyBlk を渡す
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
	// Append 後に、このブロックをトランザクションのバッファリストにピンする
	tx.myBuffers.Pin(*blk)
	return *blk
}



func (tx *Transaction) AvailableBuffs() int {
	return tx.bm.Available()
}
