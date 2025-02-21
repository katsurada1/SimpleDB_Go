package buffer

import (
	"errors"
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
	"sync"
	"time"
)

type BufferMgr struct {
	bufferpool   []*Buffer
	numAvailable int
	maxTime      time.Duration
	mutex        sync.Mutex
	cond         *sync.Cond
}

func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numBuffs int) *BufferMgr {
	bm := &BufferMgr{
		bufferpool:   make([]*Buffer, numBuffs),
		numAvailable: numBuffs,
		maxTime:      10 * time.Second,
	}
	bm.cond = sync.NewCond(&bm.mutex)
	for i := 0; i < numBuffs; i++ {
		bm.bufferpool[i] = NewBuffer(fm, lm)
	}
	return bm
}

func (bm *BufferMgr) Available() int {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	return bm.numAvailable
}

func (bm *BufferMgr) FlushAll(txnum int) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	for _, buff := range bm.bufferpool {
		if buff.ModifyingTx() == txnum {
			buff.Flush()
		}
	}
}
func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	buff.Unpin()
	if !buff.IsPinned() {
		previousAvailable := bm.numAvailable
		bm.numAvailable++
		if previousAvailable == 0 {
			bm.cond.Broadcast()
		}
	}
}

func (bm *BufferMgr) Pin(blk *file.BlockId) (*Buffer, error) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	startTime := time.Now()
	var buff *Buffer
	for {
		buff = bm.tryToPin(blk)
		if buff != nil {
			return buff, nil
		}

		remainingTime := bm.maxTime - time.Since(startTime)
		if remainingTime <= 0 {
			return nil, errors.New("BufferAbortException: no available buffer")
		}

		buff = bm.tryToPin(blk)
		if buff != nil {
			return buff, nil
		}

		bm.cond.Wait()
	}
}








func (bm *BufferMgr) tryToPin(blk *file.BlockId) *Buffer {
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(blk)
	}
	if !buff.IsPinned() {
		bm.numAvailable--
	}
	buff.Pin()
	return buff
}

func (bm *BufferMgr) findExistingBuffer(blk *file.BlockId) *Buffer {
	for _, buff := range bm.bufferpool {
		if b := buff.Block(); b != nil && *b == *blk {
			return buff
		}
	}
	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferpool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}
