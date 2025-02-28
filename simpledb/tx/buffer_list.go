package tx

import (
	"sync"

	"simpledb_go/simpledb/buffer"
	"simpledb_go/simpledb/file"
)

type BufferList struct {
	mu      sync.Mutex
	buffers map[file.BlockId]*buffer.Buffer
	pins    []file.BlockId
	bm      *buffer.BufferMgr
}

func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockId]*buffer.Buffer),
		pins:    make([]file.BlockId, 0),
		bm:      bm,
	}
}

func (bl *BufferList) GetBuffer(blk file.BlockId) *buffer.Buffer {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.buffers[blk]
}

func (bl *BufferList) Pin(blk file.BlockId) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	// bm.Pin の引数は *file.BlockId で、戻り値は (*Buffer, error)
	buff, err := bl.bm.Pin(&blk)
	if err != nil {
		panic(err) // エラー処理は必要に応じて変更してください
	}
	bl.buffers[blk] = buff
	bl.pins = append(bl.pins, blk)
}

func (bl *BufferList) Unpin(blk file.BlockId) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if buff, ok := bl.buffers[blk]; ok {
		bl.bm.Unpin(buff)
		newPins := make([]file.BlockId, 0, len(bl.pins))
		for _, b := range bl.pins {
			if b != blk {
				newPins = append(newPins, b)
			}
		}
		bl.pins = newPins
		found := false
		for _, b := range bl.pins {
			if b == blk {
				found = true
				break
			}
		}
		if !found {
			delete(bl.buffers, blk)
		}
	}
}

func (bl *BufferList) UnpinAll() {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	for _, blk := range bl.pins {
		if buff, ok := bl.buffers[blk]; ok {
			bl.bm.Unpin(buff)
		}
	}
	bl.buffers = make(map[file.BlockId]*buffer.Buffer)
	bl.pins = bl.pins[:0]
}
