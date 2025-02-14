package file

import (
	"fmt"
	"hash/fnv"
)

type BlockId struct {
	filename string
	blknum   int
}

func NewBlockId(filename string, blknum int) *BlockId {
	return &BlockId{filename: filename, blknum: blknum}
}

func (b BlockId) FileName() string {
	return b.filename
}

func (b BlockId) Number() int {
	return b.blknum
}

func (b BlockId) Equals(other BlockId) bool {
	return b.filename == other.filename && b.blknum == other.blknum
}

func (b BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blknum)
}

func (b BlockId) HashCode() uint32 {
	h := fnv.New32a()
	h.Write([]byte(b.String()))
	return h.Sum32()
}
