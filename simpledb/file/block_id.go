package file

import "fmt"

type BlockID struct {
	Filename string
	Blknum   int
}

func NewBlockID(filename string, blknum int) *BlockID {
	return &BlockID{Filename: filename, Blknum: blknum}
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Blknum)
}

func (b *BlockID) Equals(other *BlockID) bool {
	return b.Filename == other.Filename && b.Blknum == other.Blknum
}

func (b *BlockID) HashCode() int {
	return len(b.String()) // Simplified hash function
}
