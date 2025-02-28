package recovery

import "simpledb_go/simpledb/file"

type Tx interface {
	Pin(blk file.BlockId)
	Unpin(blk file.BlockId)
	SetInt(blk file.BlockId, offset, val int, okToLog bool)
	SetString(blk file.BlockId, offset int, val string, okToLog bool)
}
