package server

import (
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
	"simpledb_go/simpledb/buffer"
)

type SimpleDB struct {
	fm *file.FileMgr
	lm *log.LogMgr
	bm *buffer.BufferMgr
}

func NewSimpleDB(dirname string, blockSize, buffSize int) *SimpleDB {
	fm := file.NewFileMgr(dirname, blockSize)
	lm := log.NewLogMgr(fm, "logfile")
	if lm == nil {
		panic("NewSimpleDB: LogMgr is nil! Initialization failed.")
	}
	bm := buffer.NewBufferMgr(fm, lm, buffSize)
	return &SimpleDB{
		fm: fm,
		lm: lm,
		bm: bm,
	}
}

func (db *SimpleDB) FileMgr() *file.FileMgr {
	return db.fm
}

func (db *SimpleDB) LogMgr() *log.LogMgr {
	return db.lm
}

func (db *SimpleDB) BufferMgr() *buffer.BufferMgr {
	return db.bm
}
