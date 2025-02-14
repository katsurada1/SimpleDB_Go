package server

import (
	"fmt"
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
)

type SimpleDB struct {
	fm *file.FileMgr
	lm *log.LogMgr
}

func NewSimpleDB(dirname string, blockSize, buffSize int) *SimpleDB {
	fm := file.NewFileMgr(dirname, blockSize)
	lm := log.NewLogMgr(fm, "logfile")
	if lm == nil {
			panic("NewSimpleDB: LogMgr is nil! Initialization failed.")
	}
	return &SimpleDB{
			fm: fm,
			lm: lm,
	}
}

func (db *SimpleDB) FileMgr() *file.FileMgr {
	return db.fm
}

func (db *SimpleDB) LogMgr() *log.LogMgr {
	return db.lm
}
