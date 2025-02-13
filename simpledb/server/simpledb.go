package server

import (
	"simpledb/file"
)

type SimpleDB struct {
	fm *file.FileMgr
}

func NewSimpleDB(dirname string, blockSize, buffSize int) *SimpleDB {
	return &SimpleDB{
		fm: file.NewFileMgr(dirname, blockSize),
	}
}

func (db *SimpleDB) FileMgr() *file.FileMgr {
	return db.fm
}
