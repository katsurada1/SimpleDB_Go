package main

import (
	"fmt"

	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
	"simpledb_go/simpledb/server"
	"simpledb_go/simpledb/tx/recovery"
)

func main() {
	db := server.NewSimpleDB("studentdb", 400, 8)
	fm := db.FileMgr()
	lm := db.LogMgr()
	filename := "simpledb.log"

	lastBlock := fm.Length(filename) - 1
	blk := file.NewBlockId(filename, lastBlock)
	p := file.NewPage(make([]byte, fm.BlockSize()))
	fm.Read(blk, p)
	iter := lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := recovery.CreateLogRecord(bytes)
		fmt.Println(rec)
	}
}
