package log

import (
	"fmt"
	"sync"
	"log"
	"simpledb_go/simpledb/file"
)

type LogMgr struct {
	mu          sync.Mutex
	fm          *file.FileMgr
	logfile     string
	logpage     *file.Page
	currentblk  *file.BlockId
	latestLSN   int
	lastSavedLSN int
}

func NewLogMgr(fm *file.FileMgr, logfile string) *LogMgr {
	logpage := file.NewPage(fm.BlockSize())
	logsize := fm.Length(logfile) 
	var currentblk *file.BlockId
	if logsize == 0 {
		currentblk = appendNewBlock(fm, logfile, logpage) 
	} else {
		currentblk = file.NewBlockId(logfile, logsize-1)
		err := fm.Read(currentblk, logpage) 
		if err != nil {
			fmt.Printf("NewLogMgr: Error reading block: %v\n", err)
		}
	}
	return &LogMgr{
		fm:          fm,
		logfile:     logfile,
		logpage:     logpage,
		currentblk:  currentblk,
		latestLSN:   0,
		lastSavedLSN: 0,
	}
}


func (lm *LogMgr) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
			lm.flush()
	} else {
			fmt.Println("LogMgr.Flush: No flush needed.")
	}
}

func (lm *LogMgr) Iterator() *LogIterator {
	lm.flush()
	iter := NewLogIterator(lm.fm, lm.currentblk)
	return iter
}

func (lm *LogMgr) Append(logrec []byte) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	boundary := lm.logpage.GetInt(0)
	recsize := len(logrec)
	bytesneeded := recsize + file.IntSize
	if boundary-bytesneeded < file.IntSize {
			lm.flush()
			lm.currentblk = appendNewBlock(lm.fm, lm.logfile, lm.logpage)
			boundary = lm.logpage.GetInt(0)
	}
	recpos := boundary - bytesneeded
	lm.logpage.SetBytes(recpos, logrec)
	lm.logpage.SetInt(0, recpos)
	lm.latestLSN++
	return lm.latestLSN
}

func (lm *LogMgr) flush() {
	lm.fm.Write(lm.currentblk, lm.logpage)
	lm.lastSavedLSN = lm.latestLSN
}

func appendNewBlock(fm *file.FileMgr, logfile string, logpage *file.Page) *file.BlockId {
	blk, err := fm.Append(logfile)
	if err != nil {
		log.Fatalf("appendNewBlock: Failed to append block: %v", err)
	}
	logpage.SetInt(0, fm.BlockSize())
	err = fm.Write(blk, logpage)
	if err != nil {
		log.Fatalf("appendNewBlock: Failed to write new block: %v", err)
	}
	return blk
}

