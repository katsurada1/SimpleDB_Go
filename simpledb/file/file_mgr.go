package file

import (
	"bytes"
	"fmt"
	"os"
	"sync"
)

type FileMgr struct {
	dbDirectory string
	blocksize   int
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
	readCount   int
	writeCount  int
}

func NewFileMgr(dbDirectory string, blocksize int) *FileMgr {
	_, err := os.Stat(dbDirectory)
	isNew := os.IsNotExist(err)

	if isNew {
		os.Mkdir(dbDirectory, 0755)
	}

	return &FileMgr{
		dbDirectory: dbDirectory,
		blocksize:   blocksize,
		isNew:       isNew,
		openFiles:   make(map[string]*os.File),
	}
}

func (fm *FileMgr) Read(blk *BlockID, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.Filename)
	if err != nil {
		return fmt.Errorf("cannot read block %v", blk)
	}
	defer f.Close()

	f.Seek(int64(blk.Blknum*fm.blocksize), 0)
	_, err = f.Read(p.contents.Bytes())
	fm.readCount++

	return err
}

func (fm *FileMgr) Write(blk *BlockID, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.Filename)
	if err != nil {
		return fmt.Errorf("cannot write block %v", blk)
	}
	defer f.Close()

	f.Seek(int64(blk.Blknum*fm.blocksize), 0)
	_, err = f.Write(p.contents.Bytes())
	fm.writeCount++

	return err
}

func (fm *FileMgr) Append(filename string) (*BlockID, error) {
	newBlkNum := fm.Length(filename)
	blk := NewBlockID(filename, newBlkNum)

	b := make([]byte, fm.blocksize)
	f, err := fm.getFile(blk.Filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	f.Seek(int64(blk.Blknum*fm.blocksize), 0)
	_, err = f.Write(b)
	fm.writeCount++

	return blk, err
}

func (fm *FileMgr) Length(filename string) int {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0
	}
	defer f.Close()

	info, _ := f.Stat()
	return int(info.Size()) / fm.blocksize
}

func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	if f, ok := fm.openFiles[filename]; ok {
		return f, nil
	}

	path := fmt.Sprintf("%s/%s", fm.dbDirectory, filename)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	fm.openFiles[filename] = f

	return f, nil
}

func (fm *FileMgr) GetReadCount() int {
	return fm.readCount
}

func (fm *FileMgr) GetWriteCount() int {
	return fm.writeCount
}

func (fm *FileMgr) ResetStatistics() {
	fm.readCount = 0
	fm.writeCount = 0
}
