package file

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type FileMgr struct {
	dbDirectory string
	blockSize   int
	isNew       bool
	openFiles   map[string]*os.File
	readCount   int
	writeCount  int
	mu          sync.Mutex
}

const IntSize = 4

func NewFileMgr(dbDirectory string, blockSize int) *FileMgr {
	isNew := false
	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		isNew = true
		os.MkdirAll(dbDirectory, os.ModePerm)
	}
	files, err := os.ReadDir(dbDirectory)
	if err != nil {
			return nil
	}

	for _, file := range files {
		if file.Name()[:IntSize] == "temp" {
			os.Remove(filepath.Join(dbDirectory, file.Name()))
		}
	}
	return &FileMgr{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       isNew,
		openFiles:   make(map[string]*os.File),
	}
}

func (fm *FileMgr) Read(blk *BlockId, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("cannot read block %v: %w", blk, err)
	}

	_, err = f.Seek(int64(blk.Number()*fm.blockSize), io.SeekStart)
	if err != nil {
		return err
	}

	data := make([]byte, fm.blockSize)
	_, err = f.Read(data)
	if err != nil {
		return err
	}

	p.SetContents(bytes.NewBuffer(data))
	fm.readCount++
	return nil
}

func (fm *FileMgr) Write(blk *BlockId, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.FileName())
	if err != nil {
			return fmt.Errorf("cannot write block %v: %w", blk, err)
	}

	_, err = f.Seek(int64(blk.Number()*fm.blockSize), io.SeekStart)
	if err != nil {
			return err
	}

	_, err = f.Write(p.Contents().Bytes())
	if err != nil {
			return err
	}

	fm.writeCount++
	return nil
}

func (fm *FileMgr) Append(filename string) (*BlockId, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	newBlkNum := fm.Length(filename)
	blk := NewBlockId(filename, newBlkNum)
	emptyData := make([]byte, fm.blockSize)
	f, err := fm.getFile(blk.FileName())
	if err != nil {
			return nil, fmt.Errorf("cannot append block %v: %w", blk, err)
	}
	
	_, err = f.Seek(int64(blk.Number()*fm.blockSize), io.SeekStart)
	if err != nil {
			return nil, err
	}

	_, err = f.Write(emptyData)
	if err != nil {
			return nil, err
	}
	fm.writeCount++
	return blk, nil
}

func (fm *FileMgr) Length(filename string) int {
	f, err := fm.getFile(filename)
	if err != nil {
			return 0
	}

	info, err := f.Stat()
	if err != nil {
			return 0
	}

	filesize := int(info.Size()) / fm.blockSize
	return filesize
}



func (fm *FileMgr) IsNew() bool {
	return fm.isNew
}

func (fm *FileMgr) BlockSize() int {
	return fm.blockSize
}

func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	if f, ok := fm.openFiles[filename]; ok {
			return f, nil
	}

	filePath := filepath.Join(fm.dbDirectory, filename)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
			return nil, err
	}

	fm.openFiles[filename] = f
	return f, nil
}




