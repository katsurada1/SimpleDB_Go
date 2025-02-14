package log_test

import (
	"fmt"
	"os"
	"simpledb_go/simpledb/file"
	"simpledb_go/simpledb/log"
	"simpledb_go/simpledb/server"
	"testing"
)

var lm *log.LogMgr

func setup() {
    os.RemoveAll("logtest") 
    os.MkdirAll("logtest", os.ModePerm) 
    db := server.NewSimpleDB("logtest", 400, 8)
    lm = db.LogMgr()
}

func TestLogInitialization(t *testing.T) {
	setup() 
	iter := lm.Iterator()
	if iter.HasNext() {
		t.Errorf("Expected empty log, but found records")
	}
}

func TestLogAppend(t *testing.T) {
	setup()
	createRecords(1, 5)
	iter := lm.Iterator()
	expectedRecords := []struct {
		s string
		n int
	}{
		{"record5", 105},
		{"record4", 104},
		{"record3", 103},
		{"record2", 102},
		{"record1", 101},
	}
	i := 0
	for iter.HasNext() {
			rec := iter.Next()
			p := file.NewPageFromBytes(rec)
			s := p.GetString(0)
			npos := file.MaxLength(len(s))
			val := p.GetInt(npos)
			if i >= len(expectedRecords) {
					t.Errorf("Unexpected extra record found: [%s, %d]", s, val)
					break
			}
			if s != expectedRecords[i].s || val != expectedRecords[i].n {
					t.Errorf("Expected [%s, %d], but got [%s, %d]", expectedRecords[i].s, expectedRecords[i].n, s, val)
			}
			i++
	}
	if i != len(expectedRecords) {
			t.Errorf("Expected %d records, but found %d", len(expectedRecords), i)
	}
}

func TestLogFlush(t *testing.T) {
	setup()
	createRecords(6, 10) // 6 〜 10 のログを追加
	lm.Flush(8)
	iter := lm.Iterator()
	count := 0
	for iter.HasNext() {
			fmt.Println("TestLogFlush: Found a record")
			iter.Next()
			count++
	}
	if count != 5 {
			t.Errorf("Expected 5 records after flush, but found %d", count)
	}
}

func createRecords(start, end int) {
	for i := start; i <= end; i++ {
		rec := createLogRecord(fmt.Sprintf("record%d", i), i+100)
		lsn := lm.Append(rec)
	}
}

func createLogRecord(s string, n int) []byte {
	spos := 0
	npos := spos + file.MaxLength(len(s))
	b := make([]byte, npos+file.IntSize)
	p := file.NewPageFromBytes(b)
	p.SetString(spos, s)
	p.SetInt(npos, n)
	return b
}
