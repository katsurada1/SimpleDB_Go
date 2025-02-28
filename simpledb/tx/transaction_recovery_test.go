package tx

import (
	"os"
	"testing"
	"simpledb_go/simpledb/server"
	"simpledb_go/simpledb/tx/recovery"
)

func createTempDBDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "testdb")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	return dir
}

func removeTempDBDir(t *testing.T, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Fatalf("failed to remove temp dir: %v", err)
	}
}

func TestTransactionCommit(t *testing.T) {
	dbDir := createTempDBDir(t)
	defer removeTempDBDir(t, dbDir)
	db := server.NewSimpleDB(dbDir, 400, 8)
	tx := NewTransaction(db.FileMgr(), db.LogMgr(), db.BufferMgr())

	blk, err := tx.fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	tx.SetInt(*blk, 0, 100, true)
	tx.Commit()

	// コミットレコードは recovery.COMMIT として参照する
	iter := db.LogMgr().Iterator()
	foundCommit := false
	for iter.HasNext() {
		recBytes := iter.Next()
		rec := recovery.CreateLogRecord(recBytes)
		if rec != nil && rec.Op() == recovery.COMMIT && rec.TxNumber() == tx.txnum {
			foundCommit = true
			break
		}
	}
	if !foundCommit {
		t.Errorf("commit record not found in log")
	}
}

func TestTransactionRollback(t *testing.T) {
	dbDir := createTempDBDir(t)
	defer removeTempDBDir(t, dbDir)
	db := server.NewSimpleDB(dbDir, 400, 8)
	tx := NewTransaction(db.FileMgr(), db.LogMgr(), db.BufferMgr())

	blk, err := tx.fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	tx.SetString(*blk, 0, "hello", true)
	tx.Rollback()

	iter := db.LogMgr().Iterator()
	foundRollback := false
	for iter.HasNext() {
		recBytes := iter.Next()
		rec := recovery.CreateLogRecord(recBytes)
		if rec != nil && rec.Op() == recovery.ROLLBACK && rec.TxNumber() == tx.txnum {
			foundRollback = true
			break
		}
	}
	if !foundRollback {
		t.Errorf("rollback record not found in log")
	}
}

func TestTransactionRecovery(t *testing.T) {
	dbDir := createTempDBDir(t)
	defer removeTempDBDir(t, dbDir)
	db := server.NewSimpleDB(dbDir, 400, 8)
	fm := db.FileMgr()
	lm := db.LogMgr()
	bm := db.BufferMgr()
	tx1 := NewTransaction(fm, lm, bm)
	blk := tx1.Append("testfile")
	tx1.SetInt(blk, 0, 200, true)
	txRecovery := NewTransaction(fm, lm, bm)
	txRecovery.Recover()
	val := txRecovery.GetInt(blk, 0)
	if val != 0 {
		t.Errorf("recovery failed: expected 0 after undo, got %d", val)
	}
}
