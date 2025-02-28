package recovery

import (
	"simpledb_go/simpledb/file"
)

// 定数（Java の static final int 相当）
const (
	CHECKPOINT = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

// LogRecord インターフェース
type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Tx)
	String() string
}

// CreateLogRecord は、バイトスライスから適切な LogRecord を生成します。
func CreateLogRecord(rec []byte) LogRecord {
	p := file.NewPageFromBytes(rec)
	op := p.GetInt(0)
	switch op {
	case CHECKPOINT:
		return &CheckpointRecord{}
	case START:
		return NewStartRecordFromPage(p)
	case COMMIT:
		return NewCommitRecordFromPage(p)
	case ROLLBACK:
		return NewRollbackRecordFromPage(p)
	case SETINT:
		return NewSetIntRecordFromPage(p)
	case SETSTRING:
		return NewSetStringRecordFromPage(p)
	default:
		return nil
	}
}
