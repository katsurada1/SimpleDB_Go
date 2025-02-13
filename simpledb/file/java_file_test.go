package file

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestJavaFileSimulation(t *testing.T) {
	filename := "testfile"
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	pos1 := 88
	len1 := writeString(f, pos1, "abcdefghijklm")
	pos2 := pos1 + len1
	writeInt(f, pos2, 345)

	g, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open file for reading: %v", err)
	}
	defer g.Close()

	if got := readInt(g, pos2); got != 345 {
		t.Errorf("offset %d contains %d; want 345", pos2, got)
	}

	if got := readString(g, pos1); got != "abcdefghijklm" {
		t.Errorf("offset %d contains %s; want 'abcdefghijklm'", pos1, got)
	}
}

func readInt(f *os.File, pos int) int {
	buf := make([]byte, 4)
	f.Seek(int64(pos), 0)
	_, err := f.Read(buf)
	if err != nil {
		panic(err)
	}
	return int(binary.BigEndian.Uint32(buf))
}

func writeInt(f *os.File, pos int, n int) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(n))
	f.Seek(int64(pos), 0)
	_, err := f.Write(buf)
	if err != nil {
		panic(err)
	}
}

func readString(f *os.File, pos int) string {
	f.Seek(int64(pos), 0)
	length := readInt(f, pos)
	buf := make([]byte, length)
	f.Seek(int64(pos+4), 0)
	_, err := f.Read(buf)
	if err != nil {
		panic(err)
	}
	return string(buf)
}


func writeString(f *os.File, pos int, s string) int {
	f.Seek(int64(pos), 0)
	length := len(s)
	writeInt(f, pos, length)
	f.Seek(int64(pos+4), 0)
	_, err := f.Write([]byte(s))
	if err != nil {
		panic(err)
	}
	return 4 + length
}
