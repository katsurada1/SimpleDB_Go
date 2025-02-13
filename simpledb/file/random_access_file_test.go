package file

import (
	"os"
	"testing"
)

func TestRandomAccessFileSimulation(t *testing.T) {
	filename := "testfile"
	offset := 123

	f1, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f1.Close()

	writeInt(f1, offset, 999) 

	f2, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open file for reading: %v", err)
	}
	defer f2.Close()

	n := readInt(f2, offset) 
	writeInt(f2, offset, n+1)

	f3, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open file for final reading: %v", err)
	}
	defer f3.Close()

	newValue := readInt(f3, offset) 
	if newValue != 1000 {
		t.Errorf("Expected value 1000, got %d", newValue)
	}
}
