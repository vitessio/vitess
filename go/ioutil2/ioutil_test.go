package ioutil2

import (
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	fname := "/tmp/atomic-file-test.txt"
	err := WriteFileAtomic(fname, []byte("test string\n"), 0664)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(fname); err != nil {
		t.Fatal(err)
	}
}
