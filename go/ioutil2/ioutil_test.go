package ioutil2

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {
	data := []byte("test string\n")
	fname := fmt.Sprintf("/tmp/atomic-file-test-%v.txt", time.Now().UnixNano())
	err := WriteFileAtomic(fname, data, 0664)
	if err != nil {
		t.Fatal(err)
	}
	rData, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, rData) {
		t.Fatalf("data mismatch: %v != %v", data, rData)
	}
	if err := os.Remove(fname); err != nil {
		t.Fatal(err)
	}
}
