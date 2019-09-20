/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
