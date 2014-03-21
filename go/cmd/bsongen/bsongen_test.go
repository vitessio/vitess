// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

func TestSimple(t *testing.T) {
	input := testfiles.Locate("bson_test/simple_type.go")
	b, err := ioutil.ReadFile(input)
	if err != nil {
		log.Fatal(err)
	}
	out, err := generateCode(string(b), "MyType")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	fmt.Printf("%s\n", out)
}

// diff copied from gofmt.go
func diff(b1, b2 []byte) (data []byte, err error) {
	f1, err := ioutil.TempFile("", "bsongen")
	if err != nil {
		return
	}
	defer os.Remove(f1.Name())
	defer f1.Close()

	f2, err := ioutil.TempFile("", "bsongen")
	if err != nil {
		return
	}
	defer os.Remove(f2.Name())
	defer f2.Close()

	f1.Write(b1)
	f2.Write(b2)

	data, err = exec.Command("diff", "-u", f1.Name(), f2.Name()).CombinedOutput()
	if len(data) > 0 {
		// diff exits with a non-zero status when the files don't match.
		// Ignore that failure as long as we get output.
		err = nil
	}
	return
}
