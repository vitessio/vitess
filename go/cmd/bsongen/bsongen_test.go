// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

func TestSimple(t *testing.T) {
	inputs := testfiles.Glob("bson_test/input*.go")
	for _, input := range inputs {
		b, err := ioutil.ReadFile(input)
		if err != nil {
			t.Fatal(err)
		}
		out, err := generateCode(string(b), "MyType")
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return
		}
		want, err := ioutil.ReadFile(strings.Replace(input, "input", "output", 1))
		if err != nil {
			t.Fatal(err)
		}
		// goimports is flaky. So, let's not test that part.
		d, err := diff(skip_imports(want), skip_imports(out))
		if len(d) != 0 {
			t.Errorf("Unexpected output for %s:\n%s", input, string(d))
			if testing.Verbose() {
				t.Logf("%s: %s\n", input, out)
			}
		}
	}
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

func skip_imports(b []byte) []byte {
	buf := bytes.NewBuffer(b)
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			return b[:0]
		}
		if len(line) == 0 || line[0] != ')' {
			continue
		}
		return b[len(b)-buf.Len():]
	}
}
