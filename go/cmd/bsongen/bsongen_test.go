// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

func TestValidFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	inputs := testfiles.Glob("bson_test/input*.go")
	for _, input := range inputs {
		b, err := ioutil.ReadFile(input)
		if err != nil {
			t.Fatalf("ioutil.ReadFile error: %v", err)
		}
		want, err := ioutil.ReadFile(strings.Replace(input, "input", "output", 1))
		if err != nil {
			t.Fatalf("ioutil.ReadFile error: %v", err)
		}

		out, err := generateCode(string(b), "MyType")
		if err != nil {
			t.Fatalf("generateCode error: %v", err)
		}

		// goimports is flaky. So, let's not test that part.
		want, err = skipImports(want)
		if err != nil {
			t.Fatalf("skipImports error: %v", err)
		}
		out, err = skipImports(out)
		if err != nil {
			t.Fatalf("skipImports error: %v", err)
		}

		d, err := diff(want, out)
		if err != nil {
			t.Fatalf("diff error: %v", err)
		}
		if len(d) != 0 {
			t.Errorf("Unexpected output for %s:\n%s", input, string(d))
			if testing.Verbose() {
				t.Logf("%s:\n%s", input, out)
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

func skipImports(b []byte) ([]byte, error) {
	begin := bytes.Index(b, []byte("\nimport (\n"))
	if begin < 0 {
		return nil, errors.New("couldn't find beginning of import block")
	}
	end := bytes.Index(b, []byte("\n)\n"))
	if end < 0 {
		return nil, errors.New("couldn't find end of imports block")
	}
	return append(b[:begin], b[end+3:]...), nil
}

var invalidInputs = []struct{ title, input, err string }{
	{
		"func type",
		`package a; func MyType(){};`,
		"MyType not found",
	}, {
		"non-struct non-simple top level type",
		`package a; type MyType Custom;`,
		"MyType is not a struct or a simple type",
	}, {
		// Maybe support this in the future?
		"map type",
		`package a; type MyType map[string]Custom;`,
		"MyType is not a struct or a simple type",
	}, {
		// Maybe support this in the future?
		"slice type",
		`package a; type MyType []Custom;`,
		"MyType is not a struct or a simple type",
	}, {
		"anonymous embed",
		`package a; type MyType struct{Custom};`,
		"anonymous embeds not supported: Custom",
	}, {
		"interface with methods",
		`package a; type MyType struct{Val interface{Custom}};`,
		"is not a simple type",
	}, {
		// Maybe support this in the future?
		"array",
		`package a; type MyType struct{Val [5]int};`,
		"is not a simple type",
	},
}

func TestInvalidInputs(t *testing.T) {
	for _, tcase := range invalidInputs {
		out, err := generateCode(tcase.input, "MyType")
		if err == nil {
			t.Errorf("Expecting error for %s:\n%s", tcase.title, string(out))
		}
		if !strings.Contains(err.Error(), tcase.err) {
			t.Errorf("%s: got '%v', error should contain '%s'", tcase.title, err, tcase.err)
		}
	}
}
