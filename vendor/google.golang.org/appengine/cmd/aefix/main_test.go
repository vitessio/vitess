// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"go/ast"
	"go/parser"
	"strings"
	"testing"
)

type testCase struct {
	Name string
	Fn   func(*ast.File) bool
	In   string
	Out  string
}

var testCases []testCase

func addTestCases(t []testCase, fn func(*ast.File) bool) {
	// Fill in fn to avoid repetition in definitions.
	if fn != nil {
		for i := range t {
			if t[i].Fn == nil {
				t[i].Fn = fn
			}
		}
	}
	testCases = append(testCases, t...)
}

func fnop(*ast.File) bool { return false }

func parseFixPrint(t *testing.T, fn func(*ast.File) bool, desc, in string, mustBeGofmt bool) (out string, fixed, ok bool) {
	file, err := parser.ParseFile(fset, desc, in, parserMode)
	if err != nil {
		t.Errorf("%s: parsing: %v", desc, err)
		return
	}

	outb, err := gofmtFile(file)
	if err != nil {
		t.Errorf("%s: printing: %v", desc, err)
		return
	}
	if s := string(outb); in != s && mustBeGofmt {
		t.Errorf("%s: not gofmt-formatted.\n--- %s\n%s\n--- %s | gofmt\n%s",
			desc, desc, in, desc, s)
		tdiff(t, in, s)
		return
	}

	if fn == nil {
		for _, fix := range fixes {
			if fix.f(file) {
				fixed = true
			}
		}
	} else {
		fixed = fn(file)
	}

	outb, err = gofmtFile(file)
	if err != nil {
		t.Errorf("%s: printing: %v", desc, err)
		return
	}

	return string(outb), fixed, true
}

func TestRewrite(t *testing.T) {
	for _, tt := range testCases {
		// Apply fix: should get tt.Out.
		out, fixed, ok := parseFixPrint(t, tt.Fn, tt.Name, tt.In, true)
		if !ok {
			continue
		}

		// reformat to get printing right
		out, _, ok = parseFixPrint(t, fnop, tt.Name, out, false)
		if !ok {
			continue
		}

		if out != tt.Out {
			t.Errorf("%s: incorrect output.\n", tt.Name)
			if !strings.HasPrefix(tt.Name, "testdata/") {
				t.Errorf("--- have\n%s\n--- want\n%s", out, tt.Out)
			}
			tdiff(t, out, tt.Out)
			continue
		}

		if changed := out != tt.In; changed != fixed {
			t.Errorf("%s: changed=%v != fixed=%v", tt.Name, changed, fixed)
			continue
		}

		// Should not change if run again.
		out2, fixed2, ok := parseFixPrint(t, tt.Fn, tt.Name+" output", out, true)
		if !ok {
			continue
		}

		if fixed2 {
			t.Errorf("%s: applied fixes during second round", tt.Name)
			continue
		}

		if out2 != out {
			t.Errorf("%s: changed output after second round of fixes.\n--- output after first round\n%s\n--- output after second round\n%s",
				tt.Name, out, out2)
			tdiff(t, out, out2)
		}
	}
}

func tdiff(t *testing.T, a, b string) {
	data, err := diff([]byte(a), []byte(b))
	if err != nil {
		t.Error(err)
		return
	}
	t.Error(string(data))
}
