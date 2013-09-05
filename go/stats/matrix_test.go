// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"strings"
	"testing"
)

func TestMatrix(t *testing.T) {
	clear()
	m := NewMatrix("matrix1")
	m.Add("m1", "n1", 1)
	m.Add("m1", "n2", 1)
	m.Add("m2", "n1", 1)
	m.Add("m2", "n1", 1)
	want1 := `"m1.n1": 1`
	want2 := `"m1.n2": 1`
	want3 := `"m2.n1": 2`
	if !strings.Contains(m.String(), want1) {
		t.Errorf("want containing %s, got %s", want1, m.String())
	}
	if !strings.Contains(m.String(), want2) {
		t.Errorf("want containing %s, got %s", want2, m.String())
	}
	if !strings.Contains(m.String(), want3) {
		t.Errorf("want containing %s, got %s", want3, m.String())
	}
	counts := m.Counts()
	if counts["m1"]["n1"] != 1 {
		t.Errorf("want 1, got %d", counts["m1"]["n1"])
	}
	if counts["m1"]["n2"] != 1 {
		t.Errorf("want 1, got %d", counts["m1"]["n2"])
	}
	if counts["m2"]["n1"] != 2 {
		t.Errorf("want 2, got %d", counts["m2"]["n1"])
	}
}

func TestMatrixHook(t *testing.T) {
	var gotname string
	var gotv *Matrix
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Matrix)
	})

	v := NewMatrix("matrix2")
	if gotname != "matrix2" {
		t.Errorf("want matrix2, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}
