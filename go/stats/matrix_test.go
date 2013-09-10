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
	m := NewMatrix("matrix1", "x", "y")
	m.Add("m1", "n1", 1)
	m.Add("m1", "n2", 1)
	m.Add("m2", "n1", 1)
	m.Add("m2", "n1", 1)
	want1 := `{"x": "m1", "y": "n1", "Value": 1}`
	want2 := `{"x": "m1", "y": "n2", "Value": 1}`
	want3 := `{"x": "m2", "y": "n1", "Value": 2}`
	if !strings.Contains(m.String(), want1) {
		t.Errorf("want containing %s, got %s", want1, m.String())
	}
	if !strings.Contains(m.String(), want2) {
		t.Errorf("want containing %s, got %s", want2, m.String())
	}
	if !strings.Contains(m.String(), want3) {
		t.Errorf("want containing %s, got %s", want3, m.String())
	}
	counts := m.Data()
	if counts["m1"]["n1"] != 1 {
		t.Errorf("want 1, got %d", counts["m1"]["n1"])
	}
	if counts["m1"]["n2"] != 1 {
		t.Errorf("want 1, got %d", counts["m1"]["n2"])
	}
	if counts["m2"]["n1"] != 2 {
		t.Errorf("want 2, got %d", counts["m2"]["n1"])
	}
	f := func() map[string]map[string]int64 {
		return map[string]map[string]int64{
			"m1": map[string]int64{"n1": 1, "n2": 1},
			"m2": map[string]int64{"n1": 2},
		}
	}
	mf := NewMatrixFunc("x", "y", f)
	if !strings.Contains(mf.String(), want1) {
		t.Errorf("want containing %s, got %s", want1, mf.String())
	}
	if !strings.Contains(mf.String(), want2) {
		t.Errorf("want containing %s, got %s", want2, mf.String())
	}
	if !strings.Contains(mf.String(), want3) {
		t.Errorf("want containing %s, got %s", want3, mf.String())
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

	v := NewMatrix("matrix2", "x", "y")
	if gotname != "matrix2" {
		t.Errorf("want matrix2, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}
