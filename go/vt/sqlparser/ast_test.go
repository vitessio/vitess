// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"testing"
)

func TestSelect(t *testing.T) {
	tree, err := Parse("select * from t where a = 1")
	if err != nil {
		t.Error(err)
	}
	expr := tree.(*Select).Where.Expr

	sel := &Select{}
	sel.AddWhere(expr)
	buf := NewTrackedBuffer(nil)
	sel.Where.Format(buf)
	want := " where a = 1"
	if buf.String() != want {
		t.Errorf("where: %q, want %s", buf.String(), want)
	}
	sel.AddWhere(expr)
	buf = NewTrackedBuffer(nil)
	sel.Where.Format(buf)
	want = " where a = 1 and a = 1"
	if buf.String() != want {
		t.Errorf("where: %q, want %s", buf.String(), want)
	}
	sel = &Select{}
	sel.AddHaving(expr)
	buf = NewTrackedBuffer(nil)
	sel.Having.Format(buf)
	want = " having a = 1"
	if buf.String() != want {
		t.Errorf("having: %q, want %s", buf.String(), want)
	}
	sel.AddHaving(expr)
	buf = NewTrackedBuffer(nil)
	sel.Having.Format(buf)
	want = " having a = 1 and a = 1"
	if buf.String() != want {
		t.Errorf("having: %q, want %s", buf.String(), want)
	}

	// OR clauses must be parenthesized.
	tree, err = Parse("select * from t where a = 1 or b = 1")
	if err != nil {
		t.Error(err)
	}
	expr = tree.(*Select).Where.Expr
	sel = &Select{}
	sel.AddWhere(expr)
	buf = NewTrackedBuffer(nil)
	sel.Where.Format(buf)
	want = " where (a = 1 or b = 1)"
	if buf.String() != want {
		t.Errorf("where: %q, want %s", buf.String(), want)
	}
	sel = &Select{}
	sel.AddHaving(expr)
	buf = NewTrackedBuffer(nil)
	sel.Having.Format(buf)
	want = " having (a = 1 or b = 1)"
	if buf.String() != want {
		t.Errorf("having: %q, want %s", buf.String(), want)
	}
}

func TestWhere(t *testing.T) {
	var w *Where
	buf := NewTrackedBuffer(nil)
	w.Format(buf)
	if buf.String() != "" {
		t.Errorf("w.Format(nil): %q, want \"\"", buf.String())
	}
	w = NewWhere(WhereStr, nil)
	buf = NewTrackedBuffer(nil)
	w.Format(buf)
	if buf.String() != "" {
		t.Errorf("w.Format(&Where{nil}: %q, want \"\"", buf.String())
	}
}

func TestIsAggregate(t *testing.T) {
	f := FuncExpr{Name: "avg"}
	if !f.IsAggregate() {
		t.Error("IsAggregate: false, want true")
	}

	f = FuncExpr{Name: "Avg"}
	if !f.IsAggregate() {
		t.Error("IsAggregate: false, want true")
	}

	f = FuncExpr{Name: "foo"}
	if f.IsAggregate() {
		t.Error("IsAggregate: true, want false")
	}
}

func TestColIdent(t *testing.T) {
	str := NewColIdent("Ab")
	if str.String() != "Ab" {
		t.Errorf("String=%s, want Ab", str.Original())
	}
	if str.Original() != "Ab" {
		t.Errorf("Val=%s, want Ab", str.Original())
	}
	if str.Lowered() != "ab" {
		t.Errorf("Val=%s, want ab", str.Lowered())
	}
	if !str.Equal(NewColIdent("aB")) {
		t.Error("str.Equal(NewColIdent(aB))=false, want true")
	}
	if !str.EqualString("ab") {
		t.Error("str.EqualString(ab)=false, want true")
	}
}

func TestHexDecode(t *testing.T) {
	testcase := []struct {
		in, out string
	}{{
		in:  "313233",
		out: "123",
	}, {
		in:  "ag",
		out: "encoding/hex: invalid byte: U+0067 'g'",
	}, {
		in:  "777",
		out: "encoding/hex: odd length hex string",
	}}
	for _, tc := range testcase {
		out, err := HexVal(tc.in).Decode()
		if err != nil {
			if err.Error() != tc.out {
				t.Errorf("Decode(%q): %v, want %s", tc.in, err, tc.out)
			}
			continue
		}
		if !bytes.Equal(out, []byte(tc.out)) {
			t.Errorf("Decode(%q): %s, want %s", tc.in, out, tc.out)
		}
	}
}
