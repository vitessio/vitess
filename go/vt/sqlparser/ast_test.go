// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"unsafe"
)

func TestAppend(t *testing.T) {
	query := "select * from t where a = 1"
	tree, err := Parse(query)
	if err != nil {
		t.Error(err)
	}
	var b bytes.Buffer
	Append(&b, tree)
	got := b.String()
	want := query
	if got != want {
		t.Errorf("Append: %s, want %s", got, want)
	}
	Append(&b, tree)
	got = b.String()
	want = query + query
	if got != want {
		t.Errorf("Append: %s, want %s", got, want)
	}
}

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

func TestAddOrder(t *testing.T) {
	src, err := Parse("select foo, bar from baz order by foo")
	if err != nil {
		t.Error(err)
	}
	order := src.(*Select).OrderBy[0]
	dst, err := Parse("select * from t")
	if err != nil {
		t.Error(err)
	}
	dst.(*Select).AddOrder(order)
	buf := NewTrackedBuffer(nil)
	dst.Format(buf)
	want := "select * from t order by foo asc"
	if buf.String() != want {
		t.Errorf("order: %q, want %s", buf.String(), want)
	}
	dst, err = Parse("select * from t union select * from s")
	if err != nil {
		t.Error(err)
	}
	dst.(*Union).AddOrder(order)
	buf = NewTrackedBuffer(nil)
	dst.Format(buf)
	want = "select * from t union select * from s order by foo asc"
	if buf.String() != want {
		t.Errorf("order: %q, want %s", buf.String(), want)
	}
}

func TestSetLimit(t *testing.T) {
	src, err := Parse("select foo, bar from baz limit 4")
	if err != nil {
		t.Error(err)
	}
	limit := src.(*Select).Limit
	dst, err := Parse("select * from t")
	if err != nil {
		t.Error(err)
	}
	dst.(*Select).SetLimit(limit)
	buf := NewTrackedBuffer(nil)
	dst.Format(buf)
	want := "select * from t limit 4"
	if buf.String() != want {
		t.Errorf("limit: %q, want %s", buf.String(), want)
	}
	dst, err = Parse("select * from t union select * from s")
	if err != nil {
		t.Error(err)
	}
	dst.(*Union).SetLimit(limit)
	buf = NewTrackedBuffer(nil)
	dst.Format(buf)
	want = "select * from t union select * from s limit 4"
	if buf.String() != want {
		t.Errorf("order: %q, want %s", buf.String(), want)
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
	f := FuncExpr{Name: NewColIdent("avg")}
	if !f.IsAggregate() {
		t.Error("IsAggregate: false, want true")
	}

	f = FuncExpr{Name: NewColIdent("Avg")}
	if !f.IsAggregate() {
		t.Error("IsAggregate: false, want true")
	}

	f = FuncExpr{Name: NewColIdent("foo")}
	if f.IsAggregate() {
		t.Error("IsAggregate: true, want false")
	}
}

func TestColNameEqual(t *testing.T) {
	var c1, c2 *ColName
	if c1.Equal(c2) {
		t.Error("nil columns equal, want unequal")
	}
	c1 = &ColName{
		Name: NewColIdent("aa"),
	}
	c2 = &ColName{
		Name: NewColIdent("bb"),
	}
	if c1.Equal(c2) {
		t.Error("columns equal, want unequal")
	}
	c2.Name = NewColIdent("aa")
	if !c1.Equal(c2) {
		t.Error("columns unequal, want equal")
	}
}

func TestTableNameEqual(t *testing.T) {
	var t1, t2 *TableName
	if !t1.Equal(t2) {
		t.Error("nil tables unequal, want equal")
	}
	t2 = &TableName{}
	if !t1.Equal(t2) {
		t.Error("nil and empty table unequal, want equal")
	}
	if !t2.Equal(t1) {
		t.Error("empty and nil table unequal, want equal")
	}
	t1 = &TableName{}
	if !t1.Equal(t2) {
		t.Error("empty and empty table unequal, want equal")
	}
	t2 = &TableName{
		Qualifier: NewTableIdent("aa"),
		Name:      NewTableIdent("bb"),
	}
	if t1.Equal(t2) {
		t.Error("empty and non-empty table equal, want unequal")
	}
	if t2.Equal(t1) {
		t.Error("non-empty and empty table equal, want unequal")
	}
	t1 = &TableName{
		Qualifier: NewTableIdent("bb"),
		Name:      NewTableIdent("bb"),
	}
	if t1.Equal(t2) {
		t.Error("non-empty and non-empty table equal, want unequal")
	}
	t1.Qualifier = NewTableIdent("aa")
	if !t1.Equal(t2) {
		t.Error("tables are unequal, want equal")
	}
}

func TestColIdent(t *testing.T) {
	str := NewColIdent("Ab")
	if str.String() != "Ab" {
		t.Errorf("String=%s, want Ab", str.String())
	}
	if str.String() != "Ab" {
		t.Errorf("Val=%s, want Ab", str.String())
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
	str = NewColIdent("")
	if str.Lowered() != "" {
		t.Errorf("Val=%s, want \"\"", str.Lowered())
	}
}

func TestColIdentMarshal(t *testing.T) {
	str := NewColIdent("Ab")
	b, err := json.Marshal(str)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `"Ab"`
	if got != want {
		t.Errorf("json.Marshal()= %s, want %s", got, want)
	}
	var out ColIdent
	err = json.Unmarshal(b, &out)
	if !reflect.DeepEqual(out, str) {
		t.Errorf("Unmarshal: %v, want %v", out, str)
	}
}

func TestColIdentSize(t *testing.T) {
	size := unsafe.Sizeof(NewColIdent(""))
	want := 2 * unsafe.Sizeof("")
	if size != want {
		t.Errorf("Size of ColIdent: %d, want 32", want)
	}
}

func TestTableIdentMarshal(t *testing.T) {
	str := NewTableIdent("Ab")
	b, err := json.Marshal(str)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `"Ab"`
	if got != want {
		t.Errorf("json.Marshal()= %s, want %s", got, want)
	}
	var out TableIdent
	err = json.Unmarshal(b, &out)
	if !reflect.DeepEqual(out, str) {
		t.Errorf("Unmarshal: %v, want %v", out, str)
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
		out, err := newHexVal(tc.in).HexDecode()
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

func TestCompliantName(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "aa",
		out: "aa",
	}, {
		in:  "1a",
		out: "_a",
	}, {
		in:  "a1",
		out: "a1",
	}, {
		in:  "a.b",
		out: "a_b",
	}, {
		in:  ".ab",
		out: "_ab",
	}}
	for _, tc := range testcases {
		out := NewColIdent(tc.in).CompliantName()
		if out != tc.out {
			t.Errorf("ColIdent(%s).CompliantNamt: %s, want %s", tc.in, out, tc.out)
		}
		out = NewTableIdent(tc.in).CompliantName()
		if out != tc.out {
			t.Errorf("TableIdent(%s).CompliantNamt: %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestEscape(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "aa",
		out: "`aa`",
	}, {
		in:  "a`a",
		out: "`a``a`",
	}}
	for _, tc := range testcases {
		out := Backtick(tc.in)
		if out != tc.out {
			t.Errorf("Escape(%s): %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestColumns_FindColumn(t *testing.T) {
	cols := Columns{NewColIdent("a"), NewColIdent("c"), NewColIdent("b"), NewColIdent("0")}

	testcases := []struct {
		in  string
		out int
	}{{
		in:  "a",
		out: 0,
	}, {
		in:  "b",
		out: 2,
	},
		{
			in:  "0",
			out: 3,
		},
		{
			in:  "f",
			out: -1,
		}}

	for _, tc := range testcases {
		val := cols.FindColumn(NewColIdent(tc.in))
		if val != tc.out {
			t.Errorf("FindColumn(%s): %d, want %d", tc.in, val, tc.out)
		}
	}
}
