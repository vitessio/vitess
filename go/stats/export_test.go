// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
)

func TestNoHook(t *testing.T) {
	Register(nil)
	v := NewFloat("plainfloat")
	v.Set(1.0)
	if v.String() != "1" {
		t.Errorf("want 1, got %s", v.String())
	}
}

func TestFloatHook(t *testing.T) {
	var gotname string
	var gotv *expvar.Float
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*expvar.Float)
	})
	v := NewFloat("Float")
	if gotname != "Float" {
		t.Errorf("want Float, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}

func TestIntHook(t *testing.T) {
	var gotname string
	var gotv *expvar.Int
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*expvar.Int)
	})
	v := NewInt("Int")
	if gotname != "Int" {
		t.Errorf("want Int, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}

func TestMapHook(t *testing.T) {
	var gotname string
	var gotv *expvar.Map
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*expvar.Map)
	})
	v := NewMap("Map")
	if gotname != "Map" {
		t.Errorf("want Map, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}

func TestStringHook(t *testing.T) {
	var gotname string
	var gotv *expvar.String
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*expvar.String)
	})
	v := NewString("String")
	if gotname != "String" {
		t.Errorf("want String, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}

type Mystr string

func (m *Mystr) String() string {
	return string(*m)
}

func TestPublishHook(t *testing.T) {
	var gotname string
	var gotv expvar.Var
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Mystr)
	})
	v := Mystr("abcd")
	Publish("Mystr", &v)
	if gotname != "Mystr" {
		t.Errorf("want Mystr, got %s", gotname)
	}
	if gotv != &v {
		t.Errorf("want %#v, got %#v", &v, gotv)
	}
}

func f() string {
	return "abcd"
}

func TestPublishFunc(t *testing.T) {
	var gotname string
	var gotv strFunc
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(strFunc)
	})
	PublishFunc("Myfunc", f)
	if gotname != "Myfunc" {
		t.Errorf("want Myfunc, got %s", gotname)
	}
	if gotv.String() != f() {
		t.Errorf("want %v, got %#v", f(), gotv())
	}
}
