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

package stats

import (
	"expvar"
	"testing"
)

func clear() {
	defaultVarGroup.vars = make(map[string]expvar.Var)
	defaultVarGroup.newVarHook = nil
}

func TestNoHook(t *testing.T) {
	clear()
	v := NewCounter("plainint", "help")
	v.Add(1)
	if v.String() != "1" {
		t.Errorf("want 1, got %s", v.String())
	}
}

func TestString(t *testing.T) {
	var gotname string
	var gotv *String
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*String)
	})
	v := NewString("String")
	if gotname != "String" {
		t.Errorf("want String, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	v.Set("a\"b")
	if v.Get() != "a\"b" {
		t.Errorf("want \"a\"b\", got %#v", gotv)
	}
	if v.String() != "\"a\\\"b\"" {
		t.Errorf("want \"\"a\\\"b\"\", got %#v", gotv)
	}

	f := StringFunc(func() string {
		return "a"
	})
	if f.String() != "\"a\"" {
		t.Errorf("want \"a\", got %v", f.String())
	}
}

type Mystr string

func (m *Mystr) String() string {
	return string(*m)
}

func TestPublish(t *testing.T) {
	var gotname string
	var gotv expvar.Var
	clear()
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

type expvarFunc func() string

func (f expvarFunc) String() string {
	return f()
}

func TestPublishFunc(t *testing.T) {
	var gotname string
	var gotv expvarFunc
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(expvarFunc)
	})
	publish("Myfunc", expvarFunc(f))
	if gotname != "Myfunc" {
		t.Errorf("want Myfunc, got %s", gotname)
	}
	if gotv.String() != f() {
		t.Errorf("want %v, got %#v", f(), gotv())
	}
}
