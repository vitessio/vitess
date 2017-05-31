/*
Copyright 2017 Google Inc.

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
	"time"
)

func clear() {
	defaultVarGroup.vars = make(map[string]expvar.Var)
	defaultVarGroup.newVarHook = nil
}

func TestNoHook(t *testing.T) {
	clear()
	v := NewInt("plainint")
	v.Set(1)
	if v.String() != "1" {
		t.Errorf("want 1, got %s", v.String())
	}
}

func TestFloat(t *testing.T) {
	var gotname string
	var gotv *Float
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Float)
	})
	v := NewFloat("Float")
	if gotname != "Float" {
		t.Errorf("want Float, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	v.Set(5.1)
	if v.Get() != 5.1 {
		t.Errorf("want 5.1, got %v", v.Get())
	}
	v.Add(1.0)
	if v.Get() != 6.1 {
		t.Errorf("want 6.1, got %v", v.Get())
	}
	if v.String() != "6.1" {
		t.Errorf("want 6.1, got %v", v.Get())
	}

	f := FloatFunc(func() float64 {
		return 1.234
	})
	if f.String() != "1.234" {
		t.Errorf("want 1.234, got %v", f.String())
	}
}

func TestInt(t *testing.T) {
	var gotname string
	var gotv *Int
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Int)
	})
	v := NewInt("Int")
	if gotname != "Int" {
		t.Errorf("want Int, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	v.Set(5)
	if v.Get() != 5 {
		t.Errorf("want 5, got %v", v.Get())
	}
	v.Add(1)
	if v.Get() != 6 {
		t.Errorf("want 6, got %v", v.Get())
	}
	if v.String() != "6" {
		t.Errorf("want 6, got %v", v.Get())
	}

	f := IntFunc(func() int64 {
		return 1
	})
	if f.String() != "1" {
		t.Errorf("want 1, got %v", f.String())
	}
}

func TestDuration(t *testing.T) {
	var gotname string
	var gotv *Duration
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Duration)
	})
	v := NewDuration("Duration")
	if gotname != "Duration" {
		t.Errorf("want Duration, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	v.Set(time.Duration(5))
	if v.Get() != 5 {
		t.Errorf("want 5, got %v", v.Get())
	}
	v.Add(time.Duration(1))
	if v.Get() != 6 {
		t.Errorf("want 6, got %v", v.Get())
	}
	if v.String() != "6" {
		t.Errorf("want 6, got %v", v.Get())
	}

	f := DurationFunc(func() time.Duration {
		return time.Duration(1)
	})
	if f.String() != "1" {
		t.Errorf("want 1, got %v", f.String())
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

func TestPublishFunc(t *testing.T) {
	var gotname string
	var gotv JSONFunc
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(JSONFunc)
	})
	PublishJSONFunc("Myfunc", f)
	if gotname != "Myfunc" {
		t.Errorf("want Myfunc, got %s", gotname)
	}
	if gotv.String() != f() {
		t.Errorf("want %v, got %#v", f(), gotv())
	}
}

func TestStringMap(t *testing.T) {
	clear()
	c := NewStringMap("stringmap1")
	c.Set("c1", "val1")
	c.Set("c2", "val2")
	c.Set("c2", "val3")
	want1 := `{"c1": "val1", "c2": "val3"}`
	want2 := `{"c2": "val3", "c1": "val1"}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}

	f := StringMapFunc(func() map[string]string {
		return map[string]string{
			"c1": "val1",
			"c2": "val3",
		}
	})
	if s := f.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
}
