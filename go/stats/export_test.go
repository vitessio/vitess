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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func clear() {
	defaultVarGroup.vars = make(map[string]expvar.Var)
	defaultVarGroup.newVarHook = nil
	combineDimensions = ""
	dropVariables = ""
	combinedDimensions = nil
	droppedVars = nil
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

func TestDropVariable(t *testing.T) {
	clear()
	dropVariables = "dropTest"

	// This should not panic.
	_ = NewGaugesWithSingleLabel("dropTest", "help", "label")
	_ = NewGaugesWithSingleLabel("dropTest", "help", "label")
}

func TestStringMapToString(t *testing.T) {
	expected1 := "{\"aaa\": \"111\", \"bbb\": \"222\"}"
	expected2 := "{\"bbb\": \"222\", \"aaa\": \"111\"}"
	got := stringMapToString(map[string]string{"aaa": "111", "bbb": "222"})

	if got != expected1 && got != expected2 {
		t.Errorf("expected %v or %v, got  %v", expected1, expected2, got)
	}
}

func TestParseCommonTags(t *testing.T) {
	res := ParseCommonTags([]string{""})
	if len(res) != 0 {
		t.Errorf("expected empty result, got %v", res)
	}
	res = ParseCommonTags([]string{"s", "a:b"})
	expected1 := map[string]string{"a": "b"}
	if !reflect.DeepEqual(expected1, res) {
		t.Errorf("expected %v, got %v", expected1, res)
	}
	res = ParseCommonTags([]string{"a:b", "c:d"})
	expected2 := map[string]string{"a": "b", "c": "d"}
	if !reflect.DeepEqual(expected2, res) {
		t.Errorf("expected %v, got %v", expected2, res)
	}
}

func TestStringMapWithMultiLabels(t *testing.T) {
	clear()
	c := NewStringMapFuncWithMultiLabels("stringMap1", "help", []string{"aaa", "bbb"}, "ccc", func() map[string]string {
		m := make(map[string]string)
		m["c1a.c1b"] = "1"
		m["c2a.c2b"] = "1"
		return m
	})

	want1 := `{"c1a.c1b": "1", "c2a.c2b": "1"}`
	want2 := `{"c2a.c2b": "1", "c1a.c1b": "1"}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}

	m := c.StringMapFunc()
	require.Len(t, m, 2)
	require.Contains(t, m, "c1a.c1b")
	require.Equal(t, m["c1a.c1b"], "1")
	require.Contains(t, m, "c2a.c2b")
	require.Equal(t, m["c2a.c2b"], "1")

	keyLabels := c.KeyLabels()
	require.Len(t, keyLabels, 2)
	require.Equal(t, keyLabels[0], "aaa")
	require.Equal(t, keyLabels[1], "bbb")

	require.Equal(t, c.ValueLabel(), "ccc")
}
