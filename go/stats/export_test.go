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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clearStats() {
	defaultVarGroup.vars = make(map[string]expvar.Var)
	defaultVarGroup.newVarHook = nil
	combineDimensions = ""
	dropVariables = ""
	combinedDimensions = nil
	droppedVars = nil
}

func TestNoHook(t *testing.T) {
	clearStats()
	v := NewCounter("plainint", "help")
	v.Add(1)
	assert.Equal(t, "1", v.String())
}

func TestString(t *testing.T) {
	var gotname string
	var gotv *String
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*String)
	})
	v := NewString("String")
	assert.Equal(t, "String", gotname)
	assert.Same(t, v, gotv)
	v.Set("a\"b")
	assert.Equal(t, "a\"b", v.Get())
	assert.Equal(t, "\"a\\\"b\"", v.String())

	f := StringFunc(func() string {
		return "a"
	})
	assert.Equal(t, "\"a\"", f.String())
}

type Mystr string

func (m *Mystr) String() string {
	return string(*m)
}

func TestPublish(t *testing.T) {
	var gotname string
	var gotv expvar.Var
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Mystr)
	})
	v := Mystr("abcd")
	Publish("Mystr", &v)
	assert.Equal(t, "Mystr", gotname)
	assert.Same(t, &v, gotv)
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
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(expvarFunc)
	})
	publish("Myfunc", expvarFunc(f))
	assert.Equal(t, "Myfunc", gotname)
	assert.Equal(t, f(), gotv.String())
}

func TestDropVariable(t *testing.T) {
	clearStats()
	dropVariables = "dropTest"

	// This should not panic.
	_ = NewGaugesWithSingleLabel("dropTest", "help", "label")
	_ = NewGaugesWithSingleLabel("dropTest", "help", "label")
}

func TestStringMapToString(t *testing.T) {
	expected1 := "{\"aaa\": \"111\", \"bbb\": \"222\"}"
	expected2 := "{\"bbb\": \"222\", \"aaa\": \"111\"}"
	got := stringMapToString(map[string]string{"aaa": "111", "bbb": "222"})

	assert.Truef(t, got == expected1 || got == expected2, "expected %v or %v, got  %v", expected1, expected2, got)
}

func TestParseCommonTags(t *testing.T) {
	res := ParseCommonTags([]string{""})
	assert.Emptyf(t, res, "expected empty result, got %v", res)
	res = ParseCommonTags([]string{"s", "a:b"})
	assert.Equal(t, map[string]string{"a": "b"}, res)
	res = ParseCommonTags([]string{"a:b", "c:d"})
	assert.Equal(t, map[string]string{"a": "b", "c": "d"}, res)
}

func TestStringMapWithMultiLabels(t *testing.T) {
	clearStats()
	c := NewStringMapFuncWithMultiLabels("stringMap1", "help", []string{"aaa", "bbb"}, "ccc", func() map[string]string {
		m := make(map[string]string)
		m["c1a.c1b"] = "1"
		m["c2a.c2b"] = "1"
		return m
	})

	want1 := `{"c1a.c1b": "1", "c2a.c2b": "1"}`
	want2 := `{"c2a.c2b": "1", "c1a.c1b": "1"}`
	s := c.String()
	assert.Truef(t, s == want1 || s == want2, "want %s or %s, got %s", want1, want2, s)

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

func TestSafeJoinLabels(t *testing.T) {
	cases := []struct {
		labels   []string
		combined []bool
		want     string
	}{
		{
			labels: []string{},
			want:   "",
		},
		{
			labels: []string{"foo"},
			want:   "foo",
		},
		{
			labels: []string{"foo.bar"},
			want:   "foo_bar",
		},
		{
			labels:   []string{"foo"},
			combined: []bool{true},
			want:     "all",
		},
		{
			labels: []string{"foo", "bar"},
			want:   "foo.bar",
		},
		{
			labels:   []string{"foo", "bar"},
			combined: []bool{true, false},
			want:     "all.bar",
		},
		{
			labels:   []string{"foo", "bar"},
			combined: []bool{true, true},
			want:     "all.all",
		},
		{
			labels:   []string{"foo", "bar"},
			combined: []bool{false, true},
			want:     "foo.all",
		},
		{
			labels: []string{"foo.bar", "bar.baz"},
			want:   "foo_bar.bar_baz",
		},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.labels, ","), func(t *testing.T) {
			require.Equal(t, tc.want, safeJoinLabels(tc.labels, tc.combined))
		})
	}
}

func BenchmarkSafeJoinLabels(b *testing.B) {
	labels := [5]string{"foo:bar", "foo.bar", "c1a", "testing", "testing.a.b"}
	combined := [5]bool{true, true, true, true, true}
	b.Run("no combined", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = safeJoinLabels(labels[:], nil)
		}
	})
	b.Run("combined", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = safeJoinLabels(labels[:], combined[:])
		}
	})
}
