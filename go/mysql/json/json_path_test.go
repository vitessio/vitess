/*
Copyright 2023 The Vitess Authors.

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

package json

import (
	"testing"

	"golang.org/x/exp/slices"
)

func TestParseJSONPath(t *testing.T) {
	cases := []struct {
		P    string
		Want string
		Err  string
	}{
		{P: `$[1].b[0]`},
		{P: `$[2][2]`},
		{P: `$**.b`},
		{P: `$.c[*]`},
		{P: `$.*`},
		{P: `$.c[23 to 444]`},
		{P: `$."a fish"`},
		{P: `$."a \"fish\""`},
		{P: `$.c[last-23 to last-444]`},
		{P: `$.c[last-23 to last]`},
		{P: `$.c[last]`},
		{P: `$.c[last - 23 to last]`, Want: `$.c[last-23 to last]`},
		{P: `$ . c [     last  -  23   to   last  -  444  ]`, Want: `$.c[last-23 to last-444]`},
		{P: `$.      "a fish"     `, Want: `$."a fish"`},
		{P: `$    [last - 3 to   last - 1`, Err: "Invalid JSON path expression. The error is around character position 28."},
	}

	for _, tc := range cases {
		var p PathParser
		jp, err := p.ParseBytes([]byte(tc.P))
		if tc.Err == "" {
			if err != nil {
				t.Fatalf("failed to parse '%s': %v", tc.P, err)
			}
		} else {
			if err == nil {
				t.Fatalf("bad parse for '%s': expected an error", tc.P)
			} else if err.Error() != tc.Err {
				t.Fatalf("bad parse for '%s': expected err='%s', got err='%s'", tc.P, tc.Err, err)
			}
			continue
		}
		want := tc.Want
		if want == "" {
			want = tc.P
		}
		got := jp.String()
		if got != want {
			t.Fatalf("bad parse for '%s': want '%s', got '%s'", tc.P, want, got)
		}
	}
}

func Test1(t *testing.T) {
	var p PathParser
	_, _ = p.ParseBytes([]byte(`$.c[23 to 444]`))
}

func TestJSONExtract(t *testing.T) {
	cases := []struct {
		J        string
		JP       string
		Expected []string
	}{
		{`{"id": 14, "name": "Aztalan", "foobar" : [1, 2, 3]}`, `$.name`, []string{`"Aztalan"`}},
		{`{"a": {"b": 1}, "c": {"b": 2}}`, `$**.b`, []string{"1", "2"}},
		{`[1, 2, 3, 4, 5]`, `$[1 to 3]`, []string{"2", "3", "4"}},
		{`[1, 2, 3, 4, 5]`, `$[last-3 to last-1]`, []string{"2", "3", "4"}},
		{`{"a": 1, "b": 2, "c": [3, 4, 5]}`, `$.*`, []string{"1", "2", "[3, 4, 5]"}},
		{`true`, `$[0]`, []string{"true"}},
		{`true`, `$[last]`, []string{"true"}},
		{`true`, `$[1]`, []string{}},
		{`true`, `$[last-1]`, []string{}},
		{`[ { "a": 1 }, { "a": 2 } ]`, `$**[0]`, []string{`{"a": 1}`, `1`, `{"a": 2}`, `2`}},
		{`{ "a" : "foo", "b" : [ true, { "c" : 123, "c" : 456 } ] }`, `$.b[1].c`, []string{"456"}},
		{`{ "a" : "foo", "b" : [ true, { "c" : "123" } ] }`, `$.b[1].c`, []string{"\"123\""}},
		{`{ "a" : "foo", "b" : [ true, { "c" : 123 } ] }`, `$.b[1].c`, []string{"123"}},
	}

	for _, tc := range cases {
		var matched []string
		err := MatchPath([]byte(tc.J), []byte(tc.JP), func(value *Value) {
			if value == nil {
				return
			}
			matched = append(matched, string(value.MarshalTo(nil)))
		})
		if err != nil {
			t.Errorf("failed to match '%s'->'%s': %v", tc.J, tc.JP, err)
			continue
		}
		if !slices.Equal(tc.Expected, matched) {
			t.Errorf("'%s'->'%s' = %v (expected %v)", tc.J, tc.JP, matched, tc.Expected)
		}
	}
}

func json(t *testing.T, raw string) *Value {
	var p Parser
	v, err := p.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func path(t *testing.T, raw string) *Path {
	var p PathParser
	v, err := p.ParseBytes([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestTransformations(t *testing.T) {
	const Document1 = `["a", {"b": [true, false]}, [10, 20]]`
	const Path1 = `$[1].b[0]`
	const Path2 = `$[2][2]`

	cases := []struct {
		T        Transformation
		Document string
		Paths    []string
		Values   []string
		Expected string
	}{
		{
			T:        Set,
			Document: Document1,
			Paths:    []string{Path1, Path2},
			Values:   []string{"1", "2"},
			Expected: `["a", {"b": [1, false]}, [10, 20, 2]]`,
		},
		{
			T:        Insert,
			Document: Document1,
			Paths:    []string{Path1, Path2},
			Values:   []string{"1", "2"},
			Expected: `["a", {"b": [true, false]}, [10, 20, 2]]`,
		},
		{
			T:        Replace,
			Document: Document1,
			Paths:    []string{Path1, Path2},
			Values:   []string{"1", "2"},
			Expected: `["a", {"b": [1, false]}, [10, 20]]`,
		},
		{
			T:        Remove,
			Document: Document1,
			Paths:    []string{`$[2]`, `$[1].b[1]`, `$[1].b[1]`},
			Expected: `["a", {"b": [true]}]`,
		},
	}

	for _, tc := range cases {
		doc := json(t, tc.Document)

		var paths []*Path
		for _, p := range tc.Paths {
			paths = append(paths, path(t, p))
		}

		var values []*Value
		for _, v := range tc.Values {
			values = append(values, json(t, v))
		}

		err := ApplyTransform(tc.T, doc, paths, values)
		if err != nil {
			t.Fatal(err)
		}

		result := string(doc.MarshalTo(nil))
		if result != tc.Expected {
			t.Errorf("bad transformation (%v)\nwant: %s\ngot:  %s", tc.T, tc.Expected, result)
		}
	}
}
