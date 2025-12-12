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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
		{P: `$.c[last to last]`},
		{P: `$.c[0 to 0]`},
		{P: `$.c[last - 23 to last]`, Want: `$.c[last-23 to last]`},
		{P: `$ . c [     last  -  23   to   last  -  444  ]`, Want: `$.c[last-23 to last-444]`},
		{P: `$.      "a fish"     `, Want: `$."a fish"`},
		{P: `$    [last - 3 to   last - 1`, Err: "Invalid JSON path expression. The error is around character position 28."},
	}

	for _, tc := range cases {
		t.Run(tc.P, func(t *testing.T) {
			var p PathParser
			jp, err := p.ParseBytes([]byte(tc.P))

			if tc.Err != "" {
				require.EqualError(t, err, tc.Err)
				return
			}

			require.NoError(t, err)

			want := tc.Want
			if want == "" {
				want = tc.P
			}

			require.Equal(t, want, jp.String())
		})
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
		{`true`, `$[0 to 0]`, []string{"true"}},
		{`true`, `$[0 to 1]`, []string{"true"}},
		{`true`, `$[1 to 2]`, nil},
		{`true`, `$[last to last]`, []string{"true"}},
		{`true`, `$[last-4 to last]`, []string{"true"}},
		{`true`, `$[last-4 to last-1]`, nil},
		{`true`, `$[1]`, nil},
		{`true`, `$[last-1]`, nil},
		{`[ { "a": 1 }, { "a": 2 } ]`, `$**[0]`, []string{`{"a": 1}`, `1`, `{"a": 2}`, `2`}},
		{`{ "a" : "foo", "b" : [ true, { "c" : 123, "c" : 456 } ] }`, `$.b[1].c`, []string{"456"}},
		{`{ "a" : "foo", "b" : [ true, { "c" : "123" } ] }`, `$.b[1].c`, []string{"\"123\""}},
		{`{ "a" : "foo", "b" : [ true, { "c" : 123 } ] }`, `$.b[1].c`, []string{"123"}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("'%s' -> '%s'", tc.J, tc.JP), func(t *testing.T) {
			var matched []string
			err := MatchPath([]byte(tc.J), []byte(tc.JP), func(value *Value) {
				if value == nil {
					return
				}
				matched = append(matched, string(value.MarshalTo(nil)))
			})

			require.NoError(t, err)
			require.Equal(t, tc.Expected, matched)
		})
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

	cases := map[string]struct {
		T        Transformation
		Document string
		Paths    []string
		Values   []string
		Expected string
	}{
		"set operation": {
			T:        Set,
			Document: Document1,
			Paths:    []string{Path1, Path2},
			Values:   []string{"1", "2"},
			Expected: `["a", {"b": [1, false]}, [10, 20, 2]]`,
		},
		"insert operation": {
			T:        Insert,
			Document: Document1,
			Paths:    []string{Path1, Path2},
			Values:   []string{"1", "2"},
			Expected: `["a", {"b": [true, false]}, [10, 20, 2]]`,
		},
		"replace operation": {
			T:        Replace,
			Document: Document1,
			Paths:    []string{Path1, Path2},
			Values:   []string{"1", "2"},
			Expected: `["a", {"b": [1, false]}, [10, 20]]`,
		},
		"remove operation": {
			T:        Remove,
			Document: Document1,
			Paths:    []string{`$[2]`, `$[1].b[1]`, `$[1].b[1]`},
			Expected: `["a", {"b": [true]}]`,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
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
			require.NoError(t, err)

			require.Equal(t, tc.Expected, string(doc.MarshalTo(nil)))
		})
	}
}
