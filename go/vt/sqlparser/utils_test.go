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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeAlphabetically(t *testing.T) {
	testcases := []struct {
		in  string
		out string
	}{{
		in:  "select * from tbl",
		out: "select * from tbl",
	}, {
		in:  "select * from tbl where a=3",
		out: "select * from tbl where a = 3",
	}, {
		in:  "select * from tbl where a=3 and b=4",
		out: "select * from tbl where a = 3 and b = 4",
	}, {
		in:  "select * from tbl where b=4 and a=3",
		out: "select * from tbl where a = 3 and b = 4",
	}, {
		in:  "select * from tbl where b=4 and c>5 and a=3",
		out: "select * from tbl where a = 3 and b = 4 and c > 5",
	}, {
		in:  "select * from tbl where b=4 or a=3",
		out: "select * from tbl where b = 4 or a = 3",
	}}

	for _, tc := range testcases {
		normalized, err := NormalizeAlphabetically(tc.in)
		assert.NoError(t, err)
		assert.Equal(t, tc.out, normalized)
	}
}

func TestQueryMatchesTemplates(t *testing.T) {
	testcases := []struct {
		q    string
		tmpl []string
		out  bool
	}{{
		q: "select id from tbl",
		tmpl: []string{
			"select id from tbl",
		},
		out: true,
	}, {
		q: "select id from tbl",
		tmpl: []string{
			"select name from tbl",
			"select id from tbl",
		},
		out: true,
	}, {
		q: "select id from tbl where a=3",
		tmpl: []string{
			"select id from tbl",
		},
		out: false,
	}, {
		// int value
		q: "select id from tbl where a=3",
		tmpl: []string{
			"select name from tbl where a=17",
			"select id from tbl where a=5",
		},
		out: true,
	}, {
		// string value
		q: "select id from tbl where a='abc'",
		tmpl: []string{
			"select name from tbl where a='x'",
			"select id from tbl where a='y'",
		},
		out: true,
	}, {
		// two params
		q: "select id from tbl where a='abc' and b='def'",
		tmpl: []string{
			"select name from tbl where a='x' and b = 'y'",
			"select id from tbl where a='x' and b = 'y'",
		},
		out: true,
	}, {
		// no match
		q: "select id from tbl where a='abc' and b='def'",
		tmpl: []string{
			"select name from tbl where a='x' and b = 'y'",
			"select id from tbl where a='x' and c = 'y'",
		},
		out: false,
	}, {
		// reorder AND params
		q: "select id from tbl where a='abc' and b='def'",
		tmpl: []string{
			"select id from tbl where b='x' and a = 'y'",
		},
		out: true,
	}, {
		// no reorder OR params
		q: "select id from tbl where a='abc' or b='def'",
		tmpl: []string{
			"select id from tbl where b='x' or a = 'y'",
		},
		out: false,
	}, {
		// strict reorder OR params
		q: "select id from tbl where a='abc' or b='def'",
		tmpl: []string{
			"select id from tbl where a='x' or b = 'y'",
		},
		out: true,
	}, {
		// reorder AND params, range test
		q: "select id from tbl where a >'abc' and b<3",
		tmpl: []string{
			"select id from tbl where b<17 and a > 'y'",
		},
		out: true,
	}}
	for _, tc := range testcases {
		match, err := QueryMatchesTemplates(tc.q, tc.tmpl)
		assert.NoError(t, err)
		assert.Equal(t, tc.out, match)
	}
}
