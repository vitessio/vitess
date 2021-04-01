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

func TestBuildParsedQuery(t *testing.T) {
	testcases := []struct {
		in   string
		args []interface{}
		out  string
	}{{
		in:  "select * from tbl",
		out: "select * from tbl",
	}, {
		in:  "select * from tbl where b=4 or a=3",
		out: "select * from tbl where b=4 or a=3",
	}, {
		in:  "select * from tbl where b = 4 or a = 3",
		out: "select * from tbl where b = 4 or a = 3",
	}, {
		in:   "select * from tbl where name='%s'",
		args: []interface{}{"xyz"},
		out:  "select * from tbl where name='xyz'",
	}}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			parsed := BuildParsedQuery(tc.in, tc.args...)
			assert.Equal(t, tc.out, parsed.Query)
		})
	}
}
