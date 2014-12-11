// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"reflect"
	"testing"

	"github.com/henryanand/vitess/go/vt/tabletserver/proto"
)

var testCases = []struct {
	input          string
	outSql, outVar string
}{{
	"/",
	"/", "",
}, {
	"*/",
	"*/", "",
}, {
	"/*/",
	"/*/", "",
}, {
	"a*/",
	"a*/", "",
}, {
	"*a*/",
	"*a*/", "",
}, {
	"**a*/",
	"**a*/", "",
}, {
	"/*b**a*/",
	"", "/*b**a*/",
}, {
	"/*a*/",
	"", "/*a*/",
}, {
	"/**/",
	"", "/**/",
}, {
	"/*b*/ /*a*/",
	"", "/*b*/ /*a*/",
}, {
	"foo /* bar */",
	"foo", " /* bar */",
}, {
	"foo /** bar */",
	"foo", " /** bar */",
}, {
	"foo /*** bar */",
	"foo", " /*** bar */",
}, {
	"foo /** bar **/",
	"foo", " /** bar **/",
}, {
	"foo /*** bar ***/",
	"foo", " /*** bar ***/",
}, {
	"*** bar ***/",
	"*** bar ***/", "",
}}

func TestComments(t *testing.T) {
	for _, testCase := range testCases {
		query := proto.Query{
			Sql:           testCase.input,
			BindVariables: make(map[string]interface{}),
		}

		stripTrailing(&query)

		want := proto.Query{
			Sql: testCase.outSql,
		}
		want.BindVariables = make(map[string]interface{})
		if testCase.outVar != "" {
			want.BindVariables[TRAILING_COMMENT] = testCase.outVar
		}
		if !reflect.DeepEqual(query, want) {
			t.Errorf("test input: '%s', got\n%+v, want\n%+v", testCase.input, query, want)
		}
	}
}
