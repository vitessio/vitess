// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"reflect"
	"testing"
)

var testCases = []struct {
	input          string
	outSQL, outVar string
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
		bindVariables := make(map[string]interface{})
		gotSQL := stripTrailing(testCase.input, bindVariables)

		wantBindVariables := make(map[string]interface{})
		if testCase.outVar != "" {
			wantBindVariables[trailingComment] = testCase.outVar
		}

		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
		if !reflect.DeepEqual(bindVariables, wantBindVariables) {
			t.Errorf("test input: '%s', got bind variables\n%+v, want\n%+v", testCase.input, bindVariables, wantBindVariables)
		}
		sql := string(restoreTrailing([]byte(testCase.outSQL), wantBindVariables))
		if !reflect.DeepEqual(testCase.input, sql) {
			t.Fatalf("failed to restore to original sql, got: %s, want: %s", sql, testCase.input)
		}
	}
}
