// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import "testing"

func TestComments(t *testing.T) {
	var testCases = []struct {
		input, outSQL, outComments string
	}{{
		input:       "/",
		outSQL:      "/",
		outComments: "",
	}, {
		input:       "*/",
		outSQL:      "*/",
		outComments: "",
	}, {
		input:       "/*/",
		outSQL:      "/*/",
		outComments: "",
	}, {
		input:       "a*/",
		outSQL:      "a*/",
		outComments: "",
	}, {
		input:       "*a*/",
		outSQL:      "*a*/",
		outComments: "",
	}, {
		input:       "**a*/",
		outSQL:      "**a*/",
		outComments: "",
	}, {
		input:       "/*b**a*/",
		outSQL:      "",
		outComments: "/*b**a*/",
	}, {
		input:       "/*a*/",
		outSQL:      "",
		outComments: "/*a*/",
	}, {
		input:       "/**/",
		outSQL:      "",
		outComments: "/**/",
	}, {
		input:       "/*b*/ /*a*/",
		outSQL:      "",
		outComments: "/*b*/ /*a*/",
	}, {
		input:       "foo /* bar */",
		outSQL:      "foo",
		outComments: " /* bar */",
	}, {
		input:       "foo /** bar */",
		outSQL:      "foo",
		outComments: " /** bar */",
	}, {
		input:       "foo /*** bar */",
		outSQL:      "foo",
		outComments: " /*** bar */",
	}, {
		input:       "foo /** bar **/",
		outSQL:      "foo",
		outComments: " /** bar **/",
	}, {
		input:       "foo /*** bar ***/",
		outSQL:      "foo",
		outComments: " /*** bar ***/",
	}, {
		input:       "*** bar ***/",
		outSQL:      "*** bar ***/",
		outComments: "",
	}}
	for _, testCase := range testCases {
		gotSQL, gotComments := SplitTrailingComments(testCase.input)

		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
		if gotComments != testCase.outComments {
			t.Errorf("test input: '%s', got Comments\n%+v, want\n%+v", testCase.input, gotComments, testCase.outComments)
		}
	}
}
