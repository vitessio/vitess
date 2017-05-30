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
