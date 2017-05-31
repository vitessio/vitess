/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletserver

import (
	"reflect"
	"testing"
)

func TestComments(t *testing.T) {
	testCases := []struct {
		input          string
		outSQL, outVar string
	}{{
		input:  "/",
		outSQL: "/",
		outVar: "",
	}, {
		input:  "foo /* bar */",
		outSQL: "foo",
		outVar: " /* bar */",
	}}
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
