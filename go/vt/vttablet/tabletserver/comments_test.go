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
