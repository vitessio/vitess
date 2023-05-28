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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestSplitComments(t *testing.T) {
	var testCases = []struct {
		input, outSQL, outLeadingComments, outTrailingComments string
	}{{
		input:               "/",
		outSQL:              "/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "*/",
		outSQL:              "*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*/",
		outSQL:              "/*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "a*/",
		outSQL:              "a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "*a*/",
		outSQL:              "*a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "**a*/",
		outSQL:              "**a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*b**a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*b**a*/",
	}, {
		input:               "/*a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*a*/",
	}, {
		input:               "/**/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/**/",
	}, {
		input:               "/*b*/ /*a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*b*/ /*a*/",
	}, {
		input:               "/* before */ foo /* bar */",
		outSQL:              "foo",
		outLeadingComments:  "/* before */ ",
		outTrailingComments: " /* bar */",
	}, {
		input:               "/* before1 */ /* before2 */ foo /* after1 */ /* after2 */",
		outSQL:              "foo",
		outLeadingComments:  "/* before1 */ /* before2 */ ",
		outTrailingComments: " /* after1 */ /* after2 */",
	}, {
		input:               "/** before */ foo /** bar */",
		outSQL:              "foo",
		outLeadingComments:  "/** before */ ",
		outTrailingComments: " /** bar */",
	}, {
		input:               "/*** before */ foo /*** bar */",
		outSQL:              "foo",
		outLeadingComments:  "/*** before */ ",
		outTrailingComments: " /*** bar */",
	}, {
		input:               "/** before **/ foo /** bar **/",
		outSQL:              "foo",
		outLeadingComments:  "/** before **/ ",
		outTrailingComments: " /** bar **/",
	}, {
		input:               "/*** before ***/ foo /*** bar ***/",
		outSQL:              "foo",
		outLeadingComments:  "/*** before ***/ ",
		outTrailingComments: " /*** bar ***/",
	}, {
		input:               " /*** before ***/ foo /*** bar ***/ ",
		outSQL:              "foo",
		outLeadingComments:  "/*** before ***/ ",
		outTrailingComments: " /*** bar ***/",
	}, {
		input:               "*** bar ***/",
		outSQL:              "*** bar ***/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               " foo ",
		outSQL:              "foo",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "select 1 from t where col = '*//*'",
		outSQL:              "select 1 from t where col = '*//*'",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*! select 1 */",
		outSQL:              "/*! select 1 */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}}
	for _, testCase := range testCases {
		t.Run(testCase.input, func(t *testing.T) {
			gotSQL, gotComments := SplitMarginComments(testCase.input)
			gotLeadingComments, gotTrailingComments := gotComments.Leading, gotComments.Trailing

			if gotSQL != testCase.outSQL {
				t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
			}
			if gotLeadingComments != testCase.outLeadingComments {
				t.Errorf("test input: '%s', got LeadingComments\n%+v, want\n%+v", testCase.input, gotLeadingComments, testCase.outLeadingComments)
			}
			if gotTrailingComments != testCase.outTrailingComments {
				t.Errorf("test input: '%s', got TrailingComments\n%+v, want\n%+v", testCase.input, gotTrailingComments, testCase.outTrailingComments)
			}
		})
	}
}

func TestStripLeadingComments(t *testing.T) {
	var testCases = []struct {
		input, outSQL string
	}{{
		input:  "/",
		outSQL: "/",
	}, {
		input:  "*/",
		outSQL: "*/",
	}, {
		input:  "/*/",
		outSQL: "/*/",
	}, {
		input:  "/*a",
		outSQL: "/*a",
	}, {
		input:  "/*a*",
		outSQL: "/*a*",
	}, {
		input:  "/*a**",
		outSQL: "/*a**",
	}, {
		input:  "/*b**a*/",
		outSQL: "",
	}, {
		input:  "/*a*/",
		outSQL: "",
	}, {
		input:  "/**/",
		outSQL: "",
	}, {
		input:  "/*!*/",
		outSQL: "/*!*/",
	}, {
		input:  "/*!a*/",
		outSQL: "/*!a*/",
	}, {
		input:  "/*b*/ /*a*/",
		outSQL: "",
	}, {
		input: `/*b*/ --foo
bar`,
		outSQL: "bar",
	}, {
		input:  "foo /* bar */",
		outSQL: "foo /* bar */",
	}, {
		input:  "/* foo */ bar",
		outSQL: "bar",
	}, {
		input:  "-- /* foo */ bar",
		outSQL: "",
	}, {
		input:  "foo -- bar */",
		outSQL: "foo -- bar */",
	}, {
		input: `/*
foo */ bar`,
		outSQL: "bar",
	}, {
		input: `-- foo bar
a`,
		outSQL: "a",
	}, {
		input:  `-- foo bar`,
		outSQL: "",
	}}
	for _, testCase := range testCases {
		gotSQL := StripLeadingComments(testCase.input)

		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
	}
}

func TestExtractMysqlComment(t *testing.T) {
	var testCases = []struct {
		input, outSQL, outVersion string
	}{{
		input:      "/*!50708SET max_execution_time=5000 */",
		outSQL:     "SET max_execution_time=5000",
		outVersion: "50708",
	}, {
		input:      "/*!50708 SET max_execution_time=5000*/",
		outSQL:     "SET max_execution_time=5000",
		outVersion: "50708",
	}, {
		input:      "/*!50708* from*/",
		outSQL:     "* from",
		outVersion: "50708",
	}, {
		input:      "/*! SET max_execution_time=5000*/",
		outSQL:     "SET max_execution_time=5000",
		outVersion: "",
	}}
	for _, testCase := range testCases {
		gotVersion, gotSQL := ExtractMysqlComment(testCase.input)

		if gotVersion != testCase.outVersion {
			t.Errorf("test input: '%s', got version\n%+v, want\n%+v", testCase.input, gotVersion, testCase.outVersion)
		}
		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
	}
}

func TestExtractCommentDirectives(t *testing.T) {
	var testCases = []struct {
		input string
		vals  map[string]string
	}{{
		input: "",
		vals:  nil,
	}, {
		input: "/* not a vt comment */",
		vals:  map[string]string{},
	}, {
		input: "/*vt+ */",
		vals:  map[string]string{},
	}, {
		input: "/*vt+ SINGLE_OPTION */",
		vals: map[string]string{
			"single_option": "true",
		},
	}, {
		input: "/*vt+ ONE_OPT TWO_OPT */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "true",
		},
	}, {
		input: "/*vt+ ONE_OPT */ /* other comment */ /*vt+ TWO_OPT */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "true",
		},
	}, {
		input: "/*vt+ ONE_OPT=abc TWO_OPT=def */",
		vals: map[string]string{
			"one_opt": "abc",
			"two_opt": "def",
		},
	}, {
		input: "/*vt+ ONE_OPT=true TWO_OPT=false */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "false",
		},
	}, {
		input: "/*vt+ ONE_OPT=true TWO_OPT=\"false\" */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "\"false\"",
		},
	}, {
		input: "/*vt+ RANGE_OPT=[a:b] ANOTHER ANOTHER_WITH_VALEQ=val= AND_ONE_WITH_EQ== */",
		vals: map[string]string{
			"range_opt":          "[a:b]",
			"another":            "true",
			"another_with_valeq": "val=",
			"and_one_with_eq":    "=",
		},
	}}

	for _, testCase := range testCases {
		t.Run(testCase.input, func(t *testing.T) {
			sqls := []string{
				"select " + testCase.input + " 1 from dual",
				"update " + testCase.input + " t set i=i+1",
				"delete " + testCase.input + " from t where id>1",
				"drop " + testCase.input + " table t",
				"create " + testCase.input + " table if not exists t (id int primary key)",
				"alter " + testCase.input + " table t add column c int not null",
				"create " + testCase.input + " view v as select * from t",
				"create " + testCase.input + " or replace view v as select * from t",
				"alter " + testCase.input + " view v as select * from t",
				"drop " + testCase.input + " view v",
			}
			for _, sql := range sqls {
				t.Run(sql, func(t *testing.T) {
					var comments *ParsedComments
					stmt, _ := Parse(sql)
					switch s := stmt.(type) {
					case *Select:
						comments = s.Comments
					case *Update:
						comments = s.Comments
					case *Delete:
						comments = s.Comments
					case *DropTable:
						comments = s.Comments
					case *AlterTable:
						comments = s.Comments
					case *CreateTable:
						comments = s.Comments
					case *CreateView:
						comments = s.Comments
					case *AlterView:
						comments = s.Comments
					case *DropView:
						comments = s.Comments
					default:
						t.Errorf("Unexpected statement type %+v", s)
					}

					vals := comments.Directives()
					if vals == nil {
						require.Nil(t, vals)
						return
					}
					if !reflect.DeepEqual(vals.m, testCase.vals) {
						t.Errorf("test input: '%v', got vals %T:\n%+v, want %T\n%+v", testCase.input, vals, vals, testCase.vals, testCase.vals)
					}
				})
			}
		})
	}

	d := &CommentDirectives{m: map[string]string{
		"one_opt": "true",
		"two_opt": "false",
		"three":   "1",
		"four":    "2",
		"five":    "0",
		"six":     "true",
	}}

	assert.True(t, d.IsSet("ONE_OPT"), "d.IsSet(ONE_OPT)")
	assert.False(t, d.IsSet("TWO_OPT"), "d.IsSet(TWO_OPT)")
	assert.True(t, d.IsSet("three"), "d.IsSet(three)")
	assert.False(t, d.IsSet("four"), "d.IsSet(four)")
	assert.False(t, d.IsSet("five"), "d.IsSet(five)")
	assert.True(t, d.IsSet("six"), "d.IsSet(six)")
}

func TestSkipQueryPlanCacheDirective(t *testing.T) {
	stmt, _ := Parse("insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}

	stmt, _ = Parse("insert into user(id) values (1), (2)")
	if SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be false")
	}

	stmt, _ = Parse("update /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ users set name=1")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}

	stmt, _ = Parse("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from users")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}

	stmt, _ = Parse("delete /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ from users")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}
}

func TestIgnoreMaxPayloadSizeDirective(t *testing.T) {
	testCases := []struct {
		query    string
		expected bool
	}{
		{"insert /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ into user(id) values (1), (2)", true},
		{"insert into user(id) values (1), (2)", false},
		{"update /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ users set name=1", true},
		{"update users set name=1", false},
		{"select /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ * from users", true},
		{"select * from users", false},
		{"delete /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ from users", true},
		{"delete from users", false},
		{"show /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ create table users", false},
		{"show create table users", false},
	}

	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := Parse(test.query)
			got := IgnoreMaxPayloadSizeDirective(stmt)
			assert.Equalf(t, test.expected, got, fmt.Sprintf("IgnoreMaxPayloadSizeDirective(stmt) returned %v but expected %v", got, test.expected))
		})
	}
}

func TestIgnoreMaxMaxMemoryRowsDirective(t *testing.T) {
	testCases := []struct {
		query    string
		expected bool
	}{
		{"insert /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ into user(id) values (1), (2)", true},
		{"insert into user(id) values (1), (2)", false},
		{"update /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ users set name=1", true},
		{"update users set name=1", false},
		{"select /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ * from users", true},
		{"select * from users", false},
		{"delete /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ from users", true},
		{"delete from users", false},
		{"show /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ create table users", false},
		{"show create table users", false},
	}

	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := Parse(test.query)
			got := IgnoreMaxMaxMemoryRowsDirective(stmt)
			assert.Equalf(t, test.expected, got, fmt.Sprintf("IgnoreMaxPayloadSizeDirective(stmt) returned %v but expected %v", got, test.expected))
		})
	}
}

func TestConsolidator(t *testing.T) {
	testCases := []struct {
		query    string
		expected querypb.ExecuteOptions_Consolidator
	}{
		{"insert /*vt+ CONSOLIDATOR=enabled */ into user(id) values (1), (2)", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"update /*vt+ CONSOLIDATOR=enabled */ users set name=1", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"delete /*vt+ CONSOLIDATOR=enabled */ from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"show /*vt+ CONSOLIDATOR=enabled */ create table users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select * from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select /*vt+ CONSOLIDATOR=invalid_value */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select /*vt+ CONSOLIDATOR=disabled */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
		{"select /*vt+ CONSOLIDATOR=enabled */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_ENABLED},
		{"select /*vt+ CONSOLIDATOR=enabled_replicas */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_ENABLED_REPLICAS},
	}

	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := Parse(test.query)
			got := Consolidator(stmt)
			assert.Equalf(t, test.expected, got, fmt.Sprintf("Consolidator(stmt) returned %v but expected %v", got, test.expected))
		})
	}
}

func TestGetPriorityFromStatement(t *testing.T) {
	testCases := []struct {
		query            string
		expectedPriority string
		expectedError    error
	}{
		{
			query:            "select * from a_table",
			expectedPriority: "",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ ANOTHER_DIRECTIVE=324 */ * from another_table",
			expectedPriority: "",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ PRIORITY=33 */ * from another_table",
			expectedPriority: "33",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ PRIORITY=200 */ * from another_table",
			expectedPriority: "",
			expectedError:    ErrInvalidPriority,
		},
		{
			query:            "select /*vt+ PRIORITY=-1 */ * from another_table",
			expectedPriority: "",
			expectedError:    ErrInvalidPriority,
		},
		{
			query:            "select /*vt+ PRIORITY=some_text */ * from another_table",
			expectedPriority: "",
			expectedError:    ErrInvalidPriority,
		},
		{
			query:            "select /*vt+ PRIORITY=0 */ * from another_table",
			expectedPriority: "0",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ PRIORITY=100 */ * from another_table",
			expectedPriority: "100",
			expectedError:    nil,
		},
	}

	for _, testCase := range testCases {
		theThestCase := testCase
		t.Run(theThestCase.query, func(t *testing.T) {
			t.Parallel()
			stmt, err := Parse(theThestCase.query)
			assert.NoError(t, err)
			actualPriority, actualError := GetPriorityFromStatement(stmt)
			if theThestCase.expectedError != nil {
				assert.ErrorIs(t, actualError, theThestCase.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, theThestCase.expectedPriority, actualPriority)
			}
		})
	}
}
