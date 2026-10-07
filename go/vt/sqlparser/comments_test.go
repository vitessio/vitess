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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sysvars"

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

			assert.Equal(t, testCase.outSQL, gotSQL, "SQL mismatch")
			assert.Equal(t, testCase.outLeadingComments, gotLeadingComments, "LeadingComments mismatch")
			assert.Equal(t, testCase.outTrailingComments, gotTrailingComments, "TrailingCommints mismatch")
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
		assert.Equal(t, testCase.outSQL, gotSQL)
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
		assert.Equal(t, testCase.outVersion, gotVersion, "version mismatch")

		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
	}
}

func TestRewriteDoubleSlashComments(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{{
		input:  "select 1 from t",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t // x",
		output: "select 1 from t #/ x",
	}, {
		input:  "select 1 from t //x\n",
		output: "select 1 from t #/x\n",
	}, {
		input:  "select 1 from t //",
		output: "select 1 from t #/",
	}, {
		input:  "// x\nselect 1 from t",
		output: "#/ x\nselect 1 from t",
	}, {
		// MySQL reads this as 10 / 2; Vitess reads it as 10.
		input:  "select 10 //* x */ 2",
		output: "select 10 #/* x */ 2",
	}, {
		// The ; is inside the comment, so it does not end the statement.
		input:  "select 1 // ; drop table t\n, 2",
		output: "select 1 #/ ; drop table t\n, 2",
	}, {
		input:  "select 1 // a\r\n, 2 /// b\n, 3",
		output: "select 1 #/ a\r\n, 2 #// b\n, 3",
	}, {
		input:  "select 'http://x', `a//b`, \"//\" from t /* // */ -- //\n",
		output: "select 'http://x', `a//b`, \"//\" from t /* // */ -- //\n",
	}, {
		// Inside a versioned comment that Vitess executes, / is division.
		input:  "select /*!50000 4 //*x*/ 2 */",
		output: "select /*!50000 4 //*x*/ 2 */",
	}, {
		// A versioned comment that Vitess skips is a comment as a whole.
		input:  "select 1 /*!99999 // x */",
		output: "select 1 /*!99999 // x */",
	}, {
		// An unterminated string runs to the end of the text.
		input:  "select 1 // a\n, 'b // c",
		output: "select 1 #/ a\n, 'b // c",
	}, {
		input:  "select 'a // b",
		output: "select 'a // b",
	}, {
		input:  "select @'ab' // x\n",
		output: "select @'ab' #/ x\n",
	}, {
		// Vitess cannot read this quoted user variable name whole, so it is
		// a lexing error. The tokenizer then reads the quoted name as a
		// string, as MySQL does, and the comment after it is rewritten.
		input:  "do @'a//b' := 1 // x\n",
		output: "do @'a//b' := 1 #/ x\n",
	}, {
		input:  "select 1; select @'a b', 1 // ; select 2\n, 3",
		output: "select 1; select @'a b', 1 #/ ; select 2\n, 3",
	}}
	parser := NewTestParser()
	for _, tcase := range testCases {
		t.Run(tcase.input, func(t *testing.T) {
			out, rewritten := parser.RewriteDoubleSlashComments(tcase.input)
			assert.Equal(t, tcase.output, out)
			assert.Equal(t, tcase.output != tcase.input, rewritten)
		})
	}
}

// TestRewriteDoubleSlashCommentsKeepsStatement checks that the rewrite does
// not change what Vitess parses.
func TestRewriteDoubleSlashCommentsKeepsStatement(t *testing.T) {
	parser := NewTestParser()
	inputs := []string{
		"select 1e//+ 2\n from t",
		"select 1 from t // x",
		"select 10 //* x */ 2 from t // y\n, 3",
		"select 1 // ; drop table t\n, 2",
		"// x\nselect 1 from t",
		"select 1 from t // a\r\nwhere a = 1 /// b\n",
	}
	for _, tcase := range validSQL {
		inputs = append(inputs, tcase.input)
	}
	for _, input := range inputs {
		want, err := parser.Parse(input)
		if err != nil {
			continue
		}
		rewritten, _ := parser.RewriteDoubleSlashComments(input)
		got, err := parser.Parse(rewritten)
		require.NoError(t, err, rewritten)
		assert.Equal(t, String(want), String(got), input)
	}
}

// TestRewriteDoubleSlashCommentsLeavesNoComment checks that, after the rewrite,
// the tokenizer reads no "//" comment and splits the text into the same
// statements, also in text that does not lex or parse.
func TestRewriteDoubleSlashCommentsLeavesNoComment(t *testing.T) {
	parser := NewTestParser()
	inputs := []string{
		"SELECT @'a b', 10 //*x*/ 2",
		"select 1; select @'a b', 1 // ; select 2\n, 3",
		"select @'a//b' // x\n; select 1 // y",
		"select :1x // a\n; select 2 // b",
	}
	for _, tcase := range validSQL {
		inputs = append(inputs, tcase.input)
	}
	for _, tcase := range invalidSQL {
		inputs = append(inputs, tcase.input)
	}
	for _, input := range inputs {
		rewritten, _ := parser.RewriteDoubleSlashComments(input)
		tokenizer := parser.NewStringTokenizer(rewritten)
		for {
			typ, val := tokenizer.Scan()
			if typ == 0 {
				break
			}
			if typ == COMMENT {
				assert.False(t, strings.HasPrefix(val, "//"), "%q left %q", input, val)
			}
		}
		want, wantErr := parser.SplitStatementToPieces(input)
		got, gotErr := parser.SplitStatementToPieces(rewritten)
		assert.Equal(t, wantErr, gotErr, input)
		assert.Len(t, got, len(want), input)
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

	parser := NewTestParser()
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
					stmt, _ := parser.Parse(sql)
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

					assert.Equal(t, testCase.vals, vals.m)
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
	parser := NewTestParser()
	stmt, _ := parser.Parse("insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)")
	assert.False(t, CachePlan(stmt))

	stmt, _ = parser.Parse("insert into user(id) values (1), (2)")
	assert.True(t, CachePlan(stmt))

	stmt, _ = parser.Parse("update /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ users set name=1")
	assert.False(t, CachePlan(stmt))

	stmt, _ = parser.Parse("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from users")
	assert.False(t, CachePlan(stmt))

	stmt, _ = parser.Parse("delete /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ from users")
	assert.False(t, CachePlan(stmt))
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

	parser := NewTestParser()
	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := parser.Parse(test.query)
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

	parser := NewTestParser()
	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := parser.Parse(test.query)
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

	parser := NewTestParser()
	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := parser.Parse(test.query)
			qh, err := BuildQueryHints(stmt)
			require.NoError(t, err)
			assert.Equalf(t, test.expected, qh.Consolidator,
				"Consolidator(stmt) returned %v but expected %v", qh.Consolidator, test.expected)
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

	parser := NewTestParser()
	for _, testCase := range testCases {
		t.Run(testCase.query, func(t *testing.T) {
			t.Parallel()
			stmt, err := parser.Parse(testCase.query)
			assert.NoError(t, err)
			qh, err := BuildQueryHints(stmt)
			if testCase.expectedError != nil {
				assert.ErrorIs(t, err, testCase.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testCase.expectedPriority, qh.Priority)
			}
		})
	}
}

// TestGetMySQLSetVarValue tests the functionality of GetMySQLSetVarValue
func TestGetMySQLSetVarValue(t *testing.T) {
	tests := []struct {
		name      string
		comments  []string
		valToFind string
		want      string
	}{
		{
			name:      "SET_VAR clause in the middle",
			comments:  []string{"/*+ NO_RANGE_OPTIMIZATION(t3 PRIMARY, f2_idx) SET_VAR(foreign_key_checks=OFF) NO_ICP(t1, t2) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "OFF",
		},
		{
			name:      "Single SET_VAR clause",
			comments:  []string{"/*+ SET_VAR(sort_buffer_size = 16M) */"},
			valToFind: "sort_buffer_size",
			want:      "16M",
		},
		{
			name:      "No comments",
			comments:  nil,
			valToFind: "sort_buffer_size",
			want:      "",
		},
		{
			name:      "Multiple SET_VAR clauses",
			comments:  []string{"/*+ SET_VAR(sort_buffer_size = 16M) */", "/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') */", "/*+ SET_VAR( foReiGn_key_checks = On) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "",
		},
		{
			name:      "Verify casing",
			comments:  []string{"/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') SET_VAR( foReiGn_key_checks = On) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "On",
		},
		{
			name:      "Leading comment is a normal comment",
			comments:  []string{"/* This is a normal comment */", "/*+ MAX_EXECUTION_TIME(1000) SET_VAR( foreign_key_checks = 1) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ParsedComments{
				comments: tt.comments,
			}
			assert.Equal(t, tt.want, c.GetMySQLSetVarValue(tt.valToFind))
		})
	}
}

func TestSetMySQLSetVarValue(t *testing.T) {
	tests := []struct {
		name           string
		comments       []string
		key            string
		value          string
		commentsWanted Comments
	}{
		{
			name:           "SET_VAR clause in the middle",
			comments:       []string{"/*+ NO_RANGE_OPTIMIZATION(t3 PRIMARY, f2_idx) SET_VAR(foreign_key_checks=OFF) NO_ICP(t1, t2) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "On",
			commentsWanted: []string{"/*+ NO_RANGE_OPTIMIZATION(t3 PRIMARY, f2_idx) SET_VAR(foreign_key_checks=On) NO_ICP(t1, t2) */"},
		},
		{
			name:           "Single SET_VAR clause",
			comments:       []string{"/*+ SET_VAR(sort_buffer_size = 16M) */"},
			key:            "sort_buffer_size",
			value:          "1Mb",
			commentsWanted: []string{"/*+ SET_VAR(sort_buffer_size=1Mb) */"},
		},
		{
			name:           "No comments",
			comments:       nil,
			key:            "sort_buffer_size",
			value:          "13M",
			commentsWanted: []string{"/*+ SET_VAR(sort_buffer_size=13M) */"},
		},
		{
			name:           "Multiple SET_VAR clauses",
			comments:       []string{"/*+ SET_VAR(sort_buffer_size = 16M) */", "/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') */", "/*+ SET_VAR( foReiGn_key_checks = On) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "1",
			commentsWanted: []string{"/*+ SET_VAR(sort_buffer_size = 16M) SET_VAR(foreign_key_checks=1) */", "/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') */", "/*+ SET_VAR( foReiGn_key_checks = On) */"},
		},
		{
			name:           "Verify casing",
			comments:       []string{"/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') SET_VAR( foReiGn_key_checks = On) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "off",
			commentsWanted: []string{"/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') SET_VAR(foReiGn_key_checks=off) */"},
		},
		{
			name:           "Leading comment is a normal comment",
			comments:       []string{"/* This is a normal comment */", "/*+ MAX_EXECUTION_TIME(1000) SET_VAR( foreign_key_checks = 1) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "Off",
			commentsWanted: []string{"/* This is a normal comment */", "/*+ MAX_EXECUTION_TIME(1000) SET_VAR(foreign_key_checks=Off) */"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ParsedComments{
				comments: tt.comments,
			}
			newComments := c.SetMySQLSetVarValue(tt.key, tt.value)
			require.EqualValues(t, tt.commentsWanted, newComments)
		})
	}
}

// TestQueryTimeout tests the extraction of Query_Timeout_MS from the comments.
func TestQueryTimeout(t *testing.T) {
	testCases := []struct {
		query      string
		expTimeout int
		noTimeout  bool
	}{{
		query:     "select * from a_table",
		noTimeout: true,
	}, {
		query:      "select /*vt+ QUERY_TIMEOUT_MS=21 */ * from another_table",
		expTimeout: 21,
	}, {
		query:      "select /*vt+ QUERY_TIMEOUT_MS=0 */ * from another_table",
		expTimeout: 0,
	}, {
		query:     "select /*vt+ PRIORITY=-42 */ * from another_table",
		noTimeout: true,
	}}

	parser := NewTestParser()
	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			assert.NoError(t, err)
			qh, _ := BuildQueryHints(stmt)
			if tc.noTimeout {
				assert.Nil(t, qh.Timeout)
			} else {
				assert.Equal(t, tc.expTimeout, *qh.Timeout)
			}
		})
	}
}
