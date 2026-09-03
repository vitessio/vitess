/*
Copyright 2022 The Vitess Authors.

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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vterrors"
)

func TestEmptyErrorAndComments(t *testing.T) {
	testcases := []struct {
		input  string
		output string
		err    error
	}{
		{
			input:  "select 1",
			output: "select 1 from dual",
		}, {
			input: "",
			err:   ErrEmpty,
		}, {
			input: ";",
			err:   ErrEmpty,
		}, {
			input:  "-- sdf",
			output: "-- sdf",
		}, {
			input:  "/* sdf */",
			output: "/* sdf */",
		}, {
			input:  "# sdf",
			output: "# sdf",
		}, {
			input:  "/* sdf */ select 1",
			output: "select 1 from dual",
		},
	}
	parser := NewTestParser()
	for _, testcase := range testcases {
		t.Run(testcase.input, func(t *testing.T) {
			res, err := parser.Parse(testcase.input)
			if testcase.err != nil {
				require.Equal(t, testcase.err, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.output, String(res))
			}
		})

		t.Run(testcase.input+"-Strict DDL", func(t *testing.T) {
			res, err := parser.ParseStrictDDL(testcase.input)
			if testcase.err != nil {
				require.Equal(t, testcase.err, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.output, String(res))
			}
		})
	}
}

func TestParseNext(t *testing.T) {
	testcases := []struct {
		input   string
		stmt    string // %T of the statement; "<nil>" for an empty statement
		text    string
		rest    string
		partial bool // stmt is a partially parsed DDL
		err     string
	}{{
		input: "select 1",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
	}, {
		input: "select 1;",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
	}, {
		input: "select 1;   ",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
		rest:  "   ",
	}, {
		input: "select 1; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
		rest:  " select 2",
	}, {
		// A statement whose last state needs the lookahead token to reduce.
		input: "select a from t where a = 1 order by a; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select a from t where a = 1 order by a",
		rest:  " select 2",
	}, {
		input: "select 1;; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
		rest:  "; select 2",
	}, {
		input: "select 1;\n;\n select 2",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
		rest:  "\n;\n select 2",
	}, {
		input: "; select 2",
		stmt:  "<nil>",
		text:  "",
		rest:  " select 2",
	}, {
		input: ";",
		stmt:  "<nil>",
	}, {
		input: "",
		stmt:  "<nil>",
	}, {
		input: "   ",
		stmt:  "<nil>",
		text:  "   ",
	}, {
		input: "-- c",
		stmt:  "*sqlparser.CommentOnly",
		text:  "-- c",
	}, {
		input: "select 1; -- trailing",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
		rest:  " -- trailing",
	}, {
		input: "select 1; /* c */",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
		rest:  " /* c */",
	}, {
		// Leading whitespace and comments belong to the statement, as in MySQL.
		input: "  /* c */ select 1; select 2",
		stmt:  "*sqlparser.Select",
		text:  "  /* c */ select 1",
		rest:  " select 2",
	}, {
		input: "select 1 /* ; */; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select 1 /* ; */",
		rest:  " select 2",
	}, {
		input: "select 1 -- ;\n; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select 1 -- ;\n",
		rest:  " select 2",
	}, {
		input: "select 1 # ;\n; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select 1 # ;\n",
		rest:  " select 2",
	}, {
		input: "select ';'; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select ';'",
		rest:  " select 2",
	}, {
		input: "select 1 as `a;b`; select 2",
		stmt:  "*sqlparser.Select",
		text:  "select 1 as `a;b`",
		rest:  " select 2",
	}, {
		input: `select 1 as ";"; select 2`,
		stmt:  "*sqlparser.Select",
		text:  `select 1 as ";"`,
		rest:  " select 2",
	}, {
		input: "/*! select 1 */; select 2",
		stmt:  "*sqlparser.Select",
		text:  "/*! select 1 */",
		rest:  " select 2",
	}, {
		input: "begin; select 1; commit",
		stmt:  "*sqlparser.Begin",
		text:  "begin",
		rest:  " select 1; commit",
	}, {
		// The grammar, not the lexer, decides where a statement ends: the ';'
		// inside the procedure body does not terminate it.
		input: "create procedure p() begin select 1; select 2; end; select 3",
		stmt:  "*sqlparser.CreateProcedure",
		text:  "create procedure p() begin select 1; select 2; end",
		rest:  " select 3",
	}, {
		// A statement the grammar only skips over still ends at the top-level ';'.
		input: "load data infile 'x' into table t; select 2",
		stmt:  "*sqlparser.Load",
		text:  "load data infile 'x' into table t",
		rest:  " select 2",
	}, {
		input: "select 1 select 2",
		err:   "syntax error at position 16 near 'select'",
	}, {
		input: "select 1; bogus; select 3",
		stmt:  "*sqlparser.Select",
		text:  "select 1",
		rest:  " bogus; select 3",
	}, {
		input: "bogus; select 3",
		err:   "syntax error at position 6 near 'bogus'",
	}, {
		// Unterminated string: a syntax error, never a panic.
		// TODO(#20884): the same input parsed under NO_BACKSLASH_ESCAPES is two statements.
		input: `select 'a\'; select 2`,
		err:   "syntax error at position 23 near 'a'; select 2'",
	}, {
		input: "create function f() returns int deterministic begin return 1; end; select f()",
		err:   "syntax error at position 16 near 'function'",
	}, {
		// Partially parsed DDL is accepted the way Parse accepts it, cut at the next ';'.
		input:   "create table t1 (id int) bogus; select 1",
		stmt:    "*sqlparser.CreateTable",
		text:    "create table t1 (id int) bogus",
		rest:    " select 1",
		partial: true,
	}, {
		input:   "create table t1 (id int) bogus more",
		stmt:    "*sqlparser.CreateTable",
		text:    "create table t1 (id int) bogus more",
		partial: true,
	}, {
		// ... but not when the tokenizer cannot find the end of the statement.
		input: "create table t1 (id int) bogus 'unterminated; select 1",
		err:   "syntax error at position 31 near 'bogus'",
	}, {
		// The ';' the parser chokes on still ends the statement (Parse accepts
		// "create table t1;" as a partial DDL too).
		input:   "create table t1; select 1",
		stmt:    "*sqlparser.CreateTable",
		text:    "create table t1",
		rest:    " select 1",
		partial: true,
	}}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			var stmt Statement
			var text, rest string
			var err error
			require.NotPanics(t, func() {
				stmt, text, rest, err = parser.ParseNext(tcase.input)
			})
			if tcase.err != "" {
				require.ErrorContains(t, err, tcase.err)
				assert.Nil(t, stmt)
				assert.Empty(t, text)
				assert.Empty(t, rest)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tcase.stmt, fmt.Sprintf("%T", stmt))
			assert.Equal(t, tcase.text, text)
			assert.Equal(t, tcase.rest, rest)
			if ddl, ok := stmt.(DDLStatement); ok {
				assert.Equal(t, !tcase.partial, ddl.IsFullyParsed())
			}
		})
	}
}

// TestParseNextInheritsParserMode makes sure ParseNext tokenizes with the
// parser it was called on, so a parser configured differently (MySQL version,
// and after #20884 the session's sql_mode) sees the same statement boundaries
// as Parse does.
func TestParseNextInheritsParserMode(t *testing.T) {
	const sql = "/*!80000 select 1 */; select 2"

	stmt, text, rest, err := NewTestParser().ParseNext(sql)
	require.NoError(t, err)
	assert.IsType(t, &Select{}, stmt)
	assert.Equal(t, "/*!80000 select 1 */", text)
	assert.Equal(t, " select 2", rest)

	parser57, err := New(Options{MySQLServerVersion: "5.7.9"})
	require.NoError(t, err)
	stmt, text, rest, err = parser57.ParseNext(sql)
	require.NoError(t, err)
	assert.Nil(t, stmt, "a 5.7 parser skips the versioned comment entirely")
	assert.Equal(t, "/*!80000 select 1 */", text)
	assert.Equal(t, " select 2", rest)
}

func TestForEachStatement(t *testing.T) {
	type call struct{ text, rest string }
	const parseErrorPrefix = "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near "
	testcases := []struct {
		input string
		calls []call
		err   string
		state vterrors.State
	}{{
		input: "select 1",
		calls: []call{{"select 1", ""}},
	}, {
		// A single statement is handed over as is, without being parsed.
		input: "\n bogus ",
		calls: []call{{"\n bogus ", ""}},
	}, {
		input: "select 1;",
		calls: []call{{"select 1", ""}},
	}, {
		input: "select 1;  \n",
		calls: []call{{"select 1", ""}},
	}, {
		input: "select ';'",
		calls: []call{{"select ';'", ""}},
	}, {
		input: "select 1; select 2",
		calls: []call{{"select 1", " select 2"}, {" select 2", ""}},
	}, {
		// Statements keep the whitespace around them; the batch loses its
		// trailing ';' and whitespace, as in MySQL.
		input: "  select 1;\n\tselect 2 ; ",
		calls: []call{{"  select 1", "\n\tselect 2"}, {"\n\tselect 2", ""}},
	}, {
		input: "select 1;;",
		calls: []call{{"select 1", ""}},
	}, {
		input: "select 1 ; ;",
		calls: []call{{"select 1", ""}},
	}, {
		input: "select 1; /* c */ ;",
		calls: []call{{"select 1", " /* c */"}, {" /* c */", ""}},
	}, {
		input: "select 1; -- trailing",
		calls: []call{{"select 1", " -- trailing"}, {" -- trailing", ""}},
	}, {
		input: "select 1; /* c */",
		calls: []call{{"select 1", " /* c */"}, {" /* c */", ""}},
	}, {
		input: "select 1;; select 2",
		calls: []call{{"select 1", "; select 2"}},
		err:   parseErrorPrefix + "'; select 2' at line 1",
		state: vterrors.ParseError,
	}, {
		input: "select 1;; -- c",
		calls: []call{{"select 1", "; -- c"}},
		err:   parseErrorPrefix + "'; -- c' at line 1",
		state: vterrors.ParseError,
	}, {
		input: "select 1;\n;\n select 2",
		calls: []call{{"select 1", "\n;\n select 2"}},
		err:   parseErrorPrefix + "';\n select 2' at line 2",
		state: vterrors.ParseError,
	}, {
		input: "; select 2",
		err:   parseErrorPrefix + "'; select 2' at line 1",
		state: vterrors.ParseError,
	}, {
		input: "",
		err:   "Query was empty",
		state: vterrors.EmptyQuery,
	}, {
		input: "  \n",
		err:   "Query was empty",
		state: vterrors.EmptyQuery,
	}, {
		input: " ;; ",
		err:   "Query was empty",
		state: vterrors.EmptyQuery,
	}, {
		input: ";",
		err:   "Query was empty",
		state: vterrors.EmptyQuery,
	}, {
		input: " ; ",
		err:   "Query was empty",
		state: vterrors.EmptyQuery,
	}, {
		// A statement the grammar rejects is handed over as well, cut at the
		// next top-level ';': the callback's own parse reports it.
		input: "select 1; bogus; select 3",
		calls: []call{{"select 1", " bogus; select 3"}, {" bogus", " select 3"}, {" select 3", ""}},
	}, {
		input: "select 1 select 2; select 3",
		calls: []call{{"select 1 select 2", " select 3"}, {" select 3", ""}},
	}, {
		input: "create function f() returns int deterministic begin return 1; end; select f()",
		calls: []call{{"create function f() returns int deterministic begin return 1", " end; select f()"}, {" end", " select f()"}, {" select f()", ""}},
	}, {
		input: "create procedure p() begin select 1; select 2; end; select 3",
		calls: []call{{"create procedure p() begin select 1; select 2; end", " select 3"}, {" select 3", ""}},
	}, {
		input: "create table t1 (id int) bogus; select 1",
		calls: []call{{"create table t1 (id int) bogus", " select 1"}, {" select 1", ""}},
	}, {
		// TODO(#20884): once the session's parser is used, the SET applies to
		// the statements after it and this is three statements.
		input: `set sql_mode='NO_BACKSLASH_ESCAPES'; select 'a\'; select 2`,
		calls: []call{{"set sql_mode='NO_BACKSLASH_ESCAPES'", ` select 'a\'; select 2`}, {` select 'a\'; select 2`, ""}},
	}}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			var calls []call
			err := parser.ForEachStatement(tcase.input, func(text, rest string) error {
				calls = append(calls, call{text, rest})
				return nil
			})
			assert.Equal(t, tcase.calls, calls)
			if tcase.err == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tcase.err)
			if tcase.state != vterrors.Undefined {
				assert.Equal(t, tcase.state, vterrors.ErrState(err))
			}
		})
	}
}

// TestSingleStatementIsNotParsed pins the fast paths: a single statement is
// handed over without a grammar parse, even when a ';' sits inside a string
// literal or a comment, and so is a prepare of it. A parse costs dozens of
// allocations; the lexical check costs at most the tokenizer.
func TestSingleStatementIsNotParsed(t *testing.T) {
	parser := NewTestParser()
	inputs := []string{
		"select id, name from users where id = 1",
		"select id, name from users where id = 1;",
		"select id, name from users where id = 1;\n",
		"select ';' from users where name = 'a;b'",
		"select 1 /* ; */ from users",
		"insert into t(v) values ('{\"k\": \"v;w\"}')",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			allocs := testing.AllocsPerRun(20, func() {
				err := parser.ForEachStatement(input, func(text, rest string) error {
					return nil
				})
				require.NoError(t, err)
			})
			assert.LessOrEqual(t, allocs, 1.0, "ForEachStatement parsed a single statement")

			allocs = testing.AllocsPerRun(20, func() {
				_, _, err := parser.SplitStatement(input)
				require.NoError(t, err)
			})
			assert.LessOrEqual(t, allocs, 1.0, "SplitStatement parsed a single statement")
		})
	}
}

func TestForEachStatementStopsOnCallbackError(t *testing.T) {
	failed := errors.New("execution failed")
	var texts []string
	err := NewTestParser().ForEachStatement("select 1;select 2;bogus", func(text, rest string) error {
		texts = append(texts, text)
		if text == "select 2" {
			return failed
		}
		return nil
	})
	// The callback's error comes back as is: the unparseable third statement was never parsed.
	require.ErrorIs(t, err, failed)
	assert.Equal(t, []string{"select 1", "select 2"}, texts)
}

func TestSplitStatementToPieces(t *testing.T) {
	testcases := []struct {
		input     string
		output    string
		lenWanted int
	}{
		{
			input:  "select * from table1; \t; \n; \n\t\t ;select * from table1;",
			output: "select * from table1;select * from table1",
		}, {
			input: "select * from table",
		}, {
			input:  "select * from table;",
			output: "select * from table",
		}, {
			input:  "select * from table1;   ",
			output: "select * from table1",
		}, {
			input:  "select * from table1; select * from table2;",
			output: "select * from table1; select * from table2",
		}, {
			input:  "select * from /* comment ; */ table1;",
			output: "select * from /* comment ; */ table1",
		}, {
			input:  "select * from table where semi = ';';",
			output: "select * from table where semi = ';'",
		}, {
			input:  "select * from table1;--comment;\nselect * from table2;",
			output: "select * from table1;--comment;\nselect * from table2",
		}, {
			input: "CREATE TABLE `total_data` (`id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id', " +
				"`region` varchar(32) NOT NULL COMMENT 'region name, like zh; th; kepler'," +
				"`data_size` bigint NOT NULL DEFAULT '0' COMMENT 'data size;'," +
				"`createtime` datetime NOT NULL DEFAULT NOW() COMMENT 'create time;'," +
				"`comment` varchar(100) NOT NULL DEFAULT '' COMMENT 'comment'," +
				"PRIMARY KEY (`id`))",
		}, {
			input:  "create table t1 (id int primary key); create table t2 (id int primary key);",
			output: "create table t1 (id int primary key); create table t2 (id int primary key)",
		}, {
			input:  ";;; create table t1 (id int primary key);;; ;create table t2 (id int primary key);",
			output: " create table t1 (id int primary key);create table t2 (id int primary key)",
		}, {
			// The input doesn't have to be valid SQL statements!
			input:  ";create table t1 ;create table t2 (id;",
			output: "create table t1 ;create table t2 (id",
		}, {
			// Ignore quoted semicolon
			input:  ";create table t1 ';';;;create table t2 (id;",
			output: "create table t1 ';';create table t2 (id",
		}, {
			// Ignore quoted semicolon
			input:  "stop replica; start replica",
			output: "stop replica; start replica",
		}, {
			// Test that we don't split on semicolons inside create procedure calls.
			input:     "create procedure p1 (in country CHAR(3), out cities INT) begin select count(*) from x where d = e; end",
			lenWanted: 1,
		}, {
			// Test that we don't split on semicolons inside create procedure calls.
			input:     "select * from t1;create procedure p1 (in country CHAR(3), out cities INT) begin select count(*) from x where d = e; end;select * from t2",
			lenWanted: 3,
		}, {
			// Create procedure with comments.
			input:     "select * from t1; /* comment1 */ create /* comment2 */ procedure /* comment3 */ p1 (in country CHAR(3), out cities INT) begin select count(*) from x where d = e; end;select * from t2",
			lenWanted: 3,
		}, {
			// Create procedure with definer current_user.
			input:     "create DEFINER=CURRENT_USER procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
			lenWanted: 1,
		}, {
			// Create procedure with definer current_user().
			input:     "create DEFINER=CURRENT_USER() procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
			lenWanted: 1,
		}, {
			// Create procedure with definer string.
			input:     "create DEFINER='root' procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
			lenWanted: 1,
		}, {
			// Create procedure with definer string at_id.
			input:     "create DEFINER='root'@localhost procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
			lenWanted: 1,
		}, {
			// Create procedure with definer id.
			input:     "create DEFINER=`root` procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
			lenWanted: 1,
		}, {
			// Create procedure with definer id at_id.
			input:     "create DEFINER=`root`@`localhost` procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
			lenWanted: 1,
		}, {
			// A syntax error inside the procedure body falls back to the lexical cut.
			input:     "create procedure p1() begin bogus; end; select 1",
			lenWanted: 3,
		}, {
			// Statements the grammar does not know are cut at the next top-level ';'
			// so that tooling can pass them on to MySQL unchanged.
			input:     "create function f() returns int deterministic begin return 1; end; select f()",
			lenWanted: 3,
		}, {
			input:     "change replication source to source_host='x'; start replica",
			lenWanted: 2,
		}, {
			input:  "select 1;; select 2",
			output: "select 1; select 2",
		}, {
			// Comment-only pieces are dropped by this legacy API.
			input:  "select 1; -- trailing",
			output: "select 1",
		}, {
			input:  "select 1; /* c */",
			output: "select 1",
		}, {
			// Partially parsed DDL keeps its own text.
			input:     "create table t1 (id int) bogus; select 1",
			lenWanted: 2,
		},
	}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}

			stmtPieces, err := parser.SplitStatementToPieces(tcase.input)
			require.NoError(t, err)
			if tcase.lenWanted != 0 {
				require.Len(t, stmtPieces, tcase.lenWanted)
			}
			out := strings.Join(stmtPieces, ";")
			require.Equal(t, tcase.output, out)
		})
	}
}

// TestSplitStatementToPiecesNeverPanics guards the legacy splitter against
// truncated input; an unterminated string used to make it slice past the end
// of the buffer.
func TestSplitStatementToPiecesNeverPanics(t *testing.T) {
	inputs := []string{
		`select 'a\'; select 2`,
		`select 'a\'`,
		"select 1 /* c; select 2",
		"select 1 as `a; select 2",
		`select \`,
		"/*! select 1; select 2",
		"select @",
		`select "`,
	}
	parser := NewTestParser()
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			var pieces []string
			var err error
			require.NotPanics(t, func() {
				pieces, err = parser.SplitStatementToPieces(input)
			})
			require.NoError(t, err)
			// Whatever the cut, nothing of the input is lost.
			assert.Equal(t, strings.ReplaceAll(input, ";", ""), strings.ReplaceAll(strings.Join(pieces, ""), ";", ""))
		})
	}
}
