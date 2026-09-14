/*
Copyright 2026 The Vitess Authors.

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
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlmode"
)

// The parser reads SQL under the honored modes of a sql_mode, and only those:
// a mode it does not honor leaves the reading, and the parser, unchanged.
func TestWithSQLMode(t *testing.T) {
	def := NewTestParser()
	assert.Equal(t, sqlmode.Mode(0), def.SQLMode())

	pipes := def.WithSQLMode(sqlmode.PipesAsConcat)
	assert.NotSame(t, def, pipes)
	assert.Equal(t, sqlmode.PipesAsConcat, pipes.SQLMode())
	assert.Equal(t, sqlmode.Mode(0), def.SQLMode(), "the parser itself is unchanged")

	// the same honored modes give the same parser
	assert.Same(t, def, def.WithSQLMode(0))
	assert.Same(t, def, def.WithSQLMode(sqlmode.StrictTransTables|sqlmode.IgnoreSpace), "unhonored modes do not change the reading")
	assert.Same(t, pipes, pipes.WithSQLMode(sqlmode.PipesAsConcat|sqlmode.NoZeroDate))
	assert.Equal(t, sqlmode.PipesAsConcat|sqlmode.AnsiQuotes, def.WithSQLMode(sqlmode.Ansi).SQLMode(), "a combination mode is expanded")

	// Options.SQLMode is honored the same way
	parser, err := New(Options{SQLMode: sqlmode.Ansi | sqlmode.HighNotPrecedence})
	require.NoError(t, err)
	assert.Equal(t, sqlmode.PipesAsConcat|sqlmode.AnsiQuotes, parser.SQLMode())
}

// Under ANSI_QUOTES a double-quoted token is an identifier rather than a string
// literal, as in MySQL: a doubled quote inside it is an escaped quote, and a backslash
// is an ordinary character. The identifier serializes with backticks, so the AST and
// its serialization carry no mode: the serialized text parses back to the same AST
// under either reading.
func TestAnsiQuotes(t *testing.T) {
	def := NewTestParser()
	ansi := def.WithSQLMode(sqlmode.AnsiQuotes)
	for _, tc := range []struct {
		parser *Parser
		in     string
		out    string
	}{
		{ansi, `select * from "full"`, "select * from `full`"},
		{ansi, `select "a", 'b' from t`, "select a, 'b' from t"},
		{ansi, `select "t"."a" from "d"."t"`, "select t.a from d.t"},
		{ansi, `select a as "b" from t`, "select a as b from t"},
		{ansi, `select * from t where "a" = 'x'`, "select * from t where a = 'x'"},
		{ansi, `create table t ("blah" int)`, "create table t (\n\tblah int\n)"},
		{ansi, `insert into "t" ("a") values ('x')`, "insert into t(a) values ('x')"},
		// a doubled quote is an escaped quote inside the identifier
		{ansi, `select * from "a""b"`, "select * from `a\"b`"},
		// a backslash is an ordinary character inside the identifier
		{ansi, `select * from "a\b"`, "select * from `a\\b`"},
		// single-quoted strings are unaffected, a double quote inside them included
		{ansi, `select 'a"b', 'it''s' from t`, `select 'a"b', 'it\'s' from t`},
		// so is a double quote inside a comment
		{ansi, `select /* "a */ 1 from t`, `select /* "a */ 1 from t`},
		// without the mode a double-quoted token is a string literal
		{def, `select "a", 'b' from t`, "select 'a', 'b' from t"},
		{def, `select * from t where "a" = 'x'`, "select * from t where 'a' = 'x'"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := tc.parser.Parse(tc.in)
			require.NoError(t, err)
			out := String(stmt)
			assert.Equal(t, tc.out, out)

			// the serialized text means the same thing under either reading
			underDefault, err := def.Parse(out)
			require.NoError(t, err)
			underAnsi, err := ansi.Parse(out)
			require.NoError(t, err)
			assert.True(t, Equals.Statement(underDefault, underAnsi), "serialized %q reads differently under ANSI_QUOTES", out)
			assert.Equal(t, out, String(underAnsi))
		})
	}

	// an unterminated or empty identifier is an error under the mode, as in MySQL
	for _, in := range []string{`select * from "t`, `select "" from t`} {
		t.Run(in, func(t *testing.T) {
			_, err := ansi.Parse(in)
			require.Error(t, err)
		})
	}
}

// A double-quoted token is one token under either reading, so the boundaries of a
// batch do not move with the mode, except where a backslash before the closing quote
// escapes it in a string and not in an identifier. The splitter must read under the
// session's mode for that case.
func TestSplitStatementToPiecesAnsiQuotes(t *testing.T) {
	def := NewTestParser()
	ansi := def.WithSQLMode(sqlmode.AnsiQuotes)
	for _, tc := range []struct {
		in           string
		underDefault []string
		underAnsi    []string
	}{
		{`select "a;b" from t; select 1`, []string{`select "a;b" from t`, ` select 1`}, []string{`select "a;b" from t`, ` select 1`}},
		{`select "a""b" from t; select 1`, []string{`select "a""b" from t`, ` select 1`}, []string{`select "a""b" from t`, ` select 1`}},
		{`select "it's" from t; select 1`, []string{`select "it's" from t`, ` select 1`}, []string{`select "it's" from t`, ` select 1`}},
		{`select "a\"; select 1" from t`, []string{`select "a\"; select 1" from t`}, []string{`select "a\"`, ` select 1" from t`}},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := def.SplitStatementToPieces(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.underDefault, got)
			got, err = ansi.SplitStatementToPieces(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.underAnsi, got)
		})
	}
}

// Under PIPES_AS_CONCAT || is the concatenation operator, binding tighter than
// ^ and looser than the unary operators, as in MySQL. It parses into a
// concat() call, so the AST and its serialization carry no mode: the
// serialized text parses back to the same AST under either reading.
func TestPipesAsConcat(t *testing.T) {
	def := NewTestParser()
	pipes := def.WithSQLMode(sqlmode.PipesAsConcat)
	for _, tc := range []struct {
		parser *Parser
		in     string
		out    string
	}{
		{pipes, "select 'a' || 'b' from t", "select concat('a', 'b') from t"},
		{pipes, "select 'a' || 'b' || 'c' from t", "select concat(concat('a', 'b'), 'c') from t"},
		// || binds tighter than the comparison operators and LIKE
		{pipes, "select a || b = c from t", "select concat(a, b) = c from t"},
		{pipes, "select 'a%' like 'a!' || '%' escape '!' from t", "select 'a%' like concat('a!', '%') escape '!' from t"},
		// tighter than ^, looser than the unary operators
		{pipes, "select a ^ b || c from t", "select a ^ concat(b, c) from t"},
		{pipes, "select -a || b from t", "select concat(-a, b) from t"},
		{pipes, "select a || -b from t", "select concat(a, -b) from t"},
		{pipes, "select a || b collate utf8mb4_bin from t", "select concat(a, b collate utf8mb4_bin) from t"},
		// the keyword form of OR is unchanged
		{pipes, "select a or b from t", "select a or b from t"},
		{pipes, "select * from t where a || b", "select * from t where concat(a, b)"},
		// without the mode || is logical OR
		{def, "select 'a' || 'b' from t", "select 'a' or 'b' from t"},
		{def, "select * from t where a || b", "select * from t where a or b"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := tc.parser.Parse(tc.in)
			require.NoError(t, err)
			out := String(stmt)
			assert.Equal(t, tc.out, out)

			// the serialized text means the same thing under either reading
			underDefault, err := def.Parse(out)
			require.NoError(t, err)
			underPipes, err := pipes.Parse(out)
			require.NoError(t, err)
			assert.True(t, Equals.Statement(underDefault, underPipes), "serialized %q reads differently under PIPES_AS_CONCAT", out)
			assert.Equal(t, out, String(underPipes))
		})
	}
}
