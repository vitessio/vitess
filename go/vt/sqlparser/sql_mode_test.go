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
	assert.Same(t, def, def.WithSQLMode(sqlmode.StrictTransTables|sqlmode.AnsiQuotes), "unhonored modes do not change the reading")
	assert.Same(t, pipes, pipes.WithSQLMode(sqlmode.PipesAsConcat|sqlmode.NoZeroDate))
	assert.Equal(t, sqlmode.PipesAsConcat, def.WithSQLMode(sqlmode.Ansi).SQLMode(), "a combination mode is expanded")

	// Options.SQLMode is honored the same way
	parser, err := New(Options{SQLMode: sqlmode.Ansi | sqlmode.HighNotPrecedence})
	require.NoError(t, err)
	assert.Equal(t, sqlmode.PipesAsConcat, parser.SQLMode())
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
