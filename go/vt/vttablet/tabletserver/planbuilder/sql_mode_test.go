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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// The vttablet accepts valid sql_mode values on its own inbound entry points —
// settings and SET statements — applies them to MySQL as written, and parses queries
// under the parse-relevant modes itself. NO_BACKSLASH_ESCAPES is rejected: it cannot
// be applied to the MySQL session, and the vttablet answers @@sql_mode reads from
// that session. Invalid values are rejected with MySQL's errors.

func TestBuildSettingQuerySQLMode(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	tests := []struct {
		settings      []string
		expectedErr   string
		expectedQuery string
		expectedMode  sqlparser.SQLMode
	}{{
		settings:      []string{"set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'", "set sql_safe_updates = 1"},
		expectedQuery: "set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE', sql_safe_updates = 1",
	}, {
		settings:      []string{"set sql_mode = ''"},
		expectedQuery: "set sql_mode = ''",
	}, {
		// the ANSI combination and its members are forwarded as written: MySQL
		// enforces their resolution- and execution-time semantics itself, and their
		// lexer aspects are inert on the serialized SQL the vttablet sends
		settings:      []string{"set sql_mode = 'ANSI'"},
		expectedQuery: "set sql_mode = 'ANSI'",
		expectedMode:  sqlparser.ParseSQLMode("ANSI"),
	}, {
		settings:      []string{"set sql_safe_updates = 1", "set sql_mode = 'STRICT_TRANS_TABLES,ANSI_QUOTES'"},
		expectedQuery: "set sql_safe_updates = 1, sql_mode = 'STRICT_TRANS_TABLES,ANSI_QUOTES'",
		expectedMode:  sqlparser.SQLModeANSIQuotes,
	}, {
		settings:      []string{"set sql_mode = 'IGNORE_SPACE'"},
		expectedQuery: "set sql_mode = 'IGNORE_SPACE'",
		expectedMode:  sqlparser.SQLModeIgnoreSpace,
	}, {
		// the mode the MySQL session must not run under is rejected, however it is
		// spelled — here the numeric bitmask for NO_BACKSLASH_ESCAPES
		settings:    []string{"set sql_mode = 1048576"},
		expectedErr: "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported",
	}, {
		// HIGH_NOT_PRECEDENCE is forwarded: the vttablet-serialized SQL
		// parenthesizes NOT operands that would bind differently under it
		settings:      []string{"set sql_mode = 'STRICT_TRANS_TABLES,HIGH_NOT_PRECEDENCE'"},
		expectedQuery: "set sql_mode = 'STRICT_TRANS_TABLES,HIGH_NOT_PRECEDENCE'",
		expectedMode:  sqlparser.SQLModeHighNotPrecedence,
	}, {
		settings:    []string{"set sql_mode = 'BOGUS'"},
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// settings are applied with no read-back afterwards; a value that cannot be
		// judged and stripped upfront is rejected
		settings:    []string{"set sql_safe_updates = 1", "set sql_mode = concat('AN', 'SI')"},
		expectedErr: "non-constant sql_mode value in connection settings: set sql_mode = concat('AN', 'SI')",
	}}
	for _, tc := range tests {
		t.Run(tc.settings[len(tc.settings)-1], func(t *testing.T) {
			query, resetQuery, parseMode, _, err := BuildSettingQuery(tc.settings, parser)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedQuery, query)
			assert.Equal(t, tc.expectedMode, parseMode)
			assert.NotEmpty(t, resetQuery)
		})
	}
}

// The settings reset must not restore `default` for sql_mode: that would re-inherit the
// server's global value including its lexer modes, undoing the neutralization every
// Vitess-created connection starts with. It restores the neutralized global instead.
func TestBuildSettingQueryResetNeutralizesSQLMode(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	query, resetQuery, parseMode, setsSQLMode, err := BuildSettingQuery([]string{"set sql_mode = 'STRICT_TRANS_TABLES'", "set sql_safe_updates = 1"}, parser)
	assert.True(t, setsSQLMode)
	require.NoError(t, err)
	assert.Equal(t, sqlparser.SQLMode(0), parseMode)
	assert.Contains(t, query, "sql_mode = 'STRICT_TRANS_TABLES'")
	assert.Contains(t, resetQuery, "sql_mode = replace(replace(replace(replace(replace(replace(replace(@@global.sql_mode, 'NO_BACKSLASH_ESCAPES', ''), 'HIGH_NOT_PRECEDENCE', ''), 'PIPES_AS_CONCAT', ''), 'REAL_AS_FLOAT', ''), 'IGNORE_SPACE', ''), 'ANSI_QUOTES', ''), 'ANSI', '')")
	assert.Contains(t, resetQuery, "sql_safe_updates = 'default'")
}

// ValidateReservedSettings judges the settings a true reservation executes directly on
// its tainted connection: the same validation as the settings pool, returning the
// parse-relevant bits the settings put the session in.
func TestValidateReservedSettings(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	t.Run("parse-relevant bits are returned", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{
			"set sql_safe_updates = 1",
			"set sql_mode = 'PIPES_AS_CONCAT,STRICT_TRANS_TABLES'",
		}, parser)
		require.NoError(t, err)
		assert.Equal(t, sqlparser.SQLModePipesAsConcat, parseMode)
		assert.True(t, setsSQLMode)
	})

	t.Run("the mode the session must not run under is rejected", func(t *testing.T) {
		_, _, err := ValidateReservedSettings([]string{
			"set sql_mode = 'NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES'",
		}, parser)
		require.EqualError(t, err, "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported")
	})

	t.Run("HIGH_NOT_PRECEDENCE is accepted and carries its parse bit", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{
			"set sql_mode = 'HIGH_NOT_PRECEDENCE'",
		}, parser)
		require.NoError(t, err)
		assert.Equal(t, sqlparser.SQLModeHighNotPrecedence, parseMode)
		assert.True(t, setsSQLMode)
	})

	t.Run("mode-free settings carry no parse bits", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{"SET sql_mode = 'STRICT_TRANS_TABLES'", "set sql_safe_updates=1"}, parser)
		require.NoError(t, err)
		assert.Equal(t, sqlparser.SQLMode(0), parseMode)
		assert.True(t, setsSQLMode, "a sql_mode assignment without parse bits still puts the session in a mode")
	})

	t.Run("the last assignment decides the mode the session is in", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{
			"set sql_mode = 'NO_BACKSLASH_ESCAPES', sql_mode = 'PIPES_AS_CONCAT'",
		}, parser)
		require.NoError(t, err)
		assert.Equal(t, sqlparser.SQLModePipesAsConcat, parseMode)
		assert.True(t, setsSQLMode)
	})

	t.Run("settings without a sql_mode assignment leave the session's mode alone", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{"set sql_safe_updates=1", "set @@global.sql_mode = 'ANSI_QUOTES'"}, parser)
		require.NoError(t, err)
		assert.Equal(t, sqlparser.SQLMode(0), parseMode)
		assert.False(t, setsSQLMode)
		_, setsSQLMode, err = ValidateReservedSettings(nil, parser)
		require.NoError(t, err)
		assert.False(t, setsSQLMode)
	})

	t.Run("values that cannot be judged upfront are rejected", func(t *testing.T) {
		// the settings are applied with no read-back afterwards, so anything that
		// cannot be judged upfront must not reach the connection
		_, _, err := ValidateReservedSettings([]string{"this is not SQL"}, parser)
		require.ErrorContains(t, err, "failed to parse connection setting: this is not SQL")
		_, _, err = ValidateReservedSettings([]string{"select 1 from dual"}, parser)
		require.EqualError(t, err, "connection setting is not a SET statement: select 1 from dual")
		_, _, err = ValidateReservedSettings([]string{"set sql_mode = concat('AN', 'SI')"}, parser)
		require.EqualError(t, err, "non-constant sql_mode value in connection settings: set sql_mode = concat('AN', 'SI')")
	})

	t.Run("invalid sql_mode values are rejected", func(t *testing.T) {
		_, _, err := ValidateReservedSettings([]string{"set sql_mode = 'BOGUS'"}, parser)
		require.EqualError(t, err, "Variable 'sql_mode' can't be set to the value of 'BOGUS'")
	})
}

func TestSetPlanSQLMode(t *testing.T) {
	env := vtenv.NewTestEnv()
	tables := map[string]*schema.Table{}

	tests := []struct {
		sql             string
		expectedErr     string
		readBackSQLMode bool
		setsSQLMode     bool
		parseBits       sqlparser.SQLMode
		fullQuery       string
	}{{
		sql:         "set @@sql_mode = 'ONLY_FULL_GROUP_BY'",
		setsSQLMode: true,
		fullQuery:   "set @@sql_mode = 'ONLY_FULL_GROUP_BY'",
	}, {
		// forwarded modes stay as written; the parse bits are still recorded
		sql:         "set @@sql_mode = 'ansi_quotes'",
		setsSQLMode: true,
		parseBits:   sqlparser.SQLModeANSIQuotes,
		fullQuery:   "set @@sql_mode = 'ansi_quotes'",
	}, {
		// HIGH_NOT_PRECEDENCE is forwarded and its parse bit recorded
		sql:         "set session sql_mode = 'HIGH_NOT_PRECEDENCE'",
		setsSQLMode: true,
		parseBits:   sqlparser.SQLModeHighNotPrecedence,
		fullQuery:   "set @@sql_mode = 'HIGH_NOT_PRECEDENCE'",
	}, {
		// the mode the MySQL session must not run under is rejected, however it
		// is spelled — here the numeric bitmask for NO_BACKSLASH_ESCAPES
		sql:         "set sql_mode = 1048576",
		expectedErr: "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported",
	}, {
		sql:         "set sql_mode = 'BOGUS'",
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// the global scope is the operator's domain, not the session's
		sql:       "set @@global.sql_mode = 'ANSI'",
		fullQuery: "set @@global.sql_mode = 'ANSI'",
	}, {
		// non-constant values cannot be judged at plan time: the plan asks the executor to
		// read back and verify the applied value instead
		sql:             "set @@sql_mode = concat('IGNORE', '_SPACE')",
		readBackSQLMode: true,
		fullQuery:       "set @@sql_mode = concat('IGNORE', '_SPACE')",
	}, {
		// a non-constant assignment to another variable needs no sql_mode verification
		sql:       "set @@sql_safe_updates = if(1 = 1, 0, 1)",
		fullQuery: "set @@sql_safe_updates = if(1 = 1, 0, 1)",
	}, {
		// a non-constant global-scope assignment is the operator's domain as well
		sql:       "set @@global.sql_mode = concat('AN', 'SI')",
		fullQuery: "set @@global.sql_mode = concat('AN', 'SI')",
	}, {
		// a multi-assignment SET is applied atomically by MySQL: none of its assignments
		// take effect when one fails. A non-constant sql_mode can only be judged after
		// the statement ran, when its other assignments are already applied, so it is
		// rejected upfront
		sql:         "set @@sql_safe_updates = 1, @@sql_mode = concat('AN', 'SI')",
		expectedErr: "non-constant sql_mode value in a multi-assignment SET: set @@sql_safe_updates = 1, @@sql_mode = concat('AN', 'SI')",
	}, {
		// a constant sql_mode in a multi-assignment SET is judged at plan time as usual
		sql:         "set @@sql_safe_updates = if(1 = 1, 0, 1), @@sql_mode = 'STRICT_TRANS_TABLES'",
		setsSQLMode: true,
		fullQuery:   "set @@sql_safe_updates = if(1 = 1, 0, 1), @@sql_mode = 'STRICT_TRANS_TABLES'",
	}, {
		// MySQL applies duplicate assignments in order, so the last one decides the mode
		// the session ends up in; an earlier NO_BACKSLASH_ESCAPES is superseded before the
		// connection processes anything else
		sql:         "set @@sql_mode = 'NO_BACKSLASH_ESCAPES', @@sql_mode = ''",
		setsSQLMode: true,
		fullQuery:   "set @@sql_mode = 'NO_BACKSLASH_ESCAPES', @@sql_mode = ''",
	}, {
		sql:         "set @@sql_mode = '', @@sql_mode = 'NO_BACKSLASH_ESCAPES'",
		expectedErr: "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported",
	}, {
		sql:         "set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = 'ANSI_QUOTES'",
		setsSQLMode: true,
		parseBits:   sqlparser.SQLModeANSIQuotes,
		fullQuery:   "set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = 'ANSI_QUOTES'",
	}, {
		// every constant value is still validated as MySQL validates it in turn
		sql:         "set @@sql_mode = 'BOGUS', @@sql_mode = ''",
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// a superseded non-constant value needs no read-back: MySQL validates it itself
		// and the final constant is judged here
		sql:         "set @@sql_mode = concat('AN', 'SI'), @@sql_mode = 'PIPES_AS_CONCAT'",
		setsSQLMode: true,
		parseBits:   sqlparser.SQLModePipesAsConcat,
		fullQuery:   "set @@sql_mode = concat('AN', 'SI'), @@sql_mode = 'PIPES_AS_CONCAT'",
	}, {
		sql:         "set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = concat('AN', 'SI')",
		expectedErr: "non-constant sql_mode value in a multi-assignment SET: set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = concat('AN', 'SI')",
	}}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			statement, err := env.Parser().Parse(tc.sql)
			require.NoError(t, err)
			plan, err := Build(env, statement, tables, "dbName", false)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, PlanSet, plan.PlanID)
			assert.Equal(t, tc.readBackSQLMode, plan.ReadBackSQLMode)
			assert.Equal(t, tc.setsSQLMode, plan.SetsSQLMode)
			assert.Equal(t, tc.parseBits, plan.SQLModeParseBits)
			assert.Equal(t, tc.fullQuery, plan.FullQuery.Query)
		})
	}
}

// SET_VAR hints are not judged at plan time: a hint applies to the hinted statement's
// execution only and cannot change how that statement's own text is lexed, so the hint
// is forwarded verbatim for MySQL to judge — MySQL warns about and ignores an invalid
// value, as it does for the same hint sent to it directly. This holds for every
// spelling MySQL's hint grammar accepts: quoted, unquoted (an unquoted word is a string
// value in that grammar), and numeric.
func TestSetVarHintSQLModesAreNotJudged(t *testing.T) {
	env := vtenv.NewTestEnv()
	tables := map[string]*schema.Table{}

	for _, sql := range []string{
		"select /*+ SET_VAR(sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE') */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = ' ') */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = 'ANSI') */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = ANSI) */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = ANSI_QUOTES) */ 1 from dual",
		"update /*+ SET_VAR(sql_mode = 'NO_BACKSLASH_ESCAPES') */ t set a = 1",
		"update /*+ SET_VAR(sql_mode = NO_BACKSLASH_ESCAPES) */ t set a = 1",
		"select /*+ SET_VAR(sql_safe_updates = 1) SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = 1048576) */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = 'BOGUS') */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = BOGUS) */ 1 from dual",
		"select /*+ SET_VAR(sql_mode = a.b) */ 1 from dual",
	} {
		t.Run(sql, func(t *testing.T) {
			statement, err := env.Parser().Parse(sql)
			require.NoError(t, err)

			plan, err := Build(env, statement, tables, "dbName", false)
			require.NoError(t, err)
			assert.Contains(t, plan.FullQuery.Query, "SET_VAR(sql_mode", "the hint must reach MySQL verbatim")

			// the streaming path builds plans separately
			plan, err = BuildStreaming(env, statement, tables, "dbName")
			require.NoError(t, err)
			assert.Contains(t, plan.FullQuery.Query, "SET_VAR(sql_mode", "the hint must reach MySQL verbatim")
		})
	}
}

// Settings that do not assign sql_mode say so, so that the connection they are
// applied to keeps the parse mode its session is already in.
func TestBuildSettingQuerySetsSQLMode(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()
	_, _, parseMode, setsSQLMode, err := BuildSettingQuery([]string{"set sql_safe_updates = 1"}, parser)
	require.NoError(t, err)
	assert.False(t, setsSQLMode)
	assert.Equal(t, sqlparser.SQLMode(0), parseMode)

	_, _, parseMode, setsSQLMode, err = BuildSettingQuery([]string{"set sql_mode = ''"}, parser)
	require.NoError(t, err)
	assert.True(t, setsSQLMode, "an assignment of the empty mode still assigns it")
	assert.Equal(t, sqlparser.SQLMode(0), parseMode)
}
