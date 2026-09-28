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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// The vttablet validates sql_mode values on its own inbound entry points, settings and
// SET statements, the way vtgate does, for clients that bypass vtgate's validation:
// older vtgates in a mixed-version cluster and direct query-service clients. A value
// carrying a lexer mode the parser honors (sqlparser.HonoredSQLModes) is applied as
// written and recorded, so that the connection is read under it; the other lexer
// modes are rejected. Invalid values are rejected with MySQL's errors.

func TestBuildSettingQuerySQLMode(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	tests := []struct {
		settings      []string
		expectedErr   string
		expectedQuery string
		expectedMode  sqlmode.Mode
	}{{
		settings:      []string{"set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'", "set sql_safe_updates = 1"},
		expectedQuery: "set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE', sql_safe_updates = 1",
	}, {
		settings:      []string{"set sql_mode = ''"},
		expectedQuery: "set sql_mode = ''",
	}, {
		// a lexer mode the parser honors is applied as written and recorded, so that
		// queries on the connection are read under it
		settings:      []string{"set sql_safe_updates = 1", "set sql_mode = 'pipes_as_concat,STRICT_TRANS_TABLES'"},
		expectedQuery: "set sql_safe_updates = 1, sql_mode = 'pipes_as_concat,STRICT_TRANS_TABLES'",
		expectedMode:  sqlmode.PipesAsConcat,
	}, {
		// however the value spells it
		settings:      []string{"set sql_mode = 2"},
		expectedQuery: "set sql_mode = 2",
		expectedMode:  sqlmode.PipesAsConcat,
	}, {
		settings:    []string{"set sql_mode = 'ANSI'"},
		expectedErr: "setting the ANSI sql_mode is unsupported",
	}, {
		settings:    []string{"set sql_safe_updates = 1", "set sql_mode = 'STRICT_TRANS_TABLES,ANSI_QUOTES'"},
		expectedErr: "setting the ANSI_QUOTES sql_mode is unsupported",
	}, {
		settings:    []string{"set sql_mode = 'IGNORE_SPACE'"},
		expectedErr: "setting the IGNORE_SPACE sql_mode is unsupported",
	}, {
		settings:    []string{"set sql_mode = 'BOGUS'"},
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// settings are applied with no verification afterwards; a value that cannot be
		// judged upfront is rejected
		settings:    []string{"set sql_safe_updates = 1", "set sql_mode = concat('AN', 'SI')"},
		expectedErr: "non-constant sql_mode value in connection settings: set sql_mode = concat('AN', 'SI')",
	}, {
		// MySQL accepts an unquoted mode name as the equivalent string: it is a constant
		settings:      []string{"set sql_mode = TRADITIONAL"},
		expectedQuery: "set sql_mode = TRADITIONAL",
	}, {
		settings:      []string{"set sql_mode = strict_trans_tables"},
		expectedQuery: "set sql_mode = strict_trans_tables",
	}, {
		// and an honored lexer mode spelled unquoted is recorded like its quoted
		// spelling, so that queries on the connection are read under it
		settings:      []string{"set sql_mode = PIPES_AS_CONCAT"},
		expectedQuery: "set sql_mode = PIPES_AS_CONCAT",
		expectedMode:  sqlmode.PipesAsConcat,
	}, {
		settings:      []string{"set sql_safe_updates = 1", "set sql_mode = pipes_as_concat"},
		expectedQuery: "set sql_safe_updates = 1, sql_mode = pipes_as_concat",
		expectedMode:  sqlmode.PipesAsConcat,
	}, {
		settings:    []string{"set sql_mode = ANSI"},
		expectedErr: "setting the ANSI sql_mode is unsupported",
	}, {
		settings:    []string{"set sql_mode = BOGUS"},
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// a qualified name is never a mode name; MySQL rejects it as the wrong type
		settings:    []string{"set sql_mode = t.TRADITIONAL"},
		expectedErr: "Incorrect argument type to variable 'sql_mode'",
	}, {
		settings:    []string{"set sql_mode = t.PIPES_AS_CONCAT"},
		expectedErr: "Incorrect argument type to variable 'sql_mode'",
	}}
	for _, tc := range tests {
		t.Run(tc.settings[len(tc.settings)-1], func(t *testing.T) {
			query, resetQuery, parseMode, _, err := BuildSettingQuery(tc.settings, parser, false)
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

// Settings that do not assign sql_mode say so, so that the connection they are
// applied to keeps the mode its session is already in.
func TestBuildSettingQuerySetsSQLMode(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()
	_, _, parseMode, setsSQLMode, err := BuildSettingQuery([]string{"set sql_safe_updates = 1"}, parser, false)
	require.NoError(t, err)
	assert.False(t, setsSQLMode)
	assert.Equal(t, sqlmode.Mode(0), parseMode)

	_, _, parseMode, setsSQLMode, err = BuildSettingQuery([]string{"set sql_mode = ''"}, parser, false)
	require.NoError(t, err)
	assert.True(t, setsSQLMode, "an assignment of the empty mode still assigns it")
	assert.Equal(t, sqlmode.Mode(0), parseMode)
}

// ValidateReservedSettings judges the settings a true reservation executes directly on
// its tainted connection: the same validation as the settings pool, returning the
// lexer modes the settings put the session in.
func TestValidateReservedSettings(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	t.Run("an honored lexer mode is returned", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{
			"set sql_safe_updates = 1",
			"set sql_mode = 'PIPES_AS_CONCAT,STRICT_TRANS_TABLES'",
		}, parser, false)
		require.NoError(t, err)
		assert.Equal(t, sqlmode.PipesAsConcat, parseMode)
		assert.True(t, setsSQLMode)
	})

	t.Run("an honored lexer mode spelled unquoted is returned like its quoted spelling", func(t *testing.T) {
		// MySQL accepts an unquoted mode name as the equivalent string
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{"set sql_mode = PIPES_AS_CONCAT"}, parser, false)
		require.NoError(t, err)
		assert.Equal(t, sqlmode.PipesAsConcat, parseMode)
		assert.True(t, setsSQLMode)
		// a qualified name is never a mode name, and MySQL rejects it as the wrong type
		_, _, err = ValidateReservedSettings([]string{"set sql_mode = t.PIPES_AS_CONCAT"}, parser, false)
		require.EqualError(t, err, "Incorrect argument type to variable 'sql_mode'")
	})

	t.Run("the other lexer modes are rejected", func(t *testing.T) {
		_, _, err := ValidateReservedSettings([]string{"set sql_mode = 'ANSI_QUOTES,STRICT_TRANS_TABLES'"}, parser, false)
		require.EqualError(t, err, "setting the ANSI_QUOTES sql_mode is unsupported")
		_, _, err = ValidateReservedSettings([]string{"set sql_mode = 'PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES'"}, parser, false)
		require.EqualError(t, err, "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported")
		_, _, err = ValidateReservedSettings([]string{"set sql_mode = 'ANSI'"}, parser, false)
		require.EqualError(t, err, "setting the ANSI sql_mode is unsupported")
	})

	t.Run("mode-free settings carry no lexer modes", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{"SET sql_mode = 'STRICT_TRANS_TABLES'", "set sql_safe_updates=1"}, parser, false)
		require.NoError(t, err)
		assert.Equal(t, sqlmode.Mode(0), parseMode)
		assert.True(t, setsSQLMode, "a sql_mode assignment without lexer modes still puts the session in a mode")
	})

	t.Run("the last assignment decides the mode the session is in", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{
			"set sql_mode = 'STRICT_TRANS_TABLES', sql_mode = 'PIPES_AS_CONCAT'",
		}, parser, false)
		require.NoError(t, err)
		assert.Equal(t, sqlmode.PipesAsConcat, parseMode)
		assert.True(t, setsSQLMode)
		parseMode, _, err = ValidateReservedSettings([]string{
			"set sql_mode = 'PIPES_AS_CONCAT'", "set sql_mode = 'STRICT_TRANS_TABLES'",
		}, parser, false)
		require.NoError(t, err)
		assert.Equal(t, sqlmode.Mode(0), parseMode)
	})

	t.Run("settings without a sql_mode assignment leave the session's mode alone", func(t *testing.T) {
		parseMode, setsSQLMode, err := ValidateReservedSettings([]string{"set sql_safe_updates=1", "set @@global.sql_mode = 'ANSI_QUOTES'"}, parser, false)
		require.NoError(t, err)
		assert.Equal(t, sqlmode.Mode(0), parseMode)
		assert.False(t, setsSQLMode)
		_, setsSQLMode, err = ValidateReservedSettings(nil, parser, false)
		require.NoError(t, err)
		assert.False(t, setsSQLMode)
	})

	t.Run("values that cannot be judged upfront are rejected", func(t *testing.T) {
		// the settings are applied with no verification afterwards, so anything that
		// cannot be judged upfront must not reach the connection
		_, _, err := ValidateReservedSettings([]string{"this is not SQL"}, parser, false)
		require.ErrorContains(t, err, "failed to parse connection setting: this is not SQL")
		_, _, err = ValidateReservedSettings([]string{"select 1 from dual"}, parser, false)
		require.EqualError(t, err, "connection setting is not a SET statement: select 1 from dual")
		_, _, err = ValidateReservedSettings([]string{"set sql_mode = concat('AN', 'SI')"}, parser, false)
		require.EqualError(t, err, "non-constant sql_mode value in connection settings: set sql_mode = concat('AN', 'SI')")
	})

	t.Run("invalid sql_mode values are rejected", func(t *testing.T) {
		_, _, err := ValidateReservedSettings([]string{"set sql_mode = 'BOGUS'"}, parser, false)
		require.EqualError(t, err, "Variable 'sql_mode' can't be set to the value of 'BOGUS'")
	})
}

// The settings reset must not restore `default` for sql_mode: that would re-inherit the
// server's global value including its lexer modes, undoing the neutralization every
// Vitess-created connection starts with. It restores the neutralized global instead.
func TestBuildSettingQueryResetNeutralizesSQLMode(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	query, resetQuery, parseMode, setsSQLMode, err := BuildSettingQuery([]string{"set sql_mode = 'STRICT_TRANS_TABLES'", "set sql_safe_updates = 1"}, parser, false)
	require.NoError(t, err)
	assert.True(t, setsSQLMode)
	assert.Equal(t, sqlmode.Mode(0), parseMode)
	assert.Contains(t, query, "sql_mode = 'STRICT_TRANS_TABLES'")
	assert.Contains(t, resetQuery, "sql_mode = replace(replace(replace(replace(replace(replace(replace(@@global.sql_mode, 'NO_BACKSLASH_ESCAPES', ''), 'HIGH_NOT_PRECEDENCE', ''), 'PIPES_AS_CONCAT', ''), 'REAL_AS_FLOAT', ''), 'IGNORE_SPACE', ''), 'ANSI_QUOTES', ''), 'ANSI', '')")
	assert.Contains(t, resetQuery, "sql_safe_updates = default")
}

// Every setting other than sql_mode, foreign_key_checks and unique_checks is reset with
// the DEFAULT keyword. MySQL accepts `SET var = DEFAULT` for any system variable and
// rejects the string 'default' for most of them, so the reset must use the keyword for
// the pool to be able to reuse the connection rather than replace it.
func TestBuildSettingQueryResetUsesDefaultKeyword(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	_, resetQuery, _, _, err := BuildSettingQuery([]string{"set sql_safe_updates = 1", "set @@session.sql_select_limit = 10"}, parser, false)
	require.NoError(t, err)
	assert.Equal(t, "set sql_safe_updates = default, @@sql_select_limit = default", resetQuery)
}

// MySQL Bug#121262: `SET SESSION foreign_key_checks = DEFAULT` and the same for
// unique_checks set the session value to the opposite of the global value on every
// MySQL version, so a `default` reset would hand the next caller a pooled connection
// with the checks off. The reset restores the global value explicitly instead.
func TestBuildSettingQueryResetRestoresGlobalForeignKeyAndUniqueChecks(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	_, resetQuery, _, _, err := BuildSettingQuery([]string{"set @@foreign_key_checks = 0, @@session.unique_checks = 0", "set sql_safe_updates = 1"}, parser, false)
	require.NoError(t, err)
	assert.Equal(t, "set @@foreign_key_checks = @@global.foreign_key_checks, @@unique_checks = @@global.unique_checks, sql_safe_updates = default", resetQuery)
}

func TestSetPlanSQLMode(t *testing.T) {
	env := vtenv.NewTestEnv()
	tables := map[string]*schema.Table{}

	tests := []struct {
		sql           string
		expectedErr   string
		verifySQLMode bool
		setsSQLMode   bool
		parseBits     sqlmode.Mode
	}{{
		sql:         "set @@sql_mode = 'ONLY_FULL_GROUP_BY'",
		setsSQLMode: true,
	}, {
		// an honored lexer mode is applied as written and recorded
		sql:         "set @@sql_mode = 'pipes_as_concat,STRICT_TRANS_TABLES'",
		setsSQLMode: true,
		parseBits:   sqlmode.PipesAsConcat,
	}, {
		sql:         "set sql_mode = 2",
		setsSQLMode: true,
		parseBits:   sqlmode.PipesAsConcat,
	}, {
		sql:         "set @@sql_mode = 'ansi_quotes'",
		expectedErr: "setting the ANSI_QUOTES sql_mode is unsupported",
	}, {
		sql:         "set session sql_mode = 'HIGH_NOT_PRECEDENCE'",
		expectedErr: "setting the HIGH_NOT_PRECEDENCE sql_mode is unsupported",
	}, {
		sql:         "set sql_mode = 1048576",
		expectedErr: "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported",
	}, {
		sql:         "set sql_mode = 'BOGUS'",
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// the global scope is the operator's domain, not the session's
		sql: "set @@global.sql_mode = 'ANSI'",
	}, {
		// non-constant values cannot be judged at plan time: the plan asks the executor to
		// read back and verify the applied value instead
		sql:           "set @@sql_mode = concat('IGNORE', '_SPACE')",
		verifySQLMode: true,
	}, {
		// a non-constant assignment to another variable needs no sql_mode verification
		sql: "set @@sql_safe_updates = if(1 = 1, 0, 1)",
	}, {
		// a non-constant global-scope assignment is the operator's domain as well
		sql: "set @@global.sql_mode = concat('AN', 'SI')",
	}, {
		// a multi-assignment SET is applied atomically by MySQL: none of its assignments
		// take effect when one fails. A non-constant sql_mode can only be judged after
		// execution, by which time the other assignments would already be applied, so
		// it is rejected upfront
		sql:         "set @@sql_safe_updates = 1, @@sql_mode = concat('AN', 'SI')",
		expectedErr: "non-constant sql_mode value in a multi-assignment SET: set @@sql_safe_updates = 1, @@sql_mode = concat('AN', 'SI')",
	}, {
		// a constant sql_mode in a multi-assignment SET is judged at plan time as usual
		sql:         "set @@sql_safe_updates = if(1 = 1, 0, 1), @@sql_mode = 'STRICT_TRANS_TABLES'",
		setsSQLMode: true,
	}, {
		// MySQL applies duplicate assignments in order, so the last one decides the mode
		// the session ends up in
		sql:         "set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = ''",
		setsSQLMode: true,
	}, {
		sql:         "set @@sql_mode = '', @@sql_mode = 'PIPES_AS_CONCAT'",
		setsSQLMode: true,
		parseBits:   sqlmode.PipesAsConcat,
	}, {
		// every constant value is still validated in turn
		sql:         "set @@sql_mode = 'BOGUS', @@sql_mode = ''",
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		sql:         "set @@sql_mode = 'ANSI_QUOTES', @@sql_mode = 'PIPES_AS_CONCAT'",
		expectedErr: "setting the ANSI_QUOTES sql_mode is unsupported",
	}, {
		// a superseded non-constant value needs no read-back: MySQL validates it itself
		// and the final constant is judged here
		sql:         "set @@sql_mode = concat('AN', 'SI'), @@sql_mode = 'PIPES_AS_CONCAT'",
		setsSQLMode: true,
		parseBits:   sqlmode.PipesAsConcat,
	}, {
		sql:         "set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = concat('AN', 'SI')",
		expectedErr: "non-constant sql_mode value in a multi-assignment SET: set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = concat('AN', 'SI')",
	}, {
		// MySQL accepts an unquoted mode name as the equivalent string: it is a constant,
		// judged at plan time like its quoted spelling
		sql:         "set @@sql_mode = TRADITIONAL",
		setsSQLMode: true,
	}, {
		sql:         "set sql_mode = strict_trans_tables",
		setsSQLMode: true,
	}, {
		// and an honored lexer mode spelled unquoted is recorded like its quoted
		// spelling, so that queries on the connection are read under it
		sql:         "set @@sql_mode = PIPES_AS_CONCAT",
		setsSQLMode: true,
		parseBits:   sqlmode.PipesAsConcat,
	}, {
		sql:         "set session sql_mode = pipes_as_concat",
		setsSQLMode: true,
		parseBits:   sqlmode.PipesAsConcat,
	}, {
		sql:         "set @@sql_mode = 'PIPES_AS_CONCAT', @@sql_mode = STRICT_TRANS_TABLES",
		setsSQLMode: true,
	}, {
		sql:         "set @@sql_mode = ANSI",
		expectedErr: "setting the ANSI sql_mode is unsupported",
	}, {
		sql:         "set @@sql_mode = ANSI_QUOTES",
		expectedErr: "setting the ANSI_QUOTES sql_mode is unsupported",
	}, {
		sql:         "set session sql_mode = BOGUS",
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		sql:         "set @@sql_safe_updates = if(1 = 1, 0, 1), @@sql_mode = STRICT_TRANS_TABLES",
		setsSQLMode: true,
	}, {
		// a qualified name is never a mode name: MySQL rejects it as the wrong argument
		// type, whatever the qualifier, and so does the plan
		sql:         "set @@sql_mode = t.TRADITIONAL",
		expectedErr: "Incorrect argument type to variable 'sql_mode'",
	}, {
		sql:         "set @@sql_mode = t.PIPES_AS_CONCAT",
		expectedErr: "Incorrect argument type to variable 'sql_mode'",
	}, {
		sql:         "set sql_mode = a.b.c",
		expectedErr: "Incorrect argument type to variable 'sql_mode'",
	}, {
		sql:         "set @@sql_safe_updates = 1, @@sql_mode = t.TRADITIONAL",
		expectedErr: "Incorrect argument type to variable 'sql_mode'",
	}, {
		// a superseded qualified name is still rejected: MySQL judges every assignment
		sql:         "set @@sql_mode = t.PIPES_AS_CONCAT, @@sql_mode = ''",
		expectedErr: "Incorrect argument type to variable 'sql_mode'",
	}}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := env.Parser().Parse(tc.sql)
			require.NoError(t, err)
			plan, err := Build(env, stmt, tables, "dbName", false)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, PlanSet, plan.PlanID)
			assert.Equal(t, tc.verifySQLMode, plan.VerifySQLMode)
			assert.Equal(t, tc.setsSQLMode, plan.SetsSQLMode)
			assert.Equal(t, tc.parseBits, plan.SQLModeParseBits)
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

// A connection setting is applied to the connection with no table ACL check, so
// a subquery in one would read tables unchecked. Settings carry constants (vtgate
// evaluates a SET's expression on a shard and sends the value), so both the
// settings-pool path and the reservation path reject a subquery upfront under
// strict table ACL. Without it there is nothing to protect, and a vtgate from
// before the value was sent still sends a targeted session's SET expression as
// written, so the setting is accepted as it always was.
func TestSettingsRejectSubqueries(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	tests := []struct {
		setting  string
		subquery bool
	}{
		{setting: "set @@sql_select_limit = (select if(v = 'x', 1, 2) from secret where id = 1)", subquery: true},
		{setting: "set @@sql_safe_updates = exists (select 1 from secret)", subquery: true},
		{setting: "set @@sql_select_limit = 1 + (select count(*) from secret)", subquery: true},
		{setting: "set @@sql_select_limit = if((select v from secret limit 1) = 'x', 1, 2)", subquery: true},
		{setting: "set @@sql_select_limit = 10"},
		{setting: "set @@sql_select_limit = default"},
		// a non-constant expression that reads no table is not this check's concern
		{setting: "set @@sql_select_limit = 1 + 1"},
	}
	for _, strictTableACL := range []bool{true, false} {
		for _, tc := range tests {
			t.Run(fmt.Sprintf("strict=%t %s", strictTableACL, tc.setting), func(t *testing.T) {
				settings := []string{"set @@sql_safe_updates = 1", tc.setting}
				expectedErr := "connection setting must not contain a subquery: " + tc.setting
				rejected := tc.subquery && strictTableACL

				query, resetQuery, _, _, err := BuildSettingQuery(settings, parser, strictTableACL)
				if rejected {
					require.EqualError(t, err, expectedErr)
				} else {
					require.NoError(t, err)
					assert.NotEmpty(t, query)
					assert.NotEmpty(t, resetQuery)
				}

				_, _, err = ValidateReservedSettings(settings, parser, strictTableACL)
				if rejected {
					require.EqualError(t, err, expectedErr)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
