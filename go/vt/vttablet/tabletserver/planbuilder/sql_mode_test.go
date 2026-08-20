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

// The vttablet accepts every valid sql_mode on its own inbound entry points — settings,
// SET statements, SET_VAR hints — parses queries under the parse-relevant modes itself,
// and applies a value with those modes stripped to MySQL, which must lex the
// vttablet-generated text under the default rules. Invalid values are still rejected
// with MySQL's errors.

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
		// the ANSI combination expands; its execution-relevant members survive the strip
		settings:      []string{"set sql_mode = 'ANSI'"},
		expectedQuery: "set sql_mode = 'REAL_AS_FLOAT,ONLY_FULL_GROUP_BY'",
		expectedMode:  sqlparser.ParseSQLMode("ANSI"),
	}, {
		settings:      []string{"set sql_safe_updates = 1", "set sql_mode = 'STRICT_TRANS_TABLES,ANSI_QUOTES'"},
		expectedQuery: "set sql_safe_updates = 1, sql_mode = 'STRICT_TRANS_TABLES'",
		expectedMode:  sqlparser.SQLModeANSIQuotes,
	}, {
		settings:      []string{"set sql_mode = 'IGNORE_SPACE'"},
		expectedQuery: "set sql_mode = ''",
		expectedMode:  sqlparser.SQLModeIgnoreSpace,
	}, {
		// numeric bitmask for NO_BACKSLASH_ESCAPES
		settings:      []string{"set sql_mode = 1048576"},
		expectedQuery: "set sql_mode = ''",
		expectedMode:  sqlparser.SQLModeNoBackslashEscapes,
	}, {
		settings:    []string{"set sql_mode = 'BOGUS'"},
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// settings are applied with no verification afterwards; a value that cannot be
		// judged upfront is rejected
		settings:    []string{"set sql_safe_updates = 1", "set sql_mode = concat('AN', 'SI')"},
		expectedErr: "non-constant sql_mode value in connection settings: set sql_mode = concat('AN', 'SI')",
	}}
	for _, tc := range tests {
		t.Run(tc.settings[len(tc.settings)-1], func(t *testing.T) {
			query, resetQuery, parseMode, err := BuildSettingQuery(tc.settings, parser)
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

	query, resetQuery, parseMode, err := BuildSettingQuery([]string{"set sql_mode = 'STRICT_TRANS_TABLES'", "set sql_safe_updates = 1"}, parser)
	require.NoError(t, err)
	assert.Equal(t, sqlparser.SQLMode(0), parseMode)
	assert.Contains(t, query, "sql_mode = 'STRICT_TRANS_TABLES'")
	assert.Contains(t, resetQuery, "sql_mode = replace(replace(replace(replace(replace(replace(replace(@@global.sql_mode, 'NO_BACKSLASH_ESCAPES', ''), 'HIGH_NOT_PRECEDENCE', ''), 'PIPES_AS_CONCAT', ''), 'REAL_AS_FLOAT', ''), 'IGNORE_SPACE', ''), 'ANSI_QUOTES', ''), 'ANSI', '')")
	assert.Contains(t, resetQuery, "sql_safe_updates = 'default'")
}

// BuildReservedSettings prepares the settings a true reservation executes directly on
// its tainted connection: same validation and stripping as the settings pool, with
// non-SET strings passed through untouched for MySQL to judge.
func TestBuildReservedSettings(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	t.Run("parse-relevant modes are stripped and returned", func(t *testing.T) {
		applied, parseMode, err := BuildReservedSettings([]string{
			"set sql_safe_updates = 1",
			"set sql_mode = 'PIPES_AS_CONCAT,STRICT_TRANS_TABLES'",
		}, parser)
		require.NoError(t, err)
		assert.Equal(t, []string{
			"set sql_safe_updates = 1",
			"set sql_mode = 'STRICT_TRANS_TABLES'",
		}, applied)
		assert.Equal(t, sqlparser.SQLModePipesAsConcat, parseMode)
	})

	t.Run("mode-free settings pass through byte-identical", func(t *testing.T) {
		settings := []string{"SET sql_mode = 'STRICT_TRANS_TABLES'", "set sql_safe_updates=1"}
		applied, parseMode, err := BuildReservedSettings(settings, parser)
		require.NoError(t, err)
		assert.Equal(t, settings, applied)
		assert.Equal(t, sqlparser.SQLMode(0), parseMode)
	})

	t.Run("strings that do not parse as SET statements are left for MySQL", func(t *testing.T) {
		settings := []string{"this is not SQL"}
		applied, parseMode, err := BuildReservedSettings(settings, parser)
		require.NoError(t, err)
		assert.Equal(t, settings, applied)
		assert.Equal(t, sqlparser.SQLMode(0), parseMode)
	})

	t.Run("invalid sql_mode values are rejected", func(t *testing.T) {
		_, _, err := BuildReservedSettings([]string{"set sql_mode = 'BOGUS'"}, parser)
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
		sql:         "set @@sql_mode = 'ansi_quotes'",
		setsSQLMode: true,
		parseBits:   sqlparser.SQLModeANSIQuotes,
		fullQuery:   "set @@sql_mode = ''",
	}, {
		sql:         "set session sql_mode = 'HIGH_NOT_PRECEDENCE'",
		setsSQLMode: true,
		parseBits:   sqlparser.SQLModeHighNotPrecedence,
		fullQuery:   "set @@sql_mode = ''",
	}, {
		sql:         "set sql_mode = 1048576",
		setsSQLMode: true,
		parseBits:   sqlparser.SQLModeNoBackslashEscapes,
		fullQuery:   "set sql_mode = ''",
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

func TestSetVarHintSQLModes(t *testing.T) {
	env := vtenv.NewTestEnv()
	tables := map[string]*schema.Table{}

	tests := []struct {
		sql         string
		expectedErr string
	}{{
		sql: "select /*+ SET_VAR(sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE') */ 1 from dual",
	}, {
		// the placeholder for the empty mode
		sql: "select /*+ SET_VAR(sql_mode = ' ') */ 1 from dual",
	}, {
		// parse-relevant modes in a SET_VAR hint are accepted: the hint applies to the
		// hinted statement's execution only and does not change how that statement's own
		// text is lexed, so forwarding it is harmless
		sql: "select /*+ SET_VAR(sql_mode = 'ANSI') */ 1 from dual",
	}, {
		sql: "update /*+ SET_VAR(sql_mode = 'NO_BACKSLASH_ESCAPES') */ t set a = 1",
	}, {
		sql: "select /*+ SET_VAR(sql_safe_updates = 1) SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ 1 from dual",
	}, {
		// invalid values are still rejected with MySQL's error
		sql:         "select /*+ SET_VAR(sql_mode = 'BOGUS') */ 1 from dual",
		expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		// other variables are not this check's concern
		sql: "select /*+ SET_VAR(sql_safe_updates = 1) */ 1 from dual",
	}}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			statement, err := env.Parser().Parse(tc.sql)
			require.NoError(t, err)

			_, err = Build(env, statement, tables, "dbName", false)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}

			// the streaming path builds plans separately and must validate as well
			_, err = BuildStreaming(env, statement, tables, "dbName")
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
