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

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// The vttablet layer mirrors vtgate's sql_mode validation for clients that bypass it:
// older vtgates in a mixed-version cluster and direct query-service clients.

func TestBuildSettingQueryRejectsUnsupportedSQLModes(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	tests := []struct {
		settings    []string
		expectedErr string
	}{{
		settings: []string{"set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'", "set sql_safe_updates = 1"},
	}, {
		settings: []string{"set sql_mode = ''"},
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
	}}
	for _, tc := range tests {
		t.Run(tc.settings[len(tc.settings)-1], func(t *testing.T) {
			query, resetQuery, err := BuildSettingQuery(tc.settings, parser)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, query)
			assert.NotEmpty(t, resetQuery)
		})
	}
}

// The settings reset must not restore `default` for sql_mode: that would re-inherit the
// server's global value including its lexer modes, undoing the neutralization every
// Vitess-created connection starts with. It restores the neutralized global instead.
func TestBuildSettingQueryResetNeutralizesSQLMode(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	query, resetQuery, err := BuildSettingQuery([]string{"set sql_mode = 'STRICT_TRANS_TABLES'", "set sql_safe_updates = 1"}, parser)
	require.NoError(t, err)
	assert.Contains(t, query, "sql_mode = 'STRICT_TRANS_TABLES'")
	assert.Contains(t, resetQuery, "sql_mode = replace(replace(replace(replace(replace(replace(replace(@@global.sql_mode, 'NO_BACKSLASH_ESCAPES', ''), 'HIGH_NOT_PRECEDENCE', ''), 'PIPES_AS_CONCAT', ''), 'REAL_AS_FLOAT', ''), 'IGNORE_SPACE', ''), 'ANSI_QUOTES', ''), 'ANSI', '')")
	assert.Contains(t, resetQuery, "sql_safe_updates = 'default'")
}

func TestSetPlanRejectsUnsupportedSQLModes(t *testing.T) {
	env := vtenv.NewTestEnv()
	tables := map[string]*schema.Table{}

	tests := []struct {
		sql          string
		expectedErr  string
		verifySQLMod bool
	}{{
		sql: "set @@sql_mode = 'ONLY_FULL_GROUP_BY'",
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
		// the global scope is the operator's domain, not the session's
		sql: "set @@global.sql_mode = 'ANSI'",
	}, {
		// non-constant values cannot be judged at plan time: the plan asks the executor to
		// read back and verify the applied value instead
		sql:          "set @@sql_mode = concat('IGNORE', '_SPACE')",
		verifySQLMod: true,
	}, {
		// a non-constant assignment to another variable needs no sql_mode verification
		sql: "set @@sql_safe_updates = if(1 = 1, 0, 1)",
	}, {
		// a non-constant global-scope assignment is the operator's domain as well
		sql: "set @@global.sql_mode = concat('AN', 'SI')",
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
			assert.Equal(t, tc.verifySQLMod, plan.VerifySQLMode)
		})
	}
}

func TestSetVarHintRejectsUnsupportedSQLModes(t *testing.T) {
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
		sql:         "select /*+ SET_VAR(sql_mode = 'ANSI') */ 1 from dual",
		expectedErr: "setting the ANSI sql_mode is unsupported",
	}, {
		sql:         "update /*+ SET_VAR(sql_mode = 'NO_BACKSLASH_ESCAPES') */ t set a = 1",
		expectedErr: "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported",
	}, {
		sql:         "select /*+ SET_VAR(sql_safe_updates = 1) SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ 1 from dual",
		expectedErr: "setting the PIPES_AS_CONCAT sql_mode is unsupported",
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

			// the streaming path builds plans separately and must reject as well
			_, err = BuildStreaming(env, statement, tables, "dbName")
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
