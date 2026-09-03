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

package materialize

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
)

// The table settings are validated with a parser configured from the command
// line, so they can only be parsed once every flag has been applied: flags are
// applied in the order given, and --mysql-server-version may follow
// --table-settings.
func TestTableSettingsParsedAfterAllFlags(t *testing.T) {
	// the two flags as registerCommands registers them on the create command
	newFlags := func(ts *tableSettings) *pflag.FlagSet {
		fs := pflag.NewFlagSet("create", pflag.ContinueOnError)
		fs.Var(ts, "table-settings", "")
		fs.StringVar(&common.CreateOptions.MySQLServerVersion, "mysql-server-version", "8.0.40-Vitess", "")
		return fs
	}
	settings := `[{"target_table": "rollup", "source_expression": "select 'total' as rollupname, count(*) as kount from customer group by rollupname"}]`
	for _, args := range [][]string{
		{"--table-settings", settings, "--mysql-server-version", "5.7.31"},
		{"--mysql-server-version", "5.7.31", "--table-settings", settings},
	} {
		var ts tableSettings
		require.NoError(t, newFlags(&ts).Parse(args))
		require.NoError(t, ts.parse(), "%v", args)
		require.Len(t, ts.val, 1, "%v", args)
		assert.Equal(t, "rollup", ts.val[0].TargetTable)
		assert.False(t, ts.parser.IsMySQL80AndAbove(), "the parser must use the version given, whichever flag came first: %v", args)
	}

	var invalid tableSettings
	require.NoError(t, newFlags(&invalid).Parse([]string{"--table-settings", `[{"target_table": "t", "source_expression": "not valid sql"}]`}))
	require.ErrorContains(t, invalid.parse(), "invalid source_expression")

	// an absent flag leaves the settings empty; an explicitly empty one is an error
	var absent tableSettings
	require.NoError(t, newFlags(&absent).Parse(nil))
	require.NoError(t, absent.parse())
	assert.Nil(t, absent.val)
	var empty tableSettings
	require.NoError(t, newFlags(&empty).Parse([]string{"--table-settings", ""}))
	require.Error(t, empty.parse())
}
