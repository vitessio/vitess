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
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testRoot     *cobra.Command
	testRootOnce sync.Once
)

// testCommands registers the Materialize commands once per test binary: the
// commands are package globals, and registering their flags twice panics.
func testCommands() *cobra.Command {
	testRootOnce.Do(func() {
		testRoot = &cobra.Command{Use: "test"}
		registerCommands(testRoot)
	})
	return testRoot
}

// The table settings are validated with a parser configured by
// --mysql-server-version, and flags are applied in the order given on the
// command line, so the settings can only be parsed once every flag has been
// applied. The source expression below is only valid when the parser skips
// the version comment, which it does for a MySQL version below 8.0.
func TestTableSettingsParsedAfterAllFlags(t *testing.T) {
	createCmd, _, err := testCommands().Find([]string{"Materialize", "create"})
	require.NoError(t, err)

	// Restore the flags before each parse so the cases do not depend on each
	// other's ordering.
	parse := func(t *testing.T, args ...string) error {
		t.Helper()
		for _, name := range []string{"table-settings", "mysql-server-version"} {
			f := createCmd.Flags().Lookup(name)
			require.NotNil(t, f)
			require.NoError(t, f.Value.Set(f.DefValue))
			f.Changed = false
		}
		createOptions.tableSettings = nil
		require.NoError(t, createCmd.ParseFlags(args))
		return parseTableSettings(createCmd)
	}
	settings := `[{"target_table": "rollup", "source_expression": "select count(*) as kount /*!80000 from nowhere at all */ from customer"}]`

	t.Run("the version given after the settings is honoured", func(t *testing.T) {
		require.NoError(t, parse(t, "--table-settings", settings, "--mysql-server-version", "5.7.31"))
		require.Len(t, createOptions.tableSettings, 1)
		assert.Equal(t, "rollup", createOptions.tableSettings[0].TargetTable)
	})

	t.Run("the version given before the settings is honoured", func(t *testing.T) {
		require.NoError(t, parse(t, "--mysql-server-version", "5.7.31", "--table-settings", settings))
		require.Len(t, createOptions.tableSettings, 1)
		assert.Equal(t, "rollup", createOptions.tableSettings[0].TargetTable)
	})

	t.Run("the default version parses the version comment", func(t *testing.T) {
		require.ErrorContains(t, parse(t, "--table-settings", settings), "invalid source_expression")
	})

	t.Run("an absent flag leaves the settings empty", func(t *testing.T) {
		require.NoError(t, parse(t))
		assert.Nil(t, createOptions.tableSettings)
	})

	t.Run("an explicitly empty flag is an error", func(t *testing.T) {
		require.ErrorContains(t, parse(t, "--table-settings", ""), "table-settings")
	})
}
