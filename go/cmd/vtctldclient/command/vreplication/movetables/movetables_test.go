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

package movetables

import (
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

var (
	testRoot     *cobra.Command
	testRootOnce sync.Once
)

// registeredTestRoot registers the MoveTables commands exactly once. registerCommands
// adds flags to package-level command variables, so it panics if called more than once
// per test binary.
func registeredTestRoot() *cobra.Command {
	testRootOnce.Do(func() {
		testRoot = &cobra.Command{Use: "test"}
		registerCommands(testRoot)
	})
	return testRoot
}

func TestKeepDataHelpMentionsReverseWorkflowDefault(t *testing.T) {
	root := registeredTestRoot()

	completeCmd, _, err := root.Find([]string{"MoveTables", "complete"})
	require.NoError(t, err)
	require.Contains(t, completeCmd.Flags().Lookup("keep-data").Usage, "Defaults to true for an explicitly specified _reverse workflow unless --keep-data=false is provided.")

	cancelCmd, _, err := root.Find([]string{"MoveTables", "cancel"})
	require.NoError(t, err)
	require.Contains(t, cancelCmd.Flags().Lookup("keep-data").Usage, "Defaults to true for an explicitly specified _reverse workflow unless --keep-data=false is provided.")
}

func TestCompleteRenameTablesDefaultsToRename(t *testing.T) {
	root := registeredTestRoot()

	completeCmd, _, err := root.Find([]string{"MoveTables", "complete"})
	require.NoError(t, err)

	renameFlag := completeCmd.Flags().Lookup("rename-tables")
	require.NotNil(t, renameFlag)
	require.Equal(t, "true", renameFlag.DefValue)
	require.Contains(t, renameFlag.Usage, "--rename-tables=false")
}
