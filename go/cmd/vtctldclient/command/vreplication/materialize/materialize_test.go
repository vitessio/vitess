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

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestCancelKeepData pins the behavior that issue #20711 reported: cancelling a
// Materialize workflow must not drop the materialized tables unless the caller
// explicitly asks for it. The request must always carry keep_data, because an
// omitted keep_data is resolved server-side to false and drops the data.
func TestCancelKeepData(t *testing.T) {
	root := &cobra.Command{Use: "test"}
	registerCommands(root)

	cancelCmd, _, err := root.Find([]string{"Materialize", "cancel"})
	require.NoError(t, err)

	keepDataFlag := cancelCmd.Flags().Lookup("keep-data")
	require.NotNil(t, keepDataFlag, "Materialize cancel must expose --keep-data")
	require.Equal(t, "true", keepDataFlag.DefValue, "Materialize cancel must preserve target data by default")

	// Restore the registered default before each parse so the subtests do not
	// depend on each other's ordering.
	parseFlags := func(t *testing.T, args ...string) {
		t.Helper()
		require.NoError(t, keepDataFlag.Value.Set(keepDataFlag.DefValue))
		keepDataFlag.Changed = false
		require.NoError(t, cancelCmd.ParseFlags(args))
	}

	t.Run("omitted keep-data preserves the materialized tables", func(t *testing.T) {
		parseFlags(t)

		req := buildCancelRequest()
		require.NotNil(t, req.KeepData, "keep_data must be sent explicitly; omitting it drops the target tables")
		require.True(t, *req.KeepData)
	})

	t.Run("explicit --keep-data=false drops the materialized tables", func(t *testing.T) {
		parseFlags(t, "--keep-data=false")

		req := buildCancelRequest()
		require.NotNil(t, req.KeepData)
		require.False(t, *req.KeepData)
	})
}
