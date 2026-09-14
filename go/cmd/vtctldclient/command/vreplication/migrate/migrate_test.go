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

package migrate

import (
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// registerCommands binds flags to the package-level command vars, so it can
// only run once per test binary; the resulting tree is shared by the tests.
var (
	testRoot         = &cobra.Command{Use: "test"}
	registerTestOnce sync.Once
)

func testCommand(t *testing.T, path ...string) *cobra.Command {
	t.Helper()
	registerTestOnce.Do(func() { registerCommands(testRoot) })
	cmd, _, err := testRoot.Find(path)
	require.NoError(t, err)
	return cmd
}

// TestCreatePreRunETableSelection runs the create command's PreRunE with the
// flag forms an operator or automation actually passes. pflag marks --tables=
// and --all-tables=false as changed while producing an empty list and false,
// so a flag presence check accepts them; the selection-less forms must be
// rejected here rather than reaching the server-side guard (or, against an
// older vtctld, the late "no tables to move" failure).
func TestCreatePreRunETableSelection(t *testing.T) {
	cmd := testCommand(t, "Migrate", "create")

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "--tables", args: []string{"--tables=t1,t2"}},
		{name: "--all-tables", args: []string{"--all-tables"}},
		{name: "--all-tables with --exclude-tables", args: []string{"--all-tables", "--exclude-tables=t2"}},
		{name: "--tables with --exclude-tables", args: []string{"--tables=t1,t2", "--exclude-tables=t2"}},
		{name: "--tables with --all-tables=false", args: []string{"--tables=t1", "--all-tables=false"}},
		{name: "empty --tables= with --all-tables=true", args: []string{"--tables=", "--all-tables=true"}},
		{name: "--tables with --all-tables", args: []string{"--tables=t1", "--all-tables"}, wantErr: "mutually exclusive"},
		{name: "empty --tables= with --exclude-tables", args: []string{"--tables=", "--exclude-tables=t1"}, wantErr: "--exclude-tables requires"},
		{name: "--all-tables=false with --exclude-tables", args: []string{"--all-tables=false", "--exclude-tables=t1"}, wantErr: "--exclude-tables requires"},
		{name: "no table selection", args: nil, wantErr: "tables or all-tables are required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The flags bind to package-level options that persist across
			// parses, so reset the ones under test for each case.
			createOptions.IncludeTables = nil
			createOptions.AllTables = false
			createOptions.ExcludeTables = nil
			require.NoError(t, cmd.ParseFlags(tt.args))

			err := cmd.PreRunE(cmd, nil)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
