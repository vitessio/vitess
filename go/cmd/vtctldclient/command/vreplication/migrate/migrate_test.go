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
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestValidateTableSelectionFlags(t *testing.T) {
	// newCmd builds a command exposing only the table-selection flags and binds
	// the package-level createOptions the same way the real create command does.
	newCmd := func(tables []string, allTables bool, excludeTables []string) *cobra.Command {
		cmd := &cobra.Command{Use: "create"}
		cmd.Flags().StringSlice("tables", nil, "")
		cmd.Flags().Bool("all-tables", false, "")
		cmd.Flags().StringSlice("exclude-tables", nil, "")

		createOptions.IncludeTables = tables
		createOptions.AllTables = allTables
		createOptions.ExcludeTables = excludeTables

		if tables != nil {
			require.NoError(t, cmd.Flags().Set("tables", strings.Join(tables, ",")))
		}
		if allTables {
			require.NoError(t, cmd.Flags().Set("all-tables", "true"))
		}
		if excludeTables != nil {
			require.NoError(t, cmd.Flags().Set("exclude-tables", strings.Join(excludeTables, ",")))
		}
		return cmd
	}

	tests := []struct {
		name          string
		tables        []string
		allTables     bool
		excludeTables []string
		wantErr       string
	}{
		{name: "only --tables", tables: []string{"t1", "t2"}},
		{name: "only --all-tables", allTables: true},
		{name: "--all-tables with --exclude-tables", allTables: true, excludeTables: []string{"t2"}},
		{name: "--tables and --all-tables together", tables: []string{"t1"}, allTables: true, wantErr: "mutually exclusive"},
		{name: "--exclude-tables without --all-tables", tables: []string{"t1"}, excludeTables: []string{"t2"}, wantErr: "can only be used together with --all-tables"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				createOptions.IncludeTables = nil
				createOptions.AllTables = false
				createOptions.ExcludeTables = nil
			}()
			err := validateTableSelectionFlags(newCmd(tt.tables, tt.allTables, tt.excludeTables))
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
