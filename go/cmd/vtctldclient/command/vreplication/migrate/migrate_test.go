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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateTableSelectionFlags(t *testing.T) {
	// The helper checks the effective option values, mirroring the server-side
	// validation in workflow.Server.moveTablesCreate: only a non-empty include
	// list combined with all-tables is contradictory. An empty --tables= list
	// (pflag marks the flag as changed but produces an empty slice) must stay
	// valid with --all-tables.
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
		{name: "--tables with --exclude-tables", tables: []string{"t1", "t2"}, excludeTables: []string{"t2"}},
		{name: "empty --tables= list with --all-tables", tables: []string{}, allTables: true},
		{name: "--tables and --all-tables together", tables: []string{"t1"}, allTables: true, wantErr: "mutually exclusive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				createOptions.IncludeTables = nil
				createOptions.AllTables = false
				createOptions.ExcludeTables = nil
			}()
			createOptions.IncludeTables = tt.tables
			createOptions.AllTables = tt.allTables
			createOptions.ExcludeTables = tt.excludeTables

			err := validateTableSelectionFlags()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
