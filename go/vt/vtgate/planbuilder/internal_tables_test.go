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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestRejectInternalTableDDLAllowsInternalShapedProcedureNames verifies that
// procedure object names are not treated as table names by the internal table
// guard.
func TestRejectInternalTableDDLAllowsInternalShapedProcedureNames(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "create procedure",
			query: "create procedure _vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_() begin declare y int; set y = 1; end",
		},
		{
			name:  "drop procedure",
			query: "drop procedure _vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.query)
			require.NoError(t, err)

			ddl, ok := stmt.(sqlparser.DDLStatement)
			require.True(t, ok)

			require.NoError(t, rejectInternalTableDDL(ddl))
		})
	}
}

// TestRejectInternalTableDML verifies that DML is rejected only when it would
// modify an internal operation table, not when one is merely read.
func TestRejectInternalTableDML(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name string

		query string

		wantErr string
	}{
		{
			name:    "update internal table",
			query:   "update _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ set id = 2",
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:    "update join assigning internal column",
			query:   "update t u join _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ gc on u.id = gc.id set gc.id = 2",
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:  "update join with unqualified assignment",
			query: "update t u join _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ gc on u.id = gc.id set col = 2",
		},
		{
			name:  "update join reading internal table",
			query: "update t u join _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ gc on u.id = gc.id set u.col = 2",
		},
		{
			name:  "update regular table",
			query: "update t set col = 2",
		},
		{
			name:    "delete from internal table",
			query:   "delete from _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ where id = 1",
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:    "delete join targeting internal table",
			query:   "delete gc from t u join _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ gc on u.id = gc.id",
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:  "delete join reading internal table",
			query: "delete u from t u join _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ gc on u.id = gc.id",
		},
		{
			name:    "insert into internal table",
			query:   "insert into _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_ (id) values (1)",
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:  "insert select reading internal table",
			query: "insert into t select id from _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.query)
			require.NoError(t, err)

			err = rejectInternalTableDML(stmt)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
