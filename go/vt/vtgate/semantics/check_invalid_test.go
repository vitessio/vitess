/*
Copyright 2024 The Vitess Authors.

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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestCheckAliasedTableExpr(t *testing.T) {
	tests := []struct {
		name        string
		tableString string
		wantErr     string
	}{
		{
			name:        "Valid AliasedTable - USE VINDEX",
			tableString: "payment_pulls use vindex (lookup_vindex_name, x, t)",
		}, {
			name:        "Valid AliasedTable - IGNORE VINDEX",
			tableString: "payment_pulls ignore vindex (lookup_vindex_name, x, t)",
		}, {
			name:        "Invalid AliasedTable - multiple USE VINDEX",
			tableString: "payment_pulls use vindex (lookup_vindex_name, x, t) use vindex (x)",
			wantErr:     "VT09020: can not use multiple vindex hints for table payment_pulls",
		}, {
			name:        "Invalid AliasedTable - mixed vindex hints",
			tableString: "t.payment_pulls use vindex (lookup_vindex_name, x, t) ignore vindex (x)",
			wantErr:     "VT09020: can not use multiple vindex hints for table t.payment_pulls",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse("select * from " + tt.tableString)
			require.NoError(t, err)
			node := stmt.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr)
			err = checkAliasedTableExpr(node)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
