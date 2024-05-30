/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestGetCastTypeForColumn tests that we get the correct string value to use in a CAST function based on the type of the column.
func TestGetCastTypeForColumn(t *testing.T) {
	tests := []struct {
		name string
		typ  querypb.Type
		want string
	}{
		{
			name: "VARCHAR column",
			typ:  querypb.Type_VARCHAR,
			want: "CHAR",
		},
		{
			name: "CHAR column",
			typ:  querypb.Type_CHAR,
			want: "CHAR",
		},
		{
			name: "VARBINARY column",
			typ:  querypb.Type_VARBINARY,
			want: "BINARY",
		},
		{
			name: "BINARY column",
			typ:  querypb.Type_BINARY,
			want: "BINARY",
		},
		{
			name: "UINT16 column",
			typ:  querypb.Type_UINT16,
			want: "UNSIGNED",
		},
		{
			name: "UINT24 column",
			typ:  querypb.Type_UINT24,
			want: "UNSIGNED",
		},
		{
			name: "UINT32 column",
			typ:  querypb.Type_UINT32,
			want: "UNSIGNED",
		},
		{
			name: "UINT64 column",
			typ:  querypb.Type_UINT64,
			want: "UNSIGNED",
		},
		{
			name: "INT16 column",
			typ:  querypb.Type_INT16,
			want: "SIGNED",
		},
		{
			name: "INT24 column",
			typ:  querypb.Type_INT24,
			want: "SIGNED",
		},
		{
			name: "INT32 column",
			typ:  querypb.Type_INT32,
			want: "SIGNED",
		},
		{
			name: "INT64 column",
			typ:  querypb.Type_INT64,
			want: "SIGNED",
		},
		{
			name: "FLOAT32 column",
			typ:  querypb.Type_FLOAT32,
			want: "FLOAT",
		},
		{
			name: "FLOAT64 column",
			typ:  querypb.Type_FLOAT64,
			want: "FLOAT",
		},
		{
			name: "DECIMAL column",
			typ:  querypb.Type_DECIMAL,
			want: "DECIMAL",
		},
		{
			name: "DATETIME column",
			typ:  querypb.Type_DATETIME,
			want: "DATETIME",
		},
		{
			name: "NULL column",
			typ:  querypb.Type_NULL_TYPE,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updExpr := &sqlparser.UpdateExpr{
				Name: sqlparser.NewColName("col"),
			}
			updatedTable := &vindexes.Table{
				Columns: []vindexes.Column{
					{
						Name: sqlparser.NewIdentifierCI("col"),
						Type: tt.typ,
					},
				},
			}
			tyStr := getCastTypeForColumn(updatedTable, updExpr)
			require.EqualValues(t, tt.want, tyStr)
		})
	}
}
