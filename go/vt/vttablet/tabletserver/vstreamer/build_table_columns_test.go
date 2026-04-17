/*
Copyright 2025 The Vitess Authors.

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

package vstreamer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/binlog"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestBuildTableColumns tests the buildTableColumns vstreamer method to ensure
// that the fields we get back in various scenarios are expected/correct.
func TestBuildTableColumns(t *testing.T) {
	ctx := context.Background()

	execStatements(t, []string{
		"create table test_build_cols (id bigint primary key, txt text, val varchar(64))",
	})
	defer execStatements(t, []string{
		"drop table test_build_cols",
	})

	pos, err := replication.DecodePosition(primaryPosition(t))
	require.NoError(t, err)

	tests := []struct {
		name          string
		tableMap      *mysql.TableMap
		filter        *binlogdatapb.Filter
		expectError   bool
		errorContains string
		validate      func(t *testing.T, fields []*querypb.Field, err error)
	}{
		{
			name: "basic types",
			tableMap: &mysql.TableMap{
				Name:     "test_build_cols",
				Database: env.KeyspaceName,
				Types: []byte{
					binlog.TypeLong,      // id int
					binlog.TypeBlob,      // txt text
					binlog.TypeVarString, // val varchar(64)
				},
				ColumnCollationIDs: []collations.ID{
					collations.CollationUtf8mb4ID,    // txt text
					collations.CollationUtf8mb4BinID, // val varchar(64)
				},
				Metadata: []uint16{0, 0, 0, 0},
			},
			filter: &binlogdatapb.Filter{
				FieldEventMode: binlogdatapb.Filter_ERR_ON_MISMATCH,
				Rules: []*binlogdatapb.Rule{{
					Match:  "test_build_cols",
					Filter: "select val, id, txt from test_build_cols",
				}},
			},
			expectError: false,
			validate: func(t *testing.T, fields []*querypb.Field, err error) {
				require.NoError(t, err)
				require.Len(t, fields, 3)
				require.Equal(t, "id", fields[0].Name)
				require.Equal(t, sqltypes.Int64, fields[0].Type)
				require.Equal(t, "txt", fields[1].Name)
				require.Equal(t, sqltypes.Text, fields[1].Type)
				require.Equal(t, "val", fields[2].Name)
				require.Equal(t, sqltypes.VarChar, fields[2].Type)
			},
		},
		{
			name: "unknown table with ERR_ON_MISMATCH",
			tableMap: &mysql.TableMap{
				Name:     "non_existent_table",
				Database: env.KeyspaceName,
				Types: []byte{
					binlog.TypeLong,
				},
				Metadata: []uint16{0},
			},
			filter: &binlogdatapb.Filter{
				FieldEventMode: binlogdatapb.Filter_ERR_ON_MISMATCH,
			},
			expectError:   true,
			errorContains: "unknown table",
		},
		{
			name: "unknown table with BEST_EFFORT",
			tableMap: &mysql.TableMap{
				Name:     "non_existent_table",
				Database: env.KeyspaceName,
				Types: []byte{
					binlog.TypeLong,
					binlog.TypeVarString,
				},
				ColumnCollationIDs: []collations.ID{
					collations.CollationUtf8mb4ID,
				},
				Metadata: []uint16{0, 0},
			},
			filter: &binlogdatapb.Filter{
				FieldEventMode: binlogdatapb.Filter_BEST_EFFORT,
			},
			expectError: false,
			validate: func(t *testing.T, fields []*querypb.Field, err error) {
				require.NoError(t, err)
				require.Len(t, fields, 2)
				// Should return basic field info with @N position based names.
				require.Equal(t, "@1", fields[0].Name)
				require.Equal(t, "@2", fields[1].Name)
				require.Equal(t, sqltypes.Int32, fields[0].Type)
				require.Equal(t, sqltypes.VarChar, fields[1].Type)
			},
		},
		{
			name: "schema column count mismatch with ERR_ON_MISMATCH",
			tableMap: &mysql.TableMap{
				Name:     "test_build_cols",
				Database: env.KeyspaceName,
				Types: []byte{
					binlog.TypeLong,      // id int
					binlog.TypeBlob,      // txt text
					binlog.TypeVarString, // val varchar(64)
					binlog.TypeLong,      // Extra column not in schema
				},
				ColumnCollationIDs: []collations.ID{
					collations.CollationUtf8mb4ID,
					collations.CollationUtf8mb4BinID,
				},
				Metadata: []uint16{0, 0, 0, 0, 0},
			},
			filter: &binlogdatapb.Filter{
				FieldEventMode: binlogdatapb.Filter_ERR_ON_MISMATCH,
			},
			expectError:   true,
			errorContains: "cannot determine table columns",
		},
		{
			name: "schema column count mismatch with BEST_EFFORT",
			tableMap: &mysql.TableMap{
				Name:     "test_build_cols",
				Database: env.KeyspaceName,
				Types: []byte{
					binlog.TypeLong,      // id int
					binlog.TypeBlob,      // txt text
					binlog.TypeVarString, // val varchar(64)
					binlog.TypeLong,      // Extra column not in schema
				},
				ColumnCollationIDs: []collations.ID{
					collations.CollationUtf8mb4ID,
					collations.CollationUtf8mb4BinID,
				},
				Metadata: []uint16{0, 0, 0, 0, 0},
			},
			filter: &binlogdatapb.Filter{
				FieldEventMode: binlogdatapb.Filter_BEST_EFFORT,
			},
			expectError: false,
			validate: func(t *testing.T, fields []*querypb.Field, err error) {
				require.NoError(t, err)
				require.Len(t, fields, 4)
				// Should return basic field info with @N position based names.
				require.Equal(t, "@1", fields[0].Name)
				require.Equal(t, "@4", fields[3].Name)
				require.Equal(t, fields[3].Type, sqltypes.Int32)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := &vstreamer{
				ctx:    ctx,
				se:     engine.se,
				cp:     env.Dbcfgs.AppWithDB(),
				pos:    pos,
				filter: tt.filter,
			}

			fields, err := vs.buildTableColumns(tt.tableMap)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.ErrorContains(t, err, tt.errorContains)
				}
			} else {
				if tt.validate != nil {
					tt.validate(t, fields, err)
				} else {
					require.NoError(t, err)
					require.NotNil(t, fields)
				}
			}
		})
	}
}
