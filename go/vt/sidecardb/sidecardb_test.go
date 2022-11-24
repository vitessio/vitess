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

package sidecardb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
)

// Tests all non-error code paths in sidecardb
func TestAll(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	AddVTSchemaInitQueries(db)

	ctx := context.Background()
	cp := db.ConnParams()
	conn, err := cp.Connect(ctx)
	require.NoError(t, err)
	exec := func(ctx context.Context, query string, maxRows int, wantFields bool, useVT bool) (*sqltypes.Result, error) {
		if useVT {
			if _, err := conn.ExecuteFetch(UseVTDatabaseQuery, maxRows, wantFields); err != nil {
				return nil, err
			}
		}
		return conn.ExecuteFetch(query, maxRows, wantFields)
	}

	// tests init on empty _vt
	require.Equal(t, int64(0), GetDDLCount())
	err = Init(ctx, exec, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(vtTables)), GetDDLCount())

	// tests init on already inited _vt
	var tables []string
	for _, table := range vtTables {
		tables = append(tables, table.name)
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Table",
		"varchar"),
		tables...,
	)
	db.AddQuery(GetCurrentTablesQuery, result)
	err = Init(ctx, exec, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(vtTables)), GetDDLCount())

	// tests misc paths not covered above
	si := &vtSchemaInit{
		ctx:  ctx,
		exec: exec,
	}
	result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Database",
		"varchar"),
		"currentDB",
	)
	db.AddQuery(SelectCurrentDatabaseQuery, result)
	db.AddQuery("use dbname", &sqltypes.Result{})
	currentDB, err := si.setCurrentDatabase("dbname")
	require.NoError(t, err)
	require.Equal(t, currentDB, "currentDB")

	require.False(t, MatchesVTInitQuery("abc"))
	require.True(t, MatchesVTInitQuery(SelectCurrentDatabaseQuery))
	require.True(t, MatchesVTInitQuery("CREATE TABLE IF NOT EXISTS _vt.vreplication"))
}
