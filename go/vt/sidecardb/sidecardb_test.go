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
	AddSchemaInitQueries(db, false)
	db.AddQuery("use dbname", &sqltypes.Result{})

	ctx := context.Background()
	cp := db.ConnParams()
	conn, err := cp.Connect(ctx)
	require.NoError(t, err)
	exec := func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error) {
		if useDB {
			if _, err := conn.ExecuteFetch(UseSidecarDatabaseQuery, maxRows, true); err != nil {
				return nil, err
			}
		}
		return conn.ExecuteFetch(query, maxRows, true)
	}

	// tests init on empty db
	require.Equal(t, int64(0), GetDDLCount())
	err = Init(ctx, exec)
	require.NoError(t, err)
	require.Equal(t, int64(len(sidecarTables)), GetDDLCount())

	// tests init on already inited db
	AddSchemaInitQueries(db, true)
	err = Init(ctx, exec)
	require.NoError(t, err)
	require.Equal(t, int64(len(sidecarTables)), GetDDLCount())

	// tests misc paths not covered above
	si := &schemaInit{
		ctx:  ctx,
		exec: exec,
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Database",
		"varchar"),
		"currentDB",
	)
	db.AddQuery(SelectCurrentDatabaseQuery, result)

	currentDB, err := si.setCurrentDatabase("dbname")
	require.NoError(t, err)
	require.Equal(t, "currentDB", currentDB)

	require.False(t, MatchesInitQuery("abc"))
	require.True(t, MatchesInitQuery(SelectCurrentDatabaseQuery))
	require.True(t, MatchesInitQuery("CREATE TABLE IF NOT EXISTS `_vt`.vreplication"))
}

// test the logic that confirms that the user defined schema's table name and qualifier are valid
func TestValidateSchema(t *testing.T) {
	type testCase struct {
		testName  string
		name      string
		schema    string
		mustError bool
	}
	testCases := []testCase{
		{"valid", "t1", "create table if not exists _vt.t1(i int)", false},
		{"no if not exists", "t1", "create table _vt.t1(i int)", true},
		{"invalid table name", "t2", "create table if not exists _vt.t1(i int)", true},
		{"invalid table name", "t1", "create table if not exists _vt.t2(i int)", true},
		{"invalid qualifier", "t1", "create table if not exists vt_product.t1(i int)", true},
		{"invalid qualifier", "t1", "create table if not exists t1(i int)", true},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			_, err := validateSchemaDefinition(tc.name, tc.schema)
			if tc.mustError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
