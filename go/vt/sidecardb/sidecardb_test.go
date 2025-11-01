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
	"errors"
	"expvar"
	"sort"
	"strings"
	"testing"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
)

// TestInitErrors validates that the schema init error stats are being correctly set
func TestInitErrors(t *testing.T) {
	ctx := context.Background()

	db := fakesqldb.New(t)
	defer db.Close()
	env := vtenv.NewTestEnv()
	AddSchemaInitQueries(db, false, env.Parser())

	ddlErrorCount.Set(0)
	ddlCount.Set(0)

	cp := dbconfigs.New(db.ConnParams())
	conn, err := cp.Connect(ctx)
	require.NoError(t, err)

	type schemaError struct {
		tableName  string
		errorValue string
	}

	// simulate two errors during table creation to validate error stats
	schemaErrors := []schemaError{
		{"vreplication_log", "vreplication_log error"},
		{"copy_state", "copy_state error"},
	}

	exec := func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error) {
		if useDB {
			if _, err := conn.ExecuteFetch("use "+sidecar.GetIdentifier(), maxRows, true); err != nil {
				return nil, err
			}
		}

		// simulate errors for the table creation DDLs applied for tables specified in schemaErrors
		stmt, err := env.Parser().Parse(query)
		if err != nil {
			return nil, err
		}
		createTable, ok := stmt.(*sqlparser.CreateTable)
		if ok {
			for _, e := range schemaErrors {
				if strings.EqualFold(e.tableName, createTable.Table.Name.String()) {
					return nil, errors.New(e.errorValue)
				}
			}
		}
		return conn.ExecuteFetch(query, maxRows, true)
	}

	require.Equal(t, int64(0), getDDLCount())
	err = Init(ctx, env, exec)
	require.NoError(t, err)
	require.Equal(t, int64(len(sidecarTables)-len(schemaErrors)), getDDLCount())
	require.Equal(t, int64(len(schemaErrors)), getDDLErrorCount())

	var want []string
	for _, e := range schemaErrors {
		want = append(want, e.errorValue)
	}
	// sort expected and reported errors for easy comparison
	sort.Strings(want)
	got := getDDLErrorHistory()
	sort.Slice(got, func(i, j int) bool {
		return got[i].tableName < got[j].tableName
	})
	var gotErrors string
	stats.Register(func(name string, v expvar.Var) {
		if name == StatsKeyErrors {
			gotErrors = v.String()
		}
	})

	// for DDL errors, validate both the internal data structure and the stats endpoint
	for i := range want {
		if !strings.Contains(got[i].err.Error(), want[i]) {
			require.FailNowf(t, "incorrect schema error", "got %s, want %s", got[i], want[i])
		}
		if !strings.Contains(gotErrors, want[i]) {
			require.FailNowf(t, "schema error not published", "got %s, want %s", gotErrors, want[i])
		}
	}
}

// Tests various non-error code paths in sidecardb
func TestMiscSidecarDB(t *testing.T) {
	ctx := context.Background()

	db := fakesqldb.New(t)
	defer db.Close()
	env := vtenv.NewTestEnv()
	AddSchemaInitQueries(db, false, env.Parser())
	db.AddQuery("use dbname", &sqltypes.Result{})
	db.AddQueryPattern("set @@session.sql_mode=.*", &sqltypes.Result{})

	cp := dbconfigs.New(db.ConnParams())
	conn, err := cp.Connect(ctx)
	require.NoError(t, err)
	exec := func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error) {
		if useDB {
			if _, err := conn.ExecuteFetch("use "+sidecar.GetIdentifier(), maxRows, true); err != nil {
				return nil, err
			}
		}
		return conn.ExecuteFetch(query, maxRows, true)
	}

	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"dbexists",
		"int64"),
		sidecar.GetName(),
	)
	dbeq, err := sqlparser.ParseAndBind(sidecarDBExistsQuery, sqltypes.StringBindVariable(sidecar.GetName()))
	require.NoError(t, err)
	db.AddQuery(dbeq, result)
	db.AddQuery(sidecar.GetCreateQuery(), &sqltypes.Result{})
	AddSchemaInitQueries(db, false, env.Parser())

	// tests init on empty db
	ddlErrorCount.Set(0)
	ddlCount.Set(0)
	require.Equal(t, int64(0), getDDLCount())
	err = Init(ctx, env, exec)
	require.NoError(t, err)
	require.Equal(t, int64(len(sidecarTables)), getDDLCount())

	// Include the table DDLs in the expected queries.
	// This causes them to NOT be created again.
	AddSchemaInitQueries(db, true, env.Parser())

	// tests init on already inited db
	err = Init(ctx, env, exec)
	require.NoError(t, err)
	require.Equal(t, int64(len(sidecarTables)), getDDLCount())

	// tests misc paths not covered above
	si := &schemaInit{
		ctx:  ctx,
		exec: exec,
		env:  env,
	}

	err = si.setCurrentDatabase(sidecar.GetIdentifier())
	require.NoError(t, err)

	require.False(t, MatchesInitQuery("abc"))
	require.True(t, MatchesInitQuery("CREATE TABLE IF NOT EXISTS _vt.vreplication"))
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
		{"valid", "t1", "create table if not exists t1(i int)", false},
		{"no if not exists", "t1", "create table t1(i int)", true},
		{"invalid table name", "t2", "create table if not exists t1(i int)", true},
		{"invalid table name", "t1", "create table if not exists t2(i int)", true},
		{"qualifier", "t1", "create table if not exists vt_product.t1(i int)", true},
	}
	parser := sqlparser.NewTestParser()
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			_, err := validateSchemaDefinition(tc.name, tc.schema, parser)
			if tc.mustError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAlterTableAlgorithm confirms that we use ALGORITHM=COPY during alter tables
func TestAlterTableAlgorithm(t *testing.T) {
	type testCase struct {
		testName      string
		tableName     string
		currentSchema string
		desiredSchema string
	}
	testCases := []testCase{
		{"add column", "t1", "create table if not exists _vt.t1(i int)", "create table if not exists _vt.t1(i int, i1 int)"},
		{"modify column", "t1", "create table if not exists _vt.t1(i int)", "create table if not exists _vt.t(i float)"},
	}
	si := &schemaInit{
		env: vtenv.NewTestEnv(),
	}
	copyAlgo := sqlparser.AlgorithmValue("COPY")
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			diff, err := si.findTableSchemaDiff(tc.tableName, tc.currentSchema, tc.desiredSchema)
			require.NoError(t, err)
			stmt, err := si.env.Parser().Parse(diff)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)
			require.NotNil(t, alterTable)
			var alterAlgo sqlparser.AlterOption
			for i, opt := range alterTable.AlterOptions {
				if _, ok := opt.(sqlparser.AlgorithmValue); ok {
					alterAlgo = alterTable.AlterOptions[i]
				}
			}
			require.Equal(t, copyAlgo, alterAlgo)
		})
	}
}

// TestTableSchemaDiff ensures that the diff produced by schemaInit.findTableSchemaDiff
// is resulting in the expected alter table statements in a variety of scenarios.
func TestTableSchemaDiff(t *testing.T) {
	si := &schemaInit{
		env: vtenv.NewTestEnv(),
	}

	type testCase struct {
		name          string
		table         string
		oldSchema     string
		newSchema     string
		expectNoDiff  bool
		expectedAlter string
	}
	testCases := []testCase{
		{
			name:          "modify table charset",
			table:         "t1",
			oldSchema:     "create table if not exists _vt.t1(i int) charset=utf8mb4",
			newSchema:     "create table if not exists _vt.t(i int) charset=utf8mb3",
			expectedAlter: "alter table _vt.t1 charset utf8mb3, algorithm = copy",
		},
		{
			name:         "empty charset",
			table:        "t1",
			oldSchema:    "create table if not exists _vt.t1(i int) charset=utf8mb4",
			newSchema:    "create table if not exists _vt.t(i int)",
			expectNoDiff: true, // We're not specifying an explicit charset in the new schema, so we shouldn't see a diff.
		},
		{
			name:          "modify table engine",
			table:         "t1",
			oldSchema:     "create table if not exists _vt.t1(i int) engine=myisam",
			newSchema:     "create table if not exists _vt.t(i int) engine=innodb",
			expectedAlter: "alter table _vt.t1 engine innodb, algorithm = copy",
		},
		{
			name:          "add, modify, transfer PK",
			table:         "t1",
			oldSchema:     "create table _vt.t1 (i int primary key, i1 varchar(10)) charset utf8mb4",
			newSchema:     "create table _vt.t1 (i int, i1 varchar(20) character set utf8mb3 collate utf8mb3_bin, i2 int, primary key (i2)) charset utf8mb4",
			expectedAlter: "alter table _vt.t1 drop primary key, modify column i1 varchar(20) character set utf8mb3 collate utf8mb3_bin, add column i2 int, add primary key (i2), algorithm = copy",
		},
		{
			name:          "modify visibility and add comment",
			table:         "t1",
			oldSchema:     "create table if not exists _vt.t1(c1 int, c2 int, c3 varchar(100)) charset utf8mb4",
			newSchema:     "create table if not exists _vt.t1(c1 int, c2 int, c3 varchar(100) invisible comment 'hoping to drop') charset utf8mb4",
			expectedAlter: "alter table _vt.t1 modify column c3 varchar(100) comment 'hoping to drop' invisible, algorithm = copy",
		},
		{
			name:          "add PK and remove index",
			table:         "t1",
			oldSchema:     "create table if not exists _vt.t1(c1 int, c2 int, c3 varchar(100), key (c2)) charset utf8mb4",
			newSchema:     "create table if not exists _vt.t1(c1 int primary key, c2 int, c3 varchar(100)) charset utf8mb4",
			expectedAlter: "alter table _vt.t1 drop key c2, add primary key (c1), algorithm = copy",
		},
		{
			name:          "add generated col",
			table:         "t1",
			oldSchema:     "create table if not exists _vt.t1(c1 int primary key) charset utf8mb4",
			newSchema:     "create table if not exists _vt.t1(c1 int primary key, c2 varchar(10) generated always as ('hello')) charset utf8mb4",
			expectedAlter: "alter table _vt.t1 add column c2 varchar(10) as ('hello') virtual, algorithm = copy",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			diff, err := si.findTableSchemaDiff(tc.table, tc.oldSchema, tc.newSchema)
			require.NoError(t, err)
			if tc.expectNoDiff {
				require.Empty(t, diff)
				return
			}
			stmt, err := si.env.Parser().Parse(diff)
			require.NoError(t, err)
			alter, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)
			require.NotNil(t, alter)
			t.Logf("alter: %s", sqlparser.String(alter))
			require.Equal(t, strings.ToLower(tc.expectedAlter), strings.ToLower(sqlparser.String(alter)))
		})
	}
}
