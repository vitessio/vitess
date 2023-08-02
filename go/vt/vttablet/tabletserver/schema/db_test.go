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

package schema

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

var (
	tablesBV, _ = sqltypes.BuildBindVariable([]string{"t1", "lead"})
)

func TestGenerateFullQuery(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		bv        map[string]*querypb.BindVariable
		wantQuery string
		wantErr   string
	}{
		{
			name:  "No bind variables",
			query: "select TABLE_NAME, CREATE_TIME from `tables`",
		}, {
			name:  "List bind variables",
			query: "DELETE FROM %s.`tables` WHERE TABLE_SCHEMA = database() AND TABLE_NAME IN ::tableNames",
			bv: map[string]*querypb.BindVariable{
				"tableNames": tablesBV,
			},
			wantQuery: "delete from _vt.`tables` where TABLE_SCHEMA = database() and TABLE_NAME in ('t1', 'lead')",
		}, {
			name:  "Multiple bind variables",
			query: "INSERT INTO %s.`tables`(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, CREATE_TIME) values (database(), :table_name, :create_statement, :create_time)",
			bv: map[string]*querypb.BindVariable{
				"table_name":       sqltypes.StringBindVariable("lead"),
				"create_statement": sqltypes.StringBindVariable("create table `lead`"),
				"create_time":      sqltypes.Int64BindVariable(1),
			},
			wantQuery: "insert into _vt.`tables`(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, CREATE_TIME) values (database(), 'lead', 'create table `lead`', 1)",
		}, {
			name:    "parser error",
			query:   "insert syntax error",
			wantErr: "syntax error at position 20 near 'error'",
		}, {
			name:      "Multiple %v replacements",
			query:     fetchTablesAndViews,
			wantQuery: "select table_name, create_statement from _vt.`tables` where table_schema = database() union select table_name, create_statement from _vt.views where table_schema = database()",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantQuery == "" {
				tt.wantQuery = tt.query
			}

			got, err := generateFullQuery(tt.query)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			finalQuery, err := got.GenerateQuery(tt.bv, nil)
			require.NoError(t, err)
			require.Equal(t, tt.wantQuery, finalQuery)
		})
	}
}

func TestGetCreateStatement(t *testing.T) {
	db := fakesqldb.New(t)
	conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
	require.NoError(t, err)

	// Success view
	createStatement := "CREATE ALGORITHM=UNDEFINED DEFINER=`msandbox`@`localhost` SQL SECURITY DEFINER VIEW `lead` AS select `area`.`id` AS `id` from `area`"
	db.AddQuery("show create table `lead`", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(" View | Create View | character_set_client | collation_connection", "varchar|varchar|varchar|varchar"),
		fmt.Sprintf("lead|%v|utf8mb4|utf8mb4_0900_ai_ci", createStatement),
	))
	got, err := getCreateStatement(context.Background(), conn, "`lead`")
	require.NoError(t, err)
	require.Equal(t, createStatement, got)
	require.NoError(t, db.LastError())

	// Success table
	createStatement = "CREATE TABLE `area` (\n  `id` int NOT NULL,\n  `name` varchar(30) DEFAULT NULL,\n  `zipcode` int DEFAULT NULL,\n  `country` int DEFAULT NULL,\n  `x` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	db.AddQuery("show create table area", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(" Table | Create Table", "varchar|varchar"),
		fmt.Sprintf("area|%v", createStatement),
	))
	got, err = getCreateStatement(context.Background(), conn, "area")
	require.NoError(t, err)
	require.Equal(t, createStatement, got)
	require.NoError(t, db.LastError())

	// Failure
	errMessage := "ERROR 1146 (42S02): Table 'ks.v1' doesn't exist"
	db.AddRejectedQuery("show create table v1", errors.New(errMessage))
	got, err = getCreateStatement(context.Background(), conn, "v1")
	require.ErrorContains(t, err, errMessage)
	require.Equal(t, "", got)
}

func TestGetChangedViewNames(t *testing.T) {
	db := fakesqldb.New(t)
	conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
	require.NoError(t, err)

	// Success
	query := fmt.Sprintf(detectViewChange, sidecardb.GetIdentifier())
	db.AddQuery(query, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name", "varchar"),
		"lead",
		"v1",
		"v2",
	))
	got, err := getChangedViewNames(context.Background(), conn, true)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.ElementsMatch(t, maps.Keys(got), []string{"v1", "v2", "lead"})
	require.NoError(t, db.LastError())

	// Not serving primary
	got, err = getChangedViewNames(context.Background(), conn, false)
	require.NoError(t, err)
	require.Len(t, got, 0)
	require.NoError(t, db.LastError())

	// Failure
	errMessage := "ERROR 1146 (42S02): Table '_vt.views' doesn't exist"
	db.AddRejectedQuery(query, errors.New(errMessage))
	got, err = getChangedViewNames(context.Background(), conn, true)
	require.ErrorContains(t, err, errMessage)
	require.Nil(t, got)
}

func TestGetViewDefinition(t *testing.T) {
	db := fakesqldb.New(t)
	conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
	require.NoError(t, err)

	viewsBV, err := sqltypes.BuildBindVariable([]string{"v1", "lead"})
	require.NoError(t, err)
	bv := map[string]*querypb.BindVariable{"viewNames": viewsBV}

	// Success
	query := "select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v1', 'lead')"
	db.AddQuery(query, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|view_definition", "varchar|varchar"),
		"v1|create_view_v1",
		"lead|create_view_lead",
	))
	got, err := collectGetViewDefinitions(conn, bv)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.ElementsMatch(t, maps.Keys(got), []string{"v1", "lead"})
	require.Equal(t, "create_view_v1", got["v1"])
	require.Equal(t, "create_view_lead", got["lead"])
	require.NoError(t, db.LastError())

	// Failure
	errMessage := "some error in MySQL"
	db.AddRejectedQuery(query, errors.New(errMessage))
	got, err = collectGetViewDefinitions(conn, bv)
	require.ErrorContains(t, err, errMessage)
	require.Len(t, got, 0)

	// Failure empty bv
	bv = nil
	got, err = collectGetViewDefinitions(conn, bv)
	require.EqualError(t, err, "missing bind var viewNames")
	require.Len(t, got, 0)
}

func collectGetViewDefinitions(conn *connpool.DBConn, bv map[string]*querypb.BindVariable) (map[string]string, error) {
	viewDefinitions := make(map[string]string)
	err := getViewDefinition(context.Background(), conn, bv, func(qr *sqltypes.Result) error {
		for _, row := range qr.Rows {
			viewDefinitions[row[0].ToString()] = row[1].ToString()
		}
		return nil
	}, func() *sqltypes.Result {
		return &sqltypes.Result{}
	}, 1000)
	return viewDefinitions, err
}

func TestGetMismatchedTableNames(t *testing.T) {
	queryFields := sqltypes.MakeTestFields("TABLE_NAME|CREATE_TIME", "varchar|int64")

	testCases := []struct {
		name               string
		tables             map[string]*Table
		dbData             *sqltypes.Result
		dbError            string
		isServingPrimary   bool
		expectedTableNames []string
		expectedError      string
	}{
		{
			name: "Table create time differs",
			tables: map[string]*Table{
				"t1": {
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 31234,
				},
			},
			dbData: sqltypes.MakeTestResult(queryFields,
				"t1|2341"),
			isServingPrimary:   true,
			expectedTableNames: []string{"t1"},
		}, {
			name: "Table got deleted",
			tables: map[string]*Table{
				"t1": {
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 31234,
				},
			},
			dbData: sqltypes.MakeTestResult(queryFields,
				"t1|31234",
				"t2|2341"),
			isServingPrimary:   true,
			expectedTableNames: []string{"t2"},
		}, {
			name: "Table got created",
			tables: map[string]*Table{
				"t1": {
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 31234,
				}, "t2": {
					Name:       sqlparser.NewIdentifierCS("t2"),
					Type:       NoType,
					CreateTime: 31234,
				},
			},
			dbData: sqltypes.MakeTestResult(queryFields,
				"t1|31234"),
			isServingPrimary:   true,
			expectedTableNames: []string{"t2"},
		}, {
			name: "Dual gets ignored",
			tables: map[string]*Table{
				"dual": NewTable("dual", NoType),
				"t2": {
					Name:       sqlparser.NewIdentifierCS("t2"),
					Type:       NoType,
					CreateTime: 31234,
				},
			},
			dbData: sqltypes.MakeTestResult(queryFields,
				"t2|31234"),
			isServingPrimary:   true,
			expectedTableNames: []string{},
		}, {
			name: "All problems",
			tables: map[string]*Table{
				"dual": NewTable("dual", NoType),
				"t2": {
					Name:       sqlparser.NewIdentifierCS("t2"),
					Type:       NoType,
					CreateTime: 31234,
				},
				"t1": {
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 31234,
				},
			},
			dbData: sqltypes.MakeTestResult(queryFields,
				"t3|31234",
				"t1|1342"),
			isServingPrimary:   true,
			expectedTableNames: []string{"t1", "t2", "t3"},
		}, {
			name: "Not serving primary",
			tables: map[string]*Table{
				"t1": {
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 31234,
				},
			},
			dbData: sqltypes.MakeTestResult(queryFields,
				"t1|2341"),
			isServingPrimary:   false,
			expectedTableNames: []string{},
		}, {
			name: "Error in query",
			tables: map[string]*Table{
				"t1": {
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 31234,
				},
			},
			dbError:          "some error in MySQL",
			dbData:           nil,
			isServingPrimary: true,
			expectedError:    "some error in MySQL",
		},
	}

	query := fmt.Sprintf(readTableCreateTimes, sidecardb.GetIdentifier())
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
			require.NoError(t, err)

			if tc.dbError != "" {
				db.AddRejectedQuery(query, errors.New(tc.dbError))
			} else {
				db.AddQuery(query, tc.dbData)
			}
			se := &Engine{
				tables: tc.tables,
			}
			mismatchedTableNames, err := se.getMismatchedTableNames(context.Background(), conn, tc.isServingPrimary)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.ElementsMatch(t, maps.Keys(mismatchedTableNames), tc.expectedTableNames)
				require.NoError(t, db.LastError())
			}
		})
	}
}

func TestReloadTablesInDB(t *testing.T) {
	showCreateTableFields := sqltypes.MakeTestFields("Table | Create Table", "varchar|varchar")
	errMessage := "some error in MySQL"
	testCases := []struct {
		name            string
		tablesToReload  []*Table
		tablesToDelete  []string
		expectedQueries map[string]*sqltypes.Result
		queriesToReject map[string]error
		expectedError   string
	}{
		{
			name:           "Only tables to delete",
			tablesToDelete: []string{"t1", "lead"},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t1', 'lead')": {},
			},
		}, {
			name: "Only tables to reload",
			tablesToReload: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 1234,
				}, {
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t1', 'lead')": {},
				"show create table t1": sqltypes.MakeTestResult(showCreateTableFields,
					"t1|create_table_t1"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateTableFields,
					"lead|create_table_lead"),
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 't1', 'create_table_t1', 1234)":     {},
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 'lead', 'create_table_lead', 1234)": {},
			},
		}, {
			name: "Reload and Delete",
			tablesToReload: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 1234,
				}, {
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			tablesToDelete: []string{"t2", "from"},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t2', 'from', 't1', 'lead')": {},
				"show create table t1": sqltypes.MakeTestResult(showCreateTableFields,
					"t1|create_table_t1"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateTableFields,
					"lead|create_table_lead"),
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 't1', 'create_table_t1', 1234)":     {},
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 'lead', 'create_table_lead', 1234)": {},
			},
		}, {
			name: "Error In Insert",
			tablesToReload: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t1')": {},
				"show create table t1": sqltypes.MakeTestResult(showCreateTableFields,
					"t1|create_table_t1"),
			},
			queriesToReject: map[string]error{
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 't1', 'create_table_t1', 1234)": errors.New(errMessage),
			},
			expectedError: errMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
			require.NoError(t, err)

			// Add queries with the expected results and errors.
			for query, result := range tc.expectedQueries {
				db.AddQuery(query, result)
			}
			for query, errorToThrow := range tc.queriesToReject {
				db.AddRejectedQuery(query, errorToThrow)
			}

			err = reloadTablesDataInDB(context.Background(), conn, tc.tablesToReload, tc.tablesToDelete)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.NoError(t, db.LastError())
		})
	}
}

func TestReloadViewsInDB(t *testing.T) {
	showCreateTableFields := sqltypes.MakeTestFields(" View | Create View | character_set_client | collation_connection", "varchar|varchar|varchar|varchar")
	getViewDefinitionsFields := sqltypes.MakeTestFields("table_name|view_definition", "varchar|varchar")
	errMessage := "some error in MySQL"
	testCases := []struct {
		name            string
		viewsToReload   []*Table
		viewsToDelete   []string
		expectedQueries map[string]*sqltypes.Result
		queriesToReject map[string]error
		expectedError   string
	}{
		{
			name:          "Only views to delete",
			viewsToDelete: []string{"v1", "lead"},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v1', 'lead')": {},
			},
		}, {
			name: "Only views to reload",
			viewsToReload: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("v1"),
					Type:       View,
					CreateTime: 1234,
				}, {
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v1', 'lead')": {},
				"select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v1', 'lead')": sqltypes.MakeTestResult(
					getViewDefinitionsFields,
					"lead|select_lead",
					"v1|select_v1"),
				"show create table v1": sqltypes.MakeTestResult(showCreateTableFields,
					"v1|create_view_v1|utf8mb4|utf8mb4_0900_ai_ci"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateTableFields,
					"lead|create_view_lead|utf8mb4|utf8mb4_0900_ai_ci"),
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'v1', 'create_view_v1', 'select_v1')":       {},
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'lead', 'create_view_lead', 'select_lead')": {},
			},
		}, {
			name: "Reload and delete",
			viewsToReload: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("v1"),
					Type:       View,
					CreateTime: 1234,
				}, {
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			viewsToDelete: []string{"v2", "from"},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v2', 'from', 'v1', 'lead')": {},
				"select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v2', 'from', 'v1', 'lead')": sqltypes.MakeTestResult(
					getViewDefinitionsFields,
					"lead|select_lead",
					"v1|select_v1"),
				"show create table v1": sqltypes.MakeTestResult(showCreateTableFields,
					"v1|create_view_v1|utf8mb4|utf8mb4_0900_ai_ci"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateTableFields,
					"lead|create_view_lead|utf8mb4|utf8mb4_0900_ai_ci"),
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'v1', 'create_view_v1', 'select_v1')":       {},
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'lead', 'create_view_lead', 'select_lead')": {},
			},
		}, {
			name: "Error In Insert",
			viewsToReload: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("v1"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v1')": {},
				"select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v1')": sqltypes.MakeTestResult(
					getViewDefinitionsFields,
					"v1|select_v1"),
				"show create table v1": sqltypes.MakeTestResult(showCreateTableFields,
					"v1|create_view_v1|utf8mb4|utf8mb4_0900_ai_ci"),
			},
			queriesToReject: map[string]error{
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'v1', 'create_view_v1', 'select_v1')": errors.New(errMessage),
			},
			expectedError: errMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
			require.NoError(t, err)

			// Add queries with the expected results and errors.
			for query, result := range tc.expectedQueries {
				db.AddQuery(query, result)
			}
			for query, errorToThrow := range tc.queriesToReject {
				db.AddRejectedQuery(query, errorToThrow)
			}

			err = reloadViewsDataInDB(context.Background(), conn, tc.viewsToReload, tc.viewsToDelete)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.NoError(t, db.LastError())
		})
	}
}

func TestReloadDataInDB(t *testing.T) {
	showCreateViewFields := sqltypes.MakeTestFields(" View | Create View | character_set_client | collation_connection", "varchar|varchar|varchar|varchar")
	showCreateTableFields := sqltypes.MakeTestFields("Table | Create Table", "varchar|varchar")
	getViewDefinitionsFields := sqltypes.MakeTestFields("table_name|view_definition", "varchar|varchar")
	errMessage := "some error in MySQL"
	testCases := []struct {
		name            string
		altered         []*Table
		created         []*Table
		dropped         []*Table
		expectedQueries map[string]*sqltypes.Result
		queriesToReject map[string]error
		expectedError   string
	}{
		{
			name: "Only views to delete",
			dropped: []*Table{
				NewTable("v1", View),
				NewTable("lead", View),
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v1', 'lead')": {},
			},
		}, {
			name: "Only views to reload",
			created: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("v1"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			altered: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v1', 'lead')": {},
				"select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v1', 'lead')": sqltypes.MakeTestResult(
					getViewDefinitionsFields,
					"lead|select_lead",
					"v1|select_v1"),
				"show create table v1": sqltypes.MakeTestResult(showCreateViewFields,
					"v1|create_view_v1|utf8mb4|utf8mb4_0900_ai_ci"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateViewFields,
					"lead|create_view_lead|utf8mb4|utf8mb4_0900_ai_ci"),
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'v1', 'create_view_v1', 'select_v1')":       {},
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'lead', 'create_view_lead', 'select_lead')": {},
			},
		}, {
			name: "Reload and delete views",
			created: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("v1"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			altered: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			dropped: []*Table{
				NewTable("v2", View),
				NewTable("from", View),
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v2', 'from', 'v1', 'lead')": {},
				"select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v2', 'from', 'v1', 'lead')": sqltypes.MakeTestResult(
					getViewDefinitionsFields,
					"lead|select_lead",
					"v1|select_v1"),
				"show create table v1": sqltypes.MakeTestResult(showCreateViewFields,
					"v1|create_view_v1|utf8mb4|utf8mb4_0900_ai_ci"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateViewFields,
					"lead|create_view_lead|utf8mb4|utf8mb4_0900_ai_ci"),
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'v1', 'create_view_v1', 'select_v1')":       {},
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'lead', 'create_view_lead', 'select_lead')": {},
			},
		}, {
			name: "Error In Inserting View Data",
			created: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("v1"),
					Type:       View,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v1')": {},
				"select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v1')": sqltypes.MakeTestResult(
					getViewDefinitionsFields,
					"v1|select_v1"),
				"show create table v1": sqltypes.MakeTestResult(showCreateViewFields,
					"v1|create_view_v1|utf8mb4|utf8mb4_0900_ai_ci"),
			},
			queriesToReject: map[string]error{
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'v1', 'create_view_v1', 'select_v1')": errors.New(errMessage),
			},
			expectedError: errMessage,
		}, {
			name: "Only tables to delete",
			dropped: []*Table{
				NewTable("t1", NoType),
				NewTable("lead", NoType),
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t1', 'lead')": {},
			},
		}, {
			name: "Only tables to reload",
			created: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			altered: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t1', 'lead')": {},
				"show create table t1": sqltypes.MakeTestResult(showCreateTableFields,
					"t1|create_table_t1"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateTableFields,
					"lead|create_table_lead"),
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 't1', 'create_table_t1', 1234)":     {},
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 'lead', 'create_table_lead', 1234)": {},
			},
		}, {
			name: "Reload and delete tables",
			created: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			altered: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			dropped: []*Table{
				NewTable("t2", NoType),
				NewTable("from", NoType),
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t2', 'from', 't1', 'lead')": {},
				"show create table t1": sqltypes.MakeTestResult(showCreateTableFields,
					"t1|create_table_t1"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateTableFields,
					"lead|create_table_lead"),
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 't1', 'create_table_t1', 1234)":     {},
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 'lead', 'create_table_lead', 1234)": {},
			},
		}, {
			name: "Error In Inserting Table Data",
			altered: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t1')": {},
				"show create table t1": sqltypes.MakeTestResult(showCreateTableFields,
					"t1|create_table_t1"),
			},
			queriesToReject: map[string]error{
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 't1', 'create_table_t1', 1234)": errors.New(errMessage),
			},
			expectedError: errMessage,
		}, {
			name: "Reload and delete all",
			created: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("v1"),
					Type:       View,
					CreateTime: 1234,
				}, {
					Name:       sqlparser.NewIdentifierCS("t1"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			altered: []*Table{
				{
					Name:       sqlparser.NewIdentifierCS("lead"),
					Type:       View,
					CreateTime: 1234,
				}, {
					Name:       sqlparser.NewIdentifierCS("where"),
					Type:       NoType,
					CreateTime: 1234,
				},
			},
			dropped: []*Table{
				NewTable("v2", View),
				NewTable("from", View),
				NewTable("t2", NoType),
			},
			expectedQueries: map[string]*sqltypes.Result{
				"begin":    {},
				"commit":   {},
				"rollback": {},
				"delete from _vt.views where table_schema = database() and table_name in ('v2', 'from', 'v1', 'lead')": {},
				"select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v2', 'from', 'v1', 'lead')": sqltypes.MakeTestResult(
					getViewDefinitionsFields,
					"lead|select_lead",
					"v1|select_v1"),
				"show create table v1": sqltypes.MakeTestResult(showCreateViewFields,
					"v1|create_view_v1|utf8mb4|utf8mb4_0900_ai_ci"),
				"show create table `lead`": sqltypes.MakeTestResult(showCreateViewFields,
					"lead|create_view_lead|utf8mb4|utf8mb4_0900_ai_ci"),
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'v1', 'create_view_v1', 'select_v1')":       {},
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'lead', 'create_view_lead', 'select_lead')": {},
				"delete from _vt.`tables` where table_schema = database() and table_name in ('t2', 't1', 'where')":                                                  {},
				"show create table t1": sqltypes.MakeTestResult(showCreateTableFields,
					"t1|create_table_t1"),
				"show create table `where`": sqltypes.MakeTestResult(showCreateTableFields,
					"where|create_table_where"),
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 't1', 'create_table_t1', 1234)":       {},
				"insert into _vt.`tables`(table_schema, table_name, create_statement, create_time) values (database(), 'where', 'create_table_where', 1234)": {},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
			require.NoError(t, err)

			// Add queries with the expected results and errors.
			for query, result := range tc.expectedQueries {
				db.AddQuery(query, result)
			}
			for query, errorToThrow := range tc.queriesToReject {
				db.AddRejectedQuery(query, errorToThrow)
			}

			err = reloadDataInDB(context.Background(), conn, tc.altered, tc.created, tc.dropped)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.NoError(t, db.LastError())
		})
	}
}

// TestGetFetchViewQuery tests the functionality for getting the fetch query to retrieve views.
func TestGetFetchViewQuery(t *testing.T) {
	testcases := []struct {
		name          string
		viewNames     []string
		expectedQuery string
	}{
		{
			name:          "No views provided",
			viewNames:     []string{},
			expectedQuery: "select table_name, create_statement from _vt.views where table_schema = database()",
		}, {
			name:          "Few views provided",
			viewNames:     []string{"v1", "v2", "lead"},
			expectedQuery: "select table_name, create_statement from _vt.views where table_schema = database() and table_name in ('v1', 'v2', 'lead')",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			query, err := GetFetchViewQuery(testcase.viewNames)
			require.NoError(t, err)
			require.Equal(t, testcase.expectedQuery, query)
		})
	}
}

// TestGetFetchTableQuery tests the functionality for getting the fetch query to retrieve tables.
func TestGetFetchTableQuery(t *testing.T) {
	testcases := []struct {
		name          string
		tableNames    []string
		expectedQuery string
	}{
		{
			name:          "No tables provided",
			tableNames:    []string{},
			expectedQuery: "select table_name, create_statement from _vt.`tables` where table_schema = database()",
		}, {
			name:          "Few tables provided",
			tableNames:    []string{"v1", "v2", "lead"},
			expectedQuery: "select table_name, create_statement from _vt.`tables` where table_schema = database() and table_name in ('v1', 'v2', 'lead')",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			query, err := GetFetchTableQuery(testcase.tableNames)
			require.NoError(t, err)
			require.Equal(t, testcase.expectedQuery, query)
		})
	}
}

// TestGetFetchTableAndViewsQuery tests the functionality for getting the fetch query to retrieve tables and views.
func TestGetFetchTableAndViewsQuery(t *testing.T) {
	testcases := []struct {
		name          string
		tableNames    []string
		expectedQuery string
	}{
		{
			name:          "No tables provided",
			tableNames:    []string{},
			expectedQuery: "select table_name, create_statement from _vt.`tables` where table_schema = database() union select table_name, create_statement from _vt.views where table_schema = database()",
		}, {
			name:          "Few tables provided",
			tableNames:    []string{"t1", "t2", "v1", "v2", "lead"},
			expectedQuery: "select table_name, create_statement from _vt.`tables` where table_schema = database() and table_name in ('t1', 't2', 'v1', 'v2', 'lead') union select table_name, create_statement from _vt.views where table_schema = database() and table_name in ('t1', 't2', 'v1', 'v2', 'lead')",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			query, err := GetFetchTableAndViewsQuery(testcase.tableNames)
			require.NoError(t, err)
			require.Equal(t, testcase.expectedQuery, query)
		})
	}
}
