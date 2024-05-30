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

package mysqlctl

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var queryMap map[string]*sqltypes.Result

func mockExec(query string, maxRows int, wantFields bool) (*sqltypes.Result, error) {
	queryMap = make(map[string]*sqltypes.Result)
	getColsQuery := fmt.Sprintf(GetColumnNamesQuery, "'test'", "'t1'")
	queryMap[getColsQuery] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
			{sqltypes.NewVarChar("col2")},
			{sqltypes.NewVarChar("col3")},
		},
	}

	queryMap["SELECT `col1`, `col2`, `col3` FROM `test`.`t1` WHERE 1 != 1"] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "col1",
			Type: sqltypes.VarChar,
		}, {
			Name: "col2",
			Type: sqltypes.Int64,
		}, {
			Name: "col3",
			Type: sqltypes.VarBinary,
		}},
	}
	getColsQuery = fmt.Sprintf(GetColumnNamesQuery, "database()", "'t2'")
	queryMap[getColsQuery] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
		},
	}

	queryMap["SELECT `col1` FROM `t2` WHERE 1 != 1"] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "col1",
			Type: sqltypes.VarChar,
		}},
	}
	result, ok := queryMap[query]
	if ok {
		return result, nil
	}

	getColsQuery = fmt.Sprintf(GetColumnNamesQuery, "database()", "'with \\' quote'")
	queryMap[getColsQuery] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
		},
	}

	queryMap["SELECT `col1` FROM `with ' quote` WHERE 1 != 1"] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "col1",
			Type: sqltypes.VarChar,
		}},
	}
	result, ok = queryMap[query]
	if ok {
		return result, nil
	}

	return nil, fmt.Errorf("query %s not found in mock setup", query)
}

func TestColumnList(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	fields, _, err := GetColumns("test", "t1", mockExec)
	require.NoError(t, err)
	require.Equal(t, `[name:"col1" type:VARCHAR name:"col2" type:INT64 name:"col3" type:VARBINARY]`, fmt.Sprintf("%+v", fields))

	fields, _, err = GetColumns("", "t2", mockExec)
	require.NoError(t, err)
	require.Equal(t, `[name:"col1" type:VARCHAR]`, fmt.Sprintf("%+v", fields))

	fields, _, err = GetColumns("", "with ' quote", mockExec)
	require.NoError(t, err)
	require.Equal(t, `[name:"col1" type:VARCHAR]`, fmt.Sprintf("%+v", fields))

}

func TestGetSchemaAndSchemaChange(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	db.AddQuery("SHOW CREATE DATABASE IF NOT EXISTS `fakesqldb`", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|cmd", "varchar|varchar"), "create_db|create_db_cmd"))
	db.AddQuery("SHOW CREATE TABLE `fakesqldb`.`test_table`", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|cmd", "varchar|varchar"), "create_table|create_table_cmd"))

	db.AddQuery("SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = 'fakesqldb' AND table_type = 'BASE TABLE'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|table_type|data_length|table_rows", "varchar|varchar|uint64|uint64"), "test_table|test_type|NULL|2"))

	db.AddQuery("SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = 'fakesqldb'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|table_type|data_length|table_rows", "varchar|varchar|uint64|uint64"), "test_table|test_type|NULL|2"))

	query := fmt.Sprintf(GetColumnNamesQuery, sqltypes.EncodeStringSQL(db.Name()), sqltypes.EncodeStringSQL("test_table"))
	db.AddQuery(query, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
			{sqltypes.NewVarChar("col2")},
		},
	})

	db.AddQuery("SELECT `col1`, `col2` FROM `fakesqldb`.`test_table` WHERE 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "col1",
				Type: sqltypes.VarChar,
			},
			{
				Name: "col2",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{},
	})

	tableList, err := tableListSQL([]string{"test_table"})
	require.NoError(t, err)

	query = `
            SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN %s AND LOWER(INDEX_NAME) = 'primary'
            ORDER BY table_name, SEQ_IN_INDEX`
	query = fmt.Sprintf(query, sqltypes.EncodeStringSQL("fakesqldb"), tableList)
	db.AddQuery(query, sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|COLUMN_NAME", "varchar|varchar"), "test_table|col1", "test_table|col2"))

	ctx := context.Background()
	res, err := testMysqld.GetSchema(ctx, db.Name(), &tabletmanagerdata.GetSchemaRequest{})
	assert.NoError(t, err)
	assert.Equal(t, res.String(), `database_schema:"create_db_cmd" table_definitions:{name:"test_table" schema:"create_table_cmd" columns:"col1" columns:"col2" type:"test_type" row_count:2 fields:{name:"col1" type:VARCHAR} fields:{name:"col2" type:VARCHAR}}`)

	// Test ApplySchemaChange
	db.AddQuery("\nSET sql_log_bin = 0", &sqltypes.Result{})

	r, err := testMysqld.ApplySchemaChange(ctx, db.Name(), &tmutils.SchemaChange{})
	assert.NoError(t, err)
	assert.Equal(t, r.BeforeSchema, r.AfterSchema, "BeforeSchema should be equal to AfterSchema as no schema change was passed")
	assert.Equal(t, `database_schema:"create_db_cmd" table_definitions:{name:"test_table" schema:"create_table_cmd" columns:"col1" columns:"col2" type:"test_type" row_count:2 fields:{name:"col1" type:VARCHAR} fields:{name:"col2" type:VARCHAR}}`, r.BeforeSchema.String())

	r, err = testMysqld.ApplySchemaChange(ctx, db.Name(), &tmutils.SchemaChange{
		BeforeSchema: &tabletmanagerdata.SchemaDefinition{
			DatabaseSchema: "create_db_cmd",
			TableDefinitions: []*tabletmanagerdata.TableDefinition{
				{
					Name:   "test_table_changed",
					Schema: "create_table_cmd",
					Type:   "test_type",
				},
			},
		},
		AfterSchema: &tabletmanagerdata.SchemaDefinition{
			DatabaseSchema: "create_db_cmd",
			TableDefinitions: []*tabletmanagerdata.TableDefinition{
				{
					Name:   "test_table",
					Schema: "create_table_cmd",
					Type:   "test_type",
				},
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, r.BeforeSchema, r.AfterSchema)

	r, err = testMysqld.ApplySchemaChange(ctx, db.Name(), &tmutils.SchemaChange{
		BeforeSchema: &tabletmanagerdata.SchemaDefinition{
			DatabaseSchema: "create_db_cmd",
			TableDefinitions: []*tabletmanagerdata.TableDefinition{
				{
					Name:   "test_table",
					Schema: "create_table_cmd",
					Type:   "test_type",
				},
			},
		},
		SQL: "EXPECT THIS QUERY TO BE EXECUTED;\n",
	})
	assert.ErrorContains(t, err, "EXPECT THIS QUERY TO BE EXECUTED")
	assert.Nil(t, r)

	// Test PreflightSchemaChange
	db.AddQuery("SET sql_log_bin = 0", &sqltypes.Result{})
	db.AddQuery("\nDROP DATABASE IF EXISTS _vt_preflight", &sqltypes.Result{})
	db.AddQuery("\nCREATE DATABASE _vt_preflight", &sqltypes.Result{})
	db.AddQuery("\nUSE _vt_preflight", &sqltypes.Result{})
	db.AddQuery("\nSET foreign_key_checks = 0", &sqltypes.Result{})
	db.AddQuery("\nDROP DATABASE _vt_preflight", &sqltypes.Result{})

	l, err := testMysqld.PreflightSchemaChange(context.Background(), db.Name(), []string{})
	assert.NoError(t, err)
	assert.Empty(t, l)

	db.AddQuery("SHOW CREATE DATABASE IF NOT EXISTS `_vt_preflight`", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|cmd", "varchar|varchar"), "create_db|create_db_cmd"))

	db.AddQuery("SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = '_vt_preflight' AND table_type = 'BASE TABLE'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|table_type|data_length|table_rows", "varchar|varchar|uint64|uint64"), "test_table|test_type|NULL|2"))
	db.AddQuery("SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = '_vt_preflight'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|table_type|data_length|table_rows", "varchar|varchar|uint64|uint64"), "test_table|test_type|NULL|2"))
	db.AddQuery("SHOW CREATE TABLE `_vt_preflight`.`test_table`", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|cmd", "varchar|varchar"), "create_table|create_table_cmd"))

	query = `
            SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN %s AND LOWER(INDEX_NAME) = 'primary'
            ORDER BY table_name, SEQ_IN_INDEX`
	query = fmt.Sprintf(query, sqltypes.EncodeStringSQL("_vt_preflight"), tableList)
	db.AddQuery(query, sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|COLUMN_NAME", "varchar|varchar"), "test_table|col1", "test_table|col2"))

	query = fmt.Sprintf(GetColumnNamesQuery, sqltypes.EncodeStringSQL("_vt_preflight"), sqltypes.EncodeStringSQL("test_table"))
	db.AddQuery(query, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
			{sqltypes.NewVarChar("col2")},
		},
	})

	db.AddQuery("SELECT `col1`, `col2` FROM `_vt_preflight`.`test_table` WHERE 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "col1",
				Type: sqltypes.VarChar,
			},
			{
				Name: "col2",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{},
	})

	query = "EXPECT THIS QUERY TO BE EXECUTED"
	_, err = testMysqld.PreflightSchemaChange(context.Background(), db.Name(), []string{query})
	assert.ErrorContains(t, err, query)
}

func TestResolveTables(t *testing.T) {
	db := fakesqldb.New(t)
	testMysqld := NewFakeMysqlDaemon(db)

	defer func() {
		db.Close()
		testMysqld.Close()
	}()

	ctx := context.Background()
	res, err := ResolveTables(ctx, testMysqld, db.Name(), []string{})
	assert.ErrorContains(t, err, "no schema defined")
	assert.Nil(t, res)

	testMysqld.Schema = &tabletmanagerdata.SchemaDefinition{TableDefinitions: tableDefinitions{{
		Name:   "table1",
		Schema: "schema1",
	}, {
		Name:   "table2",
		Schema: "schema2",
	}}}

	res, err = ResolveTables(ctx, testMysqld, db.Name(), []string{"table1"})
	assert.NoError(t, err)
	assert.Len(t, res, 1)

	res, err = ResolveTables(ctx, testMysqld, db.Name(), []string{"table1", "table2"})
	assert.NoError(t, err)
	assert.Len(t, res, 2)
}

func TestGetColumns(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, db.Name())

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	tableName := "test_table"
	query := fmt.Sprintf(GetColumnNamesQuery, sqltypes.EncodeStringSQL(db.Name()), sqltypes.EncodeStringSQL(tableName))
	db.AddQuery(query, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
			{sqltypes.NewVarChar("col2")},
		},
	})
	db.AddQuery("SELECT `col1`, `col2` FROM `fakesqldb`.`test_table` WHERE 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "col1",
				Type: sqltypes.VarChar,
			},
			{
				Name: "col2",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{},
	})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()

	want := sqltypes.MakeTestFields("col1|col2", "varchar|varchar")

	field, cols, err := testMysqld.GetColumns(ctx, db.Name(), tableName)
	assert.Equal(t, want, field)
	assert.Equal(t, []string{"col1", "col2"}, cols)
	assert.NoError(t, err)
}

func TestGetPrimaryKeyColumns(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, db.Name())

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	tableList, err := tableListSQL([]string{"test_table"})
	require.NoError(t, err)

	query := `
            SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN %s AND LOWER(INDEX_NAME) = 'primary'
            ORDER BY table_name, SEQ_IN_INDEX`
	query = fmt.Sprintf(query, sqltypes.EncodeStringSQL("fakesqldb"), tableList)
	db.AddQuery(query, sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|column_name", "varchar|varchar"), "fakesqldb|col1", "fakesqldb2|col2"))

	ctx := context.Background()
	res, err := testMysqld.GetPrimaryKeyColumns(ctx, db.Name(), "test_table")
	assert.NoError(t, err)
	assert.Contains(t, res, "col1")
	assert.Len(t, res, 1)
}
