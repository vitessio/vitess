/*
Copyright 2017 Google Inc.

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

// Package schematest provides support for testing packages
// that depend on schema
package schematest

import (
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// Queries returns a default set of queries that can
// be added to load three tables into the schema.
func Queries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1427325875)},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("STRICT_TRANS_TABLES")},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1")},
			},
		},
		"show variables like 'binlog_format'": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}, {
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("binlog_format"),
				sqltypes.NewVarBinary("STATEMENT"),
			}},
		},
		mysql.BaseShowTables: {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table_01", false, ""),
				mysql.BaseShowTablesRow("test_table_02", false, ""),
				mysql.BaseShowTablesRow("test_table_03", false, ""),
			},
		},
		"select * from test_table_01 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_01": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		"select * from test_table_02 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_02": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		"select * from test_table_03 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_03": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from test_table_01": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table_01", true, "PRIMARY", 1, "pk", false),
			},
		},
		"show index from test_table_02": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table_02", true, "PRIMARY", 1, "pk", false),
			},
		},
		"show index from test_table_03": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table_03", true, "PRIMARY", 1, "pk", false),
			},
		},
		"begin":  {},
		"commit": {},
	}
}
