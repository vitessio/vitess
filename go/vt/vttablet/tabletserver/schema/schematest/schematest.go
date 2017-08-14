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
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
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
		"select * from msg where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "time_scheduled",
				Type: sqltypes.Int32,
			}, {
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "time_next",
				Type: sqltypes.Int64,
			}, {
				Name: "epoch",
				Type: sqltypes.Int64,
			}, {
				Name: "time_created",
				Type: sqltypes.Int64,
			}, {
				Name: "time_acked",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.Int64,
			}},
		},
		"describe msg": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 7,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("time_scheduled", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("id", "bigint(20)", false, "PRI", "0"),
				mysql.DescribeTableRow("time_next", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("epoch", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_created", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_acked", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("message", "bigint(20)", false, "", "0"),
			},
		},
		"show index from msg": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("msg", true, "PRIMARY", 1, "time_scheduled", false),
				mysql.ShowIndexFromTableRow("msg", true, "PRIMARY", 2, "id", false),
			},
		},
		mysql.BaseShowTablesForTable("msg"): {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		},
		"begin":  {},
		"commit": {},
	}
}
