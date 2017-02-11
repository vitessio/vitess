// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package schematest provides support for testing packages
// that depend on schema
package schematest

import (
	"github.com/youtube/vitess/go/mysqlconn"
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
				{sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1"))},
			},
		},
		mysqlconn.BaseShowTables: {
			Fields:       mysqlconn.BaseShowTablesFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysqlconn.BaseShowTablesRow("test_table_01", false, ""),
				mysqlconn.BaseShowTablesRow("test_table_02", false, ""),
				mysqlconn.BaseShowTablesRow("test_table_03", false, ""),
			},
		},
		"select * from test_table_01 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_01": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		"select * from test_table_02 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_02": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		"select * from test_table_03 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_03": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from test_table_01": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table_01", true, "PRIMARY", 1, "pk", false),
			},
		},
		"show index from test_table_02": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table_02", true, "PRIMARY", 1, "pk", false),
			},
		},
		"show index from test_table_03": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table_03", true, "PRIMARY", 1, "pk", false),
			},
		},
		"begin":  {},
		"commit": {},
	}
}
