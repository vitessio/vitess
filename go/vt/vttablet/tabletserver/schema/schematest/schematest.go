/*
Copyright 2019 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// AddDefaultQueries returns a default set of queries that can
// be added to load an initial set of tables into the schema.
func AddDefaultQueries(db *fakesqldb.DB) {
	db.ClearQueryPattern()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.Uint64,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(1427325875)},
		},
	})
	db.AddQuery("select @@global.sql_mode", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("STRICT_TRANS_TABLES")},
		},
	})
	db.AddQuery("select @@autocommit", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.Uint64,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("1")},
		},
	})
	db.AddQuery("select @@sql_auto_is_null", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.Uint64,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("0")},
		},
	})

	db.AddQuery(mysql.BaseShowPrimary, &sqltypes.Result{
		Fields: mysql.ShowPrimaryFields,
		Rows: [][]sqltypes.Value{
			mysql.ShowPrimaryRow("test_table_01", "pk"),
			mysql.ShowPrimaryRow("test_table_02", "pk"),
			mysql.ShowPrimaryRow("test_table_03", "pk"),
			mysql.ShowPrimaryRow("seq", "id"),
			mysql.ShowPrimaryRow("msg", "id"),
		},
	})

	db.AddQueryPattern("select .* from test_table_01 where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQueryPattern(queryForTable("test_table_01"), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column_name",
			"varchar",
		),
		"pk",
	))

	db.AddQueryPattern("select .* from test_table_02 where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQueryPattern(queryForTable("test_table_02"), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column_name",
			"varchar",
		),
		"pk",
	))

	db.AddQueryPattern("select .* from test_table_03 where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQueryPattern(queryForTable("test_table_03"), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column_name",
			"varchar",
		),
		"pk",
	))

	db.AddQueryPattern("select .* from seq where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int32,
		}, {
			Name: "next_id",
			Type: sqltypes.Int64,
		}, {
			Name: "cache",
			Type: sqltypes.Int64,
		}, {
			Name: "increment",
			Type: sqltypes.Int64,
		}},
	})
	db.AddQueryPattern(queryForTable("seq"), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column_name",
			"varchar",
		),
		"id", "next_id", "cache", "increment",
	))

	db.AddQueryPattern("select .* from msg where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "priority",
			Type: sqltypes.Int64,
		}, {
			Name: "time_next",
			Type: sqltypes.Int64,
		}, {
			Name: "epoch",
			Type: sqltypes.Int64,
		}, {
			Name: "time_acked",
			Type: sqltypes.Int64,
		}, {
			Name: "message",
			Type: sqltypes.Int64,
		}},
	})
	db.AddQueryPattern(queryForTable("msg"), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column_name",
			"varchar",
		),
		"id", "priority", "time_next", "epoch", "time_acked", "message",
	))

	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})

}

func queryForTable(table string) string {
	return fmt.Sprintf(mysql.GetColumnNamesQueryPatternForTable, table)
}
