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

package splitquery

// This file contains utility routines for used in splitquery tests.

import (
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// getSchema returns a fake schema object that can be given to SplitParams
func getTestSchema() map[string]*schema.Table {
	table := schema.Table{
		Name: sqlparser.NewTableIdent("test_table"),
	}
	zero := sqltypes.NewInt64(0)
	table.AddColumn("id", sqltypes.Int64, zero, "")
	table.AddColumn("int32_col", sqltypes.Int32, zero, "")
	table.AddColumn("uint32_col", sqltypes.Uint32, zero, "")
	table.AddColumn("int64_col", sqltypes.Int64, zero, "")
	table.AddColumn("uint64_col", sqltypes.Uint64, zero, "")
	table.AddColumn("float32_col", sqltypes.Float32, zero, "")
	table.AddColumn("float64_col", sqltypes.Float64, zero, "")
	table.AddColumn("user_id", sqltypes.Int64, zero, "")
	table.AddColumn("user_id2", sqltypes.Int64, zero, "")
	table.AddColumn("id2", sqltypes.Int64, zero, "")
	table.AddColumn("count", sqltypes.Int64, zero, "")
	table.PKColumns = []int{0, 7}
	addIndexToTable(&table, "PRIMARY", true, "id", "user_id")
	addIndexToTable(&table, "idx_id2", false, "id2")
	addIndexToTable(&table, "idx_int64_col", false, "int64_col")
	addIndexToTable(&table, "idx_uint64_col", false, "uint64_col")
	addIndexToTable(&table, "idx_float64_col", false, "float64_col")
	addIndexToTable(&table, "idx_id_user_id", false, "id", "user_id")
	addIndexToTable(&table, "idx_id_user_id_user_id_2", false, "id", "user_id", "user_id2")

	table.SetMysqlStats(
		sqltypes.NewInt64(1000), /* TableRows */
		sqltypes.NewInt64(100),  /* DataLength */
		sqltypes.NewInt64(123),  /* IndexLength */
		sqltypes.NewInt64(456),  /* DataFree */
		sqltypes.NewInt64(457),  /* MaxDataLength */
	)

	result := make(map[string]*schema.Table)
	result["test_table"] = &table

	tableNoPK := schema.Table{
		Name: sqlparser.NewTableIdent("test_table_no_pk"),
	}
	tableNoPK.AddColumn("id", sqltypes.Int64, zero, "")
	tableNoPK.PKColumns = []int{}
	result["test_table_no_pk"] = &tableNoPK

	return result
}

var testSchema = getTestSchema()

func getTestSchemaColumn(tableName, columnName string) *schema.TableColumn {
	tableSchema := testSchema[tableName]
	columnIndex := tableSchema.FindColumn(sqlparser.NewColIdent(columnName))
	if columnIndex < 0 {
		panic(fmt.Sprintf(
			"Can't find columnName: %v (tableName: %v) in test schema.", columnName, tableName))
	}
	return &tableSchema.Columns[columnIndex]
}

// addIndexToTable adds an index named 'indexName' to 'table' with the given 'indexCols'.
// It uses 12345 as the cardinality.
// It returns the new index.
func addIndexToTable(table *schema.Table, indexName string, unique bool, indexCols ...string) *schema.Index {
	index := table.AddIndex(indexName, unique)
	for _, indexCol := range indexCols {
		index.AddColumn(indexCol, 12345)
	}
	return index
}
