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

package mysql

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file provides a few utility variables and methods, mostly for tests.
// The assumptions made about the types of fields and data returned
// by MySQl are validated in schema_test.go. This way all tests
// can use these variables and methods to simulate a MySQL server
// (using fakesqldb/ package for instance) and still be guaranteed correct
// data.

const (
	// BaseShowPrimary is the base query for fetching primary key info.
	BaseShowPrimary = `
		SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND LOWER(INDEX_NAME) = 'primary'
		ORDER BY table_name, SEQ_IN_INDEX`
	// ShowRowsRead is the query used to find the number of rows read.
	ShowRowsRead = "show status like 'Innodb_rows_read'"

	// DetectSchemaChange query detects if there is any schema change from previous copy.
	DetectSchemaChange = `
SELECT DISTINCT table_name
FROM (
	SELECT table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
	FROM information_schema.columns
	WHERE table_schema = database()

	UNION ALL

	SELECT table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
	FROM _vt.schemacopy c
	WHERE table_schema = database()
) _inner
GROUP BY table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
HAVING COUNT(*) = 1
`

	// ClearSchemaCopy query clears the schemacopy table.
	ClearSchemaCopy = `delete from _vt.schemacopy where table_schema = database()`

	// InsertIntoSchemaCopy query copies over the schema information from information_schema.columns table.
	InsertIntoSchemaCopy = `insert _vt.schemacopy
select table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
from information_schema.columns
where table_schema = database()`

	// fetchColumns are the columns we fetch
	fetchColumns = "table_name, column_name, data_type, collation_name"

	// FetchUpdatedTables queries fetches all information about updated tables
	FetchUpdatedTables = `select  ` + fetchColumns + `
from _vt.schemacopy
where table_schema = database() and
	table_name in ::tableNames
order by table_name, ordinal_position`

	// FetchTables queries fetches all information about tables
	FetchTables = `select ` + fetchColumns + `
from _vt.schemacopy
where table_schema = database()
order by table_name, ordinal_position`

	// GetColumnNamesQueryPatternForTable is used for mocking queries in unit tests
	GetColumnNamesQueryPatternForTable = `SELECT COLUMN_NAME.*TABLE_NAME.*%s.*`

	// Views
	InsertIntoViewsTable = `INSERT INTO _vt.views (
	TABLE_NAME,
	VIEW_DEFINITION,
	CREATE_STATEMENT) VALUES (:TABLE_NAME, :VIEW_DEFINITION, :CREATE_STATEMENT)`

	ReplaceIntoViewsTable = `REPLACE INTO _vt.views (
	TABLE_NAME,
	VIEW_DEFINITION,
	CREATE_STATEMENT) VALUES (:TABLE_NAME, :VIEW_DEFINITION, :CREATE_STATEMENT)`

	UpdateViewsTable = `UPDATE _vt.views 
	SET VIEW_DEFINITION = :VIEW_DEFINITION, CREATE_STATEMENT = :CREATE_STATEMENT 
	WHERE TABLE_NAME = :TABLE_NAME`

	DeleteFromViewsTable = `DELETE FROM _vt.views WHERE TABLE_NAME IN ::TABLE_NAME`

	SelectFromViewsTable = `SELECT TABLE_NAME FROM _vt.views WHERE TABLE_NAME IN ::TABLE_NAME`

	SelectAllViews = `SELECT TABLE_NAME, UPDATED_AT FROM _vt.views`
)

// BaseShowTablesFields contains the fields returned by a BaseShowTables or a BaseShowTablesForTable command.
// They are validated by the
// testBaseShowTables test.
var BaseShowTablesFields = []*querypb.Field{{
	Name:         "t.table_name",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_NAME",
	ColumnLength: 192,
	Charset:      collations.CollationUtf8ID,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "t.table_type",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_TYPE",
	ColumnLength: 192,
	Charset:      collations.CollationUtf8ID,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "unix_timestamp(t.create_time)",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      collations.CollationBinaryID,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}, {
	Name:         "t.table_comment",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_COMMENT",
	ColumnLength: 6144,
	Charset:      collations.CollationUtf8ID,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "i.file_size",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      collations.CollationBinaryID,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}, {
	Name:         "i.allocated_size",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      collations.CollationBinaryID,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}}

// BaseShowTablesRow returns the fields from a BaseShowTables or
// BaseShowTablesForTable command.
func BaseShowTablesRow(tableName string, isView bool, comment string) []sqltypes.Value {
	tableType := "BASE TABLE"
	if isView {
		tableType = "VIEW"
	}
	return []sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(tableName)),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(tableType)),
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("1427325875")), // unix_timestamp(create_time)
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(comment)),
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("100")), // file_size
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("150")), // allocated_size
	}
}

// ShowPrimaryFields contains the fields for a BaseShowPrimary.
var ShowPrimaryFields = []*querypb.Field{{
	Name: "table_name",
	Type: sqltypes.VarChar,
}, {
	Name: "column_name",
	Type: sqltypes.VarChar,
}}

// ShowPrimaryRow returns a row for a primary key column.
func ShowPrimaryRow(tableName, colName string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(tableName)),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(colName)),
	}
}
