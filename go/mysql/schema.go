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
	BaseShowPrimary = "SELECT table_name, column_name FROM information_schema.key_column_usage WHERE table_schema=database() AND constraint_name='PRIMARY' ORDER BY table_name, ordinal_position"
	// ShowRowsRead is the query used to find the number of rows read.
	ShowRowsRead = "show status like 'Innodb_rows_read'"

	// CreateVTDatabase creates the _vt database
	CreateVTDatabase = `CREATE DATABASE IF NOT EXISTS _vt`

	// CreateSchemaCopyTable query creates schemacopy table in _vt schema.
	CreateSchemaCopyTable = `
CREATE TABLE if not exists _vt.schemacopy (
	table_schema varchar(64) NOT NULL,
	table_name varchar(64) NOT NULL,
	column_name varchar(64) NOT NULL,
	ordinal_position bigint(21) unsigned NOT NULL,
	character_set_name varchar(32) DEFAULT NULL,
	collation_name varchar(32) DEFAULT NULL,
	data_type varchar(64) NOT NULL,
	column_key varchar(3) NOT NULL,
	PRIMARY KEY (table_schema, table_name, ordinal_position))`

	detectNewColumns = `
select ISC.table_name
from information_schema.columns as ISC
	 left join _vt.schemacopy as c on 
		ISC.table_name = c.table_name and 
		ISC.table_schema=c.table_schema and 
		ISC.ordinal_position = c.ordinal_position
where ISC.table_schema = database() AND c.table_schema is null`

	detectChangeColumns = `
select ISC.table_name
from information_schema.columns as ISC
	  join _vt.schemacopy as c on 
		ISC.table_name = c.table_name and 
		ISC.table_schema=c.table_schema and 
		ISC.ordinal_position = c.ordinal_position
where ISC.table_schema = database() 
	AND (not(c.column_name <=> ISC.column_name) 
	OR not(ISC.character_set_name <=> c.character_set_name) 
	OR not(ISC.collation_name <=> c.collation_name) 
	OR not(ISC.data_type <=> c.data_type) 
	OR not(ISC.column_key <=> c.column_key))`

	detectRemoveColumns = `
select c.table_name
from information_schema.columns as ISC
	  right join _vt.schemacopy as c on 
		ISC.table_name = c.table_name and 
		ISC.table_schema=c.table_schema and 
		ISC.ordinal_position = c.ordinal_position
where c.table_schema = database() AND ISC.table_schema is null`

	// DetectSchemaChange query detects if there is any schema change from previous copy.
	DetectSchemaChange = detectChangeColumns + " UNION " + detectNewColumns + " UNION " + detectRemoveColumns

	// ClearSchemaCopy query clears the schemacopy table.
	ClearSchemaCopy = `delete from _vt.schemacopy`

	// InsertIntoSchemaCopy query copies over the schema information from information_schema.columns table.
	InsertIntoSchemaCopy = `insert _vt.schemacopy 
select table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key 
from information_schema.columns 
where table_schema = database()`

	// FetchUpdatedTables queries fetches all information about updated tables
	FetchUpdatedTables = `select table_name, column_name, data_type 
from _vt.schemacopy 
where table_schema = database() and 
	table_name in :tableNames 
order by table_name, ordinal_position`

	// FetchTables queries fetches all information about tables
	FetchTables = `select table_name, column_name, data_type 
from _vt.schemacopy 
where table_schema = database() 
order by table_name, ordinal_position`
)

// VTDatabaseInit contains all the schema creation queries needed to
var VTDatabaseInit = []string{
	CreateVTDatabase,
	CreateSchemaCopyTable,
}

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
	Charset:      CharacterSetUtf8,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "t.table_type",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_TYPE",
	ColumnLength: 192,
	Charset:      CharacterSetUtf8,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "unix_timestamp(t.create_time)",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      CharacterSetBinary,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}, {
	Name:         "t.table_comment",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_COMMENT",
	ColumnLength: 6144,
	Charset:      CharacterSetUtf8,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "i.file_size",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      CharacterSetBinary,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}, {
	Name:         "i.allocated_size",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      CharacterSetBinary,
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
