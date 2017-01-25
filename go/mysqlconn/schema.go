package mysqlconn

import (
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// This file provides a few utility variables and methods, mostly for tests.
// The assumptions made about the types of fields and data returned
// by MySQl are validated in schema_test.go. This way all tests
// can use these variables and methods to simulate a MySQL server
// (using fakesqldb/ package for instance) and still be guaranteed correct
// data.

// DescribeTableFields contains the fields returned by a
// 'describe <table>' command. They are validated by the testDescribeTable
// test.
var DescribeTableFields = []*querypb.Field{
	{
		Name:         "Field",
		Type:         querypb.Type_VARCHAR,
		Table:        "COLUMNS",
		OrgTable:     "COLUMNS",
		Database:     "information_schema",
		OrgName:      "COLUMN_NAME",
		ColumnLength: 192,
		Charset:      33,
		Flags:        1,
	},
	{
		Name:         "Type",
		Type:         querypb.Type_TEXT,
		Table:        "COLUMNS",
		OrgTable:     "COLUMNS",
		Database:     "information_schema",
		OrgName:      "COLUMN_TYPE",
		ColumnLength: 589815,
		Charset:      33,
		Flags:        17,
	},
	{
		Name:         "Null",
		Type:         querypb.Type_VARCHAR,
		Table:        "COLUMNS",
		OrgTable:     "COLUMNS",
		Database:     "information_schema",
		OrgName:      "IS_NULLABLE",
		ColumnLength: 9,
		Charset:      33,
		Flags:        1,
	},
	{
		Name:         "Key",
		Type:         querypb.Type_VARCHAR,
		Table:        "COLUMNS",
		OrgTable:     "COLUMNS",
		Database:     "information_schema",
		OrgName:      "COLUMN_KEY",
		ColumnLength: 9,
		Charset:      33,
		Flags:        1,
	},
	{
		Name:         "Default",
		Type:         querypb.Type_TEXT,
		Table:        "COLUMNS",
		OrgTable:     "COLUMNS",
		Database:     "information_schema",
		OrgName:      "COLUMN_DEFAULT",
		ColumnLength: 589815,
		Charset:      33,
		Flags:        16,
	},
	{
		Name:         "Extra",
		Type:         querypb.Type_VARCHAR,
		Table:        "COLUMNS",
		OrgTable:     "COLUMNS",
		Database:     "information_schema",
		OrgName:      "EXTRA",
		ColumnLength: 90,
		Charset:      33,
		Flags:        1,
	},
}

// DescribeTableRow returns a row for a 'describe table' command.
// 'name' is the name of the field.
// 'type' is the type of the field. Something like:
//   'int(11)' for 'int'
//   'int(10) unsigned' for 'int unsigned'
//   'bigint(20)' for 'bigint'
//   'bigint(20) unsigned' for 'bigint unsigned'
//   'varchar(128)'
// 'null' is true if the field can be NULL.
// 'key' is either:
//    - 'PRI' if part of the primary key. If not:
//    - 'UNI' if part of a unique index. If not:
//    - 'MUL' if part of a non-unique index. If not:
//    - empty if part of no key / index.
// 'def' is the default value for the field. Empty if NULL default.
func DescribeTableRow(name string, typ string, null bool, key string, def string) []sqltypes.Value {
	nullStr := "NO"
	if null {
		nullStr = "YES"
	}
	defCell := sqltypes.NULL
	if def != "" {
		defCell = sqltypes.MakeTrusted(sqltypes.Text, []byte(def))
	}
	return []sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(name)),
		sqltypes.MakeTrusted(sqltypes.Text, []byte(typ)),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(nullStr)),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(key)),
		defCell,
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte("")), //extra
	}
}
