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

package endtoend

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

// testDescribeTable makes sure the fields returned by 'describe <table>'
// are what we expect.
func testDescribeTable(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Note this specific table sets a default '0' for the 'id' column.
	// This is because without this, we have exceptions:
	// - MariaDB and MySQL 5.6 return '0' as default.
	// - MySQL 5.7 returns NULL as default.
	// So we explicitly set it, to avoid having to check both cases below.
	if _, err := conn.ExecuteFetch("create table for_describe(id int default 0, name varchar(128), primary key(id))", 0, false); err != nil {
		t.Fatal(err)
	}

	result, err := conn.ExecuteFetch("describe for_describe", 10, true)
	if err != nil {
		t.Fatal(err)
	}

	// Zero-out the column lengths, because they can't be compared.
	for i := range result.Fields {
		result.Fields[i].ColumnLength = 0
	}

	if !sqltypes.FieldsEqual(result.Fields, mysql.DescribeTableFields) {
		for i, f := range result.Fields {
			if !proto.Equal(f, mysql.DescribeTableFields[i]) {
				t.Logf("result.Fields[%v] = %v", i, f)
				t.Logf("        expected = %v", mysql.DescribeTableFields[i])
			}
		}
		t.Errorf("Fields returned by 'describe' differ from expected fields: got:\n%v\nexpected:\n%v", result.Fields, mysql.DescribeTableFields)
	}

	want := mysql.DescribeTableRow("id", "int(11)", false, "PRI", "0")
	if !reflect.DeepEqual(result.Rows[0], want) {
		t.Errorf("Row[0] returned by 'describe' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[0]), RowString(want))
	}

	want = mysql.DescribeTableRow("name", "varchar(128)", true, "", "")
	if !reflect.DeepEqual(result.Rows[1], want) {
		t.Errorf("Row[1] returned by 'describe' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[1]), RowString(want))
	}
}

// testShowIndexFromTable makes sure the fields returned by 'show index from <table>'
// are what we expect.
func testShowIndexFromTable(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("create table for_show_index(id int, name varchar(128), zipcode varchar(5), primary key(id), unique index on_name (name), index on_zipcode (zipcode), index on_zipcode_name (zipcode, name))", 0, false); err != nil {
		t.Fatal(err)
	}

	result, err := conn.ExecuteFetch("show index from for_show_index", 10, true)
	if err != nil {
		t.Fatal(err)
	}

	if !sqltypes.FieldsEqual(result.Fields, mysql.ShowIndexFromTableFields) {
		for i, f := range result.Fields {
			if i < len(mysql.ShowIndexFromTableFields) && !proto.Equal(f, mysql.ShowIndexFromTableFields[i]) {
				t.Logf("result.Fields[%v] = %v", i, f)
				t.Logf("        expected = %v", mysql.ShowIndexFromTableFields[i])
			}
		}
		t.Errorf("Fields returned by 'show index from' differ from expected fields: got:\n%v\nexpected:\n%v", result.Fields, mysql.ShowIndexFromTableFields)
	}

	if len(result.Rows) != 5 {
		t.Errorf("Got %v rows, expected 5", len(result.Rows))
	}

	want := mysql.ShowIndexFromTableRow("for_show_index", true, "PRIMARY", 1, "id", false)
	if !reflect.DeepEqual(result.Rows[0], want) {
		t.Errorf("Row[0] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[0]), RowString(want))
	}

	want = mysql.ShowIndexFromTableRow("for_show_index", true, "on_name", 1, "name", true)
	if !reflect.DeepEqual(result.Rows[1], want) {
		t.Errorf("Row[1] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[1]), RowString(want))
	}

	want = mysql.ShowIndexFromTableRow("for_show_index", false, "on_zipcode", 1, "zipcode", true)
	if !reflect.DeepEqual(result.Rows[2], want) {
		t.Errorf("Row[2] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[2]), RowString(want))
	}

	want = mysql.ShowIndexFromTableRow("for_show_index", false, "on_zipcode_name", 1, "zipcode", true)
	if !reflect.DeepEqual(result.Rows[3], want) {
		t.Errorf("Row[3] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[3]), RowString(want))
	}

	want = mysql.ShowIndexFromTableRow("for_show_index", false, "on_zipcode_name", 2, "name", true)
	if !reflect.DeepEqual(result.Rows[4], want) {
		t.Errorf("Row[4] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[4]), RowString(want))
	}
}

// testBaseShowTables makes sure the fields returned by
// BaseShowTablesForTable are what we expect.
func testBaseShowTables(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("create table for_base_show_tables(id int, name varchar(128), primary key(id)) comment 'fancy table'", 0, false); err != nil {
		t.Fatal(err)
	}

	result, err := conn.ExecuteFetch(mysql.BaseShowTablesForTable("for_base_show_tables"), 10, true)
	if err != nil {
		t.Fatal(err)
	}

	// MariaDB has length 17 for unix_timestamp(create_time).
	if conn.IsMariaDB() && result.Fields[2].ColumnLength == 17 {
		result.Fields[2].ColumnLength = 11
	}

	if !sqltypes.FieldsEqual(result.Fields, mysql.BaseShowTablesFields) {
		for i, f := range result.Fields {
			if i < len(mysql.BaseShowTablesFields) && !proto.Equal(f, mysql.BaseShowTablesFields[i]) {
				t.Logf("result.Fields[%v] = %v", i, f)
				t.Logf("        expected = %v", mysql.BaseShowTablesFields[i])
			}
		}
		t.Errorf("Fields returned by BaseShowTables differ from expected fields: got:\n%v\nexpected:\n%v", result.Fields, mysql.BaseShowTablesFields)
	}

	want := mysql.BaseShowTablesRow("for_base_show_tables", false, "fancy table")
	result.Rows[0][2] = sqltypes.MakeTrusted(sqltypes.Int64, []byte("1427325875")) // unix_timestamp(create_time)
	result.Rows[0][4] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("0"))         // table_rows
	result.Rows[0][5] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("0"))         // data_length
	result.Rows[0][6] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("0"))         // index_length
	result.Rows[0][7] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("0"))         // data_free
	result.Rows[0][8] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("0"))         // max_data_length
	if !reflect.DeepEqual(result.Rows[0], want) {
		t.Errorf("Row[0] returned by BaseShowTables differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[0]), RowString(want))
	}
}

func RowString(row []sqltypes.Value) string {
	l := len(row)
	result := fmt.Sprintf("%v values:", l)
	for _, val := range row {
		result += fmt.Sprintf(" %v", val)
	}
	return result
}

// TestSchema runs all the schema tests.
func TestSchema(t *testing.T) {
	t.Run("DescribeTable", func(t *testing.T) {
		testDescribeTable(t)
	})

	t.Run("ShowIndexFromTable", func(t *testing.T) {
		testShowIndexFromTable(t)
	})

	t.Run("BaseShowTables", func(t *testing.T) {
		testBaseShowTables(t)
	})
}
