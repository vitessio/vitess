package mysqlconn

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
)

// testDescribeTable makes sure the fields returned by 'describe <table>'
// are what we expect.
func testDescribeTable(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Note this specific table sets a default '0' for the 'id' column.
	// This is because without this, we have exceptions:
	// - MariaDB and MySQL 5.6 return '0' as default.
	// - MySQL 5.7 returns NULL as default.
	// So we explicitely set it, to avoid having to check both cases below.
	if _, err := conn.ExecuteFetch("create table for_describe(id int default 0, name varchar(128), primary key(id))", 0, false); err != nil {
		t.Fatal(err)
	}

	result, err := conn.ExecuteFetch("describe for_describe", 10, true)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result.Fields, DescribeTableFields) {
		// MariaDB has '81' instead of '90' of Extra ColumnLength.
		// Just try it and see if it's the only difference.
		if result.Fields[5].ColumnLength == 81 {
			result.Fields[5].ColumnLength = 90
		}

		if !reflect.DeepEqual(result.Fields, DescribeTableFields) {
			for i, f := range result.Fields {
				if !reflect.DeepEqual(f, DescribeTableFields[i]) {
					t.Logf("result.Fields[%v] = %v", i, f)
					t.Logf("        expected = %v", DescribeTableFields[i])
				}
			}
			t.Errorf("Fields returned by 'describe' differ from expected fields: got:\n%v\nexpected:\n%v", result.Fields, DescribeTableFields)
		}
	}

	want := DescribeTableRow("id", "int(11)", false, "PRI", "0")
	if !reflect.DeepEqual(result.Rows[0], want) {
		t.Errorf("Row[0] returned by 'describe' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[0]), RowString(want))
	}

	want = DescribeTableRow("name", "varchar(128)", true, "", "")
	if !reflect.DeepEqual(result.Rows[1], want) {
		t.Errorf("Row[1] returned by 'describe' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[1]), RowString(want))
	}
}

// testShowIndexFromTable makes sure the fields returned by 'show index from <table>'
// are what we expect.
func testShowIndexFromTable(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
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

	if !reflect.DeepEqual(result.Fields, ShowIndexFromTableFields) {
		for i, f := range result.Fields {
			if i < len(ShowIndexFromTableFields) && !reflect.DeepEqual(f, ShowIndexFromTableFields[i]) {
				t.Logf("result.Fields[%v] = %v", i, f)
				t.Logf("        expected = %v", ShowIndexFromTableFields[i])
			}
		}
		t.Errorf("Fields returned by 'show index from' differ from expected fields: got:\n%v\nexpected:\n%v", result.Fields, ShowIndexFromTableFields)
	}

	if len(result.Rows) != 5 {
		t.Errorf("Got %v rows, expected 5", len(result.Rows))
	}

	want := ShowIndexFromTableRow("for_show_index", true, "PRIMARY", 1, "id", false)
	if !reflect.DeepEqual(result.Rows[0], want) {
		t.Errorf("Row[0] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[0]), RowString(want))
	}

	want = ShowIndexFromTableRow("for_show_index", true, "on_name", 1, "name", true)
	if !reflect.DeepEqual(result.Rows[1], want) {
		t.Errorf("Row[1] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[1]), RowString(want))
	}

	want = ShowIndexFromTableRow("for_show_index", false, "on_zipcode", 1, "zipcode", true)
	if !reflect.DeepEqual(result.Rows[2], want) {
		t.Errorf("Row[2] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[2]), RowString(want))
	}

	want = ShowIndexFromTableRow("for_show_index", false, "on_zipcode_name", 1, "zipcode", true)
	if !reflect.DeepEqual(result.Rows[3], want) {
		t.Errorf("Row[3] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[3]), RowString(want))
	}

	want = ShowIndexFromTableRow("for_show_index", false, "on_zipcode_name", 2, "name", true)
	if !reflect.DeepEqual(result.Rows[4], want) {
		t.Errorf("Row[4] returned by 'show index from' differ from expected content: got:\n%v\nexpected:\n%v", RowString(result.Rows[4]), RowString(want))
	}
}

// testBaseShowTables makes sure the fields returned by
// BaseShowTablesForTable are what we expect.
func testBaseShowTables(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("create table for_base_show_tables(id int, name varchar(128), primary key(id)) comment 'fancy table'", 0, false); err != nil {
		t.Fatal(err)
	}

	result, err := conn.ExecuteFetch(BaseShowTablesForTable("for_base_show_tables"), 10, true)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result.Fields, BaseShowTablesFields) {
		// MariaDB has length 17 for unix_timestamp(create_time), see if that's the only difference.
		if result.Fields[2].ColumnLength == 17 {
			result.Fields[2].ColumnLength = 11
		}

		// And try again.
		if !reflect.DeepEqual(result.Fields, BaseShowTablesFields) {
			for i, f := range result.Fields {
				if !reflect.DeepEqual(f, BaseShowTablesFields[i]) {
					t.Logf("result.Fields[%v] = %v", i, f)
					t.Logf("        expected = %v", BaseShowTablesFields[i])
				}
			}
			t.Errorf("Fields returned by BaseShowTables differ from expected fields: got:\n%v\nexpected:\n%v", result.Fields, BaseShowTablesFields)
		}
	}

	want := BaseShowTablesRow("for_base_show_tables", false, "fancy table")
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
		if val.IsNull() {
			result += " NULL"
		} else {
			result += fmt.Sprintf(" (%v)%v", val.Type(), val.String())
		}
	}
	return result
}

// testSchema runs all the schema tests.
func testSchema(t *testing.T, params *sqldb.ConnParams) {
	t.Run("DescribeTable", func(t *testing.T) {
		testDescribeTable(t, params)
	})

	t.Run("ShowIndexFromTable", func(t *testing.T) {
		testShowIndexFromTable(t, params)
	})

	t.Run("BaseShowTables", func(t *testing.T) {
		testBaseShowTables(t, params)
	})
}
