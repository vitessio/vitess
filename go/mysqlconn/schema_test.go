package mysqlconn

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
)

// testDescribeTable makes sure the fields returned by 'describe table'
// are what we expect.
func testDescribeTable(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

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
}
