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

// TestBaseShowTables makes sure the fields returned by
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
