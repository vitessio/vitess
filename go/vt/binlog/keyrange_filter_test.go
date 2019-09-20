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

package binlog

import (
	"fmt"
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var testKeyRange = &topodatapb.KeyRange{
	Start: []byte{},
	End:   []byte{0x10},
}

func TestKeyRangeFilterPass(t *testing.T) {
	statements := []FullBinlogStatement{
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2) values(1, a) /* vtgate:: keyspace_id:02 */"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2, col3) values(1, 2, 3),(4, 5, 6) /* vtgate:: keyspace_id:01,02 *//*trailing_comments */"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2, col3) values(1, 2, 3),(4, 5, 6) /* vtgate:: keyspace_id:01,20 *//*trailing_comments */"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2, col3) values(1, 2, 3),(4, 5, 6) /* vtgate:: keyspace_id:10,20 *//*trailing_comments */"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
				Sql:      []byte("update tbl set col1=1"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_DELETE,
				Sql:      []byte("delete from tbl where col1=1"),
			},
		},
	}
	eventToken := &querypb.EventToken{
		Position: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `statement: <6, "set1"> statement: <7, "insert into tbl(col1, col2) values(1, a) /* vtgate:: keyspace_id:02 */"> statement: <7, "insert into tbl(col1, col2, col3) values (1, 2, 3), (4, 5, 6) /* vtgate:: keyspace_id:01,02 *//*trailing_comments */"> statement: <7, "insert into tbl(col1, col2, col3) values (1, 2, 3) /* vtgate:: keyspace_id:01,20 *//*trailing_comments */"> statement: <8, "update tbl set col1=1"> statement: <9, "delete from tbl where col1=1"> position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want\n%s, got\n%s", want, got)
	}
}

func TestKeyRangeFilterSkip(t *testing.T) {
	statements := []FullBinlogStatement{
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* vtgate:: keyspace_id:20 */"),
			},
		},
	}
	eventToken := &querypb.EventToken{
		Position: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyRangeFilterDDL(t *testing.T) {
	statements := []FullBinlogStatement{
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_DDL,
				Sql:      []byte("ddl"),
			},
		},
	}
	eventToken := &querypb.EventToken{
		Position: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyRangeFilterMalformed(t *testing.T) {
	statements := []FullBinlogStatement{
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("ddl"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* vtgate:: keyspace_id:20*/"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* vtgate:: keyspace_id:2 */"), // Odd-length hex string.
			},
		},
	}
	eventToken := &querypb.EventToken{
		Position: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func bltToString(tx *binlogdatapb.BinlogTransaction) string {
	result := ""
	for _, statement := range tx.Statements {
		result += fmt.Sprintf("statement: <%d, \"%s\"> ", statement.Category, string(statement.Sql))
	}
	result += fmt.Sprintf("position: \"%v\" ", tx.EventToken.Position)
	return result
}
