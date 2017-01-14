// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"testing"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var testKeyRange = &topodatapb.KeyRange{
	Start: []byte{},
	End:   []byte{0x10},
}

func TestKeyRangeFilterPass(t *testing.T) {
	input := binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2) values(1, a) /* vtgate:: keyspace_id:02 */"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2, col3) values(1, 2, 3),(4, 5, 6) /* vtgate:: keyspace_id:01,02 *//*trailing_comments */"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2, col3) values(1, 2, 3),(4, 5, 6) /* vtgate:: keyspace_id:01,20 *//*trailing_comments */"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("insert into tbl(col1, col2, col3) values(1, 2, 3),(4, 5, 6) /* vtgate:: keyspace_id:10,20 *//*trailing_comments */"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
				Sql:      []byte("update tbl set col1=1"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_DELETE,
				Sql:      []byte("delete from tbl where col1=1"),
			},
		},
		EventToken: &querypb.EventToken{
			Position: "MariaDB/0-41983-1",
		},
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <7, "insert into tbl(col1, col2) values(1, a) /* vtgate:: keyspace_id:02 */"> statement: <7, "insert into tbl(col1, col2, col3) values (1, 2, 3), (4, 5, 6) /* vtgate:: keyspace_id:01,02 *//*trailing_comments */"> statement: <7, "insert into tbl(col1, col2, col3) values (1, 2, 3) /* vtgate:: keyspace_id:01,20 *//*trailing_comments */"> statement: <8, "update tbl set col1=1"> statement: <9, "delete from tbl where col1=1"> position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want\n%s, got\n%s", want, got)
	}
}

func TestKeyRangeFilterSkip(t *testing.T) {
	input := binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* vtgate:: keyspace_id:20 */"),
			},
		},
		EventToken: &querypb.EventToken{
			Position: "MariaDB/0-41983-1",
		},
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyRangeFilterDDL(t *testing.T) {
	input := binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_DDL,
				Sql:      []byte("ddl"),
			},
		},
		EventToken: &querypb.EventToken{
			Position: "MariaDB/0-41983-1",
		},
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyRangeFilterMalformed(t *testing.T) {
	input := binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("ddl"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* vtgate:: keyspace_id:20*/"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* vtgate:: keyspace_id:2 */"), // Odd-length hex string.
			},
		},
		EventToken: &querypb.EventToken{
			Position: "MariaDB/0-41983-1",
		},
	}
	var got string
	f := KeyRangeFilterFunc(testKeyRange, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
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
