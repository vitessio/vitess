/*
Copyright 2017 Google Inc.

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
	"testing"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var testTables = []string{
	"included1",
	"included2",
}

func TestTablesFilterPass(t *testing.T) {
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
				Sql:      []byte("dml1 /* _stream included1 (id ) (500 ); */"),
			},
		},
		{
			Statement: &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml2 /* _stream included2 (id ) (500 ); */"),
			},
		},
	}
	eventToken := &querypb.EventToken{
		Position: "MariaDB/0-41983-1",
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `statement: <6, "set1"> statement: <7, "dml1 /* _stream included1 (id ) (500 ); */"> statement: <7, "dml2 /* _stream included2 (id ) (500 ); */"> position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want\n%s, got\n%s", want, got)
	}
}

func TestTablesFilterSkip(t *testing.T) {
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
				Sql:      []byte("dml1 /* _stream excluded1 (id ) (500 ); */"),
			},
		},
	}
	eventToken := &querypb.EventToken{
		Position: "MariaDB/0-41983-1",
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterDDL(t *testing.T) {
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
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterMalformed(t *testing.T) {
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
				Sql:      []byte("dml1 /* _stream excluded1*/"),
			},
		},
	}
	eventToken := &querypb.EventToken{
		Position: "MariaDB/0-41983-1",
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(eventToken, statements)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}
