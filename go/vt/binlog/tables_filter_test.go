// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	input := binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* _stream included1 (id ) (500 ); */"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml2 /* _stream included2 (id ) (500 ); */"),
			},
		},
		EventToken: &querypb.EventToken{
			Position: "MariaDB/0-41983-1",
		},
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <7, "dml1 /* _stream included1 (id ) (500 ); */"> statement: <7, "dml2 /* _stream included2 (id ) (500 ); */"> position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want\n%s, got\n%s", want, got)
	}
}

func TestTablesFilterSkip(t *testing.T) {
	input := binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("dml1 /* _stream excluded1 (id ) (500 ); */"),
			},
		},
		EventToken: &querypb.EventToken{
			Position: "MariaDB/0-41983-1",
		},
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterDDL(t *testing.T) {
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
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterMalformed(t *testing.T) {
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
				Sql:      []byte("dml1 /* _stream excluded1*/"),
			},
		},
		EventToken: &querypb.EventToken{
			Position: "MariaDB/0-41983-1",
		},
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *binlogdatapb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}
