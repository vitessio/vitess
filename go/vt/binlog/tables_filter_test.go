// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"testing"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

var testTables = []string{
	"included1",
	"included2",
}

func TestTablesFilterPass(t *testing.T) {
	input := pb.BinlogTransaction{
		Statements: []*pb.BinlogTransaction_Statement{
			{
				Category: pb.BinlogTransaction_Statement_BL_SET,
				Sql:      "set1",
			}, {
				Category: pb.BinlogTransaction_Statement_BL_DML,
				Sql:      "dml1 /* _stream included1 (id ) (500 ); */",
			}, {
				Category: pb.BinlogTransaction_Statement_BL_DML,
				Sql:      "dml2 /* _stream included2 (id ) (500 ); */",
			},
		},
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *pb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <4, "dml1 /* _stream included1 (id ) (500 ); */"> statement: <4, "dml2 /* _stream included2 (id ) (500 ); */"> transaction_id: "" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterSkip(t *testing.T) {
	input := pb.BinlogTransaction{
		Statements: []*pb.BinlogTransaction_Statement{
			{
				Category: pb.BinlogTransaction_Statement_BL_SET,
				Sql:      "set1",
			}, {
				Category: pb.BinlogTransaction_Statement_BL_DML,
				Sql:      "dml1 /* _stream excluded1 (id ) (500 ); */",
			},
		},
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *pb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `transaction_id: "" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterDDL(t *testing.T) {
	input := pb.BinlogTransaction{
		Statements: []*pb.BinlogTransaction_Statement{
			{
				Category: pb.BinlogTransaction_Statement_BL_SET,
				Sql:      "set1",
			}, {
				Category: pb.BinlogTransaction_Statement_BL_DDL,
				Sql:      "ddl",
			},
		},
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *pb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `transaction_id: "" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterMalformed(t *testing.T) {
	input := pb.BinlogTransaction{
		Statements: []*pb.BinlogTransaction_Statement{
			{
				Category: pb.BinlogTransaction_Statement_BL_SET,
				Sql:      "set1",
			}, {
				Category: pb.BinlogTransaction_Statement_BL_DML,
				Sql:      "ddl",
			}, {
				Category: pb.BinlogTransaction_Statement_BL_DML,
				Sql:      "dml1 /* _stream excluded1*/",
			},
		},
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *pb.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `transaction_id: "" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}
