// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	//"fmt"
	"testing"

	//	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var testTables = []string{
	"included1",
	"included2",
}

func TestTablesFilterPass(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml1 /* _stream included1 (id ) (500 ); */"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml2 /* _stream included2 (id ) (500 ); */"),
			},
		},
		GroupId: 1,
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <4, "dml1 /* _stream included1 (id ) (500 ); */"> statement: <4, "dml2 /* _stream included2 (id ) (500 ); */"> position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterSkip(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml1 /* _stream excluded1 (id ) (500 ); */"),
			},
		},
		GroupId: 1,
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterDDL(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: proto.BL_DDL,
				Sql:      []byte("ddl"),
			},
		},
		GroupId: 1,
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <5, "ddl"> position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestTablesFilterMalformed(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("ddl"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml1 /* _stream excluded1*/"),
			},
		},
		GroupId: 1,
	}
	var got string
	f := TablesFilterFunc(testTables, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}
