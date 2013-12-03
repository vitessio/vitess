// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var testKeyrange = key.KeyRange{
	Start: key.KeyspaceId(key.Uint64Key(0).String()),
	End:   key.KeyspaceId(key.Uint64Key(10).String()),
}

func TestKeyrangeFilterPass(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml1 /* EMD keyspace_id:20 */"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml2 /* EMD keyspace_id:2 */"),
			},
		},
		GroupId: "1",
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <4, "dml2 /* EMD keyspace_id:2 */"> position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyrangeFilterSkip(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml1 /* EMD keyspace_id:20 */"),
			},
		},
		GroupId: "1",
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyrangeFilterDDL(t *testing.T) {
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
		GroupId: "1",
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <5, "ddl"> position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyrangeFilterMalformed(t *testing.T) {
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
				Sql:      []byte("dml1 /* EMD keyspace_id:20*/"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("dml1 /* EMD keyspace_id:2a */"),
			},
		},
		GroupId: "1",
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `position: "1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func bltToString(tx *proto.BinlogTransaction) string {
	result := ""
	for _, statement := range tx.Statements {
		result += fmt.Sprintf("statement: <%d, \"%s\"> ", statement.Category, statement.Sql)
	}
	result += fmt.Sprintf("position: \"%s\" ", tx.GroupId)
	return result
}
