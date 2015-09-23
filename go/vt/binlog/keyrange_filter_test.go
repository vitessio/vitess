// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var testKeyRange = &pb.KeyRange{
	Start: []byte{},
	End:   []byte{0x10},
}

func TestKeyRangeFilterPass(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      "set1",
			}, {
				Category: proto.BL_DML,
				Sql:      "dml1 /* vtgate:: keyspace_id:20 */",
			}, {
				Category: proto.BL_DML,
				Sql:      "dml2 /* vtgate:: keyspace_id:02 */",
			},
		},
		TransactionID: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(key.KIT_UINT64, testKeyRange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <4, "dml2 /* vtgate:: keyspace_id:02 */"> transaction_id: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyRangeFilterSkip(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      "set1",
			}, {
				Category: proto.BL_DML,
				Sql:      "dml1 /* vtgate:: keyspace_id:20 */",
			},
		},
		TransactionID: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(key.KIT_UINT64, testKeyRange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `transaction_id: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyRangeFilterDDL(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      "set1",
			}, {
				Category: proto.BL_DDL,
				Sql:      "ddl",
			},
		},
		TransactionID: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(key.KIT_UINT64, testKeyRange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `transaction_id: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyRangeFilterMalformed(t *testing.T) {
	input := proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      "set1",
			}, {
				Category: proto.BL_DML,
				Sql:      "ddl",
			}, {
				Category: proto.BL_DML,
				Sql:      "dml1 /* vtgate:: keyspace_id:20*/",
			}, {
				Category: proto.BL_DML,
				Sql:      "dml1 /* vtgate:: keyspace_id:2 */", // Odd-length hex string.
			},
		},
		TransactionID: "MariaDB/0-41983-1",
	}
	var got string
	f := KeyRangeFilterFunc(key.KIT_UINT64, testKeyRange, func(reply *proto.BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `transaction_id: "MariaDB/0-41983-1" `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func bltToString(tx *proto.BinlogTransaction) string {
	result := ""
	for _, statement := range tx.Statements {
		result += fmt.Sprintf("statement: <%d, \"%s\"> ", statement.Category, statement.Sql)
	}
	result += fmt.Sprintf("transaction_id: \"%v\" ", tx.TransactionID)
	return result
}
