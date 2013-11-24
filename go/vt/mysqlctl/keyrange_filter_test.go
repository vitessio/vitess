// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
)

var testKeyrange = key.KeyRange{
	Start: key.KeyspaceId(key.Uint64Key(0).String()),
	End:   key.KeyspaceId(key.Uint64Key(10).String()),
}

func TestKeyrangeFilterPass(t *testing.T) {
	input := BinlogTransaction{
		Statements: []Statement{
			{
				Category: BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: BL_DML,
				Sql:      []byte("dml1 /* EMD keyspace_id:20 */"),
			}, {
				Category: BL_DML,
				Sql:      []byte("dml2 /* EMD keyspace_id:2 */"),
			},
		},
		Position: BinlogPosition{
			GroupId: 1,
		},
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <4, "dml2 /* EMD keyspace_id:2 */"> position: <1, 0> `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyrangeFilterSkip(t *testing.T) {
	input := BinlogTransaction{
		Statements: []Statement{
			{
				Category: BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: BL_DML,
				Sql:      []byte("dml1 /* EMD keyspace_id:20 */"),
			},
		},
		Position: BinlogPosition{
			GroupId: 1,
		},
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := ""
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyrangeFilterDDL(t *testing.T) {
	input := BinlogTransaction{
		Statements: []Statement{
			{
				Category: BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: BL_DDL,
				Sql:      []byte("ddl"),
			},
		},
		Position: BinlogPosition{
			GroupId: 1,
		},
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := `statement: <6, "set1"> statement: <5, "ddl"> position: <1, 0> `
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestKeyrangeFilterMalformed(t *testing.T) {
	input := BinlogTransaction{
		Statements: []Statement{
			{
				Category: BL_SET,
				Sql:      []byte("set1"),
			}, {
				Category: BL_DML,
				Sql:      []byte("ddl"),
			}, {
				Category: BL_DML,
				Sql:      []byte("dml1 /* EMD keyspace_id:20*/"),
			}, {
				Category: BL_DML,
				Sql:      []byte("dml1 /* EMD keyspace_id:2a */"),
			},
		},
		Position: BinlogPosition{
			GroupId: 1,
		},
	}
	var got string
	f := KeyrangeFilterFunc(testKeyrange, func(reply *BinlogTransaction) error {
		got = bltToString(reply)
		return nil
	})
	f(&input)
	want := ""
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func bltToString(tx *BinlogTransaction) string {
	result := ""
	for _, statement := range tx.Statements {
		result += fmt.Sprintf("statement: <%d, \"%s\"> ", statement.Category, statement.Sql)
	}
	result += fmt.Sprintf("position: <%d, %d> ", tx.Position.GroupId, tx.Position.ServerId)
	return result
}
