// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

func TestBatchRead(t *testing.T) {
	client := framework.NewDefaultClient()
	queries := []proto.BoundQuery{{
		Sql:           "select * from vtocc_a where id = :a",
		BindVariables: map[string]interface{}{"a": 2},
	}, {
		Sql:           "select * from vtocc_b where id = :b",
		BindVariables: map[string]interface{}{"b": 2},
	}}
	qr1 := mproto.QueryResult{
		Fields: []mproto.Field{{
			Name:  "eid",
			Type:  mysql.TypeLonglong,
			Flags: 0,
		}, {
			Name:  "id",
			Type:  mysql.TypeLong,
			Flags: 0,
		}, {
			Name:  "name",
			Type:  mysql.TypeVarString,
			Flags: 0,
		}, {
			Name:  "foo",
			Type:  mysql.TypeVarString,
			Flags: mysql.FlagBinary,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.Numeric("2")},
				sqltypes.Value{Inner: sqltypes.String("bcde")},
				sqltypes.Value{Inner: sqltypes.String("fghi")},
			},
		},
	}
	qr2 := mproto.QueryResult{
		Fields: []mproto.Field{{
			Name:  "eid",
			Type:  mysql.TypeLonglong,
			Flags: 0,
		}, {
			Name:  "id",
			Type:  mysql.TypeLong,
			Flags: 0,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.Numeric("2")},
			},
		},
	}
	want := &proto.QueryResultList{
		List: []mproto.QueryResult{qr1, qr2},
	}

	qrl, err := client.ExecuteBatch(queries, false)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl, want) {
		t.Errorf("ExecueBatch: \n%#v, want \n%#v", qrl, want)
	}
}

func TestBatchTransaction(t *testing.T) {
	client := framework.NewDefaultClient()
	queries := []proto.BoundQuery{{
		Sql: "insert into vtocc_test values(4, null, null, null)",
	}, {
		Sql: "select * from vtocc_test where intval = 4",
	}, {
		Sql: "delete from vtocc_test where intval = 4",
	}}

	wantRows := [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.Value{Inner: sqltypes.Numeric("4")},
			sqltypes.Value{},
			sqltypes.Value{},
			sqltypes.Value{},
		},
	}

	// Not in transaction, AsTransaction false
	qrl, err := client.ExecuteBatch(queries, false)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl.List[1].Rows, wantRows) {
		t.Errorf("Rows: \n%#v, want \n%#v", qrl.List[1].Rows, wantRows)
	}

	// Not in transaction, AsTransaction true
	qrl, err = client.ExecuteBatch(queries, true)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl.List[1].Rows, wantRows) {
		t.Errorf("Rows: \n%#v, want \n%#v", qrl.List[1].Rows, wantRows)
	}

	// In transaction, AsTransaction false
	func() {
		err = client.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Commit()
		qrl, err = client.ExecuteBatch(queries, false)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(qrl.List[1].Rows, wantRows) {
			t.Errorf("Rows: \n%#v, want \n%#v", qrl.List[1].Rows, wantRows)
		}
	}()

	// In transaction, AsTransaction true
	func() {
		err = client.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Rollback()
		qrl, err = client.ExecuteBatch(queries, true)
		want := "error: cannot start a new transaction in the scope of an existing one"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want %s", err, want)
		}
	}()
}
