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

package endtoend

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"
)

func TestBatchRead(t *testing.T) {
	client := framework.NewClient()
	queries := []*querypb.BoundQuery{{
		Sql:           "select * from vitess_a where id = :a",
		BindVariables: map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(2)},
	}, {
		Sql:           "select * from vitess_b where id = :b",
		BindVariables: map[string]*querypb.BindVariable{"b": sqltypes.Int64BindVariable(2)},
	}}
	qr1 := sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:         "eid",
			Type:         sqltypes.Int64,
			Table:        "vitess_a",
			OrgTable:     "vitess_a",
			Database:     "vttest",
			OrgName:      "eid",
			ColumnLength: 20,
			Charset:      63,
			Flags:        49155,
		}, {
			Name:         "id",
			Type:         sqltypes.Int32,
			Table:        "vitess_a",
			OrgTable:     "vitess_a",
			Database:     "vttest",
			OrgName:      "id",
			ColumnLength: 11,
			Charset:      63,
			Flags:        49155,
		}, {
			Name:         "name",
			Type:         sqltypes.VarChar,
			Table:        "vitess_a",
			OrgTable:     "vitess_a",
			Database:     "vttest",
			OrgName:      "name",
			ColumnLength: 384,
			Charset:      33,
		}, {
			Name:         "foo",
			Type:         sqltypes.VarBinary,
			Table:        "vitess_a",
			OrgTable:     "vitess_a",
			Database:     "vttest",
			OrgName:      "foo",
			ColumnLength: 128,
			Charset:      63,
			Flags:        128,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt64(1),
				sqltypes.NewInt32(2),
				sqltypes.NewVarChar("bcde"),
				sqltypes.NewVarBinary("fghi"),
			},
		},
	}
	qr2 := sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:         "eid",
			Type:         sqltypes.Int64,
			Table:        "vitess_b",
			OrgTable:     "vitess_b",
			Database:     "vttest",
			OrgName:      "eid",
			ColumnLength: 20,
			Charset:      63,
			Flags:        49155,
		}, {
			Name:         "id",
			Type:         sqltypes.Int32,
			Table:        "vitess_b",
			OrgTable:     "vitess_b",
			Database:     "vttest",
			OrgName:      "id",
			ColumnLength: 11,
			Charset:      63,
			Flags:        49155,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt64(1),
				sqltypes.NewInt32(2),
			},
		},
	}
	want := []sqltypes.Result{qr1, qr2}

	qrl, err := client.ExecuteBatch(queries, false)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl, want) {
		t.Errorf("ExecueBatch: \n%#v, want \n%#v", prettyPrintArr(qrl), prettyPrintArr(want))
	}
}

func TestBatchTransaction(t *testing.T) {
	client := framework.NewClient()
	queries := []*querypb.BoundQuery{{
		Sql: "insert into vitess_test values(4, null, null, null)",
	}, {
		Sql: "select * from vitess_test where intval = 4",
	}, {
		Sql: "delete from vitess_test where intval = 4",
	}}

	wantRows := [][]sqltypes.Value{
		{
			sqltypes.NewInt32(4),
			{},
			{},
			{},
		},
	}

	// Not in transaction, AsTransaction false
	qrl, err := client.ExecuteBatch(queries, false)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl[1].Rows, wantRows) {
		t.Errorf("Rows: \n%#v, want \n%#v", qrl[1].Rows, wantRows)
	}

	// Not in transaction, AsTransaction true
	qrl, err = client.ExecuteBatch(queries, true)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl[1].Rows, wantRows) {
		t.Errorf("Rows: \n%#v, want \n%#v", qrl[1].Rows, wantRows)
	}

	// In transaction, AsTransaction false
	func() {
		err = client.Begin(false)
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
		if !reflect.DeepEqual(qrl[1].Rows, wantRows) {
			t.Errorf("Rows: \n%#v, want \n%#v", qrl[1].Rows, wantRows)
		}
	}()

	// In transaction, AsTransaction true
	func() {
		err = client.Begin(false)
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Rollback()
		qrl, err = client.ExecuteBatch(queries, true)
		want := "cannot start a new transaction in the scope of an existing one"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want %s", err, want)
		}
	}()
}
