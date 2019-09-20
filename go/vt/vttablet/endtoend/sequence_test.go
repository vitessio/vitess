/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestSequence(t *testing.T) {
	want := sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(0),
		}},
	}
	for wantval := int64(1); wantval < 10; wantval += 2 {
		want.Rows[0][0] = sqltypes.NewInt64(wantval)
		qr, err := framework.NewClient().Execute("select next 2 values from vitess_seq", nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*qr, want) {
			t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
		}
	}
	// Verify that the table got updated according to chunk size.
	want = sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(13),
			sqltypes.NewInt64(3),
		}},
	}
	qr, err := framework.NewClient().Execute("select next_id, cache from vitess_seq", nil)
	if err != nil {
		t.Fatal(err)
	}
	qr.Fields = nil
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestResetSequence(t *testing.T) {
	client := framework.NewClient()
	want := sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
	}
	qr, err := client.Execute("select next value from vitess_reset_seq", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}

	// Reset mastership
	err = client.SetServingType(topodatapb.TabletType_REPLICA)
	if err != nil {
		t.Fatal(err)
	}
	err = client.SetServingType(topodatapb.TabletType_MASTER)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the next value skips previously cached values.
	want.Rows[0][0] = sqltypes.NewInt64(4)
	qr, err = client.Execute("select next value from vitess_reset_seq", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}
