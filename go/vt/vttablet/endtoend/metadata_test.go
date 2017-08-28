/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func getAndSetup(t *testing.T) *framework.QueryClient {
	client := framework.NewClient()

	_, err := client.Execute(
		"insert into vitess_b values(:eid, :id)",
		map[string]*querypb.BindVariable{
			"id":  sqltypes.Int64BindVariable(-2147483648),
			"eid": sqltypes.Int64BindVariable(-9223372036854775808),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	return client
}

func cleanup(client *framework.QueryClient) {
	client.Execute("delete from vitess_b where id = -2147483648 and eid = -9223372036854775808", nil)
}

// Should return all fields, because we pass ExecuteOptions_ALL
func TestMetadataSpecificExecOptions(t *testing.T) {
	client := getAndSetup(t)
	defer cleanup(client)

	qr, err := client.ExecuteWithOptions("select * from vitess_b where id = -2147483648 and eid = -9223372036854775808",
		nil,
		&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
	if err != nil {
		t.Fatal(err)
	}

	streamQr, err := client.StreamExecuteWithOptions("select * from vitess_b where id = -2147483648 and eid = -9223372036854775808",
		nil,
		&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
	if err != nil {
		t.Fatal(err)
	}

	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "eid",
				Type:         sqltypes.Int64,
				Table:        "vitess_b",
				OrgTable:     "vitess_b",
				Database:     "vttest",
				OrgName:      "eid",
				ColumnLength: 20,
				Charset:      63,
				Flags:        49155,
			},
			{
				Name:         "id",
				Type:         sqltypes.Int32,
				Table:        "vitess_b",
				OrgTable:     "vitess_b",
				Database:     "vttest",
				OrgName:      "id",
				ColumnLength: 11,
				Charset:      63,
				Flags:        49155,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt64(-9223372036854775808),
				sqltypes.NewInt32(-2147483648),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
	if !reflect.DeepEqual(*streamQr, want) {
		t.Errorf("StreamExecute: \n%#v, want \n%#v", prettyPrint(*streamQr), prettyPrint(want))
	}
}

// should return Name and Type, because we pass an empty ExecuteOptions and that is the default
func TestMetadataDefaultExecOptions(t *testing.T) {
	client := getAndSetup(t)
	defer cleanup(client)

	qr, err := client.ExecuteWithOptions("select * from vitess_b where id = -2147483648 and eid = -9223372036854775808", nil, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	streamQr, err := client.StreamExecuteWithOptions("select * from vitess_b where id = -2147483648 and eid = -9223372036854775808", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "eid",
				Type: sqltypes.Int64,
			},
			{
				Name: "id",
				Type: sqltypes.Int32,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt64(-9223372036854775808),
				sqltypes.NewInt32(-2147483648),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
	if !reflect.DeepEqual(*streamQr, want) {
		t.Errorf("StreamExecute: \n%#v, want \n%#v", prettyPrint(*streamQr), prettyPrint(want))
	}
}

// should return Name and Type, because if nil ExecuteOptions are passed, we normalize to TYPE_AND_NAME
func TestMetadataNoExecOptions(t *testing.T) {
	client := getAndSetup(t)
	defer cleanup(client)

	qr, err := client.ExecuteWithOptions("select * from vitess_b where id = -2147483648 and eid = -9223372036854775808", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	streamQr, err := client.StreamExecuteWithOptions("select * from vitess_b where id = -2147483648 and eid = -9223372036854775808", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "eid",
				Type: sqltypes.Int64,
			},
			{
				Name: "id",
				Type: sqltypes.Int32,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt64(-9223372036854775808),
				sqltypes.NewInt32(-2147483648),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
	if !reflect.DeepEqual(*streamQr, want) {
		t.Errorf("StreamExecute: \n%#v, want \n%#v", prettyPrint(*streamQr), prettyPrint(want))
	}
}
