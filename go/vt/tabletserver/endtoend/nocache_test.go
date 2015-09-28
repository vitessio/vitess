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
)

func TestSimpleRead(t *testing.T) {
	vstart := debugVars()
	_, err := newClient(defaultServer).Execute("select * from vtocc_test where intval=1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	vend := debugVars()
	v1 := fetchInt(vstart, "Queries.TotalCount")
	v2 := fetchInt(vend, "Queries.TotalCount")
	if v1+1 != v2 {
		t.Errorf("Queries.TotalCount: %d, want %d", v1+1, v2)
	}
	v1 = fetchInt(vstart, "Queries.Histograms.PASS_SELECT.Count")
	v2 = fetchInt(vend, "Queries.Histograms.PASS_SELECT.Count")
	if v1+1 != v2 {
		t.Errorf("Queries...Count: %d, want %d", v1+1, v2)
	}
}

func TestBinary(t *testing.T) {
	client := newClient(defaultServer)
	defer client.Execute("delete from vtocc_test where intval in (4,5)", nil)

	binaryData := "\x00'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff"
	// Test without bindvars.
	_, err := client.Execute(
		"insert into vtocc_test values "+
			"(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')",
		nil,
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select binval from vtocc_test where intval=4", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "binval",
				Type:  mysql.TypeVarString,
				Flags: mysql.FlagBinary,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.String(binaryData)},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}

	// Test with bindvars.
	_, err = client.Execute(
		"insert into vtocc_test values(5, null, null, :bindata)",
		map[string]interface{}{"bindata": binaryData},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err = client.Execute("select binval from vtocc_test where intval=5", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestNocacheListArgs(t *testing.T) {
	client := newClient(defaultServer)
	query := "select * from vtocc_test where intval in ::list"

	qr, err := client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{2, 3, 4},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 2 {
		t.Errorf("rows affected: %d, want 2", qr.RowsAffected)
	}

	qr, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{3, 4},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 1 {
		t.Errorf("rows affected: %d, want 1", qr.RowsAffected)
	}

	qr, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{3},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 1 {
		t.Errorf("rows affected: %d, want 1", qr.RowsAffected)
	}

	// Error case
	_, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{},
		},
	)
	want := "error: empty list supplied for list"
	if err == nil || err.Error() != want {
		t.Errorf("error returned: %v, want %s", err, want)
		return
	}
}
