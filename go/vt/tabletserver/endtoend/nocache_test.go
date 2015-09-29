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
)

func TestSimpleRead(t *testing.T) {
	vstart := framework.DebugVars()
	_, err := framework.NewDefaultClient().Execute("select * from vtocc_test where intval=1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	vend := framework.DebugVars()
	v1 := framework.FetchInt(vstart, "Queries.TotalCount")
	v2 := framework.FetchInt(vend, "Queries.TotalCount")
	if v1+1 != v2 {
		t.Errorf("Queries.TotalCount: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.PASS_SELECT.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.PASS_SELECT.Count")
	if v1+1 != v2 {
		t.Errorf("Queries...Count: %d, want %d", v2, v1+1)
	}
}

func TestBinary(t *testing.T) {
	client := framework.NewDefaultClient()
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
	client := framework.NewDefaultClient()
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

func TestCommit(t *testing.T) {
	client := framework.NewDefaultClient()
	defer client.Execute("delete from vtocc_test where intval=4", nil)

	fetcher := framework.NewTxFetcher()
	vstart := framework.DebugVars()

	query := "insert into vtocc_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	err := client.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Commit()
	if err != nil {
		t.Error(err)
		return
	}
	tx := fetcher.Fetch()
	want := []string{query}
	if !reflect.DeepEqual(tx.Queries, want) {
		t.Errorf("queries: %v, want %v", tx.Queries, want)
	}

	qr, err := client.Execute("select * from vtocc_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 4 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}

	_, err = client.Execute("delete from vtocc_test where intval=4", nil)
	if err != nil {
		t.Error(err)
		return
	}

	qr, err = client.Execute("select * from vtocc_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 3 {
		t.Errorf("rows affected: %d, want 4", qr.RowsAffected)
	}

	vend := framework.DebugVars()
	v1 := framework.FetchInt(vstart, "Transactions.TotalCount")
	v2 := framework.FetchInt(vend, "Transactions.TotalCount")
	if v1+2 != v2 {
		t.Errorf("Transactions.TotalCount: %d, want %d", v2, v1+2)
	}
	v1 = framework.FetchInt(vstart, "Transactions.Histograms.Completed.Count")
	v2 = framework.FetchInt(vend, "Transactions.Histograms.Completed.Count")
	if v1+2 != v2 {
		t.Errorf("Transactions.Histograms.Completed.Count: %d, want %d", v2, v1+2)
	}
	v1 = framework.FetchInt(vstart, "Queries.TotalCount")
	v2 = framework.FetchInt(vend, "Queries.TotalCount")
	if v1+6 != v2 {
		t.Errorf("Queries.TotalCount: %d, want %d", v2, v1+6)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.BEGIN.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.BEGIN.Count")
	if v1+1 != v2 {
		t.Errorf("Queries.Histograms.BEGIN.Count: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.COMMIT.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.COMMIT.Count")
	if v1+1 != v2 {
		t.Errorf("Queries.Histograms.COMMIT.Count: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.INSERT_PK.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.INSERT_PK.Count")
	if v1+1 != v2 {
		t.Errorf("Queries.Histograms.INSERT_PK.Count: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.DML_PK.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.DML_PK.Count")
	if v1+1 != v2 {
		t.Errorf("Queries.Histograms.DML_PK.Count: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.PASS_SELECT.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.PASS_SELECT.Count")
	if v1+2 != v2 {
		t.Errorf("Queries.Histograms.PASS_SELECT.Count: %d, want %d", v2, v1+2)
	}
}
