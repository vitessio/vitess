// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

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
	if !reflect.DeepEqual(tx.Conclusion, "commit") {
		t.Errorf("conclusion: %s, want commit", tx.Conclusion)
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

func TestRollback(t *testing.T) {
	client := framework.NewDefaultClient()

	fetcher := framework.NewTxFetcher()
	vstart := framework.DebugVars()

	query := "insert into vtocc_test values(4, null, null, null)"
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
	err = client.Rollback()
	if err != nil {
		t.Error(err)
		return
	}
	tx := fetcher.Fetch()
	want := []string{query}
	if !reflect.DeepEqual(tx.Queries, want) {
		t.Errorf("queries: %v, want %v", tx.Queries, want)
	}
	if !reflect.DeepEqual(tx.Conclusion, "rollback") {
		t.Errorf("conclusion: %s, want rollback", tx.Conclusion)
	}

	qr, err := client.Execute("select * from vtocc_test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 3 {
		t.Errorf("rows affected: %d, want 3", qr.RowsAffected)
	}

	vend := framework.DebugVars()
	v1 := framework.FetchInt(vstart, "Transactions.TotalCount")
	v2 := framework.FetchInt(vend, "Transactions.TotalCount")
	if v1+1 != v2 {
		t.Errorf("Transactions.TotalCount: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Transactions.Histograms.Aborted.Count")
	v2 = framework.FetchInt(vend, "Transactions.Histograms.Aborted.Count")
	if v1+1 != v2 {
		t.Errorf("Transactions.Histograms.Aborted.Count: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.BEGIN.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.BEGIN.Count")
	if v1+1 != v2 {
		t.Errorf("Queries.Histograms.BEGIN.Count: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.ROLLBACK.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.ROLLBACK.Count")
	if v1+1 != v2 {
		t.Errorf("Queries.Histograms.ROLLBACK.Count: %d, want %d", v2, v1+1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.INSERT_PK.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.INSERT_PK.Count")
	if v1+1 != v2 {
		t.Errorf("Queries.Histograms.INSERT_PK.Count: %d, want %d", v2, v1+1)
	}
}

func TestAutoCommit(t *testing.T) {
	client := framework.NewDefaultClient()
	defer client.Execute("delete from vtocc_test where intval=4", nil)

	fetcher := framework.NewTxFetcher()
	vstart := framework.DebugVars()

	query := "insert into vtocc_test (intval, floatval, charval, binval) " +
		"values(4, null, null, null)"
	_, err := client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	tx := fetcher.Fetch()
	want := []string{query}
	if !reflect.DeepEqual(tx.Queries, want) {
		t.Errorf("queries: %v, want %v", tx.Queries, want)
	}
	if !reflect.DeepEqual(tx.Conclusion, "commit") {
		t.Errorf("conclusion: %s, want commit", tx.Conclusion)
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
	if v1+4 != v2 {
		t.Errorf("Queries.TotalCount: %d, want %d", v2, v1+6)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.BEGIN.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.BEGIN.Count")
	if v1 != v2 {
		t.Errorf("Queries.Histograms.BEGIN.Count: %d, want %d", v2, v1)
	}
	v1 = framework.FetchInt(vstart, "Queries.Histograms.COMMIT.Count")
	v2 = framework.FetchInt(vend, "Queries.Histograms.COMMIT.Count")
	if v1 != v2 {
		t.Errorf("Queries.Histograms.COMMIT.Count: %d, want %d", v2, v1)
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
