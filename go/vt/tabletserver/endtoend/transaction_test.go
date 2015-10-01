// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

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

	expectedDiffs := []struct {
		tag  string
		diff int
	}{{
		tag:  "Transactions.TotalCount",
		diff: 2,
	}, {
		tag:  "Transactions.Histograms.Completed.Count",
		diff: 2,
	}, {
		tag:  "Queries.TotalCount",
		diff: 6,
	}, {
		tag:  "Queries.Histograms.BEGIN.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.COMMIT.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.INSERT_PK.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.DML_PK.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.PASS_SELECT.Count",
		diff: 2,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		if err := compareIntDiff(vend, expected.tag, vstart, expected.diff); err != nil {
			t.Error(err)
		}
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

	expectedDiffs := []struct {
		tag  string
		diff int
	}{{
		tag:  "Transactions.TotalCount",
		diff: 1,
	}, {
		tag:  "Transactions.Histograms.Aborted.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.BEGIN.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.ROLLBACK.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.INSERT_PK.Count",
		diff: 1,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		if err := compareIntDiff(vend, expected.tag, vstart, expected.diff); err != nil {
			t.Error(err)
		}
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

	expectedDiffs := []struct {
		tag  string
		diff int
	}{{
		tag:  "Transactions.TotalCount",
		diff: 2,
	}, {
		tag:  "Transactions.Histograms.Completed.Count",
		diff: 2,
	}, {
		tag:  "Queries.TotalCount",
		diff: 4,
	}, {
		tag:  "Queries.Histograms.BEGIN.Count",
		diff: 0,
	}, {
		tag:  "Queries.Histograms.COMMIT.Count",
		diff: 0,
	}, {
		tag:  "Queries.Histograms.INSERT_PK.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.DML_PK.Count",
		diff: 1,
	}, {
		tag:  "Queries.Histograms.PASS_SELECT.Count",
		diff: 2,
	}}
	vend := framework.DebugVars()
	for _, expected := range expectedDiffs {
		if err := compareIntDiff(vend, expected.tag, vstart, expected.diff); err != nil {
			t.Error(err)
		}
	}
}

func TestTxPoolSize(t *testing.T) {
	vstart := framework.DebugVars()

	client1 := framework.NewDefaultClient()
	err := client1.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	defer client1.Rollback()
	if err := verifyIntValue(framework.DebugVars(), "TransactionPoolAvailable", framework.BaseConfig.TransactionCap-1); err != nil {
		t.Error(err)
	}

	defer framework.DefaultServer.SetTxPoolSize(framework.DefaultServer.TxPoolSize())
	framework.DefaultServer.SetTxPoolSize(1)
	defer framework.DefaultServer.BeginTimeout.Set(framework.DefaultServer.BeginTimeout.Get())
	timeout := 1 * time.Millisecond
	framework.DefaultServer.BeginTimeout.Set(timeout)
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "TransactionPoolAvailable", 0); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "TransactionPoolCapacity", 1); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "BeginTimeout", int(timeout)); err != nil {
		t.Error(err)
	}

	client2 := framework.NewDefaultClient()
	err = client2.Begin()
	want := "tx_pool_full"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Error: %v, must contain %s", err, want)
	}
	if err := compareIntDiff(framework.DebugVars(), "Errors.TxPoolFull", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestTxTimeout(t *testing.T) {
	vstart := framework.DebugVars()

	defer framework.DefaultServer.SetTxTimeout(framework.DefaultServer.TxTimeout())
	framework.DefaultServer.SetTxTimeout(1 * time.Millisecond)
	if err := verifyIntValue(framework.DebugVars(), "TransactionPoolTimeout", int(1*time.Millisecond)); err != nil {
		t.Error(err)
	}

	fetcher := framework.NewTxFetcher()
	client := framework.NewDefaultClient()
	err := client.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(5 * time.Millisecond)
	err = client.Commit()
	want := "not_in_tx: Transaction"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, must contain %s", err, want)
	}
	tx := fetcher.Fetch()
	if tx.Conclusion != "kill" {
		t.Errorf("Conclusion: %s, want kill", tx.Conclusion)
	}
	if err := compareIntDiff(framework.DebugVars(), "Kills.Transactions", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestForUpdate(t *testing.T) {
	for _, mode := range []string{"for update", "lock in share mode"} {
		client := framework.NewDefaultClient()
		query := fmt.Sprintf("select * from vtocc_test where intval=2 %s", mode)
		_, err := client.Execute(query, nil)
		want := "error: Disallowed"
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("Error: %v, must have prefix %s", err, want)
		}

		// We should not get errors here
		err = client.Begin()
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
	}
}
