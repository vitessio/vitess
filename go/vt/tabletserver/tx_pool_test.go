// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
)

func TestTxPoolExecuteRollback(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})

	txPool := newTxPool(false)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID, "for query")
	if err != nil {
		t.Fatal(err)
	}
	defer txPool.Rollback(ctx, transactionID)
	txConn.RecordQuery(sql)
	_, err = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
}

func TestTxPoolRollbackNonBusy(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})

	txPool := newTxPool(false)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	txid1, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn1, err := txPool.Get(txid1, "for query")
	if err != nil {
		t.Fatal(err)
	}
	// This should rollback only txid2.
	txPool.RollbackNonBusy(ctx)
	if sz := txPool.activePool.Size(); sz != 1 {
		t.Errorf("txPool.activePool.Size(): %d, want 1", sz)
	}
	conn1.Recycle()
	// This should rollback txid1.
	txPool.RollbackNonBusy(ctx)
	if sz := txPool.activePool.Size(); sz != 0 {
		t.Errorf("txPool.activePool.Size(): %d, want 0", sz)
	}
}

func TestTxPoolTransactionKiller(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool(false)
	// make sure transaction killer will run frequent enough
	txPool.SetTimeout(1 * time.Millisecond)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	killCount := txPool.queryServiceStats.KillStats.Counts()["Transactions"]
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID, "for query")
	if err != nil {
		t.Fatal(err)
	}
	txConn.RecordQuery(sql)
	txConn.Recycle()
	// transaction killer should kill the query
	txPool.WaitForEmpty()
	killCountDiff := txPool.queryServiceStats.KillStats.Counts()["Transactions"] - killCount
	if killCountDiff != 1 {
		t.Fatalf("query: %s should be killed by transaction killer", sql)
	}
}

func TestTxPoolBeginAfterConnPoolClosed(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	txPool := newTxPool(false)
	txPool.SetTimeout(time.Duration(10))
	txPool.Open(db.ConnParams(), db.ConnParams())
	txPool.Close()
	ctx := context.Background()
	_, err := txPool.Begin(ctx)
	if err == nil {
		t.Fatalf("expect to get an error")
	}
	terr, ok := err.(*TabletError)
	if !ok || terr != ErrConnPoolClosed {
		t.Fatalf("get error: %v, but expect: %v", terr, ErrConnPoolClosed)
	}
}

func TestTxPoolBeginWithPoolConnectionError(t *testing.T) {
	db := fakesqldb.New(t)
	db.Close()
	//	db.EnableConnFail()
	txPool := newTxPool(false)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	_, err := txPool.Begin(ctx)
	want := "errno 2003"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestTxPoolBeginWithExecError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddRejectedQuery("begin", errRejected)
	txPool := newTxPool(false)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	_, err := txPool.Begin(ctx)
	want := "error: rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestTxPoolRollbackFail(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddRejectedQuery("rollback", errRejected)

	txPool := newTxPool(false)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID, "for query")
	if err != nil {
		t.Fatal(err)
	}
	txConn.RecordQuery(sql)
	_, err = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	err = txPool.Rollback(ctx, transactionID)
	want := "error: rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestTxPoolGetConnFail(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	txPool := newTxPool(false)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	_, err := txPool.Get(12345, "for query")
	want := "not_in_tx: Transaction 12345: not found"
	if err == nil || err.Error() != want {
		t.Errorf("Get: %v, want %s", err, want)
	}
}

func TestTxPoolExecFailDueToConnFail(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool(false)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	txPool.Begin(ctx)
	sql := "alter table test_table add test_column int"

	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID, "for query")
	if err != nil {
		t.Fatal(err)
	}
	db.EnableConnFail()
	_, err = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err == nil {
		t.Fatalf("exec should fail because of a conn error")
	}
}

func newTxPool(enablePublishStats bool) *TxPool {
	randID := rand.Int63()
	poolName := fmt.Sprintf("TestTransactionPool-%d", randID)
	txStatsPrefix := fmt.Sprintf("TxStats-%d-", randID)
	transactionCap := 300
	transactionTimeout := time.Duration(30 * time.Second)
	idleTimeout := time.Duration(30 * time.Second)
	queryServiceStats := NewQueryServiceStats("", enablePublishStats)
	return NewTxPool(
		poolName,
		txStatsPrefix,
		transactionCap,
		transactionTimeout,
		idleTimeout,
		enablePublishStats,
		queryServiceStats,
		DummyChecker,
	)
}
