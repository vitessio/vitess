// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
)

func TestTxPoolExecuteCommit(t *testing.T) {
	tableName := "test_table"
	sql := fmt.Sprintf("alter table %s add test_column int", tableName)
	db := fakesqldb.Register()
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery(sql, &sqltypes.Result{})

	txPool := newTxPool(true)
	txPool.SetTimeout(1 * time.Second)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID)
	if err != nil {
		t.Fatal(err)
	}
	defer txPool.Commit(ctx, transactionID)
	txConn.RecordQuery(sql)
	_, err = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err != nil {
		t.Fatal(err)
	}
	txPool.LogActive()
	txPool.LogActive()
	// start another transaction which should be killed
	// in txPool.Close()
	_, err = txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxPoolExecuteRollback(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.Register()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})

	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID)
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

func TestTxPoolTransactionKiller(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.Register()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool(false)
	// make sure transaction killer will run frequent enough
	txPool.SetTimeout(time.Duration(10))
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	killCount := txPool.queryServiceStats.KillStats.Counts()["Transactions"]
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID)
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
	db := fakesqldb.Register()
	txPool := newTxPool(false)
	txPool.SetTimeout(time.Duration(10))
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
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

func TestTxPoolBeginWithShortDeadline(t *testing.T) {
	db := fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	// set pool capacity to 1
	txPool.pool.SetCapacity(1)
	defer txPool.Close()

	// A timeout < 10ms should always fail.
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Millisecond)
	_, err := txPool.Begin(ctx)
	want := "tx_pool_full: Transaction pool connection limit exceeded"
	if err == nil || err.Error() != want {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestTxPoolBeginWithPoolConnectionError(t *testing.T) {
	db := fakesqldb.Register()
	db.EnableConnFail()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	_, err := txPool.Begin(ctx)
	want := "fatal: connection fail (errno 2012) (sqlstate )"
	if err == nil || err.Error() != want {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestTxPoolBeginWithExecError(t *testing.T) {
	db := fakesqldb.Register()
	db.AddRejectedQuery("begin", errRejected)
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	_, err := txPool.Begin(ctx)
	want := "error: error: rejected"
	if err == nil || err.Error() != want {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestTxPoolSafeCommitFail(t *testing.T) {
	db := fakesqldb.Register()
	sql := fmt.Sprintf("alter table test_table add test_column int")
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddRejectedQuery("commit", errRejected)
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err != nil {
		t.Fatalf("got exec error: %v", err)
	}
	err = txPool.Commit(ctx, transactionID)
	want := "error: error: rejected"
	if err == nil || err.Error() != want {
		t.Errorf("Commit: %v, want %s", err, want)
	}
}

func TestTxPoolRollbackFail(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.Register()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddRejectedQuery("rollback", errRejected)

	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID)
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
	want := "error: error: rejected"
	if err == nil || err.Error() != want {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestTxPoolGetConnFail(t *testing.T) {
	db := fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	_, err := txPool.Get(12345)
	want := "not_in_tx: Transaction 12345: not found"
	if err == nil || err.Error() != want {
		t.Errorf("Get: %v, want %s", err, want)
	}
}

func TestTxPoolExecFailDueToConnFail(t *testing.T) {
	db := fakesqldb.Register()
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	txPool.Begin(ctx)
	sql := "alter table test_table add test_column int"

	transactionID, err := txPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, err := txPool.Get(transactionID)
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

func TestTxPoolLocal(t *testing.T) {
	db := fakesqldb.Register()
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	ctx := context.Background()
	conn, err := txPool.LocalBegin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = txPool.LocalCommit(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	// Call LocalConclude twice to ensure nothing blows up.
	txPool.LocalConclude(ctx, conn)
	txPool.LocalConclude(ctx, conn)

	conn, err = txPool.LocalBegin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Call LocalConclude twice to ensure nothing blows up.
	txPool.LocalConclude(ctx, conn)
	txPool.LocalConclude(ctx, conn)

	txPool.Close()
	_, err = txPool.LocalBegin(ctx)
	want := "fatal: connection pool is closed"
	if err == nil || err.Error() != want {
		t.Errorf("Begin err: %v, want %v", err, want)
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
