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

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
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
	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
	defer txPool.Commit(ctx, transactionID)
	txConn.RecordQuery(sql)
	_, err := txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	txPool.LogActive()
	txPool.LogActive()
	// start another transaction which should be killed
	// in txPool.Close()
	_ = txPool.Begin(ctx)
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
	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
	defer txPool.Rollback(ctx, transactionID)
	txConn.RecordQuery(sql)
	_, err := txConn.Exec(ctx, sql, 1, true)
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
	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
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
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expect to get an error")
		}
		err, ok := r.(*TabletError)
		if !ok || err != ErrConnPoolClosed {
			t.Fatalf("get error: %v, but expect: %v", err, ErrConnPoolClosed)
		}
	}()
	txPool.Begin(ctx)
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
	defer handleAndVerifyTabletError(t, "expect to get an error", vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED)
	txPool.Begin(ctx)
}

func TestTxPoolBeginWithPoolConnectionError(t *testing.T) {
	db := fakesqldb.Register()
	db.EnableConnFail()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	defer handleAndVerifyTabletError(t, "expect to get an error", vtrpcpb.ErrorCode_INTERNAL_ERROR)
	ctx := context.Background()
	txPool.Begin(ctx)
}

func TestTxPoolBeginWithExecError(t *testing.T) {
	db := fakesqldb.Register()
	db.AddRejectedQuery("begin", errRejected)
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	defer handleAndVerifyTabletError(t, "expect to get an error", vtrpcpb.ErrorCode_UNKNOWN_ERROR)
	ctx := context.Background()
	txPool.Begin(ctx)
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
	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
	_, err := txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err != nil {
		t.Fatalf("got exec error: %v", err)
	}
	defer handleAndVerifyTabletError(t, "commit should get exec failure", vtrpcpb.ErrorCode_UNKNOWN_ERROR)
	txPool.Commit(ctx, transactionID)
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
	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
	txConn.RecordQuery(sql)
	_, err := txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	defer handleAndVerifyTabletError(t, "rollback should get exec failure", vtrpcpb.ErrorCode_UNKNOWN_ERROR)
	txPool.Rollback(ctx, transactionID)
}

func TestTxPoolGetConnFail(t *testing.T) {
	db := fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	defer handleAndVerifyTabletError(t, "txpool.Get should fail", vtrpcpb.ErrorCode_NOT_IN_TX)
	txPool.Get(12345)
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

	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
	db.EnableConnFail()
	_, err := txConn.Exec(ctx, sql, 1, true)
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
