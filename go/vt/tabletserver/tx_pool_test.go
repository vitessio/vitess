// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/tabletserver/fakesqldb"
	"golang.org/x/net/context"
)

func TestExecuteCommit(t *testing.T) {
	tableName := "test_table"
	sql := fmt.Sprintf("ALTER TABLE %s ADD test_column INT", tableName)
	fakesqldb.Register()
	txPool := newTxPool(true)
	txPool.SetTimeout(1 * time.Second)
	txPool.SetPoolTimeout(1 * time.Second)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
	defer txPool.SafeCommit(ctx, transactionID)
	txConn.RecordQuery(sql)
	_, err := txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	txConn.DirtyKeys(tableName)
	dk := txConn.DirtyKeys(tableName)
	dk.Delete(tableName)
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	txPool.LogActive()
	txPool.LogActive()
	// start another transaction which should be killed
	// in txPool.Close()
	_ = txPool.Begin(ctx)
}

func TestExecuteRollback(t *testing.T) {
	sql := "ALTER TABLE test_table ADD test_column INT"
	fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
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

func TestTransactionKiller(t *testing.T) {
	sql := "ALTER TABLE test_table ADD test_column INT"
	fakesqldb.Register()
	txPool := newTxPool(false)
	// make sure transaction killer will run frequent enough
	txPool.SetTimeout(time.Duration(10))
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	ctx := context.Background()
	killCount := killStats.Counts()["Transactions"]
	transactionID := txPool.Begin(ctx)
	txConn := txPool.Get(transactionID)
	txConn.RecordQuery(sql)
	txConn.Recycle()
	// transaction killer should kill the query
	txPool.WaitForEmpty()
	killCountDiff := killStats.Counts()["Transactions"] - killCount
	if killCountDiff != 1 {
		t.Fatalf("query: %s should be killed by transaction killer", sql)
	}
}

func TestBeginAfterConnPoolClosed(t *testing.T) {
	fakesqldb.Register()
	txPool := newTxPool(false)
	txPool.SetTimeout(time.Duration(10))
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	txPool.Close()
	ctx := context.Background()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expect to get an error")
		}
		err, ok := r.(*TabletError)
		if !ok || err != connPoolClosedErr {
			t.Fatalf("get error: %v, but expect: %v", err, connPoolClosedErr)
		}
	}()
	txPool.Begin(ctx)
}

func TestBeginWithPoolTimeout(t *testing.T) {
	fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	// set pool capacity to 1
	txPool.pool.SetCapacity(1)
	defer txPool.Close()
	// start the first transaction
	txPool.Begin(context.Background())
	// start the second transaction, which should fail due to
	// ErrTxPoolFull error
	ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer handleAndVerifyTabletError(t, "expect to get an error", ErrTxPoolFull)
	txPool.Begin(ctx)
}

func TestBeginWithShortDeadline(t *testing.T) {
	fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	// set pool capacity to 1
	txPool.pool.SetCapacity(1)
	defer txPool.Close()

	// A timeout < 10ms should always fail.
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer handleAndVerifyTabletError(t, "expect to get an error", ErrTxPoolFull)
	txPool.Begin(ctx)
}

func TestBeginWithPoolConnectionError(t *testing.T) {
	db := fakesqldb.Register()
	db.EnableConnFail()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	defer handleAndVerifyTabletError(t, "expect to get an error", ErrFatal)
	ctx := context.Background()
	txPool.Begin(ctx)
}

func TestBeginWithExecError(t *testing.T) {
	db := fakesqldb.Register()
	db.AddRejectedQuery("begin")
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	defer handleAndVerifyTabletError(t, "expect to get an error", ErrFail)
	ctx := context.Background()
	txPool.Begin(ctx)
}

func TestTxPoolSafeCommitFail(t *testing.T) {
	db := fakesqldb.Register()
	sql := fmt.Sprintf("alter table test_table add test_column int")
	db.AddQuery("begin", &proto.QueryResult{})
	db.AddQuery(sql, &proto.QueryResult{})
	db.AddRejectedQuery("commit")
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
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
	_, err = txPool.SafeCommit(ctx, transactionID)
	if err == nil {
		t.Fatalf("comit should get exec failure")
	}
}

func TestTxPoolRollbackFail(t *testing.T) {
	db := fakesqldb.Register()
	db.AddRejectedQuery("rollback")
	sql := "ALTER TABLE test_table ADD test_column INT"
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
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
	defer handleAndVerifyTabletError(t, "rollback should get exec failure", ErrFail)
	txPool.Rollback(ctx, transactionID)
}

func TestTxPoolGetConnFail(t *testing.T) {
	fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	txPool.Open(&appParams, &dbaParams)
	defer txPool.Close()
	defer handleAndVerifyTabletError(t, "txpool.Get should fail", ErrNotInTx)
	txPool.Get(12345)
}

func TestTxPoolExecFailDueToConnFail(t *testing.T) {
	db := fakesqldb.Register()
	txPool := newTxPool(false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
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
	txPoolTimeout := time.Duration(30 * time.Second)
	idleTimeout := time.Duration(30 * time.Second)
	return NewTxPool(
		poolName,
		txStatsPrefix,
		transactionCap,
		transactionTimeout,
		txPoolTimeout,
		idleTimeout,
		enablePublishStats,
	)
}

func init() {
	rand.Seed(time.Now().UnixNano())
	name := "TestTxPool"
	mysqlStats = stats.NewTimings(name + "Mysql")
	queryStats = stats.NewTimings(name + "Queries")
	waitStats = stats.NewTimings(name + "Waits")
	killStats = stats.NewCounters(name + "Kills")
	infoErrors = stats.NewCounters(name + "InfoErrors")
	errorStats = stats.NewCounters(name + "Errors")
	internalErrors = stats.NewCounters(name + "InternalErrors")
	resultStats = stats.NewHistogram(name+"Results", resultBuckets)
	spotCheckCount = stats.NewInt(name + "RowcacheSpotCheckCount")
	qpsRates = stats.NewRates(name+"QPS", queryStats, 15, 60*time.Second)
}
