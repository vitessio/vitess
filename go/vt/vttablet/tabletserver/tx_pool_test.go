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

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vterrors"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestTxPoolExecuteRollback(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})

	txPool := newTxPool()
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

	txPool := newTxPool()
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

	txPool := newTxPool()
	// make sure transaction killer will run frequent enough
	txPool.SetTimeout(1 * time.Millisecond)
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	killCount := tabletenv.KillStats.Counts()["Transactions"]
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
	killCountDiff := tabletenv.KillStats.Counts()["Transactions"] - killCount
	if killCountDiff != 1 {
		t.Fatalf("query: %s should be killed by transaction killer", sql)
	}
}

// TestTxPoolBeginWithPoolConnectionError_TransientErrno2006 tests the case
// where we see a transient errno 2006 e.g. because MySQL killed the
// db connection. DBConn.Exec() is going to reconnect and retry automatically
// due to this connection error and the BEGIN will succeed.
func TestTxPoolBeginWithPoolConnectionError_Errno2006_Transient(t *testing.T) {
	db, txPool, err := primeTxPoolWithConnection(t)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer txPool.Close()

	// Close the connection on the server side.
	db.CloseAllConnections()
	if err := db.WaitForClose(2 * time.Second); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	txConn, err := txPool.LocalBegin(ctx)
	if err != nil {
		t.Fatalf("Begin should have succeeded after the retry in DBConn.Exec(): %v", err)
	}
	txPool.LocalConclude(ctx, txConn)
}

// TestTxPoolBeginWithPoolConnectionError_Errno2006_Permanent tests the case
// where a transient errno 2006 is followed by permanent connection rejections.
// For example, if all open connections are killed and new connections are
// rejected.
func TestTxPoolBeginWithPoolConnectionError_Errno2006_Permanent(t *testing.T) {
	db, txPool, err := primeTxPoolWithConnection(t)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer txPool.Close()

	// Close the connection on the server side.
	db.CloseAllConnections()
	if err := db.WaitForClose(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	// Prevent new connections as well.
	db.EnableConnFail()

	// This Begin will error with 2006.
	// After that, vttablet will automatically try to reconnect and this fail.
	// DBConn.Exec() will return the reconnect error as final error and not the
	// initial connection error.
	_, err = txPool.LocalBegin(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Lost connection to MySQL server") || !strings.Contains(err.Error(), "(errno 2013)") {
		t.Fatalf("Begin did not return the reconnect error: %v", err)
	}
	sqlErr, ok := err.(*sqldb.SQLError)
	if !ok {
		t.Fatalf("Unexpected error type: %T, want %T", err, &sqldb.SQLError{})
	}
	if got, want := sqlErr.Number(), mysqlconn.CRServerLost; got != want {
		t.Errorf("Unexpected error code: %d, want %d", got, want)
	}
}

func TestTxPoolBeginWithPoolConnectionError_Errno2013(t *testing.T) {
	db, txPool, err := primeTxPoolWithConnection(t)
	if err != nil {
		t.Fatal(err)
	}
	// No db.Close() needed. We close it below.
	defer txPool.Close()

	// Close the connection *after* the server received the query.
	// This will provoke a MySQL client error with errno 2013.
	db.EnableShouldClose()

	// 2013 is not retryable. DBConn.Exec() fails after the first attempt.
	_, err = txPool.Begin(context.Background())
	if err == nil || !strings.Contains(err.Error(), "(errno 2013)") {
		t.Fatalf("Begin must return connection error with MySQL errno 2013: %v", err)
	}
	if got, want := vterrors.Code(err), vtrpcpb.Code_UNKNOWN; got != want {
		t.Errorf("wrong error code for Begin error: got = %v, want = %v", got, want)
	}
}

// primeTxPoolWithConnection is a helper function. It reconstructs the
// scenario where future transactions are going to reuse an open db connection.
func primeTxPoolWithConnection(t *testing.T) (*fakesqldb.DB, *TxPool, error) {
	db := fakesqldb.New(t)
	txPool := newTxPool()
	// Set the capacity to 1 to ensure that the db connection is reused.
	txPool.conns.SetCapacity(1)
	txPool.Open(db.ConnParams(), db.ConnParams())

	// Run a query to trigger a database connection. That connection will be
	// reused by subsequent transactions.
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	ctx := context.Background()
	txConn, err := txPool.LocalBegin(ctx)
	if err != nil {
		return nil, nil, err
	}
	txPool.LocalConclude(ctx, txConn)

	return db, txPool, nil
}

func TestTxPoolBeginWithError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddRejectedQuery("begin", errRejected)
	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	_, err := txPool.Begin(ctx)
	want := "error: rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Begin: %v, want %s", err, want)
	}
	if got, want := vterrors.Code(err), vtrpcpb.Code_UNKNOWN; got != want {
		t.Errorf("wrong error code for Begin error: got = %v, want = %v", got, want)
	}
}

func TestTxPoolRollbackFail(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddRejectedQuery("rollback", errRejected)

	txPool := newTxPool()
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

func TestTxPoolGetConnNonExistentTransaction(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	_, err := txPool.Get(12345, "for query")
	want := "transaction 12345: not found"
	if err == nil || err.Error() != want {
		t.Errorf("Get: %v, want %s", err, want)
	}
}

func TestTxPoolExecFailDueToConnFail_Errno2006(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()

	// Start the transaction.
	txConn, err := txPool.LocalBegin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Close the connection on the server side. Future queries will fail.
	db.CloseAllConnections()
	if err := db.WaitForClose(2 * time.Second); err != nil {
		t.Fatal(err)
	}

	// Query is going to fail with connection error because the connection was closed.
	sql := "alter table test_table add test_column int"
	_, err = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err == nil || !strings.Contains(err.Error(), "(errno 2006)") {
		t.Fatalf("Exec must return connection error with MySQL errno 2006: %v", err)
	}
	sqlErr, ok := err.(*sqldb.SQLError)
	if !ok {
		t.Fatalf("Unexpected error type: %T, want %T", err, &sqldb.SQLError{})
	}
	if num := sqlErr.Number(); num != mysqlconn.CRServerGone {
		t.Errorf("Unexpected error code: %d, want %d", num, mysqlconn.CRServerGone)
	}
}

func TestTxPoolExecFailDueToConnFail_Errno2013(t *testing.T) {
	db := fakesqldb.New(t)
	// No db.Close() needed. We close it below.
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()

	// Start the transaction.
	txConn, err := txPool.LocalBegin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Close the connection *after* the server received the query.
	// This will provoke a MySQL client error with errno 2013.
	db.EnableShouldClose()

	// Query is going to fail with connection error because the connection was closed.
	sql := "alter table test_table add test_column int"
	db.AddQuery(sql, &sqltypes.Result{})
	_, err = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()
	if err == nil || !strings.Contains(err.Error(), "(errno 2013)") {
		t.Fatalf("Exec must return connection error with MySQL errno 2013: %v", err)
	}
	if got, want := vterrors.Code(err), vtrpcpb.Code_UNKNOWN; got != want {
		t.Errorf("wrong error code for Exec error: got = %v, want = %v", got, want)
	}
}

func TestTxPoolCloseKillsStrayTransactions(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams())

	// Start stray transaction.
	_, err := txPool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Close kills stray transaction.
	txPool.Close()
	if got, want := tabletenv.InternalErrors.Counts()["StrayTransactions"], int64(1); got != want {
		t.Fatalf("internal error count for stray transactions not increased: got = %v, want = %v", got, want)
	}
	if got, want := txPool.conns.Capacity(), int64(0); got != want {
		t.Fatalf("resource pool was not closed. capacity: got = %v, want = %v", got, want)
	}
}

func newTxPool() *TxPool {
	randID := rand.Int63()
	poolName := fmt.Sprintf("TestTransactionPool-%d", randID)
	transactionCap := 300
	transactionTimeout := time.Duration(30 * time.Second)
	idleTimeout := time.Duration(30 * time.Second)
	return NewTxPool(
		poolName,
		transactionCap,
		transactionTimeout,
		idleTimeout,
		DummyChecker,
	)
}
