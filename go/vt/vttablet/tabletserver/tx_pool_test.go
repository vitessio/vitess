/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletserver

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"regexp"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/messager"
)

func TestTxPoolExecuteCommit(t *testing.T) {
	sql := "update test_column set x=1 where 1!=1"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	transactionID, beginSQL, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if beginSQL != "begin" {
		t.Errorf("beginSQL got %q want 'begin'", beginSQL)
	}
	txConn, err := txPool.Get(transactionID, "for query")
	if err != nil {
		t.Fatal(err)
	}
	txConn.RecordQuery(sql)
	_, _ = txConn.Exec(ctx, sql, 1, true)
	txConn.Recycle()

	commitSQL, err := txPool.Commit(ctx, transactionID, &fakeMessageCommitter{})
	if err != nil {
		t.Fatal(err)
	}
	if commitSQL != "commit" {
		t.Errorf("commitSQL got %q want 'commit'", commitSQL)
	}
}

func TestTxPoolExecuteRollback(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sql, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	transactionID, beginSQL, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if beginSQL != "begin" {
		t.Errorf("beginSQL got %q want 'begin'", beginSQL)
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
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	txid1, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = txPool.Begin(ctx, &querypb.ExecuteOptions{})
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

func TestTxPoolTransactionKillerEnforceTimeoutEnabled(t *testing.T) {
	sqlWithTimeout := "alter table test_table add test_column int"
	sqlWithoutTimeout := "alter table test_table add test_column_no_timeout int"
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(sqlWithTimeout, &sqltypes.Result{})
	db.AddQuery(sqlWithoutTimeout, &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})

	txPool := newTxPool()
	// make sure transaction killer will run frequent enough
	txPool.SetTimeout(1 * time.Millisecond)
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	killCount := tabletenv.KillStats.Counts()["Transactions"]

	txWithoutTimeout, err := addQuery(ctx, sqlWithoutTimeout, txPool, querypb.ExecuteOptions_DBA)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := addQuery(ctx, sqlWithTimeout, txPool, querypb.ExecuteOptions_UNSPECIFIED); err != nil {
		t.Fatal(err)
	}

	var (
		killCountDiff int64
		expectedKills = int64(1)
		timeoutCh     = time.After(5 * time.Second)
	)

	// transaction killer should kill the query the second query
	for {
		killCountDiff = tabletenv.KillStats.Counts()["Transactions"] - killCount
		if killCountDiff >= expectedKills {
			break
		}

		select {
		case <-timeoutCh:
			t.Fatal("waited too long for timed transaction to be killed by transaction killer")
		default:
		}
	}

	if killCountDiff > expectedKills {
		t.Fatalf("expected only %v query to be killed, but got %v killed", expectedKills, killCountDiff)
	}

	txPool.Rollback(ctx, txWithoutTimeout)
	txPool.WaitForEmpty()

	if got, expected := db.GetQueryCalledNum("begin"), 2; got != expected {
		t.Fatalf("'begin' called: got=%v, expected=%v", got, expected)
	}
	if got, expected := db.GetQueryCalledNum(sqlWithoutTimeout), 1; got != expected {
		t.Fatalf("'%v' called: got=%v, expected=%v", sqlWithoutTimeout, got, expected)
	}
	if got, expected := db.GetQueryCalledNum(sqlWithTimeout), 1; got != expected {
		t.Fatalf("'%v' called: got=%v, expected=%v", sqlWithTimeout, got, expected)
	}
	if got, expected := db.GetQueryCalledNum("rollback"), 1; got != expected {
		t.Fatalf("'rollback' called: got=%v, expected=%v", got, expected)
	}

}
func addQuery(ctx context.Context, sql string, txPool *TxPool, workload querypb.ExecuteOptions_Workload) (int64, error) {
	transactionID, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{Workload: workload})
	if err != nil {
		return 0, err
	}
	txConn, err := txPool.Get(transactionID, "for query")
	if err != nil {
		return 0, err
	}
	txConn.Exec(ctx, sql, 1, false)
	txConn.Recycle()
	return transactionID, nil
}

func TestTxPoolClientRowsFound(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})
	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	ctx := context.Background()

	startNormalSize := txPool.conns.Available()
	startFoundRowsSize := txPool.foundRowsPool.Available()

	// Start a 'normal' transaction. It should take a connection
	// for the normal 'conns' pool.
	id1, beginSQL, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if beginSQL != "begin" {
		t.Errorf("beginSQL got %q want 'begin'", beginSQL)
	}
	if got, want := txPool.conns.Available(), startNormalSize-1; got != want {
		t.Errorf("Normal pool size: %d, want %d", got, want)
	}
	if got, want := txPool.foundRowsPool.Available(), startFoundRowsSize; got != want {
		t.Errorf("foundRows pool size: %d, want %d", got, want)
	}

	// Start a 'foundRows' transaction. It should take a connection
	// from the foundRows pool.
	id2, beginSQL, err := txPool.Begin(ctx, &querypb.ExecuteOptions{ClientFoundRows: true})
	if err != nil {
		t.Fatal(err)
	}
	if beginSQL != "begin" {
		t.Errorf("beginSQL got %q want 'begin'", beginSQL)
	}
	if got, want := txPool.conns.Available(), startNormalSize-1; got != want {
		t.Errorf("Normal pool size: %d, want %d", got, want)
	}
	if got, want := txPool.foundRowsPool.Available(), startFoundRowsSize-1; got != want {
		t.Errorf("foundRows pool size: %d, want %d", got, want)
	}

	// Rollback the first transaction. The conn should be returned to
	// the conns pool.
	txPool.Rollback(ctx, id1)
	if got, want := txPool.conns.Available(), startNormalSize; got != want {
		t.Errorf("Normal pool size: %d, want %d", got, want)
	}
	if got, want := txPool.foundRowsPool.Available(), startFoundRowsSize-1; got != want {
		t.Errorf("foundRows pool size: %d, want %d", got, want)
	}

	// Rollback the second transaction. The conn should be returned to
	// the foundRows pool.
	txPool.Rollback(ctx, id2)
	if got, want := txPool.conns.Available(), startNormalSize; got != want {
		t.Errorf("Normal pool size: %d, want %d", got, want)
	}
	if got, want := txPool.foundRowsPool.Available(), startFoundRowsSize; got != want {
		t.Errorf("foundRows pool size: %d, want %d", got, want)
	}
}

func TestTxPoolTransactionIsolation(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})
	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	ctx := context.Background()

	// Start a transaction with default. It should not change isolation.
	_, beginSQL, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if beginSQL != "begin" {
		t.Errorf("beginSQL got %q want 'begin'", beginSQL)
	}

	db.AddQuery("set transaction isolation level READ COMMITTED", &sqltypes.Result{})
	_, beginSQL, err = txPool.Begin(ctx, &querypb.ExecuteOptions{TransactionIsolation: querypb.ExecuteOptions_READ_COMMITTED})
	if err != nil {
		t.Fatal(err)
	}
	wantBeginSQL := "READ COMMITTED; begin"
	if beginSQL != wantBeginSQL {
		t.Errorf("beginSQL got %q want %q", beginSQL, wantBeginSQL)
	}
}

func TestTxPoolAutocommit(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	ctx := context.Background()

	// Start a transaction with autocommit. This will ensure that the executor does not send begin/commit statements
	// to mysql.
	// This test is meaningful because if txPool.Begin were to send a BEGIN statement to the connection, it will fatal
	// because is not in the list of expected queries (i.e db.AddQuery hasn't been called).
	txid, beginSQL, err := txPool.Begin(ctx, &querypb.ExecuteOptions{TransactionIsolation: querypb.ExecuteOptions_AUTOCOMMIT})
	if err != nil {
		t.Fatal(err)
	}
	if beginSQL != "" {
		t.Errorf("beginSQL got %q want ''", beginSQL)
	}
	commitSQL, err := txPool.Commit(ctx, txid, &fakeMessageCommitter{})
	if err != nil {
		t.Fatal(err)
	}
	if commitSQL != "" {
		t.Errorf("commitSQL got %q want ''", commitSQL)
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
	txConn, _, err := txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
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
	_, _, err = txPool.LocalBegin(context.Background(), &querypb.ExecuteOptions{})
	if err == nil || !strings.Contains(err.Error(), "(errno 2013)") {
		t.Fatalf("Begin did not return the reconnect error: %v", err)
	}
	sqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		t.Fatalf("Unexpected error type: %T, want %T", err, &mysql.SQLError{})
	}
	if got, want := sqlErr.Number(), mysql.CRServerLost; got != want {
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
	_, _, err = txPool.Begin(context.Background(), &querypb.ExecuteOptions{})
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
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	// Run a query to trigger a database connection. That connection will be
	// reused by subsequent transactions.
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	ctx := context.Background()
	txConn, _, err := txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
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
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	_, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
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
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()
	transactionID, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
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
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	_, err := txPool.Get(12345, "for query")
	want := "transaction 12345: not found"
	if err == nil || err.Error() != want {
		t.Errorf("Get: %v, want %s", err, want)
	}
}

func TestTxPoolGetConnRecentlyRemovedTransaction(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ctx := context.Background()
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	id, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	txPool.Close()

	assertErrorMatch := func(id int64, reason string) {
		_, err = txPool.Get(id, "for query")
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		want := fmt.Sprintf("transaction %v: ended at .* \\(%v\\)", id, reason)
		if m, _ := regexp.MatchString(want, err.Error()); !m {
			t.Errorf("Get: %v, want match %s", err, want)
		}
	}

	assertErrorMatch(id, "pool closed")

	txPool = newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	id, _, err = txPool.Begin(ctx, &querypb.ExecuteOptions{})
	if _, err := txPool.Commit(ctx, id, &fakeMessageCommitter{}); err != nil {
		t.Fatalf("got error: %v", err)
	}

	assertErrorMatch(id, "transaction committed")

	id, _, err = txPool.Begin(ctx, &querypb.ExecuteOptions{})
	if err := txPool.Rollback(ctx, id); err != nil {
		t.Fatalf("got error: %v", err)
	}

	assertErrorMatch(id, "transaction rolled back")

	txPool.Close()
	txPool = newTxPool()
	txPool.SetTimeout(1 * time.Millisecond)
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()

	id, _, err = txPool.Begin(ctx, &querypb.ExecuteOptions{})
	time.Sleep(5 * time.Millisecond)

	assertErrorMatch(id, "exceeded timeout: 1ms")

	txPool.SetTimeout(1 * time.Hour)
	id, _, err = txPool.Begin(ctx, &querypb.ExecuteOptions{})
	txc, err := txPool.Get(id, "for close")
	if err != nil {
		t.Fatalf("got error: %v", err)
	}

	txc.Close()
	txc.Recycle()

	assertErrorMatch(id, "closed")
}

type fakeMessageCommitter struct {
}

func (f *fakeMessageCommitter) LockDB(newMessages map[string][]*messager.MessageRow, changedMessages map[string][]string) func() {
	return func() {}
}

func (f *fakeMessageCommitter) UpdateCaches(newMessages map[string][]*messager.MessageRow, changedMessages map[string][]string) {
}

func TestTxPoolExecFailDueToConnFail_Errno2006(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()

	// Start the transaction.
	txConn, _, err := txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
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
	sqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		t.Fatalf("Unexpected error type: %T, want %T", err, &mysql.SQLError{})
	}
	if num := sqlErr.Number(); num != mysql.CRServerGone {
		t.Errorf("Unexpected error code: %d, want %d", num, mysql.CRServerGone)
	}
}

func TestTxPoolExecFailDueToConnFail_Errno2013(t *testing.T) {
	db := fakesqldb.New(t)
	// No db.Close() needed. We close it below.
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()
	ctx := context.Background()

	// Start the transaction.
	txConn, _, err := txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
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
	startingStray := tabletenv.InternalErrors.Counts()["StrayTransactions"]
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	// Start stray transaction.
	_, _, err := txPool.Begin(context.Background(), &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Close kills stray transaction.
	txPool.Close()
	if got, want := tabletenv.InternalErrors.Counts()["StrayTransactions"]-startingStray, int64(1); got != want {
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
	waiterCap := 500000
	idleTimeout := time.Duration(30 * time.Second)
	limiter := &txlimiter.TxAllowAll{}
	return NewTxPool(
		poolName,
		transactionCap,
		transactionCap,
		0,
		transactionTimeout,
		idleTimeout,
		waiterCap,
		DummyChecker,
		limiter,
	)
}
