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
	"context"
	"fmt"
	"testing"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"gotest.tools/assert"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTxPoolExecuteCommit(t *testing.T) {
	db, txPool, closer := setup(t)
	defer closer()

	sql := "select 'this is a query'"
	// begin a transaction and then return the connection
	conn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)

	id := conn.ID()
	conn.Unlock()

	// get the connection and execute a query on it
	conn2, err := txPool.GetAndLock(id, "")
	require.NoError(t, err)
	_, _ = conn2.Exec(ctx, sql, 1, true)
	conn2.Unlock()

	// get the connection again and now commit it
	conn3, err := txPool.GetAndLock(id, "")
	require.NoError(t, err)

	_, err = txPool.Commit(ctx, conn3)
	require.NoError(t, err)

	// try committing again. this should fail
	_, err = txPool.Commit(ctx, conn)
	require.EqualError(t, err, "not in a transaction")

	// wrap everything up and assert
	assert.Equal(t, "begin;"+sql+";commit", db.QueryLog())
	conn3.Release(tx.TxCommit)
}

func TestTxPoolExecuteRollback(t *testing.T) {
	db, txPool, closer := setup(t)
	defer closer()

	conn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)

	err = txPool.Rollback(ctx, conn)
	require.NoError(t, err)

	// try rolling back again. this should fail
	_, err = txPool.Commit(ctx, conn)
	require.EqualError(t, err, "not in a transaction")

	conn.Release(tx.TxRollback)
	assert.Equal(t, "begin;rollback", db.QueryLog())
}

func TestTxPoolRollbackNonBusy(t *testing.T) {
	db, txPool, closer := setup(t)
	defer closer()

	// start two transactions, and mark one of them as unused
	conn1, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn2, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn2.Unlock() // this marks conn2 as NonBusy

	// This should rollback only txid2.
	txPool.RollbackNonBusy(ctx)

	// committing tx1 should not be an issue
	_, err = txPool.Commit(ctx, conn1)
	require.NoError(t, err)

	// Trying to get back to conn2 should not work since the transaction has been rolled back
	_, err = txPool.GetAndLock(conn2.ID(), "")
	require.Error(t, err)

	conn1.Release(tx.TxCommit)
	assert.Equal(t, "begin;begin;rollback;commit", db.QueryLog())
}

//
//func TestTxPoolTransactionKillerEnforceTimeoutEnabled(t *testing.T) {
//	t.Skip("you are so slow")
//	sqlWithTimeout := "alter table test_table add test_column int"
//	sqlWithoutTimeout := "alter table test_table add test_column_no_timeout int"
//	db := fakesqldb.New(t)
//	defer db.Close()
//	db.AddQuery(sqlWithTimeout, &sqltypes.Result{})
//	db.AddQuery(sqlWithoutTimeout, &sqltypes.Result{})
//	db.AddQuery("begin", &sqltypes.Result{})
//	db.AddQuery("rollback", &sqltypes.Result{})
//
//	txPool := newTxPool()
//	// make sure transaction killer will run frequent enough
//	txPool.SetTimeout(1 * time.Millisecond)
//	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
//	defer txPool.Close()
//	ctx := context.Background()
//	killCount := txPool.env.Stats().KillCounters.Counts()["Transactions"]
//
//	txWithoutTimeout, err := addQuery(ctx, sqlWithoutTimeout, txPool, querypb.ExecuteOptions_DBA)
//	require.NoError(t, err)
//
//	_, err = addQuery(ctx, sqlWithTimeout, txPool, querypb.ExecuteOptions_UNSPECIFIED)
//	require.NoError(t, err)
//
//	var (
//		killCountDiff int64
//		expectedKills = int64(1)
//		timeoutCh     = time.After(5 * time.Second)
//	)
//
//	// transaction killer should kill the query the second query
//	for {
//		killCountDiff = txPool.env.Stats().KillCounters.Counts()["Transactions"] - killCount
//		if killCountDiff >= expectedKills {
//			break
//		}
//
//		select {
//		case <-timeoutCh:
//			txPool.Rollback(ctx, txWithoutTimeout)
//			t.Fatal("waited too long for timed transaction to be killed by transaction killer")
//		default:
//		}
//	}
//
//	require.True(t, killCountDiff > expectedKills,
//		"expected only %v query to be killed, but got %v killed", expectedKills, killCountDiff)
//
//	txPool.Rollback(ctx, txWithoutTimeout)
//	txPool.WaitForEmpty()
//
//	require.Equal(t, db.GetQueryCalledNum("begin"), 2)
//	require.Equal(t, db.GetQueryCalledNum(sqlWithoutTimeout), 1)
//	require.Equal(t, db.GetQueryCalledNum(sqlWithTimeout), 1)
//	require.Equal(t, db.GetQueryCalledNum("rollback"), 1)
//}
//func addQuery(ctx context.Context, sql string, txPool *TxPool, workload querypb.ExecuteOptions_Workload) (int64, error) {
//	transactionID, _, err := txPool.begin(ctx, &querypb.ExecuteOptions{Workload: workload})
//	if err != nil {
//		return 0, err
//	}
//	txConn, err := txPool.GetAndLock(transactionID, "for query")
//	if err != nil {
//		return 0, err
//	}
//	txConn.Exec(ctx, sql, 1, false)
//	txConn.Unlock()
//	return transactionID, nil
//}
//
func TestTxPoolTransactionIsolation(t *testing.T) {
	db, txPool, closer := setup(t)
	defer closer()

	c2, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{TransactionIsolation: querypb.ExecuteOptions_READ_COMMITTED})
	require.NoError(t, err)
	c2.Release(tx.TxClose)

	assert.Equal(t, "set transaction isolation level read committed;begin", db.QueryLog())
}

func TestTxPoolAutocommit(t *testing.T) {
	db, txPool, closer := setup(t)
	defer closer()

	// Start a transaction with autocommit. This will ensure that the executor does not send begin/commit statements
	// to mysql.
	// This test is meaningful because if txPool.Begin were to send a BEGIN statement to the connection, it will fatal
	// because is not in the list of expected queries (i.e db.AddQuery hasn't been called).
	conn1, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{TransactionIsolation: querypb.ExecuteOptions_AUTOCOMMIT})
	require.NoError(t, err)

	// run a query to see it in the query log
	query := "select 3"
	conn1.Exec(ctx, query, 1, false)

	_, err = txPool.Commit(ctx, conn1)
	require.NoError(t, err)
	conn1.Release(tx.TxCommit)

	// finally, we should only see the query, no begin/commit
	assert.Equal(t, query, db.QueryLog())
}

// TestTxPoolBeginWithPoolConnectionError_TransientErrno2006 tests the case
// where we see a transient errno 2006 e.g. because MySQL killed the
// db connection. DBConn.Exec() is going to reconnect and retry automatically
// due to this connection error and the BEGIN will succeed.
func TestTxPoolBeginWithPoolConnectionError_Errno2006_Transient(t *testing.T) {
	db, txPool := primeTxPoolWithConnection(t)
	defer db.Close()
	defer txPool.Close()

	// Close the connection on the server side.
	db.CloseAllConnections()
	err := db.WaitForClose(2 * time.Second)
	require.NoError(t, err)

	txConn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err, "Begin should have succeeded after the retry in DBConn.Exec()")
	txConn.Release(tx.TxCommit)
}

//// TestTxPoolBeginWithPoolConnectionError_Errno2006_Permanent tests the case
//// where a transient errno 2006 is followed by permanent connection rejections.
//// For example, if all open connections are killed and new connections are
//// rejected.
//func TestTxPoolBeginWithPoolConnectionError_Errno2006_Permanent(t *testing.T) {
//	db, txPool, err := primeTxPoolWithConnection(t)
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer db.Close()
//	defer txPool.Close()
//
//	// Close the connection on the server side.
//	db.CloseAllConnections()
//	if err := db.WaitForClose(2 * time.Second); err != nil {
//		t.Fatal(err)
//	}
//	// Prevent new connections as well.
//	db.EnableConnFail()
//
//	// This Begin will error with 2006.
//	// After that, vttablet will automatically try to reconnect and this fail.
//	// DBConn.Exec() will return the reconnect error as final error and not the
//	// initial connection error.
//	_, _, err = txPool.Begin(context.Background(), &querypb.ExecuteOptions{})
//	if err == nil || !strings.Contains(err.Error(), "(errno 2013)") {
//		t.Fatalf("Begin did not return the reconnect error: %v", err)
//	}
//	sqlErr, ok := err.(*mysql.SQLError)
//	if !ok {
//		t.Fatalf("Unexpected error type: %T, want %T", err, &mysql.SQLError{})
//	}
//	if got, want := sqlErr.Number(), mysql.CRServerLost; got != want {
//		t.Errorf("Unexpected error code: %d, want %d", got, want)
//	}
//}
//
//func TestTxPoolBeginWithPoolConnectionError_Errno2013(t *testing.T) {
//	db, txPool, err := primeTxPoolWithConnection(t)
//	if err != nil {
//		t.Fatal(err)
//	}
//	// No db.Close() needed. We close it below.
//	defer txPool.Close()
//
//	// Close the connection *after* the server received the query.
//	// This will provoke a MySQL client error with errno 2013.
//	db.EnableShouldClose()
//
//	// 2013 is not retryable. DBConn.Exec() fails after the first attempt.
//	_, _, err = txPool.begin(context.Background(), &querypb.ExecuteOptions{})
//	if err == nil || !strings.Contains(err.Error(), "(errno 2013)") {
//		t.Fatalf("Begin must return connection error with MySQL errno 2013: %v", err)
//	}
//	if got, want := vterrors.Code(err), vtrpcpb.Code_UNKNOWN; got != want {
//		t.Errorf("wrong error code for Begin error: got = %v, want = %v", got, want)
//	}
//}

// primeTxPoolWithConnection is a helper function. It reconstructs the
// scenario where future transactions are going to reuse an open db connection.
func primeTxPoolWithConnection(t *testing.T) (*fakesqldb.DB, *TxPool) {
	t.Helper()
	db := fakesqldb.New(t)
	txPool := newTxPool()
	// Set the capacity to 1 to ensure that the db connection is reused.
	txPool.scp.conns.SetCapacity(1)
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	// Run a query to trigger a database connection. That connection will be
	// reused by subsequent transactions.
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	txConn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	txConn.Release(tx.TxCommit)

	return db, txPool
}

func TestTxPoolBeginWithError(t *testing.T) {
	db, txPool, closer := setup(t)
	defer closer()
	db.AddRejectedQuery("begin", errRejected)
	_, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "error: rejected")
	require.Equal(t, vtrpcpb.Code_UNKNOWN, vterrors.Code(err), "wrong error code for Begin error")
}

func TestTxPoolCancelledContextError(t *testing.T) {
	// given
	db, txPool, closer := setup(t)
	defer closer()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	// when
	_, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})

	// then
	require.Error(t, err)
	require.Contains(t, err.Error(), "transaction pool aborting request due to already expired context")
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Empty(t, db.QueryLog())
}

func TestTxPoolRollbackFailIsPassedThrough(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db, txPool, closer := setup(t)
	defer closer()
	db.AddRejectedQuery("rollback", errRejected)

	conn1, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)

	_, err = conn1.Exec(ctx, sql, 1, true)
	require.NoError(t, err)

	// rollback is refused by the underlying db and the error is passed on
	err = txPool.Rollback(ctx, conn1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error: rejected")

	conn1.Unlock()
}

func TestTxPoolGetConnRecentlyRemovedTransaction(t *testing.T) {
	db, txPool, _ := setup(t)
	defer db.Close()
	conn1, _, _ := txPool.Begin(ctx, &querypb.ExecuteOptions{})
	id := conn1.ID()
	conn1.Unlock()
	txPool.Close()

	assertErrorMatch := func(id int64, reason string) {
		conn, err := txPool.GetAndLock(id, "for query")
		if err == nil { //
			conn.Releasef("fail")
			t.Errorf("expected to get an error")
			return
		}

		want := fmt.Sprintf("transaction %v: ended at .* \\(%v\\)", id, reason)
		require.Regexp(t, want, err.Error())
	}

	assertErrorMatch(id, "pool closed")

	txPool = newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	conn1, _, _ = txPool.Begin(ctx, &querypb.ExecuteOptions{})
	id = conn1.ID()
	_, err := txPool.Commit(ctx, conn1)
	require.NoError(t, err)

	conn1.Releasef("transaction committed")

	assertErrorMatch(id, "transaction committed")

	txPool = newTxPool()
	txPool.SetTimeout(1 * time.Millisecond)
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()

	conn1, _, _ = txPool.Begin(ctx, &querypb.ExecuteOptions{})
	conn1.Unlock()
	id = conn1.ID()
	time.Sleep(20 * time.Millisecond)

	assertErrorMatch(id, "exceeded timeout: 1ms")
}

//func TestTxPoolExecFailDueToConnFail_Errno2006(t *testing.T) {
//	db := fakesqldb.New(t)
//	defer db.Close()
//	db.AddQuery("begin", &sqltypes.Result{})
//
//	txPool := newTxPool()
//	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
//	defer txPool.Close()
//	ctx := context.Background()
//
//	// Start the transaction.
//	txConn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// Close the connection on the server side. Future queries will fail.
//	db.CloseAllConnections()
//	if err := db.WaitForClose(2 * time.Second); err != nil {
//		t.Fatal(err)
//	}
//
//	// Query is going to fail with connection error because the connection was closed.
//	sql := "alter table test_table add test_column int"
//	_, err = txConn.Exec(ctx, sql, 1, true)
//	txConn.Unlock()
//	if err == nil || !strings.Contains(err.Error(), "(errno 2006)") {
//		t.Fatalf("Exec must return connection error with MySQL errno 2006: %v", err)
//	}
//	sqlErr, ok := err.(*mysql.SQLError)
//	if !ok {
//		t.Fatalf("Unexpected error type: %T, want %T", err, &mysql.SQLError{})
//	}
//	if num := sqlErr.Number(); num != mysql.CRServerGone {
//		t.Errorf("Unexpected error code: %d, want %d", num, mysql.CRServerGone)
//	}
//}
//
//func TestTxPoolExecFailDueToConnFail_Errno2013(t *testing.T) {
//	db := fakesqldb.New(t)
//	// No db.Close() needed. We close it below.
//	db.AddQuery("begin", &sqltypes.Result{})
//
//	txPool := newTxPool()
//	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
//	defer txPool.Close()
//	ctx := context.Background()
//
//	// Start the transaction.
//	txConn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{})
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// Close the connection *after* the server received the query.
//	// This will provoke a MySQL client error with errno 2013.
//	db.EnableShouldClose()
//
//	// Query is going to fail with connection error because the connection was closed.
//	sql := "alter table test_table add test_column int"
//	db.AddQuery(sql, &sqltypes.Result{})
//	_, err = txConn.Exec(ctx, sql, 1, true)
//	txConn.Unlock()
//	if err == nil || !strings.Contains(err.Error(), "(errno 2013)") {
//		t.Fatalf("Exec must return connection error with MySQL errno 2013: %v", err)
//	}
//	if got, want := vterrors.Code(err), vtrpcpb.Code_UNKNOWN; got != want {
//		t.Errorf("wrong error code for Exec error: got = %v, want = %v", got, want)
//	}
//}

func TestTxPoolCloseKillsStrayTransactions(t *testing.T) {
	_, txPool, closer := setup(t)
	defer closer()

	startingStray := txPool.env.Stats().InternalErrors.Counts()["StrayTransactions"]

	// Start stray transaction.
	_, _, err := txPool.Begin(context.Background(), &querypb.ExecuteOptions{})
	require.NoError(t, err)

	// Close kills stray transaction.
	txPool.Close()
	require.Equal(t, int64(1), txPool.env.Stats().InternalErrors.Counts()["StrayTransactions"]-startingStray)
	require.Equal(t, 0, txPool.scp.Capacity())
}

func newTxPool() *TxPool {
	env := newEnv("TabletServerTest")
	limiter := &txlimiter.TxAllowAll{}
	return NewTxPool(env, limiter)
}

func newEnv(exporterName string) tabletenv.Env {
	config := tabletenv.NewDefaultConfig()
	config.TxPool.Size = 300
	config.Oltp.TxTimeoutSeconds = 30
	config.TxPool.TimeoutSeconds = 40
	config.TxPool.MaxWaiters = 500000
	config.OltpReadPool.IdleTimeoutSeconds = 30
	config.OlapReadPool.IdleTimeoutSeconds = 30
	config.TxPool.IdleTimeoutSeconds = 30
	env := tabletenv.NewEnv(config, exporterName)
	return env
}

func setup(t *testing.T) (*fakesqldb.DB, *TxPool, func()) {
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})

	txPool := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	return db, txPool, func() {
		txPool.Close()
		db.Close()
	}
}
