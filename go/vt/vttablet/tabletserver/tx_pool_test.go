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
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/callerid"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"gotest.tools/assert"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTxPoolExecuteCommit(t *testing.T) {
	db, txPool, _, closer := setup(t)
	defer closer()

	sql := "select 'this is a query'"
	// begin a transaction and then return the connection
	conn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)

	id := conn.ReservedID()
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
	db, txPool, _, closer := setup(t)
	defer closer()

	conn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	defer conn.Release(tx.TxRollback)

	err = txPool.Rollback(ctx, conn)
	require.NoError(t, err)

	// try rolling back again, this should be no-op.
	err = txPool.Rollback(ctx, conn)
	require.NoError(t, err, "not in a transaction")

	assert.Equal(t, "begin;rollback", db.QueryLog())
}

func TestTxPoolExecuteRollbackOnClosedConn(t *testing.T) {
	db, txPool, _, closer := setup(t)
	defer closer()

	conn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	defer conn.Release(tx.TxRollback)

	conn.Close()

	// rollback should not be logged.
	err = txPool.Rollback(ctx, conn)
	require.NoError(t, err)

	assert.Equal(t, "begin", db.QueryLog())
}

func TestTxPoolRollbackNonBusy(t *testing.T) {
	db, txPool, _, closer := setup(t)
	defer closer()

	// start two transactions, and mark one of them as unused
	conn1, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	conn2, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	conn2.Unlock() // this marks conn2 as NonBusy

	// This should rollback only txid2.
	txPool.Shutdown(ctx)

	// committing tx1 should not be an issue
	_, err = txPool.Commit(ctx, conn1)
	require.NoError(t, err)

	// Trying to get back to conn2 should not work since the transaction has been rolled back
	_, err = txPool.GetAndLock(conn2.ReservedID(), "")
	require.Error(t, err)

	conn1.Release(tx.TxCommit)
	assert.Equal(t, "begin;begin;rollback;commit", db.QueryLog())
}

func TestTxPoolTransactionIsolation(t *testing.T) {
	db, txPool, _, closer := setup(t)
	defer closer()

	c2, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{TransactionIsolation: querypb.ExecuteOptions_READ_COMMITTED}, false, 0, nil)
	require.NoError(t, err)
	c2.Release(tx.TxClose)

	assert.Equal(t, "set transaction isolation level read committed;begin", db.QueryLog())
}

func TestTxPoolAutocommit(t *testing.T) {
	db, txPool, _, closer := setup(t)
	defer closer()

	// Start a transaction with autocommit. This will ensure that the executor does not send begin/commit statements
	// to mysql.
	// This test is meaningful because if txPool.Begin were to send a BEGIN statement to the connection, it will fatal
	// because is not in the list of expected queries (i.e db.AddQuery hasn't been called).
	conn1, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{TransactionIsolation: querypb.ExecuteOptions_AUTOCOMMIT}, false, 0, nil)
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

	txConn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err, "Begin should have succeeded after the retry in DBConn.Exec()")
	txConn.Release(tx.TxCommit)
}

// primeTxPoolWithConnection is a helper function. It reconstructs the
// scenario where future transactions are going to reuse an open db connection.
func primeTxPoolWithConnection(t *testing.T) (*fakesqldb.DB, *TxPool) {
	t.Helper()
	db := fakesqldb.New(t)
	txPool, _ := newTxPool()
	// Set the capacity to 1 to ensure that the db connection is reused.
	txPool.scp.conns.SetCapacity(1)
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	// Run a query to trigger a database connection. That connection will be
	// reused by subsequent transactions.
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	txConn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	txConn.Release(tx.TxCommit)

	return db, txPool
}

func TestTxPoolBeginWithError(t *testing.T) {
	db, txPool, limiter, closer := setup(t)
	defer closer()
	db.AddRejectedQuery("begin", errRejected)

	im := &querypb.VTGateCallerID{
		Username: "user",
	}
	ef := &vtrpcpb.CallerID{
		Principal: "principle",
	}

	ctxWithCallerID := callerid.NewContext(ctx, ef, im)
	_, _, err := txPool.Begin(ctxWithCallerID, &querypb.ExecuteOptions{}, false, 0, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error: rejected")
	require.Equal(t, vtrpcpb.Code_UNKNOWN, vterrors.Code(err), "wrong error code for Begin error")

	// Regression test for #6727: make sure the tx limiter is decremented if grabbing a connection
	// errors for whatever reason.
	require.Equal(t,
		[]fakeLimiterEntry{
			{
				immediate: im,
				effective: ef,
				isRelease: false,
			},
			{
				immediate: im,
				effective: ef,
				isRelease: true,
			},
		}, limiter.Actions())
}

func TestTxPoolBeginWithPreQueryError(t *testing.T) {
	db, txPool, _, closer := setup(t)
	defer closer()
	db.AddRejectedQuery("pre_query", errRejected)
	_, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, []string{"pre_query"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "error: rejected")
	require.Equal(t, vtrpcpb.Code_UNKNOWN, vterrors.Code(err), "wrong error code for Begin error")
}

func TestTxPoolCancelledContextError(t *testing.T) {
	// given
	db, txPool, _, closer := setup(t)
	defer closer()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	// when
	_, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)

	// then
	require.Error(t, err)
	require.Contains(t, err.Error(), "transaction pool aborting request due to already expired context")
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Empty(t, db.QueryLog())
}

func TestTxPoolWaitTimeoutError(t *testing.T) {
	env := newEnv("TabletServerTest")
	env.Config().TxPool.Size = 1
	env.Config().TxPool.MaxWaiters = 0
	env.Config().TxPool.TimeoutSeconds = 1
	// given
	db, txPool, _, closer := setupWithEnv(t, env)
	defer closer()

	// lock the only connection in the pool.
	conn, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	defer conn.Unlock()

	// try locking one more connection.
	_, _, err = txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)

	// then
	require.Error(t, err)
	require.Contains(t, err.Error(), "transaction pool connection limit exceeded")
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	require.Equal(t, "begin", db.QueryLog())
	require.True(t, conn.TxProperties().LogToFile)
}

func TestTxPoolRollbackFailIsPassedThrough(t *testing.T) {
	sql := "alter table test_table add test_column int"
	db, txPool, _, closer := setup(t)
	defer closer()
	db.AddRejectedQuery("rollback", errRejected)

	conn1, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
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
	db, txPool, _, _ := setup(t)
	defer db.Close()
	conn1, _, _ := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	id := conn1.ReservedID()
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

	txPool, _ = newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	conn1, _, _ = txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	id = conn1.ReservedID()
	_, err := txPool.Commit(ctx, conn1)
	require.NoError(t, err)

	conn1.Releasef("transaction committed")

	assertErrorMatch(id, "transaction committed")

	txPool, _ = newTxPool()
	txPool.SetTimeout(1 * time.Millisecond)
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer txPool.Close()

	conn1, _, _ = txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	conn1.Unlock()
	id = conn1.ReservedID()
	time.Sleep(20 * time.Millisecond)

	assertErrorMatch(id, "exceeded timeout: 1ms")
}

func TestTxPoolCloseKillsStrayTransactions(t *testing.T) {
	_, txPool, _, closer := setup(t)
	defer closer()

	startingStray := txPool.env.Stats().InternalErrors.Counts()["StrayTransactions"]

	// Start stray transaction.
	conn, _, err := txPool.Begin(context.Background(), &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	conn.Unlock()

	// Close kills stray transaction.
	txPool.Close()
	require.Equal(t, int64(1), txPool.env.Stats().InternalErrors.Counts()["StrayTransactions"]-startingStray)
	require.Equal(t, 0, txPool.scp.Capacity())
}

func TestTxTimeoutKillsTransactions(t *testing.T) {
	env := newEnv("TabletServerTest")
	env.Config().TxPool.Size = 1
	env.Config().TxPool.MaxWaiters = 0
	env.Config().Oltp.TxTimeoutSeconds = 1
	_, txPool, limiter, closer := setupWithEnv(t, env)
	defer closer()
	startingKills := txPool.env.Stats().KillCounters.Counts()["Transactions"]

	im := &querypb.VTGateCallerID{
		Username: "user",
	}
	ef := &vtrpcpb.CallerID{
		Principal: "principle",
	}

	ctxWithCallerID := callerid.NewContext(ctx, ef, im)

	// Start transaction.
	conn, _, err := txPool.Begin(ctxWithCallerID, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	conn.Unlock()

	// Let it time out and get killed by the tx killer.
	time.Sleep(1200 * time.Millisecond)

	// Verify that the tx killer rand.
	require.Equal(t, int64(1), txPool.env.Stats().KillCounters.Counts()["Transactions"]-startingKills)

	// Regression test for #6727: make sure the tx limiter is decremented when the tx killer closes
	// a transaction.
	require.Equal(t,
		[]fakeLimiterEntry{
			{
				immediate: im,
				effective: ef,
				isRelease: false,
			},
			{
				immediate: im,
				effective: ef,
				isRelease: true,
			},
		}, limiter.Actions())
}

func newTxPool() (*TxPool, *fakeLimiter) {
	return newTxPoolWithEnv(newEnv("TabletServerTest"))
}

func newTxPoolWithEnv(env tabletenv.Env) (*TxPool, *fakeLimiter) {
	limiter := &fakeLimiter{}
	return NewTxPool(env, limiter), limiter
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

type fakeLimiterEntry struct {
	immediate *querypb.VTGateCallerID
	effective *vtrpcpb.CallerID
	isRelease bool
}

type fakeLimiter struct {
	actions []fakeLimiterEntry
	mu      sync.Mutex
}

func (fl *fakeLimiter) Get(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.actions = append(fl.actions, fakeLimiterEntry{
		immediate: immediate,
		effective: effective,
		isRelease: false,
	})
	return true
}

func (fl *fakeLimiter) Release(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.actions = append(fl.actions, fakeLimiterEntry{
		immediate: immediate,
		effective: effective,
		isRelease: true,
	})
}

func (fl *fakeLimiter) Actions() []fakeLimiterEntry {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	result := make([]fakeLimiterEntry, len(fl.actions))
	copy(result, fl.actions)
	return result
}

func setup(t *testing.T) (*fakesqldb.DB, *TxPool, *fakeLimiter, func()) {
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})

	txPool, limiter := newTxPool()
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	return db, txPool, limiter, func() {
		txPool.Close()
		db.Close()
	}
}

func setupWithEnv(t *testing.T, env tabletenv.Env) (*fakesqldb.DB, *TxPool, *fakeLimiter, func()) {
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})

	txPool, limiter := newTxPoolWithEnv(env)
	txPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	return db, txPool, limiter, func() {
		txPool.Close()
		db.Close()
	}
}
