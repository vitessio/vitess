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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTxEngineClose(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	ctx := context.Background()
	config := tabletenv.NewDefaultConfig()
	config.DB = newDBConfigs(db)
	config.TxPool.Size = 10
	config.Oltp.TxTimeoutSeconds = 0.1
	config.GracePeriods.ShutdownSeconds = 0
	te := NewTxEngine(tabletenv.NewEnv(config, "TabletServerTest"))

	// Normal close.
	te.AcceptReadWrite()
	start := time.Now()
	te.Close()
	assert.Greater(t, int64(50*time.Millisecond), int64(time.Since(start)))

	// Normal close with timeout wait.
	te.AcceptReadWrite()
	c, beginSQL, err := te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	require.Equal(t, "begin", beginSQL)
	c.Unlock()
	c, beginSQL, err = te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	require.Equal(t, "begin", beginSQL)
	c.Unlock()
	start = time.Now()
	te.Close()
	assert.Less(t, int64(50*time.Millisecond), int64(time.Since(start)))
	assert.EqualValues(t, 2, te.txPool.env.Stats().KillCounters.Counts()["Transactions"])
	te.txPool.env.Stats().KillCounters.ResetAll()

	// Immediate close.
	te.AcceptReadOnly()
	c, _, err = te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Unlock()
	start = time.Now()
	te.Close()
	assert.Greater(t, int64(50*time.Millisecond), int64(time.Since(start)))

	// Normal close with short grace period.
	te.shutdownGracePeriod = 25 * time.Millisecond
	te.AcceptReadWrite()
	c, _, err = te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	c.Unlock()
	start = time.Now()
	te.Close()
	assert.Less(t, int64(1*time.Millisecond), int64(time.Since(start)))
	assert.Greater(t, int64(50*time.Millisecond), int64(time.Since(start)))

	// Normal close with short grace period, but pool gets empty early.
	te.shutdownGracePeriod = 25 * time.Millisecond
	te.AcceptReadWrite()
	c, _, err = te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	c.Unlock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, err := te.txPool.GetAndLock(c.ReservedID(), "return")
		assert.NoError(t, err)
		te.txPool.RollbackAndRelease(ctx, c)
	}()
	start = time.Now()
	te.Close()
	assert.Less(t, int64(10*time.Millisecond), int64(time.Since(start)))
	assert.Greater(t, int64(25*time.Millisecond), int64(time.Since(start)))

	// Immediate close, but connection is in use.
	te.AcceptReadOnly()
	c, _, err = te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	go func() {
		time.Sleep(100 * time.Millisecond)
		te.txPool.RollbackAndRelease(ctx, c)
	}()
	start = time.Now()
	te.Close()
	if diff := time.Since(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Since(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}

	// Normal close with Reserved connection timeout wait.
	te.shutdownGracePeriod = 0 * time.Millisecond
	te.AcceptReadWrite()
	te.AcceptReadWrite()
	_, err = te.Reserve(ctx, &querypb.ExecuteOptions{}, 0, nil)
	require.NoError(t, err)
	_, err = te.ReserveBegin(ctx, &querypb.ExecuteOptions{}, nil)
	require.NoError(t, err)
	start = time.Now()
	te.Close()
	assert.Less(t, int64(50*time.Millisecond), int64(time.Since(start)))
	assert.EqualValues(t, 1, te.txPool.env.Stats().KillCounters.Counts()["Transactions"])
	assert.EqualValues(t, 1, te.txPool.env.Stats().KillCounters.Counts()["ReservedConnection"])
}

func TestTxEngineBegin(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQueryPattern(".*", &sqltypes.Result{})
	config := tabletenv.NewDefaultConfig()
	config.DB = newDBConfigs(db)
	te := NewTxEngine(tabletenv.NewEnv(config, "TabletServerTest"))
	te.AcceptReadOnly()
	tx1, _, err := te.Begin(ctx, nil, 0, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	_, _, err = te.Commit(ctx, tx1)
	require.NoError(t, err)
	require.Equal(t, "start transaction read only;commit", db.QueryLog())
	db.ResetQueryLog()

	te.AcceptReadWrite()
	tx2, _, err := te.Begin(ctx, nil, 0, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	_, _, err = te.Commit(ctx, tx2)
	require.NoError(t, err)
	require.Equal(t, "begin;commit", db.QueryLog())
}

func TestTxEngineRenewFails(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQueryPattern(".*", &sqltypes.Result{})
	config := tabletenv.NewDefaultConfig()
	config.DB = newDBConfigs(db)
	te := NewTxEngine(tabletenv.NewEnv(config, "TabletServerTest"))
	te.AcceptReadOnly()
	options := &querypb.ExecuteOptions{}
	connID, err := te.ReserveBegin(ctx, options, nil)
	require.NoError(t, err)

	conn, err := te.txPool.GetAndLock(connID, "for test")
	require.NoError(t, err)
	conn.Unlock() // but we keep holding on to it... sneaky....

	// this next bit sets up the scp so our renew will fail
	conn2, err := te.txPool.scp.NewConn(ctx, options)
	require.NoError(t, err)
	defer conn2.Release(tx.TxCommit)
	te.txPool.scp.lastID.Set(conn2.ConnID - 1)

	// commit will do a renew
	dbConn := conn.dbConn
	_, _, err = te.Commit(ctx, connID)
	require.Error(t, err)
	assert.True(t, conn.IsClosed(), "connection was not closed")
	assert.True(t, dbConn.IsClosed(), "underlying connection was not closed")
}

type TxType int

const (
	NoTx TxType = iota
	ReadOnlyAccepted
	WriteAccepted
	ReadOnlyRejected
	WriteRejected
)

func (t TxType) String() string {
	names := [...]string{
		"no transaction",
		"read only transaction accepted",
		"write transaction accepted",
		"read only transaction rejected",
		"write transaction rejected",
	}

	if t < NoTx || t > WriteRejected {
		return "unknown"
	}

	return names[t]
}

type TestCase struct {
	startState     txEngineState
	TxEngineStates []txEngineState
	tx             TxType
	stateAssertion func(state txEngineState) error
}

func (test TestCase) String() string {
	var sb strings.Builder
	sb.WriteString("start from ")
	sb.WriteString(test.startState.String())
	sb.WriteString(" with ")
	sb.WriteString(test.tx.String())

	for _, change := range test.TxEngineStates {
		sb.WriteString(" change state to ")
		sb.WriteString(change.String())
	}

	return sb.String()
}

func changeState(te *TxEngine, state txEngineState) {
	switch state {
	case AcceptingReadAndWrite:
		te.AcceptReadWrite()
	case AcceptingReadOnly:
		te.AcceptReadOnly()
	case NotServing:
		te.Close()
	}
}

func TestWithInnerTests(outerT *testing.T) {

	tests := []TestCase{
		// Start from RW and test all single hop transitions with and without tx
		{AcceptingReadAndWrite, []txEngineState{
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite},
			WriteAccepted, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing},
			ReadOnlyAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite},
			ReadOnlyAccepted, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly},
			ReadOnlyAccepted, assertEndStateIs(AcceptingReadOnly)},

		// Start from RW and test all transitions with and without tx, plus a concurrent Stop()
		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		// Start from RW and test all transitions with and without tx, plus a concurrent ReadOnly()
		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		// Start from RO and test all single hop transitions with and without tx
		{AcceptingReadOnly, []txEngineState{
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadOnly, []txEngineState{
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly},
			WriteRejected, assertEndStateIs(AcceptingReadOnly)},

		// Start from RO and test all transitions with and without tx, plus a concurrent Stop()
		{AcceptingReadOnly, []txEngineState{
			NotServing,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			NotServing,
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		// Start from RO and test all transitions with and without tx, plus a concurrent ReadWrite()
		{AcceptingReadOnly, []txEngineState{
			NotServing,
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			NotServing,
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		// Make sure that all transactions are rejected when we are not serving
		{NotServing, []txEngineState{},
			WriteRejected, assertEndStateIs(NotServing)},

		{NotServing, []txEngineState{},
			ReadOnlyRejected, assertEndStateIs(NotServing)},
	}

	for _, test := range tests {
		outerT.Run(test.String(), func(t *testing.T) {

			db := setUpQueryExecutorTest(t)
			db.AddQuery("set transaction isolation level REPEATABLE READ", &sqltypes.Result{})
			db.AddQuery("start transaction with consistent snapshot, read only", &sqltypes.Result{})
			defer db.Close()
			te := setupTxEngine(db)

			changeState(te, test.startState)

			switch test.tx {
			case NoTx:
				// nothing to do
			case WriteAccepted:
				require.NoError(t,
					startTransaction(te, true))
			case ReadOnlyAccepted:
				require.NoError(t,
					startTransaction(te, false))
			case WriteRejected:
				err := startTransaction(te, true)
				require.Error(t, err)
			case ReadOnlyRejected:
				err := startTransaction(te, false)
				require.Error(t, err)
			default:
				t.Fatalf("don't know how to [%v]", test.tx)
			}

			wg := sync.WaitGroup{}
			for _, newState := range test.TxEngineStates {
				wg.Add(1)
				go func(s txEngineState) {
					defer wg.Done()

					changeState(te, s)
				}(newState)

				// We give the state changes a chance to get started
				time.Sleep(10 * time.Millisecond)
			}

			// Let's wait for all transitions to wrap up
			wg.Wait()

			require.NoError(t,
				test.stateAssertion(te.state))
		})
	}
}

func setupTxEngine(db *fakesqldb.DB) *TxEngine {
	config := tabletenv.NewDefaultConfig()
	config.DB = newDBConfigs(db)
	config.TxPool.Size = 10
	config.Oltp.TxTimeoutSeconds = 0.1
	config.GracePeriods.ShutdownSeconds = 0
	te := NewTxEngine(tabletenv.NewEnv(config, "TabletServerTest"))
	return te
}

func assertEndStateIs(expected txEngineState) func(actual txEngineState) error {
	return func(actual txEngineState) error {
		if actual != expected {
			return fmt.Errorf("expected the end state to be %v, but it was %v", expected, actual)
		}
		return nil
	}
}

func startTransaction(te *TxEngine, writeTransaction bool) error {
	options := &querypb.ExecuteOptions{}
	if writeTransaction {
		options.TransactionIsolation = querypb.ExecuteOptions_DEFAULT
	} else {
		options.TransactionIsolation = querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY
	}
	_, _, err := te.Begin(context.Background(), nil, 0, options)
	return err
}

func TestTxEngineFailReserve(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQueryPattern(".*", &sqltypes.Result{})
	config := tabletenv.NewDefaultConfig()
	config.DB = newDBConfigs(db)
	te := NewTxEngine(tabletenv.NewEnv(config, "TabletServerTest"))

	options := &querypb.ExecuteOptions{}
	_, err := te.Reserve(ctx, options, 0, nil)
	require.EqualError(t, err, "TxEngine.Reserve: cannot provide new connection in state NotServing")

	_, err = te.ReserveBegin(ctx, options, nil)
	require.EqualError(t, err, "TxEngine.ReserveBegin: cannot provide new connection in state NotServing")

	te.AcceptReadOnly()

	db.AddRejectedQuery("dummy_query", errors.New("failed executing dummy_query"))
	_, err = te.Reserve(ctx, options, 0, []string{"dummy_query"})
	require.EqualError(t, err, "TxEngine.Reserve: unknown error: failed executing dummy_query (errno 1105) (sqlstate HY000) during query: dummy_query")

	_, err = te.ReserveBegin(ctx, options, []string{"dummy_query"})
	require.EqualError(t, err, "TxEngine.ReserveBegin: unknown error: failed executing dummy_query (errno 1105) (sqlstate HY000) during query: dummy_query")

	nonExistingID := int64(42)
	_, err = te.Reserve(ctx, options, nonExistingID, nil)
	require.EqualError(t, err, "TxEngine.Reserve: transaction 42: not found")

	txID, _, err := te.Begin(ctx, nil, 0, options)
	require.NoError(t, err)
	conn, err := te.txPool.GetAndLock(txID, "for test")
	require.NoError(t, err)
	conn.Unlock() // but we keep holding on to it... sneaky....

	_, err = te.Reserve(ctx, options, txID, []string{"dummy_query"})
	require.EqualError(t, err, "TxEngine.Reserve: unknown error: failed executing dummy_query (errno 1105) (sqlstate HY000) during query: dummy_query")

	connID, _, err := te.Commit(ctx, txID)
	require.Error(t, err)
	assert.Zero(t, connID)
}
