/*
Copyright 2020 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
)

var ctx = context.Background()

func TestActivePoolClientRowsFound(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("begin", &sqltypes.Result{})

	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	startNormalSize := pool.conns.Available()
	startFoundRowsSize := pool.foundRowsPool.Available()

	conn1, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.Equal(t, startNormalSize-1, pool.conns.Available(), "default pool not used")

	conn2, err := pool.NewConn(ctx, &querypb.ExecuteOptions{ClientFoundRows: true})
	require.NoError(t, err)
	assert.Equal(t, startFoundRowsSize-1, pool.conns.Available(), "foundRows pool not used")

	conn1.Release(tx.TxClose)
	assert.Equal(t, startNormalSize, pool.conns.Available(), "default pool not restored after release")

	conn2.Release(tx.TxClose)
	assert.Equal(t, startFoundRowsSize, pool.conns.Available(), "default pool not restored after release")
}

func TestActivePoolForAllTxProps(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	conn1, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn1.txProps = &tx.Properties{}

	conn2, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	// for the second connection, we are not going to set a tx state

	conn3, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn3.txProps = &tx.Properties{}

	pool.ForAllTxProperties(func(p *tx.Properties) {
		p.LogToFile = true
	})

	require.True(t, conn1.txProps.LogToFile, "connection missed")
	require.Nil(t, conn2.txProps)
	require.True(t, conn3.txProps.LogToFile, "connection missed")
}

func TestStatefulPoolShutdownNonTx(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	// conn1 non-tx, not in use.
	conn1, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn1.Taint(ctx, nil)
	conn1.Unlock()

	// conn2 tx, not in use.
	conn2, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn2.Taint(ctx, nil)
	conn2.txProps = &tx.Properties{}
	conn2.Unlock()

	// conn3 non-tx, in use.
	conn3, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn3.Taint(ctx, nil)

	// After ShutdownNonTx, conn1 should be closed, but not conn3.
	pool.ShutdownNonTx()
	assert.Equal(t, int64(2), pool.active.Size())
	assert.True(t, conn1.IsClosed())
	assert.False(t, conn3.IsClosed())

	// conn3 should get closed on Unlock.
	conn3.Unlock()
	assert.True(t, conn3.IsClosed())

	// conn2 should be unaffected.
	assert.False(t, conn2.IsClosed())
}

func TestStatefulPoolShutdownAll(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())

	// conn1 not in use
	conn1, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn1.txProps = &tx.Properties{}
	conn1.Unlock()

	// conn2 in use.
	conn2, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn2.txProps = &tx.Properties{}

	conns := pool.ShutdownAll()
	wantconns := []*StatefulConnection{conn1}
	assert.Equal(t, wantconns, conns)

	// conn2 should get closed on Unlock.
	conn2.Unlock()
	assert.True(t, conn2.IsClosed())
}

func TestActivePoolGetConnNonExistentTransaction(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	_, err := pool.GetAndLock(12345, "for query")
	require.EqualError(t, err, "not found")
}

func TestExecWithAbortedCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	conn, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	cancel()
	_, err = conn.Exec(ctx, "", 0, false)
	require.Error(t, err)
}

func TestExecWithDbconnClosed(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	conn, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn.Close()

	_, err = conn.Exec(ctx, "", 0, false)
	require.EqualError(t, err, "connection was aborted")
}

func TestExecWithDbconnClosedHavingTx(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	conn, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	conn.txProps = &tx.Properties{Conclusion: "foobar"}
	conn.Close()

	_, err = conn.Exec(ctx, "", 0, false)
	require.EqualError(t, err, "transaction was aborted: foobar")
}

func TestFailOnConnectionRegistering(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	conn, err := pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	defer conn.Close()

	pool.lastID.Set(conn.ConnID - 1)

	_, err = pool.NewConn(ctx, &querypb.ExecuteOptions{})
	require.Error(t, err, "already present")
}

func newActivePool() *StatefulConnectionPool {
	env := newEnv("ActivePoolTest")

	return NewStatefulConnPool(env)
}
