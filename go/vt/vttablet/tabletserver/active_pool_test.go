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

	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
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

	id1, err := pool.NewConn(ctx, &querypb.ExecuteOptions{}, noOp)
	require.NoError(t, err)
	assert.Equal(t, startNormalSize-1, pool.conns.Available(), "default pool not used")

	id2, err := pool.NewConn(ctx, &querypb.ExecuteOptions{ClientFoundRows: true}, noOp)
	require.NoError(t, err)
	assert.Equal(t, startFoundRowsSize-1, pool.conns.Available(), "foundRows pool not used")

	conn, err := pool.Get(id1, "reason")
	require.NoError(t, err)
	conn.Release(tx.TxClose)
	assert.Equal(t, startNormalSize, pool.conns.Available(), "default pool not restored after release")

	conn, err = pool.Get(id2, "reason")
	require.NoError(t, err)
	conn.Release(tx.TxClose)
	assert.Equal(t, startFoundRowsSize, pool.conns.Available(), "default pool not restored after release")
}

func TestActivePoolForAllTxProps(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	id1, err := pool.NewConn(ctx, &querypb.ExecuteOptions{}, noOp)

	require.NoError(t, err)
	conn1, err := pool.Get(id1, "reason")
	require.NoError(t, err)
	conn1.TxProps = &TxProperties{}
	conn1.Recycle()

	id2, err := pool.NewConn(ctx, &querypb.ExecuteOptions{}, noOp)
	require.NoError(t, err)
	// for the second connection, we are not going to set a tx state

	id3, err := pool.NewConn(ctx, &querypb.ExecuteOptions{}, noOp)
	require.NoError(t, err)
	conn3, err := pool.Get(id3, "reason")
	conn3.TxProps = &TxProperties{}
	require.NoError(t, err)
	conn3.Recycle()

	pool.ForAllTxProperties(func(p *TxProperties) {
		p.LogToFile = true
	})

	conn1, err = pool.Get(id1, "reason")
	require.NoError(t, err)
	require.True(t, conn1.TxProps.LogToFile, "connection missed")
	conn2, err := pool.Get(id2, "reason")
	require.NoError(t, err)
	require.Nil(t, conn2.TxProps)
	conn3, err = pool.Get(id3, "reason")
	require.NoError(t, err)
	require.True(t, conn3.TxProps.LogToFile, "connection missed")
}

func TestActivePoolGetConnNonExistentTransaction(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	pool := newActivePool()
	pool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	_, err := pool.Get(12345, "for query")
	require.EqualError(t, err, "not found")
}

func noOp(*DedicatedConnection) error { return nil }

func newActivePool() *ActivePool {
	env := newEnv("ActivePoolTest")

	return NewActivePool(env)
}
