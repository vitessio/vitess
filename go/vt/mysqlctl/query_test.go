/*
Copyright 2026 The Vitess Authors.

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

package mysqlctl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

// TestExecuteSuperQueryListMulti checks that an entry of the list may hold
// several statements, which is what operator written SQL such as the init SQL of
// a backup does, and that such an entry neither fails nor stops the entry after
// it.
func TestExecuteSuperQueryListMulti(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(".*", &sqltypes.Result{})

	params := *db.ConnParams()
	testMysqld := NewMysqld(dbconfigs.NewTestDBConfigs(params, params, "fakesqldb"))
	defer testMysqld.Close()

	require.NoError(t, testMysqld.ExecuteSuperQueryListMulti(t.Context(), []string{
		"set global read_only=off;set global super_read_only=off",
		"optimize no_write_to_binlog table mysql.gtid_executed",
	}))

	// Every statement has to reach the server on its own. An entry holding
	// several would otherwise arrive as a single query that the server cannot
	// parse, which is also what would keep the entry after it from running.
	for _, statement := range []string{
		"set global read_only=off",
		"set global super_read_only=off",
		"optimize no_write_to_binlog table mysql.gtid_executed",
	} {
		require.Equal(t, 1, db.GetQueryCalledNum(statement), "statement %q did not reach the server on its own, query log: %v", statement, db.QueryLog())
	}

	// The connection is discarded rather than handed back to the dba pool: it can
	// send a batch, which nothing else drawing on the pool expects, and operator
	// written SQL may have changed session state such as sql_mode.
	connIDs := db.QueryConnIDs("optimize no_write_to_binlog table mysql.gtid_executed")
	require.Len(t, connIDs, 1)
	assert.Eventually(t, func() bool {
		return !db.IsConnectionOpen(connIDs[0])
	}, 30*time.Second, 10*time.Millisecond, "the connection went back to the dba pool instead of being discarded")

	conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
	require.NoError(t, err)
	t.Cleanup(conn.Recycle)

	// An exchange already in flight is interrupted by killing it and, if that
	// does not bring it back, by closing the connection underneath it. Without
	// the second half a server that answers nothing holds the caller for as long
	// as it likes.
	t.Run("a stuck exchange is interrupted", func(t *testing.T) {
		killGraceTimeout = 100 * time.Millisecond
		t.Cleanup(func() { killGraceTimeout = 5 * time.Second })

		conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
		require.NoError(t, err)
		t.Cleanup(conn.Recycle)

		ctx, cancel := context.WithCancel(t.Context())
		running := make(chan struct{})
		go func() {
			<-running
			cancel()
		}()

		err = testMysqld.executeWithContext(ctx, conn, comSetOption, func() error {
			close(running)
			// The fake server never sends anything unasked, so this is a read the
			// server never answers: it comes back when the connection is closed,
			// and not before.
			_, err := conn.Conn.ReadPacket()
			return err
		})
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, conn.Conn.IsClosed(), "the connection has to be closed to interrupt the exchange")
	})

	// The capability exchanges are a blocking write and read each, so they run
	// under the caller's context like the queries do.
	t.Run("a done context stops an exchange before it starts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		ran := false
		err := testMysqld.executeWithContext(ctx, conn, comSetOption, func() error {
			ran = true
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ran)
	})
}
