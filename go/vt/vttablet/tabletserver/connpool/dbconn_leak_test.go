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

package connpool

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// outstandingGoroutines counts the goroutines whose stack contains any of the
// given function names, whatever they are waiting on.
func outstandingGoroutines(functions ...string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	count := 0
	for _, goroutine := range strings.Split(string(buf), "\n\n") {
		for _, function := range functions {
			if strings.Contains(goroutine, function) {
				count++
				break
			}
		}
	}
	return count
}

// outstandingQueryWorkers counts goroutines started by execOnce or streamOnce
// that have not exited, whatever they are waiting on.
func outstandingQueryWorkers() int {
	return outstandingGoroutines("connpool.(*Conn).execOnce", "connpool.(*Conn).streamOnce")
}

// outstandingKillWorkers counts goroutines started by kill or killQuery that
// have not exited, whatever they are waiting on.
func outstandingKillWorkers() int {
	return outstandingGoroutines("connpool.(*Conn).kill.func", "connpool.(*Conn).killQuery.func")
}

// TestDBConnKillDoesNotLeakGoroutines kills statements whose context is
// cancelled while they run, through Exec, ExecOnce, Stream and StreamOnce, and
// verifies that every goroutine the statements started exits. Each statement is
// cancelled only after it has reached the server, and the server ends it only
// once it receives the kill, so every kill hits a running statement.
func TestDBConnKillDoesNotLeakGoroutines(t *testing.T) {
	const kills = 5
	const query = "delete from t where id in (1, 2, 3)"

	testCases := []struct {
		name    string
		execute func(ctx context.Context, dbConn *Conn) error
	}{
		{
			name: "non-tx exec kills the query",
			execute: func(ctx context.Context, dbConn *Conn) error {
				_, err := dbConn.Exec(ctx, query, 1, false)
				return err
			},
		},
		{
			name: "tx exec kills the connection",
			execute: func(ctx context.Context, dbConn *Conn) error {
				_, err := dbConn.ExecOnce(ctx, query, 1, false)
				return err
			},
		},
		{
			name: "non-tx stream kills the query",
			execute: func(ctx context.Context, dbConn *Conn) error {
				return dbConn.Stream(ctx, query, func(*sqltypes.Result) error {
					return nil
				}, alloc, 1, querypb.ExecuteOptions_ALL)
			},
		},
		{
			name: "tx stream kills the connection",
			execute: func(ctx context.Context, dbConn *Conn) error {
				return dbConn.StreamOnce(ctx, query, func(*sqltypes.Result) error {
					return nil
				}, alloc, 1, querypb.ExecuteOptions_ALL)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			defer db.Close()
			connPool := newPool()
			params := dbconfigs.New(db.ConnParams())
			connPool.Open(params, params, params)
			defer connPool.Close()

			queryStarted := make(chan struct{}, kills)
			killReceived := make(chan struct{}, kills)
			release := make(chan struct{})
			defer close(release)

			db.AddQuery(query, &sqltypes.Result{})
			db.SetBeforeFunc(query, func() {
				queryStarted <- struct{}{}
				select {
				case <-killReceived:
				case <-release:
				}
			})
			db.AddQueryPatternWithCallback(`kill( query)? \d+`, &sqltypes.Result{}, func(string) {
				killReceived <- struct{}{}
			})

			workersBefore := outstandingQueryWorkers()

			for range kills {
				dbConn, err := newPooledConn(context.Background(), connPool, params)
				require.NoError(t, err)

				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					<-queryStarted
					cancel()
				}()

				err = testCase.execute(ctx, dbConn)
				require.ErrorContains(t, err, "errno 1317")
				require.ErrorContains(t, err, "killing")
				dbConn.Close()
			}

			assert.Eventually(t, func() bool {
				return outstandingQueryWorkers() <= workersBefore
			}, 5*time.Second, 20*time.Millisecond, "query goroutines still running after %d kills", kills)
		})
	}
}

// TestDBConnKillTimeoutDoesNotLeakGoroutines issues kills that MySQL does not
// answer before the kill timeout, through Kill and KillQuery, and verifies that
// the goroutines that sent the KILL statements exit once the timeout has
// abandoned them. The server holds each KILL until the test ends, so every kill
// times out.
func TestDBConnKillTimeoutDoesNotLeakGoroutines(t *testing.T) {
	const kills = 5

	testCases := []struct {
		name string
		kill func(dbConn *Conn) error
		hung string
	}{
		{
			name: "kill connection",
			kill: func(dbConn *Conn) error { return dbConn.Kill("test", 0) },
			hung: "HungConnection",
		},
		{
			name: "kill query",
			kill: func(dbConn *Conn) error { return dbConn.KillQuery("test", 0) },
			hung: "HungQuery",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			defer db.Close()
			connPool := newPool()
			params := dbconfigs.New(db.ConnParams())
			connPool.Open(params, params, params)
			defer connPool.Close()

			release := make(chan struct{})
			defer close(release)
			db.AddQueryPatternWithCallback(`kill( query)? \d+`, &sqltypes.Result{}, func(string) {
				<-release
			})

			workersBefore := outstandingKillWorkers()
			hungBefore := connPool.env.Stats().InternalErrors.Counts()[testCase.hung]

			for range kills {
				dbConn, err := newPooledConn(context.Background(), connPool, params)
				require.NoError(t, err)
				// The timeout also covers getting the connection that sends the KILL, so it
				// needs room to spare when the machine is busy.
				dbConn.killTimeout = 250 * time.Millisecond

				err = testCase.kill(dbConn)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				dbConn.Close()
			}

			assert.EqualValues(t, kills, connPool.env.Stats().InternalErrors.Counts()[testCase.hung]-hungBefore, "every kill must have timed out")
			assert.Eventually(t, func() bool {
				return outstandingKillWorkers() <= workersBefore
			}, 5*time.Second, 20*time.Millisecond, "kill goroutines still running after %d timed out kills", kills)
		})
	}
}
