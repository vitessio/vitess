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

// queryGoroutinesBlockedOnSend counts goroutines started by execOnce or
// streamOnce that are parked sending their result on a channel nobody is
// receiving from.
func queryGoroutinesBlockedOnSend() int {
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
		header, _, _ := strings.Cut(goroutine, "\n")
		blockedOnSend := strings.Contains(header, "chan send")
		startedByQuery := strings.Contains(goroutine, "connpool.(*Conn).execOnce") ||
			strings.Contains(goroutine, "connpool.(*Conn).streamOnce")
		if blockedOnSend && startedByQuery {
			count++
		}
	}
	return count
}

// TestDBConnKillDoesNotLeakGoroutines kills statements whose context is
// cancelled while they run, through Exec, ExecOnce, Stream and StreamOnce, and
// verifies that no goroutine is left behind.
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

			db.AddQuery(query, &sqltypes.Result{})
			db.SetBeforeFunc(query, func() {
				time.Sleep(100 * time.Millisecond)
			})
			db.AddQueryPattern(`kill( query)? \d+`, &sqltypes.Result{})

			blockedBefore := queryGoroutinesBlockedOnSend()

			for range kills {
				dbConn, err := newPooledConn(context.Background(), connPool, params)
				require.NoError(t, err)

				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(10*time.Millisecond, cancel)

				err = testCase.execute(ctx, dbConn)
				require.ErrorContains(t, err, "errno 1317")
				dbConn.Close()
			}

			leaked := func() int {
				return queryGoroutinesBlockedOnSend() - blockedBefore
			}
			assert.Eventually(t, func() bool { return leaked() == 0 }, 2*time.Second, 20*time.Millisecond)
			assert.Zero(t, leaked(), "goroutines still blocked sending the query result after %d kills", kills)
		})
	}
}
