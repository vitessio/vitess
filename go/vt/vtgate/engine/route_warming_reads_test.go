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

package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

type warmingReadsVCursor struct {
	*loggingVCursor
	warmingReadsPercent     int
	warmingReadsChannel     chan bool
	warmingReadsTimeout     time.Duration
	warmingReadsExecuteFunc func(context.Context, Primitive, []*srvtopo.ResolvedShard, []*querypb.BoundQuery, bool, bool)
}

func (vc *warmingReadsVCursor) GetWarmingReadsPercent() int {
	return vc.warmingReadsPercent
}

func (vc *warmingReadsVCursor) GetWarmingReadsChannel() chan bool {
	return vc.warmingReadsChannel
}

func (vc *warmingReadsVCursor) CloneForReplicaWarming(ctx context.Context) VCursor {
	clonedLogging := &loggingVCursor{
		shards:                  vc.shards,
		results:                 vc.results,
		onResolveDestinationsFn: vc.onResolveDestinationsFn,
	}
	clone := &warmingReadsVCursor{
		loggingVCursor:          clonedLogging,
		warmingReadsPercent:     vc.warmingReadsPercent,
		warmingReadsChannel:     vc.warmingReadsChannel,
		warmingReadsTimeout:     vc.warmingReadsTimeout,
		warmingReadsExecuteFunc: vc.warmingReadsExecuteFunc,
	}
	clone.onExecuteMultiShardFn = vc.warmingReadsExecuteFunc
	return clone
}

func (vc *warmingReadsVCursor) WarmingReadsContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), vc.warmingReadsTimeout)
}

func TestWarmingReadsSkipsForUpdate(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	testCases := []struct {
		name                 string
		query                string
		expectedWarmingQuery string
	}{
		{
			name:                 "SELECT FOR UPDATE",
			query:                "SELECT * FROM users WHERE id = 1 FOR UPDATE",
			expectedWarmingQuery: "select * from users where id = 1",
		},
		{
			name:                 "SELECT FOR UPDATE mixed case",
			query:                "SELECT * FROM users WHERE id = 1 FoR UpDaTe",
			expectedWarmingQuery: "select * from users where id = 1",
		},
		{
			name:                 "SELECT FOR UPDATE with extra spaces",
			query:                "SELECT * FROM users WHERE id = 1 FOR     UPDATE",
			expectedWarmingQuery: "select * from users where id = 1",
		},
		{
			name:                 "SELECT FOR UPDATE with comment",
			query:                "SELECT * FROM users WHERE id = 1 FOR /* comment */ UPDATE",
			expectedWarmingQuery: "select * from users where id = 1",
		},
		{
			name:                 "SELECT FOR UPDATE NOWAIT",
			query:                "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT",
			expectedWarmingQuery: "select * from users where id = 1",
		},
		{
			name:                 "SELECT FOR UPDATE SKIP LOCKED",
			query:                "SELECT * FROM users WHERE id = 1 FOR UPDATE SKIP LOCKED",
			expectedWarmingQuery: "select * from users where id = 1",
		},
		{
			name:                 "UNION FOR UPDATE",
			query:                "SELECT * FROM users WHERE id = 1 UNION SELECT * FROM users WHERE id = 2 FOR UPDATE",
			expectedWarmingQuery: "select * from users where id = 1 union select * from users where id = 2",
		},
		{
			name:                 "Regular SELECT",
			query:                "SELECT * FROM users WHERE id = 1",
			expectedWarmingQuery: "SELECT * FROM users WHERE id = 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			route := NewRoute(
				EqualUnique,
				&vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				tc.query,
				"dummy_select_field",
			)
			// Parse and set QueryStatement to match how Routes are created in production
			parser, _ := sqlparser.NewTestParser().Parse(tc.query)
			route.QueryStatement = parser
			route.Vindex = vindex.(vindexes.SingleColumn)
			route.Values = []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			}

			var warmingReadExecuted atomic.Bool
			var capturedQuery string
			var capturedCtxHasDeadline atomic.Bool
			var capturedCtxErr atomic.Pointer[error]
			var resolveDestCtxHasDeadline atomic.Bool
			// done is closed by the test to unblock the warming read goroutine
			// after context assertions have been made.
			done := make(chan struct{})
			vc := &warmingReadsVCursor{
				loggingVCursor: &loggingVCursor{
					shards:  []string{"-20", "20-"},
					results: []*sqltypes.Result{defaultSelectResult},
					onResolveDestinationsFn: func(ctx context.Context) {
						_, hasDeadline := ctx.Deadline()
						resolveDestCtxHasDeadline.Store(hasDeadline)
					},
				},
				warmingReadsPercent: 100,
				warmingReadsChannel: make(chan bool, 1),
				warmingReadsTimeout: 5 * time.Second,
			}
			vc.warmingReadsExecuteFunc = func(ctx context.Context, primitive Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) {
				if len(queries) > 0 {
					capturedQuery = queries[0].Sql
				}
				_, hasDeadline := ctx.Deadline()
				capturedCtxHasDeadline.Store(hasDeadline)
				ctxErr := ctx.Err()
				capturedCtxErr.Store(&ctxErr)
				warmingReadExecuted.Store(true)
				// Block until the test has checked our context assertions,
				// preventing defer cancel() from running.
				select {
				case <-done:
				case <-t.Context().Done():
				}
			}

			// Use a cancelable parent context to verify the warming read
			// context is independent of the parent request context.
			parentCtx, parentCancel := context.WithCancel(t.Context())
			_, err := route.TryExecute(parentCtx, vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)

			// Cancel the parent context to simulate the primary request completing.
			parentCancel()

			require.Eventually(t, func() bool {
				return warmingReadExecuted.Load()
			}, time.Second, 10*time.Millisecond, "warming read should be executed")

			require.Equal(t, tc.expectedWarmingQuery, capturedQuery, "warming read query should match expected")
			require.True(t, capturedCtxHasDeadline.Load(), "warming read context should have a deadline from the timeout")

			// The warming read context should still be active even though the
			// parent request context was canceled.
			require.NoError(t, *capturedCtxErr.Load(), "warming read context should not be canceled when parent context is canceled")

			// Verify findRoute received the warming context (with deadline), not the parent context.
			require.True(t, resolveDestCtxHasDeadline.Load(), "ResolveDestinations should receive a context with deadline from the warming timeout")

			// Unblock the warming read goroutine.
			close(done)
		})
	}
}

func TestWarmingReadsDroppedWhenChannelFull(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	route := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"SELECT * FROM users WHERE id = 1",
		"dummy_select_field",
	)
	parser, _ := sqlparser.NewTestParser().Parse("SELECT * FROM users WHERE id = 1")
	route.QueryStatement = parser
	route.Vindex = vindex.(vindexes.SingleColumn)
	route.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}

	var warmingReadExecuted atomic.Bool
	vc := &warmingReadsVCursor{
		loggingVCursor: &loggingVCursor{
			shards:  []string{"-20", "20-"},
			results: []*sqltypes.Result{defaultSelectResult},
		},
		warmingReadsPercent: 100,
		warmingReadsChannel: make(chan bool, 1),
		warmingReadsTimeout: 5 * time.Second,
	}
	vc.warmingReadsExecuteFunc = func(ctx context.Context, primitive Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) {
		warmingReadExecuted.Store(true)
	}

	// Pre-fill the channel to simulate a full pool.
	vc.warmingReadsChannel <- true

	_, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	// Verify over a short window that no warming read is executed while the channel is full.
	require.Never(t, func() bool {
		return warmingReadExecuted.Load()
	}, 100*time.Millisecond, 5*time.Millisecond, "warming read should not execute when the channel is full")
	// Drain the channel.
	<-vc.warmingReadsChannel
}

func TestWarmingReadsContextTimeout(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	route := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"SELECT * FROM users WHERE id = 1",
		"dummy_select_field",
	)
	parser, _ := sqlparser.NewTestParser().Parse("SELECT * FROM users WHERE id = 1")
	route.QueryStatement = parser
	route.Vindex = vindex.(vindexes.SingleColumn)
	route.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}

	var capturedCtxErr atomic.Pointer[error]
	var warmingReadExecuted atomic.Bool
	vc := &warmingReadsVCursor{
		loggingVCursor: &loggingVCursor{
			shards:  []string{"-20", "20-"},
			results: []*sqltypes.Result{defaultSelectResult},
		},
		warmingReadsPercent: 100,
		warmingReadsChannel: make(chan bool, 1),
		warmingReadsTimeout: 1 * time.Millisecond,
	}
	vc.warmingReadsExecuteFunc = func(ctx context.Context, primitive Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) {
		// Block until the warming context times out.
		<-ctx.Done()
		ctxErr := ctx.Err()
		capturedCtxErr.Store(&ctxErr)
		warmingReadExecuted.Store(true)
	}

	_, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return warmingReadExecuted.Load()
	}, time.Second, 10*time.Millisecond, "warming read should have been executed and timed out")

	require.ErrorIs(t, *capturedCtxErr.Load(), context.DeadlineExceeded, "warming read context should have timed out")
}
