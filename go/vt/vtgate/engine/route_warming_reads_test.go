/*
Copyright 2024 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

type warmingReadsVCursor struct {
	*loggingVCursor
	warmingReadsPercent     int
	warmingReadsChannel     chan bool
	warmingReadsExecuteFunc func(context.Context, Primitive, []*srvtopo.ResolvedShard, []*querypb.BoundQuery, bool, bool)
}

func (vc *warmingReadsVCursor) GetWarmingReadsPercent() int {
	return vc.warmingReadsPercent
}

func (vc *warmingReadsVCursor) GetWarmingReadsChannel() chan bool {
	return vc.warmingReadsChannel
}

func (vc *warmingReadsVCursor) CloneForReplicaWarming(ctx context.Context) VCursor {
	clone := &warmingReadsVCursor{
		loggingVCursor:          vc.loggingVCursor,
		warmingReadsPercent:     vc.warmingReadsPercent,
		warmingReadsChannel:     vc.warmingReadsChannel,
		warmingReadsExecuteFunc: vc.warmingReadsExecuteFunc,
	}
	clone.onExecuteMultiShardFn = vc.warmingReadsExecuteFunc
	return clone
}

func TestWarmingReadsSkipsForUpdate(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	testCases := []struct {
		name       string
		query      string
		shouldSkip bool
	}{
		{
			name:       "SELECT FOR UPDATE",
			query:      "SELECT * FROM users WHERE id = 1 FOR UPDATE",
			shouldSkip: true,
		},
		{
			name:       "SELECT FOR UPDATE mixed case",
			query:      "SELECT * FROM users WHERE id = 1 FoR UpDaTe",
			shouldSkip: true,
		},
		{
			name:       "Regular SELECT",
			query:      "SELECT * FROM users WHERE id = 1",
			shouldSkip: false,
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
			}
			vc.warmingReadsExecuteFunc = func(ctx context.Context, primitive Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) {
				warmingReadExecuted.Store(true)
			}

			_, err := route.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)

			if tc.shouldSkip {
				time.Sleep(50 * time.Millisecond)
				require.False(t, warmingReadExecuted.Load(), "warming read should not be executed for FOR UPDATE queries")
			} else {
				require.Eventually(t, func() bool {
					return warmingReadExecuted.Load()
				}, time.Second, 10*time.Millisecond, "warming read should be executed for regular SELECT queries")
			}
		})
	}
}
