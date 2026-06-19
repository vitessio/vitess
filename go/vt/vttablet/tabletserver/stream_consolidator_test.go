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

package tabletserver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func consolidationCount(items []sync2.ConsolidatorCacheItem, query string) int64 {
	for _, item := range items {
		if item.Query == query {
			return item.Count
		}
	}
	return 0
}

// TestStreamConsolidatorRecordsFollowers verifies that a streaming query which
// consolidates onto an in-flight leader is recorded for /debug/consolidations,
// while the leader itself is not. The test is deterministic: the leader registers
// itself in the inflight map before its leaderCallback runs, so gating the leader
// open until the follower has recorded guarantees the follower attaches rather
// than becoming a new leader.
func TestStreamConsolidatorRecordsFollowers(t *testing.T) {
	sc := NewStreamConsolidator(128*1024, 2*1024, nocleanup)

	const sql = "select 1"

	newTimings := func() *servenv.TimingsWrapper {
		return servenv.NewExporter("ConsolidatorTest", "").NewTimings("ConsolidatorWaits", "", "StreamConsolidations")
	}
	newLogStats := func() *tabletenv.LogStats {
		return tabletenv.NewLogStats(context.Background(), "StreamConsolidation", streamlog.NewQueryLogConfigForTest())
	}

	leaderInflight := make(chan struct{}) // closed once the leader is registered + streaming
	releaseLeader := make(chan struct{})  // unblocks the leader so it can finish

	var (
		leaderErr, followerErr error
		followerRanLeader      atomic.Bool
		wg                     sync.WaitGroup
	)
	wg.Go(func() {
		leaderErr = sc.Consolidate(newTimings(), newLogStats(), sql,
			func(*sqltypes.Result) error { return nil },
			func(stream StreamCallback) error {
				close(leaderInflight)
				<-releaseLeader
				return stream(&sqltypes.Result{})
			})
	})

	<-leaderInflight

	wg.Go(func() {
		followerErr = sc.Consolidate(newTimings(), newLogStats(), sql,
			func(*sqltypes.Result) error { return nil },
			func(StreamCallback) error {
				// A follower must never run the leader callback.
				followerRanLeader.Store(true)
				return nil
			})
	})

	// The follower records as soon as it attaches to the leader; once that's
	// visible, it's safe to let the leader finish.
	require.Eventually(t, func() bool {
		return consolidationCount(sc.Items(), sql) == 1
	}, 5*time.Second, time.Millisecond, "follower consolidation was not recorded")

	close(releaseLeader)
	wg.Wait()

	require.NoError(t, leaderErr)
	require.NoError(t, followerErr)
	require.False(t, followerRanLeader.Load(), "follower wrongly executed the leader callback")
	// Exactly one follower consolidated; the leader is not counted.
	require.Equal(t, int64(1), consolidationCount(sc.Items(), sql))
}
