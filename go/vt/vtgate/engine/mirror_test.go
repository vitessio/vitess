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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestMirror(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("xxhash", "xxhash_vdx", nil)

	primitive := NewRoute(
		Unsharded,
		&vindexes.Keyspace{
			Name: "ks1",
		},
		"select f.bar from foo f where f.id = 1",
		"select 1 from foo f where f.id = 1 and 1 != 1",
	)

	mirrorPrimitive1 := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks2",
			Sharded: true,
		},
		"select f.bar from foo f where f.id = 1",
		"select 1 from foo f where f.id = 1 and 1 != 1",
	)
	mirrorPrimitive1.Vindex = vindex.(vindexes.SingleColumn)
	mirrorPrimitive1.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}

	mirror := NewPercentBasedMirror(100, primitive, mirrorPrimitive1)

	mirrorVC := &loggingVCursor{
		shards: []string{"-20", "20-"},
		ksShardMap: map[string][]string{
			"ks2": {"-20", "20-"},
		},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"bar",
					"varchar",
				),
				"hello",
			),
		},
	}

	sourceExecTime := atomic.Pointer[time.Duration]{}
	targetExecTime := atomic.Pointer[time.Duration]{}
	targetErr := atomic.Pointer[error]{}

	// mirrorStatsDone is closed when the mirror goroutine calls
	// RecordMirrorStats. Tests that inspect mirror stats should wait
	// on this channel before asserting.
	mirrorStatsDone := make(chan struct{}, 1)

	// Stats callback goes on mirrorVC because fire-and-forget mirrors
	// call RecordMirrorStats on the cloned cursor, not the original.
	mirrorVC.onRecordMirrorStatsFn = func(sourceTime time.Duration, targetTime time.Duration, err error) {
		sourceExecTime.Store(&sourceTime)
		targetExecTime.Store(&targetTime)
		targetErr.Store(&err)
		select {
		case mirrorStatsDone <- struct{}{}:
		default:
		}
	}

	vc := &loggingVCursor{
		shards: []string{"0"},
		ksShardMap: map[string][]string{
			"ks1": {"0"},
		},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"bar",
					"varchar",
				),
				"hello",
			),
		},
		onMirrorClonesFn: func(ctx context.Context) VCursor {
			return mirrorVC
		},
	}

	t.Run("TryExecute success", func(t *testing.T) {
		defer func() {
			vc.Rewind()
			mirrorVC.Rewind()
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Equal(t, want, res)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})

		// Wait for async mirror goroutine to complete.
		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})
		require.NotNil(t, targetExecTime.Load())
		require.Nil(t, *targetErr.Load())
	})

	t.Run("TryExecute return primitive error", func(t *testing.T) {
		results := vc.results

		defer func() {
			vc.Rewind()
			vc.results = results
			vc.resultErr = nil
			mirrorVC.Rewind()
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		vc.results = nil
		vc.resultErr = errors.New("return me")

		ctx := context.Background()
		res, err := mirror.TryExecute(ctx, vc, map[string]*querypb.BindVariable{}, true)
		require.Nil(t, res)
		require.Error(t, err)
		require.Equal(t, vc.resultErr, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})

		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})
		require.NotNil(t, targetExecTime.Load())
		require.Nil(t, *targetErr.Load())
	})

	t.Run("TryExecute ignore mirror target error", func(t *testing.T) {
		results := mirrorVC.results

		defer func() {
			vc.Rewind()
			mirrorVC.Rewind()
			mirrorVC.results = results
			mirrorVC.resultErr = nil
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		mirrorVC.results = nil
		mirrorVC.resultErr = errors.New("ignore me")

		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Equal(t, res, want)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})

		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})

		require.NotNil(t, targetExecTime.Load())
		mirrorErr := targetErr.Load()
		require.ErrorContains(t, *mirrorErr, "ignore me")
	})

	t.Run("TryExecute mirror target completes independently", func(t *testing.T) {
		defer func() {
			vc.Rewind()
			vc.onExecuteMultiShardFn = nil
			mirrorVC.Rewind()
			mirrorVC.onExecuteMultiShardFn = nil
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Equal(t, res, want)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})

		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})

		require.NotNil(t, targetExecTime.Load())
		require.Nil(t, *targetErr.Load())
	})

	t.Run("TryExecute slow mirror target does not block primary", func(t *testing.T) {
		defer func() {
			vc.Rewind()
			vc.onExecuteMultiShardFn = nil
			mirrorVC.Rewind()
			mirrorVC.onExecuteMultiShardFn = nil
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		primitiveLatency := 10 * time.Millisecond
		vc.onExecuteMultiShardFn = func(ctx context.Context, _ Primitive, _ []*srvtopo.ResolvedShard, _ []*querypb.BoundQuery, _ bool, _ bool) {
			time.Sleep(primitiveLatency)
		}

		mirrorStarted := make(chan struct{})
		mirrorDone := make(chan struct{})
		mirrorVC.onExecuteMultiShardFn = func(ctx context.Context, _ Primitive, _ []*srvtopo.ResolvedShard, _ []*querypb.BoundQuery, _ bool, _ bool) {
			close(mirrorStarted)
			// Block until the test releases us — this simulates a slow
			// mirror target that far outlasts the primary.
			<-mirrorDone
		}

		// TryExecute should return the primary result without waiting
		// for the slow mirror target.
		start := time.Now()
		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		elapsed := time.Since(start)
		require.Equal(t, res, want)
		require.NoError(t, err)

		// Primary should return in roughly primitiveLatency, not blocked
		// by the mirror.
		require.Less(t, elapsed, primitiveLatency+50*time.Millisecond)

		// Mirror goroutine should have started.
		select {
		case <-mirrorStarted:
		case <-time.After(time.Second):
			require.Fail(t, "mirror goroutine did not start")
		}

		// Release the mirror goroutine and wait for it to finish
		// recording stats so it doesn't leak into the next test.
		close(mirrorDone)
		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
		}
	})

	t.Run("TryStreamExecute success", func(t *testing.T) {
		defer func() {
			vc.Rewind()
			mirrorVC.Rewind()
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		want := vc.results[0]
		err := mirror.TryStreamExecute(
			context.Background(),
			vc,
			map[string]*querypb.BindVariable{},
			true,
			func(result *sqltypes.Result) error {
				require.Equal(t, want, result)
				return nil
			},
		)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks1.0: {} ",
		})

		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		require.NotNil(t, targetExecTime.Load())
		require.Nil(t, *targetErr.Load())
	})

	t.Run("TryStreamExecute return primitive error", func(t *testing.T) {
		results := vc.results

		defer func() {
			vc.Rewind()
			vc.results = results
			vc.resultErr = nil
			mirrorVC.Rewind()
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		vc.results = nil
		vc.resultErr = errors.New("return me")

		err := mirror.TryStreamExecute(
			context.Background(),
			vc,
			map[string]*querypb.BindVariable{},
			true,
			func(result *sqltypes.Result) error {
				require.Nil(t, result)
				return nil
			},
		)
		require.Error(t, err)
		require.Equal(t, vc.resultErr, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks1.0: {} ",
		})

		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		require.NotNil(t, targetExecTime.Load())
		require.Nil(t, *targetErr.Load())
	})

	t.Run("TryStreamExecute ignore mirror target error", func(t *testing.T) {
		results := mirrorVC.results

		defer func() {
			vc.Rewind()
			mirrorVC.Rewind()
			mirrorVC.results = results
			mirrorVC.resultErr = nil
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		mirrorVC.results = nil
		mirrorVC.resultErr = errors.New("ignore me")

		want := vc.results[0]
		err := mirror.TryStreamExecute(
			context.Background(),
			vc,
			map[string]*querypb.BindVariable{},
			true,
			func(result *sqltypes.Result) error {
				require.Equal(t, want, result)
				return nil
			},
		)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks1.0: {} ",
		})

		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		require.NotNil(t, targetExecTime.Load())
		require.ErrorContains(t, *targetErr.Load(), "ignore me")
	})

	t.Run("TryStreamExecute mirror target completes independently", func(t *testing.T) {
		defer func() {
			vc.Rewind()
			mirrorVC.Rewind()
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		want := vc.results[0]
		err := mirror.TryStreamExecute(
			context.Background(),
			vc,
			map[string]*querypb.BindVariable{},
			true,
			func(result *sqltypes.Result) error {
				require.Equal(t, want, result)
				return nil
			},
		)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks1.0: {} ",
		})

		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "mirror stats not recorded")
		}

		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		require.NotNil(t, targetExecTime.Load())
		require.Nil(t, *targetErr.Load())
	})

	t.Run("TryStreamExecute slow mirror target does not block primary", func(t *testing.T) {
		defer func() {
			vc.Rewind()
			vc.onStreamExecuteMultiFn = nil
			mirrorVC.Rewind()
			mirrorVC.onStreamExecuteMultiFn = nil
			sourceExecTime.Store(nil)
			targetExecTime.Store(nil)
			targetErr.Store(nil)
		}()

		primitiveLatency := 10 * time.Millisecond
		vc.onStreamExecuteMultiFn = func(ctx context.Context, _ Primitive, _ string, _ []*srvtopo.ResolvedShard, _ []map[string]*querypb.BindVariable, _ bool, _ bool, _ func(*sqltypes.Result) error) {
			time.Sleep(primitiveLatency)
		}

		mirrorStarted := make(chan struct{})
		mirrorDone := make(chan struct{})
		mirrorVC.onStreamExecuteMultiFn = func(ctx context.Context, _ Primitive, _ string, _ []*srvtopo.ResolvedShard, _ []map[string]*querypb.BindVariable, _ bool, _ bool, _ func(*sqltypes.Result) error) {
			close(mirrorStarted)
			<-mirrorDone
		}

		start := time.Now()
		want := vc.results[0]
		err := mirror.TryStreamExecute(
			context.Background(),
			vc,
			map[string]*querypb.BindVariable{},
			true,
			func(result *sqltypes.Result) error {
				require.Equal(t, want, result)
				return nil
			},
		)
		elapsed := time.Since(start)
		require.NoError(t, err)

		require.Less(t, elapsed, primitiveLatency+50*time.Millisecond)

		select {
		case <-mirrorStarted:
		case <-time.After(time.Second):
			require.Fail(t, "mirror goroutine did not start")
		}

		close(mirrorDone)
		select {
		case <-mirrorStatsDone:
		case <-time.After(5 * time.Second):
		}
	})
}

// TestMirrorDropsWhenSemaphoreFull verifies that when the bounded mirror
// concurrency semaphore is at capacity, new mirror queries are dropped
// (not queued) and the drop hook fires.
func TestMirrorDropsWhenSemaphoreFull(t *testing.T) {
	primitive := NewRoute(
		Unsharded,
		&vindexes.Keyspace{Name: "ks1"},
		"select 1 from foo",
		"select 1 from foo where 1 != 1",
	)
	target := NewRoute(
		Unsharded,
		&vindexes.Keyspace{Name: "ks2"},
		"select 1 from foo",
		"select 1 from foo where 1 != 1",
	)
	mirror := NewPercentBasedMirror(100, primitive, target)

	vc := &loggingVCursor{
		shards:  []string{"-"},
		results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("a", "varchar"), "x")},
	}

	// Pre-acquire the only slot so the dispatch path falls through to the
	// drop branch. Capacity 1, weight 1 already taken => TryAcquire fails.
	sem := semaphore.NewWeighted(1)
	require.True(t, sem.TryAcquire(1))
	vc.mirrorTrafficSemaphore = sem

	var droppedCount atomic.Int32
	vc.onRecordMirrorDroppedFn = func() { droppedCount.Add(1) }
	vc.onMirrorClonesFn = func(_ context.Context) VCursor {
		t.Fatalf("mirror should not have been cloned when semaphore is full")
		return nil
	}

	_, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	require.Equal(t, int32(1), droppedCount.Load(), "drop hook should have fired exactly once")
}
