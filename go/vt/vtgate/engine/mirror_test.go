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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
		onRecordMirrorStatsFn: func(sourceTime time.Duration, targetTime time.Duration, err error) {
			sourceExecTime.Store(&sourceTime)
			targetExecTime.Store(&targetTime)
			targetErr.Store(&err)
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
		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})

		require.NotNil(t, targetExecTime.Load())
		mirrorErr := targetErr.Load()
		require.ErrorContains(t, *mirrorErr, "ignore me")
	})

	t.Run("TryExecute fast mirror target", func(t *testing.T) {
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
			select {
			case <-ctx.Done():
				require.Fail(t, "primitive context done")
			default:
			}
		}

		var wg sync.WaitGroup
		defer wg.Wait()
		mirrorVC.onExecuteMultiShardFn = func(ctx context.Context, _ Primitive, _ []*srvtopo.ResolvedShard, _ []*querypb.BoundQuery, _ bool, _ bool) {
			wg.Add(1)
			defer wg.Done()
			time.Sleep(primitiveLatency / 2)
			select {
			case <-ctx.Done():
				require.Fail(t, "mirror target context done")
			default:
			}
		}

		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Equal(t, res, want)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})
		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})

		wg.Wait()

		require.Greater(t, *sourceExecTime.Load(), *targetExecTime.Load())
	})

	t.Run("TryExecute slow mirror target", func(t *testing.T) {
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
			select {
			case <-ctx.Done():
				require.Fail(t, "primitive context done")
			default:
			}
		}

		var wg sync.WaitGroup
		defer wg.Wait()
		mirrorVC.onExecuteMultiShardFn = func(ctx context.Context, _ Primitive, _ []*srvtopo.ResolvedShard, _ []*querypb.BoundQuery, _ bool, _ bool) {
			wg.Add(1)
			defer wg.Done()
			time.Sleep(primitiveLatency + maxMirrorTargetLag + (5 * time.Millisecond))
			select {
			case <-ctx.Done():
				require.NotNil(t, ctx.Err())
				require.ErrorContains(t, ctx.Err(), "context canceled")
			default:
				require.Fail(t, "mirror target context not done")
			}
		}

		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Equal(t, res, want)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})
		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})

		wg.Wait()

		require.Greater(t, *targetExecTime.Load(), *sourceExecTime.Load())
		require.ErrorContains(t, *targetErr.Load(), "Mirror target query took too long")
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
		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		require.NotNil(t, targetExecTime.Load())
		require.ErrorContains(t, *targetErr.Load(), "ignore me")
	})

	t.Run("TryStreamExecute fast mirror target", func(t *testing.T) {
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
			select {
			case <-ctx.Done():
				require.Fail(t, "primitive context done")
			default:
			}
		}

		var wg sync.WaitGroup
		defer wg.Wait()
		mirrorVC.onStreamExecuteMultiFn = func(ctx context.Context, _ Primitive, _ string, _ []*srvtopo.ResolvedShard, _ []map[string]*querypb.BindVariable, _ bool, _ bool, _ func(*sqltypes.Result) error) {
			wg.Add(1)
			defer wg.Done()
			time.Sleep(primitiveLatency / 2)
			select {
			case <-ctx.Done():
				require.Fail(t, "mirror target context done")
			default:
			}
		}

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
		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		require.Greater(t, *sourceExecTime.Load(), *targetExecTime.Load())
	})

	t.Run("TryStreamExecute slow mirror target", func(t *testing.T) {
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
			select {
			case <-ctx.Done():
				require.Fail(t, "primitive context done")
			default:
			}
		}

		var wg sync.WaitGroup
		defer wg.Wait()
		mirrorVC.onStreamExecuteMultiFn = func(ctx context.Context, _ Primitive, _ string, _ []*srvtopo.ResolvedShard, _ []map[string]*querypb.BindVariable, _ bool, _ bool, _ func(*sqltypes.Result) error) {
			wg.Add(1)
			defer wg.Done()
			time.Sleep(primitiveLatency + maxMirrorTargetLag + (5 * time.Millisecond))
			select {
			case <-ctx.Done():
			default:
				require.Fail(t, "mirror target context not done")
			}
		}

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
		mirrorVC.ExpectLog(t, []string{
			fmt.Sprintf(`ResolveDestinations ks2 [%v] Destinations:DestinationKeyspaceID(d46405367612b4b7)`, sqltypes.Int64BindVariable(1)),
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		require.Greater(t, *targetExecTime.Load(), *sourceExecTime.Load())
		require.ErrorContains(t, *targetErr.Load(), "Mirror target query took too long")
	})
}
