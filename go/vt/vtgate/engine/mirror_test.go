/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

	mirror := &Mirror{
		Primitive: primitive,
		Target: &PercentMirrorTarget{
			Percent:   100,
			Primitive: mirrorPrimitive1,
		},
	}

	mirrorVc := &loggingVCursor{
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
		mirrorClonesFn: func(ctx context.Context) VCursor {
			return mirrorVc
		},
	}

	t.Run("TryExecute success", func(t *testing.T) {
		defer func() {
			vc.Rewind()
			mirrorVc.Rewind()
		}()

		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Equal(t, want, res)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})
		mirrorVc.ExpectLog(t, []string{
			`ResolveDestinations ks2 [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(d46405367612b4b7)`,
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})
	})

	t.Run("TryExecute return primitive error", func(t *testing.T) {
		results := vc.results

		defer func() {
			vc.Rewind()
			vc.results = results
			vc.resultErr = nil
			mirrorVc.Rewind()
		}()

		vc.results = nil
		vc.resultErr = fmt.Errorf("return me")

		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Nil(t, res)
		require.Error(t, err)
		require.Equal(t, vc.resultErr, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})
		mirrorVc.ExpectLog(t, []string{
			`ResolveDestinations ks2 [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(d46405367612b4b7)`,
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})
	})

	t.Run("TryExecute ignore mirror target error", func(t *testing.T) {
		results := mirrorVc.results

		defer func() {
			vc.Rewind()
			mirrorVc.Rewind()
			mirrorVc.results = results
			mirrorVc.resultErr = nil
		}()

		mirrorVc.results = nil
		mirrorVc.resultErr = fmt.Errorf("ignore me")

		want := vc.results[0]
		res, err := mirror.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.Equal(t, res, want)
		require.NoError(t, err)

		vc.ExpectLog(t, []string{
			"ResolveDestinations ks1 [] Destinations:DestinationAllShards()",
			"ExecuteMultiShard ks1.0: select f.bar from foo f where f.id = 1 {} false false",
		})
		mirrorVc.ExpectLog(t, []string{
			`ResolveDestinations ks2 [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(d46405367612b4b7)`,
			"ExecuteMultiShard ks2.-20: select f.bar from foo f where f.id = 1 {} false false",
		})
	})

	t.Run("TryStreamExecute success", func(t *testing.T) {
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
		mirrorVc.ExpectLog(t, []string{
			`ResolveDestinations ks2 [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(d46405367612b4b7)`,
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})

		vc.Rewind()
		mirrorVc.Rewind()
	})

	t.Run("TryStreamExecute return primitive error", func(t *testing.T) {
		results := vc.results

		defer func() {
			vc.Rewind()
			vc.results = results
			vc.resultErr = nil
			mirrorVc.Rewind()
		}()

		vc.results = nil
		vc.resultErr = fmt.Errorf("return me")

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
		mirrorVc.ExpectLog(t, []string{
			`ResolveDestinations ks2 [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(d46405367612b4b7)`,
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})
	})

	t.Run("TryStreamExecute ignore mirror target error", func(t *testing.T) {
		results := mirrorVc.results

		defer func() {
			vc.Rewind()
			mirrorVc.Rewind()
			mirrorVc.results = results
			mirrorVc.resultErr = nil
		}()

		mirrorVc.results = nil
		mirrorVc.resultErr = fmt.Errorf("ignore me")

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
		mirrorVc.ExpectLog(t, []string{
			`ResolveDestinations ks2 [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(d46405367612b4b7)`,
			"StreamExecuteMulti select f.bar from foo f where f.id = 1 ks2.-20: {} ",
		})
	})
}
