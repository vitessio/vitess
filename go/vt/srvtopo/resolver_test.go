/*
Copyright 2019 The Vitess Authors.

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

package srvtopo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func initResolver(t *testing.T, ctx context.Context) *Resolver {
	cell := "cell1"
	ts := memorytopo.NewServer(ctx, cell)
	counts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	rs := NewResilientServer(ctx, ts, counts)

	// Create sharded keyspace and shards.
	if err := ts.CreateKeyspace(ctx, "sks", &topodatapb.Keyspace{}); err != nil {
		require.NoError(t, err)
	}
	shardKrArray, err := key.ParseShardingSpec("-20-40-60-80-a0-c0-e0-")
	require.NoError(t, err)
	for _, kr := range shardKrArray {
		shard := key.KeyRangeString(kr)
		require.NoErrorf(t, ts.CreateShard(ctx, "sks", shard), "CreateShard(%q) failed", shard)
	}

	// Create unsharded keyspace and shard.
	if err := ts.CreateKeyspace(ctx, "uks", &topodatapb.Keyspace{}); err != nil {
		require.NoError(t, err)
	}
	if err := ts.CreateShard(ctx, "uks", "0"); err != nil {
		require.NoError(t, err)
	}

	// And rebuild both.
	for _, keyspace := range []string{"sks", "uks"} {
		require.NoErrorf(t, topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, keyspace, []string{cell}, false), "RebuildKeyspace(%v) failed", keyspace)
	}

	// Create snapshot keyspace and shard.
	err = ts.CreateKeyspace(ctx, "rks", &topodatapb.Keyspace{KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT})
	require.NoError(t, err, "CreateKeyspace(rks) failed: %v")
	err = ts.CreateShard(ctx, "rks", "-80")
	require.NoError(t, err, "CreateShard(-80) failed: %v")

	// Rebuild should error because allowPartial is false and shard does not cover full keyrange
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "rks", []string{cell}, false)
	require.Error(t, err, "RebuildKeyspace(rks) failed")
	require.EqualError(t, err, "keyspace partition for PRIMARY in cell cell1 does not end with max key")

	// Rebuild should succeed with allowPartial true
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "rks", []string{cell}, true)
	require.NoError(t, err, "RebuildKeyspace(rks) failed")

	// Create missing shard
	err = ts.CreateShard(ctx, "rks", "80-")
	require.NoError(t, err, "CreateShard(80-) failed: %v")

	// Rebuild should now succeed even with allowPartial false
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "rks", []string{cell}, false)
	require.NoError(t, err, "RebuildKeyspace(rks) failed")

	return NewResolver(rs, &tabletconntest.FakeQueryService{}, cell)
}

func TestResolveDestinations(t *testing.T) {
	ctx := t.Context()
	resolver := initResolver(t, ctx)

	id1 := &querypb.Value{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}
	id2 := &querypb.Value{
		Type:  sqltypes.VarChar,
		Value: []byte("2"),
	}

	kr2040 := &topodatapb.KeyRange{
		Start: []byte{0x20},
		End:   []byte{0x40},
	}
	kr80a0 := &topodatapb.KeyRange{
		Start: []byte{0x80},
		End:   []byte{0xa0},
	}
	kr2830 := &topodatapb.KeyRange{
		Start: []byte{0x28},
		End:   []byte{0x30},
	}

	testCases := []struct {
		name           string
		keyspace       string
		ids            []*querypb.Value
		destinations   []key.ShardDestination
		errString      string
		expectedShards []string
		expectedValues [][]*querypb.Value
	}{
		{
			name:     "unsharded keyspace, regular shard, no ids",
			keyspace: "uks",
			destinations: []key.ShardDestination{
				key.DestinationShard("0"),
			},
			expectedShards: []string{"0"},
		},
		{
			name:     "unsharded keyspace, regular shard, with ids",
			keyspace: "uks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.ShardDestination{
				key.DestinationShard("0"),
				key.DestinationShard("0"),
			},
			expectedShards: []string{"0"},
			expectedValues: [][]*querypb.Value{
				{id1, id2},
			},
		},
		{
			name:     "sharded keyspace, keyrange destinations, with ids",
			keyspace: "sks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.ShardDestination{
				key.DestinationExactKeyRange{KeyRange: kr2040},
				key.DestinationExactKeyRange{KeyRange: kr80a0},
			},
			expectedShards: []string{"20-40", "80-a0"},
			expectedValues: [][]*querypb.Value{
				{id1},
				{id2},
			},
		},
		{
			name:     "sharded keyspace, keyspace id destinations, with ids",
			keyspace: "sks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.ShardDestination{
				key.DestinationKeyspaceID{0x28},
				key.DestinationKeyspaceID{0x78, 0x23},
			},
			expectedShards: []string{"20-40", "60-80"},
			expectedValues: [][]*querypb.Value{
				{id1},
				{id2},
			},
		},
		{
			name:     "sharded keyspace, multi keyspace id destinations, with ids",
			keyspace: "sks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.ShardDestination{
				key.DestinationKeyspaceIDs{
					{0x28},
					{0x47},
				},
				key.DestinationKeyspaceIDs{
					{0x78},
					{0x23},
				},
			},
			expectedShards: []string{"20-40", "40-60", "60-80"},
			expectedValues: [][]*querypb.Value{
				{id1, id2},
				{id1},
				{id2},
			},
		},
		{
			name:     "using non-mapping keyranges should fail",
			keyspace: "sks",
			destinations: []key.ShardDestination{
				key.DestinationExactKeyRange{
					KeyRange: kr2830,
				},
			},
			errString: "keyrange 28-30 does not exactly match shards",
		},
	}
	for _, testCase := range testCases {
		ctx := t.Context()
		rss, values, err := resolver.ResolveDestinations(ctx, testCase.keyspace, topodatapb.TabletType_REPLICA, testCase.ids, testCase.destinations)
		if err != nil {
			if testCase.errString == "" {
				assert.Failf(t, testCase.name, "expected success but got error: %v", err)
			} else {
				require.EqualErrorf(t, err, testCase.errString, "%v: expected error '%v' but got error: %v", testCase.name, testCase.errString, err)
			}
			continue
		}

		if testCase.errString != "" {
			assert.Failf(t, testCase.name, "expected error '%v' but got success", testCase.errString)
			continue
		}

		// Check the ResolvedShard are correct.
		if len(rss) != len(testCase.expectedShards) {
			assert.Failf(t, testCase.name, "expected %v ResolvedShard, but got: %v", len(testCase.expectedShards), rss)
			continue
		}
		badShards := false
		for i, rs := range rss {
			if rs.Target.Shard != testCase.expectedShards[i] {
				assert.Failf(t, testCase.name, "expected rss[%v] to be '%v', but got: %v", i, testCase.expectedShards[i], rs.Target.Shard)
				badShards = true
			}
		}
		if badShards {
			continue
		}

		// Check the values are correct, if we passed some in.
		if testCase.ids == nil {
			continue
		}
		assert.Lenf(t, values, len(rss), "%v: len(values) != len(rss): %v != %v", testCase.name, len(values), len(rss))
		assert.True(t, ValuesEqual(values, testCase.expectedValues), "values != testCase.expectedValues: got values=%v", values)
	}
}

// BenchmarkResolveDestinations includes the warm serving-topology cache lookup,
// keyspace ID resolution and grouping of IN-list values by shard, but no RPCs.
func BenchmarkResolveDestinations(b *testing.B) {
	for _, shardCount := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			ctx := b.Context()
			ts := memorytopo.NewServer(ctx, "cell")
			b.Cleanup(ts.Close)
			shards := make([]*topodatapb.ShardReference, shardCount)
			for i := range shards {
				kr, err := key.EvenShardsKeyRange(i, shardCount)
				require.NoError(b, err)
				shards[i] = &topodatapb.ShardReference{Name: key.KeyRangeString(kr), KeyRange: kr}
			}
			require.NoError(b, ts.UpdateSrvKeyspace(ctx, "cell", "ks", &topodatapb.SrvKeyspace{
				Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{
					ServedType:      topodatapb.TabletType_PRIMARY,
					ShardReferences: shards,
				}},
			}))
			counts := stats.NewCountersWithSingleLabel("", "", "type")
			server := NewResilientServer(ctx, ts, counts)
			resolver := NewResolver(server, nil, "cell")
			_, err := server.GetSrvKeyspace(ctx, "cell", "ks")
			require.NoError(b, err)

			for _, tc := range []struct {
				name       string
				valueCount int
				groupIDs   bool
			}{
				{name: "equal", valueCount: 1},
				{name: "in=16", valueCount: 16, groupIDs: true},
				{name: "in=256", valueCount: 256, groupIDs: true},
			} {
				b.Run(tc.name, func(b *testing.B) {
					ctx := b.Context()
					ids := make([]*querypb.Value, tc.valueCount)
					destinations := make([]key.ShardDestination, tc.valueCount)
					expected := make(map[string][]*querypb.Value)
					for i := range ids {
						// Walk the keyspace rather than measuring only the first shard.
						ksid := byte(128 + i*37)
						ids[i] = sqltypes.ValueToProto(sqltypes.NewInt64(int64(i)))
						destinations[i] = key.DestinationKeyspaceID{ksid}
						shard := shards[int(ksid)*shardCount/256].Name
						expected[shard] = append(expected[shard], ids[i])
					}
					if !tc.groupIDs {
						ids = nil
					}
					var resolved []*ResolvedShard
					var values [][]*querypb.Value
					var err error
					b.ReportAllocs()
					for b.Loop() {
						resolved, values, err = resolver.ResolveDestinations(ctx, "ks", topodatapb.TabletType_PRIMARY, ids, destinations)
						if err != nil {
							b.Fatal(err)
						}
					}
					require.Len(b, resolved, len(expected))
					if tc.groupIDs {
						require.Len(b, values, len(expected))
						for i, shard := range resolved {
							require.Equal(b, expected[shard.Target.Shard], values[i])
						}
					} else {
						require.Nil(b, values)
						require.Contains(b, expected, resolved[0].Target.Shard)
					}
				})
			}
		})
	}
}
