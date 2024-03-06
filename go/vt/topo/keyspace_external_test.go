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

package topo_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestServerFindAllShardsInKeyspace(t *testing.T) {
	tests := []struct {
		name   string
		shards int
		opt    *topo.FindAllShardsInKeyspaceOptions
	}{
		{
			name:   "negative concurrency",
			shards: 1,
			// Ensure this doesn't panic.
			opt: &topo.FindAllShardsInKeyspaceOptions{Concurrency: -1},
		},
		{
			name:   "unsharded",
			shards: 1,
			// Make sure the defaults apply as expected.
			opt: nil,
		},
		{
			name:   "sharded",
			shards: 32,
			opt:    &topo.FindAllShardsInKeyspaceOptions{Concurrency: 8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ts := memorytopo.NewServer(ctx)
			defer ts.Close()

			// Create an ephemeral keyspace and generate shard records within
			// the keyspace to fetch later.
			const keyspace = "keyspace"
			require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))

			shards, err := key.GenerateShardRanges(tt.shards)
			require.NoError(t, err)

			for _, s := range shards {
				require.NoError(t, ts.CreateShard(ctx, keyspace, s))
			}

			// Verify that we return a complete list of shards and that each
			// key range is present in the output.
			out, err := ts.FindAllShardsInKeyspace(ctx, keyspace, tt.opt)
			require.NoError(t, err)
			require.Len(t, out, tt.shards)

			for _, s := range shards {
				if _, ok := out[s]; !ok {
					t.Errorf("shard %q was not found", s)
				}
			}
		})
	}
}

func TestServerGetServingShards(t *testing.T) {
	keyspace := "ks1"
	errNoListImpl := topo.NewError(topo.NoImplementation, "don't be doing no listing round here")

	// This is needed because memorytopo doesn't implement locks using
	// keys in the topo. So we simulate the behavior of other topo server
	// implementations and how they implement TopoServer.LockShard().
	createSimulatedShardLock := func(ctx context.Context, ts *topo.Server, keyspace, shard string) error {
		conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
		if err != nil {
			return err
		}
		lockKey := fmt.Sprintf("keyspaces/%s/shards/%s/locks/1234", keyspace, shard)
		_, err = conn.Create(ctx, lockKey, []byte("lock"))
		return err
	}

	tests := []struct {
		shards   int    // Number of shards to create
		err      string // Error message we expect, if any
		fallback bool   // Should we fallback to the shard by shard method
	}{
		{
			shards: 0,
			err:    fmt.Sprintf("%s has no serving shards", keyspace),
		},
		{
			shards: 2,
		},
		{
			shards: 128,
		},
		{
			shards:   512,
			fallback: true,
		},
		{
			shards: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d shards with fallback = %t", tt.shards, tt.fallback), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ts, factory := memorytopo.NewServerAndFactory(ctx)
			defer ts.Close()
			stats := factory.GetCallStats()
			require.NotNil(t, stats)

			if tt.fallback {
				factory.SetListError(errNoListImpl)
			}

			err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{})
			require.NoError(t, err)
			var shardNames []string
			if tt.shards > 0 {
				shardNames, err = key.GenerateShardRanges(tt.shards)
				require.NoError(t, err)
				require.Equal(t, tt.shards, len(shardNames))
				for _, shardName := range shardNames {
					err = ts.CreateShard(ctx, keyspace, shardName)
					require.NoError(t, err)
				}
				// A shard lock typically becomes a key in the topo like this:
				// /vitess/global/keyspaces/<keyspace>/shards/<shardname>/locks/XXXX
				// We want to confirm that this key is ignored when building
				// the results.
				err = createSimulatedShardLock(ctx, ts, keyspace, shardNames[0])
				require.NoError(t, err)
			}

			// Verify that we return a complete list of shards and that each
			// key range is present in the output.
			stats.ResetAll() // We only want the stats for GetServingShards
			shardInfos, err := ts.GetServingShards(ctx, keyspace)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Len(t, shardInfos, tt.shards)
			for _, shardName := range shardNames {
				f := func(si *topo.ShardInfo) bool {
					return key.KeyRangeString(si.Shard.KeyRange) == shardName
				}
				require.True(t, slices.ContainsFunc(shardInfos, f), "shard %q was not found in the results",
					shardName)
			}

			// Now we check the stats based on the number of shards and whether or not
			// we should have had a List error and fell back to the shard by shard method.
			callcounts := stats.Counts()
			require.NotNil(t, callcounts)
			require.Equal(t, int64(1), callcounts["List"]) // We should always try
			switch {
			case tt.fallback: // We get the shards one by one from the list
				require.Equal(t, int64(1), callcounts["ListDir"])     // GetShardNames
				require.Equal(t, int64(tt.shards), callcounts["Get"]) // GetShard
			case tt.shards < 1: // We use a Get to check that the keyspace exists
				require.Equal(t, int64(0), callcounts["ListDir"])
				require.Equal(t, int64(1), callcounts["Get"])
			default: // We should not make any ListDir or Get calls
				require.Equal(t, int64(0), callcounts["ListDir"])
				require.Equal(t, int64(0), callcounts["Get"])
			}
		})
	}
}
