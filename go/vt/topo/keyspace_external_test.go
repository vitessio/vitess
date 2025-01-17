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
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestServerFindAllShardsInKeyspace(t *testing.T) {
	const defaultKeyspace = "keyspace"
	tests := []struct {
		name     string
		shards   int
		keyspace string // If you want to override the default
		opt      *topo.FindAllShardsInKeyspaceOptions
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
		{
			name:     "SQL escaped keyspace",
			shards:   32,
			keyspace: "`my-keyspace`",
			opt:      &topo.FindAllShardsInKeyspaceOptions{Concurrency: 8},
		},
	}

	for _, tt := range tests {
		keyspace := defaultKeyspace
		if tt.keyspace != "" {
			// Most calls such as CreateKeyspace will not accept invalid characters
			// in the value so we'll only use the original test case value in
			// FindAllShardsInKeyspace. This allows us to test and confirm that
			// FindAllShardsInKeyspace can handle SQL escaped or backtick'd names.
			keyspace, _ = sqlescape.UnescapeID(tt.keyspace)
		} else {
			tt.keyspace = defaultKeyspace
		}
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ts := memorytopo.NewServer(ctx)
			defer ts.Close()

			// Create an ephemeral keyspace and generate shard records within
			// the keyspace to fetch later.
			require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))

			shards, err := key.GenerateShardRanges(tt.shards)
			require.NoError(t, err)

			for _, s := range shards {
				require.NoError(t, ts.CreateShard(ctx, keyspace, s))
			}

			// Verify that we return a complete list of shards and that each
			// key range is present in the output.
			out, err := ts.FindAllShardsInKeyspace(ctx, tt.keyspace, tt.opt)
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
				factory.AddOperationError(memorytopo.List, ".*", errNoListImpl)
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

// TestWatchAllKeyspaceRecords tests the WatchAllKeyspaceRecords method.
// We test out different updates and see if we receive the correct update
// from the watch.
func TestWatchAllKeyspaceRecords(t *testing.T) {
	ksDef := &topodatapb.Keyspace{
		KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
		DurabilityPolicy: policy.DurabilitySemiSync,
		SidecarDbName:    "_vt",
	}
	tests := []struct {
		name            string
		setupFunc       func(t *testing.T, ts *topo.Server)
		updateFunc      func(t *testing.T, ts *topo.Server)
		wantInitRecords []*topo.WatchKeyspacePrefixData
		wantChanRecords []*topo.WatchKeyspacePrefixData
	}{
		{
			name: "Update Durability Policy in 1 Keyspace",
			setupFunc: func(t *testing.T, ts *topo.Server) {
				err := ts.CreateKeyspace(context.Background(), "ks", ksDef)
				require.NoError(t, err)
			},
			updateFunc: func(t *testing.T, ts *topo.Server) {
				ki, err := ts.GetKeyspace(context.Background(), "ks")
				require.NoError(t, err)
				ki.DurabilityPolicy = policy.DurabilityCrossCell
				ctx, unlock, err := ts.LockKeyspace(context.Background(), "ks", "test")
				require.NoError(t, err)
				defer unlock(&err)
				err = ts.UpdateKeyspace(ctx, ki)
				require.NoError(t, err)
			},
			wantInitRecords: []*topo.WatchKeyspacePrefixData{
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks", ksDef),
				},
			},
			wantChanRecords: []*topo.WatchKeyspacePrefixData{
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks", &topodatapb.Keyspace{
						KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
						DurabilityPolicy: policy.DurabilityCrossCell,
						SidecarDbName:    "_vt",
					}),
				},
			},
		},
		{
			name: "New Keyspace Created",
			setupFunc: func(t *testing.T, ts *topo.Server) {
			},
			updateFunc: func(t *testing.T, ts *topo.Server) {
				err := ts.CreateKeyspace(context.Background(), "ks", ksDef)
				require.NoError(t, err)
			},
			wantInitRecords: []*topo.WatchKeyspacePrefixData{},
			wantChanRecords: []*topo.WatchKeyspacePrefixData{
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks", ksDef),
				},
			},
		},
		{
			name: "Update KeyspaceType in 1 Keyspace",
			setupFunc: func(t *testing.T, ts *topo.Server) {
				err := ts.CreateKeyspace(context.Background(), "ks", ksDef)
				require.NoError(t, err)
			},
			updateFunc: func(t *testing.T, ts *topo.Server) {
				ki, err := ts.GetKeyspace(context.Background(), "ks")
				require.NoError(t, err)
				ki.KeyspaceType = topodatapb.KeyspaceType_SNAPSHOT
				ctx, unlock, err := ts.LockKeyspace(context.Background(), "ks", "test")
				require.NoError(t, err)
				defer unlock(&err)
				err = ts.UpdateKeyspace(ctx, ki)
				require.NoError(t, err)
			},
			wantInitRecords: []*topo.WatchKeyspacePrefixData{
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks", ksDef),
				},
			},
			wantChanRecords: []*topo.WatchKeyspacePrefixData{
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks", &topodatapb.Keyspace{
						KeyspaceType:     topodatapb.KeyspaceType_SNAPSHOT,
						DurabilityPolicy: policy.DurabilitySemiSync,
						SidecarDbName:    "_vt",
					}),
				},
			},
		},
		{
			name: "Multiple updates in multiple keyspaces",
			setupFunc: func(t *testing.T, ts *topo.Server) {
				err := ts.CreateKeyspace(context.Background(), "ks", ksDef)
				require.NoError(t, err)
				err = ts.CreateKeyspace(context.Background(), "ks2", ksDef)
				require.NoError(t, err)
				err = ts.CreateShard(context.Background(), "ks2", "-")
				require.NoError(t, err)
			},
			updateFunc: func(t *testing.T, ts *topo.Server) {
				func() {
					ki, err := ts.GetKeyspace(context.Background(), "ks")
					require.NoError(t, err)
					ki.KeyspaceType = topodatapb.KeyspaceType_SNAPSHOT
					ctx, unlock, err := ts.LockKeyspace(context.Background(), "ks", "test")
					require.NoError(t, err)
					defer unlock(&err)
					err = ts.UpdateKeyspace(ctx, ki)
					require.NoError(t, err)
				}()
				func() {
					ki, err := ts.GetKeyspace(context.Background(), "ks2")
					require.NoError(t, err)
					ki.DurabilityPolicy = policy.DurabilityCrossCell
					ctx, unlock, err := ts.LockKeyspace(context.Background(), "ks2", "test")
					require.NoError(t, err)
					defer unlock(&err)
					err = ts.UpdateKeyspace(ctx, ki)
					require.NoError(t, err)
				}()
				func() {
					_, err := ts.UpdateShardFields(context.Background(), "ks2", "-", func(info *topo.ShardInfo) error {
						info.IsPrimaryServing = true
						return nil
					})
					require.NoError(t, err)
				}()
			},
			wantInitRecords: []*topo.WatchKeyspacePrefixData{
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks", ksDef),
				},
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks2", ksDef),
				},
			},
			wantChanRecords: []*topo.WatchKeyspacePrefixData{
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks", &topodatapb.Keyspace{
						KeyspaceType:     topodatapb.KeyspaceType_SNAPSHOT,
						DurabilityPolicy: policy.DurabilitySemiSync,
						SidecarDbName:    "_vt",
					}),
				},
				{
					KeyspaceInfo: topo.NewKeyspaceInfo("ks2", &topodatapb.Keyspace{
						KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
						DurabilityPolicy: policy.DurabilityCrossCell,
						SidecarDbName:    "_vt",
					}),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a memory topo server for the test.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ts := memorytopo.NewServer(ctx)
			defer ts.Close()

			// Do the initial setup before starting the watch.
			tt.setupFunc(t, ts)

			// Start the watch and verify the initial records received.
			watchCtx, watchCancel := context.WithCancel(ctx)
			defer watchCancel()
			initRecords, ch, err := ts.WatchAllKeyspaceRecords(watchCtx)
			require.NoError(t, err)
			elementsMatchFunc(t, tt.wantInitRecords, initRecords, watchKeyspacePrefixDataMatches)

			// We start a go routine to collect all the records from the channel.
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				idx := 0
				for data := range ch {
					if topo.IsErrType(data.Err, topo.Interrupted) {
						continue
					}
					require.True(t, watchKeyspacePrefixDataMatches(tt.wantChanRecords[idx], data))
					idx++
					// Stop the watch after we have verified we have received all required updates.
					if idx == len(tt.wantChanRecords) {
						watchCancel()
					}
				}
			}()

			// Update the records and verify the records received on the channel.
			tt.updateFunc(t, ts)
			if len(tt.wantChanRecords) == 0 {
				// If there are no records to verify, we can stop the watch.
				watchCancel()
			}
			// Wait for the go routine to finish.
			wg.Wait()
		})
	}
}

// elementsMatchFunc checks if two slices have the same elements (ignoring order),
// using a custom equality function to compare elements.
func elementsMatchFunc[T any](t *testing.T, expected, actual []T, equalFn func(a, b T) bool) {
	require.Len(t, actual, len(expected))

	visited := make([]bool, len(actual))
	for _, exp := range expected {
		found := false
		for i, act := range actual {
			if visited[i] {
				continue
			}
			if equalFn(exp, act) {
				visited[i] = true
				found = true
				break
			}
		}
		require.True(t, found, "Expected element %+v not found in actual slice", exp)
	}
}

// watchKeyspacePrefixDataMatches is a helper function to check equality of two topo.WatchKeyspacePrefixData.
func watchKeyspacePrefixDataMatches(a, b *topo.WatchKeyspacePrefixData) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !errors.Is(a.Err, b.Err) {
		return false
	}
	if a.KeyspaceInfo == nil || b.KeyspaceInfo == nil {
		if a.KeyspaceInfo != b.KeyspaceInfo {
			return false
		}
	} else {
		if !proto.Equal(a.KeyspaceInfo.Keyspace, b.KeyspaceInfo.Keyspace) {
			return false
		}
		if a.KeyspaceInfo.KeyspaceName() != b.KeyspaceInfo.KeyspaceName() {
			return false
		}
	}

	return true
}
