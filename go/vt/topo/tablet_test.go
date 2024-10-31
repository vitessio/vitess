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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// Test various cases of calls to GetTabletsByCell.
// GetTabletsByCell first tries to get all the tablets using List.
// If the response is too large, we will get an error, and fall back to one tablet at a time.
func TestServerGetTabletsByCell(t *testing.T) {
	const cell = "zone1"
	const keyspace = "keyspace"
	const shard = "shard"

	tests := []struct {
		name               string
		createShardTablets int
		opt                *topo.GetTabletsByCellOptions
		listError          error
		keyspaceShards     map[string]string
	}{
		{
			name:               "negative concurrency",
			keyspaceShards:     map[string]string{keyspace: shard},
			createShardTablets: 1,
			// Ensure this doesn't panic.
			opt: &topo.GetTabletsByCellOptions{Concurrency: -1},
		},
		{
			name:               "single",
			keyspaceShards:     map[string]string{keyspace: shard},
			createShardTablets: 1,
			// Make sure the defaults apply as expected.
			opt: nil,
		},
		{
			name:           "multiple",
			keyspaceShards: map[string]string{keyspace: shard},
			// should work with more than 1 tablet
			createShardTablets: 32,
			opt:                &topo.GetTabletsByCellOptions{Concurrency: 8},
		},
		{
			name:           "multiple with list error",
			keyspaceShards: map[string]string{keyspace: shard},
			// should work with more than 1 tablet when List returns an error
			createShardTablets: 32,
			opt:                &topo.GetTabletsByCellOptions{Concurrency: 8},
			listError:          topo.NewError(topo.ResourceExhausted, ""),
		},
		{
			name: "filtered by keyspace and shard",
			keyspaceShards: map[string]string{
				keyspace:   shard,
				"filtered": "-",
			},
			// should create 2 tablets in 2 different shards (4 total)
			// but only a single shard is returned
			createShardTablets: 2,
			opt: &topo.GetTabletsByCellOptions{
				Concurrency: 1,
				Keyspace:    keyspace,
				Shard:       shard,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			if tt.listError != nil {
				factory.AddOperationError(memorytopo.List, ".*", tt.listError)
			}

			// Create an ephemeral keyspace and generate shard records within
			// the keyspace to fetch later.
			for k, s := range tt.keyspaceShards {
				require.NoError(t, ts.CreateKeyspace(ctx, k, &topodatapb.Keyspace{}))
				require.NoError(t, ts.CreateShard(ctx, k, s))
			}

			tablets := make([]*topo.TabletInfo, tt.createShardTablets)

			var uid uint32 = 1
			for k, s := range tt.keyspaceShards {
				for i := 0; i < tt.createShardTablets; i++ {
					tablet := &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: cell,
							Uid:  uid,
						},
						Hostname: "host1",
						PortMap: map[string]int32{
							"vt": int32(uid),
						},
						Keyspace: k,
						Shard:    s,
					}
					tInfo := &topo.TabletInfo{Tablet: tablet}
					tablets[i] = tInfo
					require.NoError(t, ts.CreateTablet(ctx, tablet))
					uid++
				}
			}

			// Verify that we return a complete list of tablets and that each
			// tablet matches what we expect.
			out, err := ts.GetTabletsByCell(ctx, cell, tt.opt)
			require.NoError(t, err)
			require.Len(t, out, tt.createShardTablets)

			for i, tab := range tablets {
				require.Equal(t, tab.Tablet, tablets[i].Tablet)
			}

			for _, tablet := range out {
				require.Equal(t, keyspace, tablet.Keyspace)
				require.Equal(t, shard, tablet.Shard)
			}
		})
	}
}

func TestServerGetTabletsByCellPartialResults(t *testing.T) {
	const cell = "zone1"
	const keyspace = "keyspace"
	const shard = "shard"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
	defer ts.Close()

	// Create an ephemeral keyspace and generate shard records within
	// the keyspace to fetch later.
	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

	tablets := make([]*topo.TabletInfo, 3)

	for i := 0; i < len(tablets); i++ {
		tablet := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: cell,
				Uid:  uint32(i),
			},
			Hostname: "host1",
			PortMap: map[string]int32{
				"vt": int32(i),
			},
			Keyspace: keyspace,
			Shard:    shard,
		}
		tInfo := &topo.TabletInfo{Tablet: tablet}
		tablets[i] = tInfo
		require.NoError(t, ts.CreateTablet(ctx, tablet))
	}

	// Force fallback to getting tablets individually.
	factory.AddOperationError(memorytopo.List, ".*", topo.NewError(topo.NoImplementation, "List not supported"))

	// Cause the Get for the second tablet to fail.
	factory.AddOperationError(memorytopo.Get, "tablets/zone1-0000000001/Tablet", errors.New("fake error"))

	// Verify that we return a partial list of tablets and that each
	// tablet matches what we expect.
	out, err := ts.GetTabletsByCell(ctx, cell, nil)
	assert.Error(t, err)
	assert.True(t, topo.IsErrType(err, topo.PartialResult), "Not a partial result: %v", err)
	assert.Len(t, out, 2)
	assert.True(t, proto.Equal(tablets[0].Tablet, out[0].Tablet), "Got: %v, want %v", tablets[0].Tablet, out[0].Tablet)
	assert.True(t, proto.Equal(tablets[2].Tablet, out[1].Tablet), "Got: %v, want %v", tablets[2].Tablet, out[1].Tablet)
}
