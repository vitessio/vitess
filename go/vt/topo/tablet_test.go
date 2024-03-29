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
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// Test various cases of calls to GetTabletsByCell.
// GetTabletsByCell first tries to get all the tablets using List.
// If the response is too large, we will get an error, and fall back to one tablet at a time.
func TestServerGetTabletsByCell(t *testing.T) {
	tests := []struct {
		name      string
		tablets   int
		opt       *topo.GetTabletsByCellOptions
		listError error
	}{
		{
			name:    "negative concurrency",
			tablets: 1,
			// Ensure this doesn't panic.
			opt: &topo.GetTabletsByCellOptions{Concurrency: -1},
		},
		{
			name:    "single",
			tablets: 1,
			// Make sure the defaults apply as expected.
			opt: nil,
		},
		{
			name: "multiple",
			// should work with more than 1 tablet
			tablets: 32,
			opt:     &topo.GetTabletsByCellOptions{Concurrency: 8},
		},
		{
			name: "multiple with list error",
			// should work with more than 1 tablet when List returns an error
			tablets:   32,
			opt:       &topo.GetTabletsByCellOptions{Concurrency: 8},
			listError: topo.NewError(topo.ResourceExhausted, ""),
		},
	}

	const cell = "zone1"
	const keyspace = "keyspace"
	const shard = "shard"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			if tt.listError != nil {
				factory.SetListError(tt.listError)
			}

			// Create an ephemeral keyspace and generate shard records within
			// the keyspace to fetch later.
			require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
			require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

			tablets := make([]*topo.TabletInfo, tt.tablets)

			for i := 0; i < tt.tablets; i++ {
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

			// Verify that we return a complete list of tablets and that each
			// tablet matches what we expect.
			out, err := ts.GetTabletsByCell(ctx, cell, tt.opt)
			require.NoError(t, err)
			require.Len(t, out, tt.tablets)

			for i, tab := range tablets {
				require.Equal(t, tab.Tablet, tablets[i].Tablet)
			}
		})
	}
}
