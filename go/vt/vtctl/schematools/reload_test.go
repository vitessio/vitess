/*
Copyright 2021 The Vitess Authors.

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

package schematools

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type reloadSchemaTMC struct {
	tmclient.TabletManagerClient

	// tablet alias => positionStr => error
	results map[string]map[string]error

	m        sync.Mutex
	calls    []*reloadSchemaCall
	errCount int
}

type reloadSchemaCall struct {
	Alias    *topodatapb.TabletAlias
	Position string
}

func (tmc *reloadSchemaTMC) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, position string) (err error) {
	rpcErr := err
	defer func() {
		tmc.m.Lock()
		defer tmc.m.Unlock()

		tmc.calls = append(tmc.calls, &reloadSchemaCall{
			Alias:    tablet.Alias,
			Position: position,
		})

		if rpcErr != nil {
			tmc.errCount++
		}
	}()

	if tmc.results == nil {
		return fmt.Errorf("no results set on reloadSchemaTMC")
	}

	key := topoproto.TabletAliasString(tablet.Alias)
	posMap, ok := tmc.results[key]
	if !ok {
		return fmt.Errorf("no ReloadSchema fakes set for %s", key)
	}

	rpcErr, ok = posMap[position]
	if !ok {
		return fmt.Errorf("no ReloadSchema fake set for (%s, %s)", key, position)
	}

	return rpcErr
}

func TestReloadShard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name    string
		cells   []string
		tablets []*topodatapb.Tablet
		results map[string]map[string]error
		req     *struct {
			Keyspace       string
			Shard          string
			Position       string
			IncludePrimary bool
		}
		expected *struct {
			IsPartial bool
			Ok        bool
		}
		expectedCalls    []*reloadSchemaCall
		expectedErrCount int
	}{
		{
			name:  "ok",
			cells: []string{"zone1", "zone2"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			results: map[string]map[string]error{
				"zone1-0000000101": {
					"pos1": nil,
				},
				"zone2-0000000200": {
					"pos1": nil,
				},
			},
			req: &struct {
				Keyspace       string
				Shard          string
				Position       string
				IncludePrimary bool
			}{
				Keyspace: "ks",
				Shard:    "-",
				Position: "pos1",
			},
			expected: &struct {
				IsPartial bool
				Ok        bool
			}{
				IsPartial: false,
				Ok:        true,
			},
			expectedCalls: []*reloadSchemaCall{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Position: "pos1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Position: "pos1",
				},
			},
			expectedErrCount: 0,
		},
		{
			name:  "best effort",
			cells: []string{"zone1", "zone2"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			results: map[string]map[string]error{
				"zone1-0000000101": {
					"pos1": nil,
				},
				"zone2-0000000200": {
					"pos1": assert.AnError,
				},
			},
			req: &struct {
				Keyspace       string
				Shard          string
				Position       string
				IncludePrimary bool
			}{
				Keyspace: "ks",
				Shard:    "-",
				Position: "pos1",
			},
			expected: &struct {
				IsPartial bool
				Ok        bool
			}{
				IsPartial: false,
				Ok:        true,
			},
			expectedCalls: []*reloadSchemaCall{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Position: "pos1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Position: "pos1",
				},
			},
			expectedErrCount: 1,
		},
		{
			name:  "include_primary",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			results: map[string]map[string]error{
				"zone1-0000000100": {
					"":              nil,
					"some position": assert.AnError,
				},
			},
			req: &struct {
				Keyspace       string
				Shard          string
				Position       string
				IncludePrimary bool
			}{
				Keyspace:       "ks",
				Shard:          "-",
				Position:       "some position",
				IncludePrimary: true,
			},
			expected: &struct {
				IsPartial bool
				Ok        bool
			}{
				IsPartial: false,
				Ok:        true,
			},
			expectedCalls: []*reloadSchemaCall{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Position: "",
				},
			},
		},
		{
			name: "fail to load tabletmap",
			req: &struct {
				Keyspace       string
				Shard          string
				Position       string
				IncludePrimary bool
			}{
				Keyspace: "doesnotexist",
				Shard:    "-",
			},
			expected: &struct {
				IsPartial bool
				Ok        bool
			}{
				IsPartial: false,
				Ok:        false,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer(tt.cells...)
			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
			}, tt.tablets...)
			tmc := &reloadSchemaTMC{
				results: tt.results,
			}

			isPartial, ok := ReloadShard(ctx, ts, tmc, logutil.NewMemoryLogger(), tt.req.Keyspace, tt.req.Shard, tt.req.Position, semaphore.NewWeighted(1), tt.req.IncludePrimary)
			assert.Equal(t, tt.expected.IsPartial, isPartial, "incorrect value for isPartial")
			assert.Equal(t, tt.expected.Ok, ok, "incorrect value for ok")

			tmc.m.Lock()
			defer tmc.m.Unlock()

			assert.ElementsMatch(t, tt.expectedCalls, tmc.calls)
			assert.Equal(t, tt.expectedErrCount, tmc.errCount, "rpc error count incorrect")
		})
	}
}
