/*
Copyright 20201 The Vitess Authors.

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

package reparentutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

type chooseNewPrimaryTestTMClient struct {
	tmclient.TabletManagerClient
	replicationStatuses map[string]*replicationdatapb.Status
}

func (fake *chooseNewPrimaryTestTMClient) ReplicationStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	if fake.replicationStatuses == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if status, ok := fake.replicationStatuses[key]; ok {
		return status, nil
	}

	return nil, assert.AnError
}

func TestChooseNewPrimary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	tests := []struct {
		name              string
		tmc               *chooseNewPrimaryTestTMClient
		shardInfo         *topo.ShardInfo
		tabletMap         map[string]*topo.TabletInfo
		avoidPrimaryAlias *topodatapb.TabletAlias
		expected          *topodatapb.TabletAlias
		shouldErr         bool
	}{
		{
			name: "found a replica",
			tmc: &chooseNewPrimaryTestTMClient{
				// zone1-101 is behind zone1-102
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
					},
					"zone1-0000000102": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
				MasterAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, nil),
			tabletMap: map[string]*topo.TabletInfo{
				"primary": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"replica1": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"replica2": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			avoidPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  0,
			},
			expected: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			},
			shouldErr: false,
		},
		{
			name: "no active primary in shard",
			tmc: &chooseNewPrimaryTestTMClient{
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{}, nil),
			tabletMap: map[string]*topo.TabletInfo{
				"primary": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"replica1": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			avoidPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  0,
			},
			expected: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			shouldErr: false,
		},
		{
			name: "primary alias is nil",
			tmc: &chooseNewPrimaryTestTMClient{
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
				MasterAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, nil),
			tabletMap: map[string]*topo.TabletInfo{
				"primary": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"replica1": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			avoidPrimaryAlias: nil,
			expected:          nil,
			shouldErr:         true,
		},
		{
			name: "no replicas in primary cell",
			tmc: &chooseNewPrimaryTestTMClient{
				// zone1-101 is behind zone1-102
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
					},
					"zone1-0000000102": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
				MasterAlias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			}, nil),
			tabletMap: map[string]*topo.TabletInfo{
				"primary": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"replica1": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"replica2": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			avoidPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  0,
			},
			expected:  nil,
			shouldErr: false,
		},
		{
			name: "only available tablet is AvoidPrimary",
			tmc: &chooseNewPrimaryTestTMClient{
				// zone1-101 is behind zone1-102
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
					},
					"zone1-0000000102": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
				MasterAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, nil),
			tabletMap: map[string]*topo.TabletInfo{
				"avoid-primary": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			avoidPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			expected:  nil,
			shouldErr: false,
		},
		{
			name: "no replicas in shard",
			tmc:  &chooseNewPrimaryTestTMClient{},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
				MasterAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, nil),
			tabletMap: map[string]*topo.TabletInfo{
				"primary": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
			},
			avoidPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  0,
			},
			expected:  nil,
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, err := ChooseNewPrimary(ctx, tt.tmc, tt.shardInfo, tt.tabletMap, tt.avoidPrimaryAlias, time.Millisecond*50, logger)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestFindCurrentPrimary(t *testing.T) {
	t.Parallel()

	// The exact values of the tablet aliases don't matter to this function, but
	// we need them to be non-nil, so we'll just make one and reuse it.
	alias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  100,
	}
	logger := logutil.NewMemoryLogger()
	tests := []struct {
		name     string
		in       map[string]*topo.TabletInfo
		expected *topo.TabletInfo
	}{
		{
			name: "single current primary",
			in: map[string]*topo.TabletInfo{
				"primary": {
					Tablet: &topodatapb.Tablet{
						Alias: alias,
						Type:  topodatapb.TabletType_MASTER,
						MasterTermStartTime: &vttime.Time{
							Seconds: 100,
						},
						Hostname: "primary-tablet",
					},
				},
				"replica": {
					Tablet: &topodatapb.Tablet{
						Alias:    alias,
						Type:     topodatapb.TabletType_REPLICA,
						Hostname: "replica-tablet",
					},
				},
				"rdonly": {
					Tablet: &topodatapb.Tablet{
						Alias:    alias,
						Type:     topodatapb.TabletType_RDONLY,
						Hostname: "rdonly-tablet",
					},
				},
			},
			expected: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: alias,
					Type:  topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Hostname: "primary-tablet",
				},
			},
		},
		{
			name: "no primaries",
			in: map[string]*topo.TabletInfo{
				"replica1": {
					Tablet: &topodatapb.Tablet{
						Alias:    alias,
						Type:     topodatapb.TabletType_REPLICA,
						Hostname: "replica-tablet-1",
					},
				},
				"replica2": {
					Tablet: &topodatapb.Tablet{
						Alias:    alias,
						Type:     topodatapb.TabletType_REPLICA,
						Hostname: "replica-tablet-2",
					},
				},
				"rdonly": {
					Tablet: &topodatapb.Tablet{
						Alias:    alias,
						Type:     topodatapb.TabletType_RDONLY,
						Hostname: "rdonly-tablet",
					},
				},
			},
			expected: nil,
		},
		{
			name: "multiple primaries with one true primary",
			in: map[string]*topo.TabletInfo{
				"stale-primary": {
					Tablet: &topodatapb.Tablet{
						Alias: alias,
						Type:  topodatapb.TabletType_MASTER,
						MasterTermStartTime: &vttime.Time{
							Seconds: 100,
						},
						Hostname: "stale-primary-tablet",
					},
				},
				"true-primary": {
					Tablet: &topodatapb.Tablet{
						Alias: alias,
						Type:  topodatapb.TabletType_MASTER,
						MasterTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						Hostname: "true-primary-tablet",
					},
				},
				"rdonly": {
					Tablet: &topodatapb.Tablet{
						Alias:    alias,
						Type:     topodatapb.TabletType_RDONLY,
						Hostname: "rdonly-tablet",
					},
				},
			},
			expected: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: alias,
					Type:  topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
					Hostname: "true-primary-tablet",
				},
			},
		},
		{
			name: "multiple primaries with same term start",
			in: map[string]*topo.TabletInfo{
				"primary1": {
					Tablet: &topodatapb.Tablet{
						Alias: alias,
						Type:  topodatapb.TabletType_MASTER,
						MasterTermStartTime: &vttime.Time{
							Seconds: 100,
						},
						Hostname: "primary-tablet-1",
					},
				},
				"primary2": {
					Tablet: &topodatapb.Tablet{
						Alias: alias,
						Type:  topodatapb.TabletType_MASTER,
						MasterTermStartTime: &vttime.Time{
							Seconds: 100,
						},
						Hostname: "primary-tablet-2",
					},
				},
				"rdonly": {
					Tablet: &topodatapb.Tablet{
						Alias:    alias,
						Type:     topodatapb.TabletType_RDONLY,
						Hostname: "rdonly-tablet",
					},
				},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := FindCurrentPrimary(tt.in, logger)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
