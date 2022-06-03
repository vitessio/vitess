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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/topoproto"
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
				PrimaryAlias: &topodatapb.TabletAlias{
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
						Type: topodatapb.TabletType_PRIMARY,
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
			name: "found a replica - more advanced relay log position",
			tmc: &chooseNewPrimaryTestTMClient{
				// zone1-101 is behind zone1-102
				// since the relay log position for zone1-102 is more advanced
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2",
					},
					"zone1-0000000102": {
						Position:         "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
				PrimaryAlias: &topodatapb.TabletAlias{
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
						Type: topodatapb.TabletType_PRIMARY,
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
						Type: topodatapb.TabletType_PRIMARY,
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
			name: "avoid primary alias is nil",
			tmc: &chooseNewPrimaryTestTMClient{
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
				PrimaryAlias: &topodatapb.TabletAlias{
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
						Type: topodatapb.TabletType_PRIMARY,
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
			expected: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			shouldErr: false,
		}, {
			name: "avoid primary alias and shard primary are nil",
			tmc: &chooseNewPrimaryTestTMClient{
				replicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000100": {
						Position: "",
					},
					"zone1-0000000101": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
					},
				},
			},
			shardInfo: topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{}, nil),
			tabletMap: map[string]*topo.TabletInfo{
				"replica1": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"replica2": {
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
			expected: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			shouldErr: false,
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
				PrimaryAlias: &topodatapb.TabletAlias{
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
						Type: topodatapb.TabletType_PRIMARY,
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
				PrimaryAlias: &topodatapb.TabletAlias{
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
				PrimaryAlias: &topodatapb.TabletAlias{
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
						Type: topodatapb.TabletType_PRIMARY,
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

	durability, err := GetDurabilityPolicy("none")
	require.NoError(t, err)
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, err := ChooseNewPrimary(ctx, tt.tmc, tt.shardInfo, tt.tabletMap, tt.avoidPrimaryAlias, time.Millisecond*50, durability, logger)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, actual)
		})
	}
}

func TestFindPositionForTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	tests := []struct {
		name             string
		tmc              *testutil.TabletManagerClient
		tablet           *topodatapb.Tablet
		expectedPosition string
		expectedErr      string
	}{
		{
			name: "executed gtid set",
			tmc: &testutil.TabletManagerClient{
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000100": {
						Position: &replicationdatapb.Status{
							Position: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
						},
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedPosition: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		}, {
			name: "no replication status",
			tmc: &testutil.TabletManagerClient{
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000100": {
						Error: vterrors.ToGRPC(vterrors.Wrap(mysql.ErrNotReplica, "before status failed")),
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedPosition: "",
		}, {
			name: "relay log",
			tmc: &testutil.TabletManagerClient{
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000100": {
						Position: &replicationdatapb.Status{
							Position:         "unused",
							RelayLogPosition: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
						},
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedPosition: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		}, {
			name: "error in parsing position",
			tmc: &testutil.TabletManagerClient{
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000100": {
						Position: &replicationdatapb.Status{
							Position: "unused",
						},
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedErr: `parse error: unknown GTIDSet flavor ""`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pos, err := findPositionForTablet(ctx, test.tablet, logger, test.tmc, 10*time.Second)
			if test.expectedErr != "" {
				require.EqualError(t, err, test.expectedErr)
				return
			}
			require.NoError(t, err)
			posString := mysql.EncodePosition(pos)
			require.Equal(t, test.expectedPosition, posString)
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
						Type:  topodatapb.TabletType_PRIMARY,
						PrimaryTermStartTime: &vttime.Time{
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
					Type:  topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
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
						Type:  topodatapb.TabletType_PRIMARY,
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 100,
						},
						Hostname: "stale-primary-tablet",
					},
				},
				"true-primary": {
					Tablet: &topodatapb.Tablet{
						Alias: alias,
						Type:  topodatapb.TabletType_PRIMARY,
						PrimaryTermStartTime: &vttime.Time{
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
					Type:  topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
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
						Type:  topodatapb.TabletType_PRIMARY,
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 100,
						},
						Hostname: "primary-tablet-1",
					},
				},
				"primary2": {
					Tablet: &topodatapb.Tablet{
						Alias: alias,
						Type:  topodatapb.TabletType_PRIMARY,
						PrimaryTermStartTime: &vttime.Time{
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

func TestGetValidCandidatesAndPositionsAsList(t *testing.T) {
	sid1 := mysql.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	mysqlGTID1 := mysql.Mysql56GTID{
		Server:   sid1,
		Sequence: 9,
	}
	mysqlGTID2 := mysql.Mysql56GTID{
		Server:   sid1,
		Sequence: 10,
	}
	mysqlGTID3 := mysql.Mysql56GTID{
		Server:   sid1,
		Sequence: 11,
	}

	positionMostAdvanced := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}
	positionMostAdvanced.GTIDSet = positionMostAdvanced.GTIDSet.AddGTID(mysqlGTID1)
	positionMostAdvanced.GTIDSet = positionMostAdvanced.GTIDSet.AddGTID(mysqlGTID2)
	positionMostAdvanced.GTIDSet = positionMostAdvanced.GTIDSet.AddGTID(mysqlGTID3)

	positionIntermediate1 := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}
	positionIntermediate1.GTIDSet = positionIntermediate1.GTIDSet.AddGTID(mysqlGTID1)

	positionIntermediate2 := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}
	positionIntermediate2.GTIDSet = positionIntermediate2.GTIDSet.AddGTID(mysqlGTID1)
	positionIntermediate2.GTIDSet = positionIntermediate2.GTIDSet.AddGTID(mysqlGTID2)

	tests := []struct {
		name            string
		validCandidates map[string]mysql.Position
		tabletMap       map[string]*topo.TabletInfo
		tabletRes       []*topodatapb.Tablet
	}{
		{
			name: "test conversion",
			validCandidates: map[string]mysql.Position{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionIntermediate1,
				"zone1-0000000102": positionIntermediate2,
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Hostname: "primary-elect",
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Hostname: "requires force start",
					},
				},
				"zone1-0000000404": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  404,
						},
						Hostname: "ignored tablet",
					},
				},
			},
			tabletRes: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Hostname: "primary-elect",
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Hostname: "requires force start",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tabletRes, posRes, err := getValidCandidatesAndPositionsAsList(test.validCandidates, test.tabletMap)
			assert.NoError(t, err)
			assert.ElementsMatch(t, test.tabletRes, tabletRes)
			assert.Equal(t, len(tabletRes), len(posRes))
			for i, tablet := range tabletRes {
				assert.Equal(t, test.validCandidates[topoproto.TabletAliasString(tablet.Alias)], posRes[i])
			}
		})
	}
}

func TestWaitForCatchUp(t *testing.T) {
	tests := []struct {
		name       string
		tmc        tmclient.TabletManagerClient
		source     *topodatapb.Tablet
		newPrimary *topodatapb.Tablet
		err        string
	}{
		{
			name: "success",
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "abc",
						Error:    nil,
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000101": {
						"abc": nil,
					},
				},
			},
			source: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
		}, {
			name: "error in primary position",
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "abc",
						Error:    fmt.Errorf("found error in primary position"),
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000101": {
						"abc": nil,
					},
				},
			},
			source: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
			err: "found error in primary position",
		}, {
			name: "error in waiting for position",
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "abc",
						Error:    nil,
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000101": {
						"abc": fmt.Errorf("found error in waiting for position"),
					},
				},
			},
			source: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
			err: "found error in waiting for position",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			logger := logutil.NewMemoryLogger()
			err := waitForCatchUp(ctx, test.tmc, logger, test.newPrimary, test.source, 2*time.Second)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRestrictValidCandidates(t *testing.T) {
	tests := []struct {
		name            string
		validCandidates map[string]mysql.Position
		tabletMap       map[string]*topo.TabletInfo
		result          map[string]mysql.Position
	}{
		{
			name: "remove invalid tablets",
			validCandidates: map[string]mysql.Position{
				"zone1-0000000100": {},
				"zone1-0000000101": {},
				"zone1-0000000102": {},
				"zone1-0000000103": {},
				"zone1-0000000104": {},
				"zone1-0000000105": {},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_PRIMARY,
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_RESTORE,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_DRAINED,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_SPARE,
					},
				},
				"zone1-0000000105": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_BACKUP,
					},
				},
			},
			result: map[string]mysql.Position{
				"zone1-0000000100": {},
				"zone1-0000000101": {},
				"zone1-0000000104": {},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := restrictValidCandidates(test.validCandidates, test.tabletMap)
			assert.NoError(t, err)
			assert.Equal(t, res, test.result)
		})
	}
}

func Test_findCandidate(t *testing.T) {
	tests := []struct {
		name               string
		intermediateSource *topodatapb.Tablet
		possibleCandidates []*topodatapb.Tablet
		candidate          *topodatapb.Tablet
	}{
		{
			name:      "empty possible candidates list",
			candidate: nil,
		}, {
			name: "intermediate source in possible candidates list",
			intermediateSource: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  103,
				},
			},
			possibleCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  103,
					},
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
				},
			},
			candidate: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  103,
				},
			},
		}, {
			name: "intermediate source not in possible candidates list",
			intermediateSource: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  103,
				},
			},
			possibleCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  104,
					},
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
				},
			},
			candidate: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := findCandidate(tt.intermediateSource, tt.possibleCandidates)
			if tt.candidate == nil {
				require.Nil(t, res)
			} else {
				require.NotNil(t, res)
				require.Equal(t, topoproto.TabletAliasString(tt.candidate.Alias), topoproto.TabletAliasString(res.Alias))
			}
		})
	}
}

func Test_getTabletsWithPromotionRules(t *testing.T) {
	var (
		primaryTablet = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone-1",
				Uid:  1,
			},
			Type: topodatapb.TabletType_PRIMARY,
		}
		replicaTablet = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone-1",
				Uid:  2,
			},
			Type: topodatapb.TabletType_REPLICA,
		}
		rdonlyTablet = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone-1",
				Uid:  3,
			},
			Type: topodatapb.TabletType_RDONLY,
		}
		replicaCrossCellTablet = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone-2",
				Uid:  2,
			},
			Type: topodatapb.TabletType_REPLICA,
		}
		rdonlyCrossCellTablet = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone-2",
				Uid:  3,
			},
			Type: topodatapb.TabletType_RDONLY,
		}
	)
	allTablets := []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, replicaCrossCellTablet, rdonlyCrossCellTablet}
	tests := []struct {
		name            string
		tablets         []*topodatapb.Tablet
		rule            promotionrule.CandidatePromotionRule
		filteredTablets []*topodatapb.Tablet
	}{
		{
			name:            "filter candidates with Neutral promotion rule",
			tablets:         allTablets,
			rule:            promotionrule.Neutral,
			filteredTablets: []*topodatapb.Tablet{primaryTablet, replicaTablet, replicaCrossCellTablet},
		},
		{
			name:            "filter candidates with MustNot promotion rule",
			tablets:         allTablets,
			rule:            promotionrule.MustNot,
			filteredTablets: []*topodatapb.Tablet{rdonlyTablet, rdonlyCrossCellTablet},
		},
		{
			name:            "filter candidates with Must promotion rule",
			tablets:         allTablets,
			rule:            promotionrule.Must,
			filteredTablets: nil,
		},
	}
	durability, _ := GetDurabilityPolicy("none")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := getTabletsWithPromotionRules(durability, tt.tablets, tt.rule)
			require.EqualValues(t, tt.filteredTablets, res)
		})
	}
}
