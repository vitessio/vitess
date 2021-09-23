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

	"k8s.io/apimachinery/pkg/util/sets"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"

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
			name: "primary alias is nil",
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
			utils.MustMatch(t, tt.expected, actual)
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

func TestCheckIfConstraintsSatisfied(t *testing.T) {
	testcases := []struct {
		name                    string
		newPrimary, prevPrimary *topodatapb.Tablet
		opts                    EmergencyReparentOptions
		err                     string
	}{
		{
			name: "no constraint failure",
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell1",
				},
				Type: topodatapb.TabletType_REPLICA,
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell1",
				},
			},
			opts: EmergencyReparentOptions{preventCrossCellPromotion: true},
			err:  "",
		}, {
			name: "promotion rule constraint failure",
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell1",
					Uid:  100,
				},
				Type: topodatapb.TabletType_RDONLY,
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell1",
				},
			},
			opts: EmergencyReparentOptions{preventCrossCellPromotion: true},
			err:  "elected primary does not satisfy promotion rule constraint - cell1-0000000100",
		}, {
			name: "cross cell constraint failure",
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell1",
					Uid:  100,
				},
				Type: topodatapb.TabletType_REPLICA,
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell2",
				},
			},
			opts: EmergencyReparentOptions{preventCrossCellPromotion: true},
			err:  "elected primary does not satisfy geographic constraint - cell1-0000000100",
		}, {
			name: "cross cell but no constraint failure",
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell1",
					Uid:  100,
				},
				Type: topodatapb.TabletType_REPLICA,
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell2",
				},
			},
			opts: EmergencyReparentOptions{preventCrossCellPromotion: false},
			err:  "",
		},
	}

	_ = SetDurabilityPolicy("none", nil)
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			err := checkIfConstraintsSatisfied(testcase.newPrimary, testcase.prevPrimary, testcase.opts)
			if testcase.err == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testcase.err)
			}
		})
	}
}

func TestReparentReplicas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		emergencyReparentOps  EmergencyReparentOptions
		tmc                   *testutil.TabletManagerClient
		unlockTopo            bool
		newPrimaryTabletAlias string
		ts                    *topo.Server
		keyspace              string
		shard                 string
		tablets               []*topodatapb.Tablet
		tabletMap             map[string]*topo.TabletInfo
		statusMap             map[string]*replicationdatapb.StopReplicationStatus
		shouldErr             bool
		errShouldContain      string
	}{
		{
			name:                 "success",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, sets.NewString("zone1-0000000404"), 0, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
					"zone1-0000000404": assert.AnError, // okay, because we're ignoring it.
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": { // forceStart = false
					Before: &replicationdatapb.Status{
						IoThreadRunning:  false,
						SqlThreadRunning: false,
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoThreadRunning:  true,
						SqlThreadRunning: true,
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
		{
			name:                 "MasterPosition error",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, nil, 0, false),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: fmt.Errorf("primary position error"),
					},
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
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
			},
			statusMap:        map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "primary position error",
		},
		{
			name:                 "cannot repopulate reparent journal on new primary",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, nil, 0, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
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
			},
			statusMap:        map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "failed to PopulateReparentJournal on primary",
		},
		{
			name:                 "all replicas failing to SetMaster does fail the promotion",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, nil, 0, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},

				SetMasterResults: map[string]error{
					// everyone fails, we all fail
					"zone1-0000000101": assert.AnError,
					"zone1-0000000102": assert.AnError,
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
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
				"zone1-00000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			statusMap:        map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: " replica(s) failed",
		},
		{
			name:                 "all replicas slow to SetMaster does fail the promotion",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, nil, time.Millisecond*10, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetMasterDelays: map[string]time.Duration{
					// nothing is failing, we're just slow
					"zone1-0000000101": time.Millisecond * 100,
					"zone1-0000000102": time.Millisecond * 75,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
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
					},
				},
			},
			statusMap:        map[string]*replicationdatapb.StopReplicationStatus{},
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			errShouldContain: "context deadline exceeded",
		},
		{
			name:                 "one replica failing to SetMaster does not fail the promotion",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, nil, 0, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000101": nil, // this one succeeds, so we're good
					"zone1-0000000102": assert.AnError,
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
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
					},
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{}

			testutil.AddShards(ctx, t, tt.ts, &vtctldatapb.Shard{
				Keyspace: tt.keyspace,
				Name:     tt.shard,
			})

			if !tt.unlockTopo {
				var (
					unlock func(*error)
					lerr   error
				)

				ctx, unlock, lerr = tt.ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, lerr, "could not lock %s/%s for test", tt.keyspace, tt.shard)

				defer func() {
					unlock(&lerr)
					require.NoError(t, lerr, "could not unlock %s/%s after test", tt.keyspace, tt.shard)
				}()
			}
			tabletInfo := tt.tabletMap[tt.newPrimaryTabletAlias]

			_, err := reparentReplicas(ctx, ev, logger, tt.tmc, tabletInfo.Tablet, "", tt.tabletMap, tt.statusMap, tt.emergencyReparentOps, false, true)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestPromoteIntermediatePrimary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		emergencyReparentOps  EmergencyReparentOptions
		tmc                   *testutil.TabletManagerClient
		unlockTopo            bool
		newPrimaryTabletAlias string
		ts                    *topo.Server
		keyspace              string
		shard                 string
		tablets               []*topodatapb.Tablet
		tabletMap             map[string]*topo.TabletInfo
		statusMap             map[string]*replicationdatapb.StopReplicationStatus
		shouldErr             bool
		errShouldContain      string
		result                []*topodatapb.Tablet
	}{
		{
			name:                 "success",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, sets.NewString("zone1-0000000404"), 0, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
					"zone1-0000000404": assert.AnError, // okay, because we're ignoring it.
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": { // forceStart = false
					Before: &replicationdatapb.Status{
						IoThreadRunning:  false,
						SqlThreadRunning: false,
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoThreadRunning:  true,
						SqlThreadRunning: true,
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
			result: []*topodatapb.Tablet{
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
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Hostname: "requires force start",
				},
			},
		},
		{
			name:                 "all replicas failed",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, nil, 0, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},

				SetMasterResults: map[string]error{
					// everyone fails, we all fail
					"zone1-0000000101": assert.AnError,
					"zone1-0000000102": assert.AnError,
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
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
				"zone1-00000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			statusMap:        map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: " replica(s) failed",
		},
		{
			name:                 "one replica failed",
			emergencyReparentOps: NewEmergencyReparentOptions(nil, nil, 0, false),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000101": nil, // this one succeeds, so we're good
					"zone1-0000000102": assert.AnError,
				},
			},
			newPrimaryTabletAlias: "zone1-0000000100",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
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
					},
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
			result: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{}

			testutil.AddShards(ctx, t, tt.ts, &vtctldatapb.Shard{
				Keyspace: tt.keyspace,
				Name:     tt.shard,
			})

			if !tt.unlockTopo {
				var (
					unlock func(*error)
					lerr   error
				)

				ctx, unlock, lerr = tt.ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, lerr, "could not lock %s/%s for test", tt.keyspace, tt.shard)

				defer func() {
					unlock(&lerr)
					require.NoError(t, lerr, "could not unlock %s/%s after test", tt.keyspace, tt.shard)
				}()
			}
			tabletInfo := tt.tabletMap[tt.newPrimaryTabletAlias]

			res, err := promoteIntermediatePrimary(ctx, tt.tmc, ev, logger, tabletInfo.Tablet, "", tt.tabletMap, tt.statusMap, tt.emergencyReparentOps)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.result, res)
		})
	}
}
