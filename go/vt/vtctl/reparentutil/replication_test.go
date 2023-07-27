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

package reparentutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}

func TestFindValidEmergencyReparentCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		statusMap        map[string]*replicationdatapb.StopReplicationStatus
		primaryStatusMap map[string]*replicationdatapb.PrimaryStatus
		// Note: for these tests, it's simpler to compare keys than actual
		// mysql.Postion structs, which are just thin wrappers around the
		// mysql.GTIDSet interface. If a tablet alias makes it into the map, we
		// know it was chosen by the method, and that either
		// mysql.DecodePosition was successful (in the primary case) or
		// status.FindErrantGTIDs was successful (in the replica case). If the
		// former is not true, then the function should return an error. If the
		// latter is not true, then the tablet alias will not be in the map. The
		// point is, the combination of (1) whether the test should error and
		// (2) the set of keys we expect in the map is enough to fully assert on
		// the correctness of the behavior of this functional unit.
		expected  []string
		shouldErr bool
	}{
		{
			name: "success",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
				"r2": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"p1": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
				},
			},
			expected:  []string{"r1", "r2", "p1"},
			shouldErr: false,
		}, {
			name: "success for single tablet",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,AAAAAAAA-71CA-11E1-9E33-C80AA9429562:1",
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			expected:         []string{"r1"},
			shouldErr:        false,
		},
		{
			name: "mixed replication modes",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
				"r2": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "FilePos/mysql-bin.0001:10",
					},
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "tablet without relay log position",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
				"r2": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "",
					},
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "non-GTID-based",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "FilePos/mysql-bin.0001:100",
					},
				},
				"r2": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "FilePos/mysql-bin.0001:10",
					},
				},
			},
			expected:  []string{"r1", "r2"},
			shouldErr: false,
		},
		{
			name: "tablet with errant GTIDs is excluded",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
				"errant": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,AAAAAAAA-71CA-11E1-9E33-C80AA9429562:1",
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"p1": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
				},
			},
			expected:  []string{"r1", "p1"},
			shouldErr: false,
		},
		{
			name: "bad primary position fails the call",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"p1": {
					Position: "InvalidFlavor/1234",
				},
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, err := FindValidEmergencyReparentCandidates(tt.statusMap, tt.primaryStatusMap)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			keys := make([]string, 0, len(actual))
			for key := range actual {
				keys = append(keys, key)
			}
			assert.ElementsMatch(t, tt.expected, keys)
		})
	}
}

// stopReplicationAndBuildStatusMapsTestTMClient implements
// tmclient.TabletManagerClient to facilitate testing of
// stopReplicationAndBuildStatusMaps.
type stopReplicationAndBuildStatusMapsTestTMClient struct {
	tmclient.TabletManagerClient

	demotePrimaryResults map[string]*struct {
		PrimaryStatus *replicationdatapb.PrimaryStatus
		Err           error
	}
	demotePrimaryDelays map[string]time.Duration

	stopReplicationAndGetStatusResults map[string]*struct {
		StopStatus *replicationdatapb.StopReplicationStatus
		Err        error
	}
	stopReplicationAndGetStatusDelays map[string]time.Duration
}

func (fake *stopReplicationAndBuildStatusMapsTestTMClient) DemotePrimary(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if delay, ok := fake.demotePrimaryDelays[key]; ok {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if result, ok := fake.demotePrimaryResults[key]; ok {
		return result.PrimaryStatus, result.Err
	}

	return nil, assert.AnError
}

func (fake *stopReplicationAndBuildStatusMapsTestTMClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, mode replicationdatapb.StopReplicationMode) (*replicationdatapb.StopReplicationStatus, error) {
	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if delay, ok := fake.stopReplicationAndGetStatusDelays[key]; ok {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if result, ok := fake.stopReplicationAndGetStatusResults[key]; ok {
		return result.StopStatus, result.Err
	}

	return nil, assert.AnError
}

func Test_stopReplicationAndBuildStatusMaps(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	tests := []struct {
		name                     string
		durability               string
		tmc                      *stopReplicationAndBuildStatusMapsTestTMClient
		tabletMap                map[string]*topo.TabletInfo
		stopReplicasTimeout      time.Duration
		ignoredTablets           sets.Set[string]
		tabletToWaitFor          *topodatapb.TabletAlias
		timeSpent                time.Duration
		waitForAllTablets        bool
		expectedStatusMap        map[string]*replicationdatapb.StopReplicationStatus
		expectedPrimaryStatusMap map[string]*replicationdatapb.PrimaryStatus
		expectedTabletsReachable []*topodatapb.Tablet
		shouldErr                bool
	}{
		{
			name:       "success",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
				},
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			shouldErr: false,
		}, {
			name:       "success with wait for all tablets",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
				},
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			waitForAllTablets: true,
			shouldErr:         false,
		}, {
			name:       "timing check with wait for all tablets",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
				stopReplicationAndGetStatusDelays: map[string]time.Duration{
					// We want `zone1-0000000102` to take a lot of time to respond.
					// Simulating a tablet being unreachable.
					"zone1-0000000102": time.Hour,
				},
			},
			stopReplicasTimeout: 1 * time.Second,
			timeSpent:           900 * time.Millisecond,
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				}, "zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
				},
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			waitForAllTablets: true,
			shouldErr:         false,
		},
		{
			name:       "success - 2 rdonly failures",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
					"zone1-0000000102": {
						Err: assert.AnError,
					},
					"zone1-0000000103": {
						Err: assert.AnError,
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_RDONLY,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_RDONLY,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
				},
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			shouldErr: false,
		},
		{
			name:       "success - 1 rdonly and 1 replica failures",
			durability: "semi_sync",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
					"zone1-0000000102": {
						Err: assert.AnError,
					},
					"zone1-0000000103": {
						Err: assert.AnError,
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_RDONLY,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
				},
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			shouldErr: false,
		},
		{
			name:       "ignore tablets",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets: sets.New[string]("zone1-0000000100"),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			shouldErr:                false,
		},
		{
			name:       "have PRIMARY tablet and can demote",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				demotePrimaryResults: map[string]*struct {
					PrimaryStatus *replicationdatapb.PrimaryStatus
					Err           error
				}{
					"zone1-0000000100": {
						PrimaryStatus: &replicationdatapb.PrimaryStatus{
							Position: "primary-position-100",
						},
					},
				},
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						// In the tabletManager implementation of StopReplicationAndGetStatus
						// we wrap the error and then send it via GRPC. This should still work as expected.
						Err: vterrors.ToGRPC(vterrors.Wrap(mysql.ErrNotReplica, "before status failed")),
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_PRIMARY,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"zone1-0000000100": {
					Position: "primary-position-100",
				},
			},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			shouldErr: false,
		},
		{
			name:       "one tablet is PRIMARY and cannot demote",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				demotePrimaryResults: map[string]*struct {
					PrimaryStatus *replicationdatapb.PrimaryStatus
					Err           error
				}{
					"zone1-0000000100": {
						Err: assert.AnError,
					},
				},
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						Err: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_PRIMARY,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{}, // zone1-0000000100 fails to demote, so does not appear
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			shouldErr: false,
		},
		{
			name:       "multiple tablets are PRIMARY and cannot demote",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				demotePrimaryResults: map[string]*struct {
					PrimaryStatus *replicationdatapb.PrimaryStatus
					Err           error
				}{
					"zone1-0000000100": {
						Err: assert.AnError,
					},
					"zone1-0000000101": {
						Err: assert.AnError,
					},
				},
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						Err: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						Err: mysql.ErrNotReplica,
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_PRIMARY,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_PRIMARY,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets:           sets.New[string](),
			expectedStatusMap:        nil,
			expectedPrimaryStatusMap: nil,
			expectedTabletsReachable: nil,
			shouldErr:                true, // we get multiple errors, so we fail
		},
		{
			name:       "stopReplicasTimeout exceeded",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusDelays: map[string]time.Duration{
					"zone1-0000000100": time.Minute, // zone1-0000000100 will timeout and not be included
				},
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			stopReplicasTimeout: time.Millisecond * 5,
			ignoredTablets:      sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			shouldErr:                false,
		},
		{
			name:       "one tablet fails to StopReplication",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						Err: assert.AnError, // not being mysql.ErrNotReplica will not cause us to call DemotePrimary
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
			},
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}},
			shouldErr: false,
		},
		{
			name:       "multiple tablets fail StopReplication",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						Err: assert.AnError,
					},
					"zone1-0000000101": {
						Err: assert.AnError,
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets:           sets.New[string](),
			expectedStatusMap:        nil,
			expectedPrimaryStatusMap: nil,
			expectedTabletsReachable: nil,
			shouldErr:                true,
		}, {
			name:       "1 tablets fail StopReplication and 1 has replication stopped",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						Err: assert.AnError,
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5"},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			ignoredTablets:           sets.New[string](),
			expectedStatusMap:        nil,
			expectedPrimaryStatusMap: nil,
			expectedTabletsReachable: nil,
			shouldErr:                true,
		},
		{
			name:       "slow tablet is the new primary requested",
			durability: "none",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusDelays: map[string]time.Duration{
					"zone1-0000000102": 1 * time.Second, // zone1-0000000102 is slow to respond but has to be included since it is the requested primary
				},
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429102:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429102:1-9"},
						},
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Type: topodatapb.TabletType_REPLICA,
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			tabletToWaitFor: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			},
			ignoredTablets: sets.New[string](),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429100:1-9"},
				},
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429101:1-9"},
				},
				"zone1-0000000102": {
					Before: &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429102:1-5", IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After:  &replicationdatapb.Status{Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429102:1-9"},
				},
			},
			expectedTabletsReachable: []*topodatapb.Tablet{{
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			}, {
				Type: topodatapb.TabletType_REPLICA,
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			}},
			stopReplicasTimeout:      time.Minute,
			expectedPrimaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{},
			shouldErr:                false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			durability, err := GetDurabilityPolicy(tt.durability)
			require.NoError(t, err)
			startTime := time.Now()
			res, err := stopReplicationAndBuildStatusMaps(ctx, tt.tmc, &events.Reparent{}, tt.tabletMap, tt.stopReplicasTimeout, tt.ignoredTablets, tt.tabletToWaitFor, durability, tt.waitForAllTablets, logger)
			totalTimeSpent := time.Since(startTime)
			if tt.timeSpent != 0 {
				assert.Greater(t, totalTimeSpent, tt.timeSpent)
			}
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedStatusMap, res.statusMap, "StopReplicationStatus mismatch")
			assert.Equal(t, tt.expectedPrimaryStatusMap, res.primaryStatusMap, "PrimaryStatusMap mismatch")
			require.Equal(t, len(tt.expectedTabletsReachable), len(res.reachableTablets), "TabletsReached length mismatch")
			for idx, tablet := range res.reachableTablets {
				assert.True(t, topoproto.IsTabletInList(tablet, tt.expectedTabletsReachable), "TabletsReached[%d] not found - %s", idx, topoproto.TabletAliasString(tablet.Alias))
			}
		})
	}
}

func TestReplicaWasRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        *replicationdatapb.StopReplicationStatus
		expected  bool
		shouldErr bool
	}{
		{
			name: "io thread running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateRunning),
					SqlState: int32(mysql.ReplicationStateStopped),
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "sql thread running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateStopped),
					SqlState: int32(mysql.ReplicationStateRunning),
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "io and sql threads running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateRunning),
					SqlState: int32(mysql.ReplicationStateRunning),
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "no replication threads running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateStopped),
					SqlState: int32(mysql.ReplicationStateStopped),
				},
			},
			expected:  false,
			shouldErr: false,
		},
		{
			name:      "passing nil pointer results in an error",
			in:        nil,
			expected:  false,
			shouldErr: true,
		},
		{
			name: "status.Before is nil results in an error",
			in: &replicationdatapb.StopReplicationStatus{
				Before: nil,
			},
			expected:  false,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, err := ReplicaWasRunning(tt.in)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestSQLThreadWasRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        *replicationdatapb.StopReplicationStatus
		expected  bool
		shouldErr bool
	}{
		{
			name: "io thread running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateRunning),
					SqlState: int32(mysql.ReplicationStateStopped),
				},
			},
			expected:  false,
			shouldErr: false,
		},
		{
			name: "sql thread running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateStopped),
					SqlState: int32(mysql.ReplicationStateRunning),
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "io and sql threads running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateRunning),
					SqlState: int32(mysql.ReplicationStateRunning),
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "no replication threads running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoState:  int32(mysql.ReplicationStateStopped),
					SqlState: int32(mysql.ReplicationStateStopped),
				},
			},
			expected:  false,
			shouldErr: false,
		},
		{
			name:      "passing nil pointer results in an error",
			in:        nil,
			expected:  false,
			shouldErr: true,
		},
		{
			name: "status.Before is nil results in an error",
			in: &replicationdatapb.StopReplicationStatus{
				Before: nil,
			},
			expected:  false,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, err := SQLThreadWasRunning(tt.in)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

// waitForRelayLogsToApplyTestTMClient implements just the WaitForPosition
// method of the tmclient.TabletManagerClient interface for
// TestWaitForRelayLogsToApply, with the necessary trackers to facilitate
// testing that unit.
type waitForRelayLogsToApplyTestTMClient struct {
	tmclient.TabletManagerClient
	calledPositions []string
	shouldErr       bool
}

func (fake *waitForRelayLogsToApplyTestTMClient) WaitForPosition(_ context.Context, _ *topodatapb.Tablet, position string) error {
	if fake.shouldErr {
		return assert.AnError
	}

	fake.calledPositions = append(fake.calledPositions, position)
	return nil
}

func TestWaitForRelayLogsToApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name                    string
		client                  *waitForRelayLogsToApplyTestTMClient
		status                  *replicationdatapb.StopReplicationStatus
		expectedCalledPositions []string
		shouldErr               bool
	}{
		{
			name:   "using relay log position",
			client: &waitForRelayLogsToApplyTestTMClient{},
			status: &replicationdatapb.StopReplicationStatus{
				After: &replicationdatapb.Status{
					RelayLogPosition: "relay-pos",
				},
			},
			expectedCalledPositions: []string{"relay-pos"},
			shouldErr:               false,
		},
		{
			name:   "using file relay log position",
			client: &waitForRelayLogsToApplyTestTMClient{},
			status: &replicationdatapb.StopReplicationStatus{
				After: &replicationdatapb.Status{
					RelayLogSourceBinlogEquivalentPosition: "file-relay-pos",
				},
			},
			expectedCalledPositions: []string{"file-relay-pos"},
			shouldErr:               false,
		},
		{
			name:   "when both are set, relay log position takes precedence over file relay log position",
			client: &waitForRelayLogsToApplyTestTMClient{},
			status: &replicationdatapb.StopReplicationStatus{
				After: &replicationdatapb.Status{
					RelayLogPosition: "relay-pos",
					FilePosition:     "file-relay-pos",
				},
			},
			expectedCalledPositions: []string{"relay-pos"},
			shouldErr:               false,
		},
		{
			name: "error waiting for position",
			client: &waitForRelayLogsToApplyTestTMClient{
				shouldErr: true,
			},
			status: &replicationdatapb.StopReplicationStatus{
				After: &replicationdatapb.Status{
					RelayLogPosition: "relay-pos",
				},
			},
			expectedCalledPositions: nil,
			shouldErr:               true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := WaitForRelayLogsToApply(ctx, tt.client, &topo.TabletInfo{}, tt.status)
			defer assert.Equal(t, tt.expectedCalledPositions, tt.client.calledPositions)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}
