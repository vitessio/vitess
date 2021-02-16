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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestFindValidEmergencyReparentCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		statusMap        map[string]*replicationdatapb.StopReplicationStatus
		primaryStatusMap map[string]*replicationdatapb.MasterStatus
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
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
				"r2": {
					After: &replicationdatapb.Status{
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.MasterStatus{
				"p1": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
				},
			},
			expected:  []string{"r1", "r2", "p1"},
			shouldErr: false,
		},
		{
			name: "mixed replication modes",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
				"r2": {
					After: &replicationdatapb.Status{
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "FilePos/mysql-bin.0001:100",
					},
				},
				"r2": {
					After: &replicationdatapb.Status{
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
				"errant": {
					After: &replicationdatapb.Status{
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,AAAAAAAA-71CA-11E1-9E33-C80AA9429562:1",
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.MasterStatus{
				"p1": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
				},
			},
			expected:  []string{"r1", "p1"},
			shouldErr: false,
		},
		{
			name: "bad master position fails the call",
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"r1": {
					After: &replicationdatapb.Status{
						MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.MasterStatus{
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
// StopReplicationAndBuildStatusMaps.
type stopReplicationAndBuildStatusMapsTestTMClient struct {
	tmclient.TabletManagerClient

	demoteMasterResults map[string]*struct {
		MasterStatus *replicationdatapb.MasterStatus
		Err          error
	}
	demoteMasterDelays map[string]time.Duration

	stopReplicationAndGetStatusResults map[string]*struct {
		StopStatus *replicationdatapb.StopReplicationStatus
		Err        error
	}
	stopReplicationAndGetStatusDelays map[string]time.Duration
}

func (fake *stopReplicationAndBuildStatusMapsTestTMClient) DemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.MasterStatus, error) {
	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if delay, ok := fake.demoteMasterDelays[key]; ok {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if result, ok := fake.demoteMasterResults[key]; ok {
		return result.MasterStatus, result.Err
	}

	return nil, assert.AnError
}

func (fake *stopReplicationAndBuildStatusMapsTestTMClient) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, mode replicationdatapb.StopReplicationMode) (*replicationdatapb.Status, *replicationdatapb.StopReplicationStatus, error) {
	if tablet.Alias == nil {
		return nil, nil, assert.AnError
	}

	key := topoproto.TabletAliasString(tablet.Alias)

	if delay, ok := fake.stopReplicationAndGetStatusDelays[key]; ok {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	if result, ok := fake.stopReplicationAndGetStatusResults[key]; ok {
		return /* unused by the code under test */ nil, result.StopStatus, result.Err
	}

	return nil, nil, assert.AnError
}

func TestStopReplicationAndBuildStatusMaps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	tests := []struct {
		name                    string
		tmc                     *stopReplicationAndBuildStatusMapsTestTMClient
		tabletMap               map[string]*topo.TabletInfo
		waitReplicasTimeout     time.Duration
		ignoredTablets          sets.String
		expectedStatusMap       map[string]*replicationdatapb.StopReplicationStatus
		expectedMasterStatusMap map[string]*replicationdatapb.MasterStatus
		shouldErr               bool
	}{
		{
			name: "success",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "100-before"},
							After:  &replicationdatapb.Status{Position: "100-after"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "101-before"},
							After:  &replicationdatapb.Status{Position: "101-after"},
						},
					},
				},
			},
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
			ignoredTablets: sets.NewString(),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					Before: &replicationdatapb.Status{Position: "100-before"},
					After:  &replicationdatapb.Status{Position: "100-after"},
				},
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "101-before"},
					After:  &replicationdatapb.Status{Position: "101-after"},
				},
			},
			expectedMasterStatusMap: map[string]*replicationdatapb.MasterStatus{},
			shouldErr:               false,
		},
		{
			name: "ignore tablets",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "100-before"},
							After:  &replicationdatapb.Status{Position: "100-after"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "101-before"},
							After:  &replicationdatapb.Status{Position: "101-after"},
						},
					},
				},
			},
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
			ignoredTablets: sets.NewString("zone1-0000000100"),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "101-before"},
					After:  &replicationdatapb.Status{Position: "101-after"},
				},
			},
			expectedMasterStatusMap: map[string]*replicationdatapb.MasterStatus{},
			shouldErr:               false,
		},
		{
			name: "have MASTER tablet and can demote",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				demoteMasterResults: map[string]*struct {
					MasterStatus *replicationdatapb.MasterStatus
					Err          error
				}{
					"zone1-0000000100": {
						MasterStatus: &replicationdatapb.MasterStatus{
							Position: "master-position-100",
						},
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
							Before: &replicationdatapb.Status{Position: "101-before"},
							After:  &replicationdatapb.Status{Position: "101-after"},
						},
					},
				},
			},
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
			ignoredTablets: sets.NewString(),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "101-before"},
					After:  &replicationdatapb.Status{Position: "101-after"},
				},
			},
			expectedMasterStatusMap: map[string]*replicationdatapb.MasterStatus{
				"zone1-0000000100": {
					Position: "master-position-100",
				},
			},
			shouldErr: false,
		},
		{
			name: "one tablet is MASTER and cannot demote",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				demoteMasterResults: map[string]*struct {
					MasterStatus *replicationdatapb.MasterStatus
					Err          error
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
							Before: &replicationdatapb.Status{Position: "101-before"},
							After:  &replicationdatapb.Status{Position: "101-after"},
						},
					},
				},
			},
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
			ignoredTablets: sets.NewString(),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "101-before"},
					After:  &replicationdatapb.Status{Position: "101-after"},
				},
			},
			expectedMasterStatusMap: map[string]*replicationdatapb.MasterStatus{}, // zone1-0000000100 fails to demote, so does not appear
			shouldErr:               false,
		},
		{
			name: "multiple tablets are MASTER and cannot demote",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				demoteMasterResults: map[string]*struct {
					MasterStatus *replicationdatapb.MasterStatus
					Err          error
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
			ignoredTablets:          sets.NewString(),
			expectedStatusMap:       nil,
			expectedMasterStatusMap: nil,
			shouldErr:               true, // we get multiple errors, so we fail
		},
		{
			name: "waitReplicasTimeout exceeded",
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
							Before: &replicationdatapb.Status{Position: "100-before"},
							After:  &replicationdatapb.Status{Position: "100-after"},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "101-before"},
							After:  &replicationdatapb.Status{Position: "101-after"},
						},
					},
				},
			},
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
			waitReplicasTimeout: time.Millisecond * 5,
			ignoredTablets:      sets.NewString(),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "101-before"},
					After:  &replicationdatapb.Status{Position: "101-after"},
				},
			},
			expectedMasterStatusMap: map[string]*replicationdatapb.MasterStatus{},
			shouldErr:               false,
		},
		{
			name: "one tablet fails to StopReplication",
			tmc: &stopReplicationAndBuildStatusMapsTestTMClient{
				stopReplicationAndGetStatusResults: map[string]*struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Err        error
				}{
					"zone1-0000000100": {
						Err: assert.AnError, // not being mysql.ErrNotReplica will not cause us to call DemoteMaster
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{Position: "101-before"},
							After:  &replicationdatapb.Status{Position: "101-after"},
						},
					},
				},
			},
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
			ignoredTablets: sets.NewString(),
			expectedStatusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": {
					Before: &replicationdatapb.Status{Position: "101-before"},
					After:  &replicationdatapb.Status{Position: "101-after"},
				},
			},
			expectedMasterStatusMap: map[string]*replicationdatapb.MasterStatus{},
			shouldErr:               false,
		},
		{
			name: "multiple tablets fail StopReplication",
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
			ignoredTablets:          sets.NewString(),
			expectedStatusMap:       nil,
			expectedMasterStatusMap: nil,
			shouldErr:               true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			statusMap, masterStatusMap, err := StopReplicationAndBuildStatusMaps(
				ctx,
				tt.tmc,
				&events.Reparent{},
				tt.tabletMap,
				tt.waitReplicasTimeout,
				tt.ignoredTablets,
				logger,
			)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedStatusMap, statusMap, "StopReplicationStatus mismatch")
			assert.Equal(t, tt.expectedMasterStatusMap, masterStatusMap, "MasterStatusMap mismatch")
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
					IoThreadRunning:  true,
					SqlThreadRunning: false,
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "sql thread running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoThreadRunning:  false,
					SqlThreadRunning: true,
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "io and sql threads running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoThreadRunning:  true,
					SqlThreadRunning: true,
				},
			},
			expected:  true,
			shouldErr: false,
		},
		{
			name: "no replication threads running",
			in: &replicationdatapb.StopReplicationStatus{
				Before: &replicationdatapb.Status{
					IoThreadRunning:  false,
					SqlThreadRunning: false,
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
					FileRelayLogPosition: "file-relay-pos",
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
