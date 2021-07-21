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

package grpcvtctldserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestEmergencyReparentShardSlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ts      *topo.Server
		tmc     tmclient.TabletManagerClient
		tablets []*topodatapb.Tablet

		req                 *vtctldatapb.EmergencyReparentShardRequest
		expected            *vtctldatapb.EmergencyReparentShardResponse
		expectEventsToOccur bool
		shouldErr           bool
	}{
		{
			// Note: this test case and the one below combine to assert that a
			// nil WaitReplicasTimeout in the request results in a default 30
			// second WaitReplicasTimeout.
			//
			// They are also very slow, because they require waiting 29 seconds
			// and 30 seconds, respectively. Fortunately, we can run them
			// concurrently, so the total time is only around 30 seconds, but
			// that's still a long time for a unit test!
			name: "nil WaitReplicasTimeout and request takes 29 seconds is ok",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
					},
				},
				PopulateReparentJournalDelays: map[string]time.Duration{
					"zone1-0000000200": time.Second * 29,
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						Error: assert.AnError,
					},
					"zone1-0000000200": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
								Position:         "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
					"zone1-0000000200": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
				},
			},
			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: nil,
			},
			expected: &vtctldatapb.EmergencyReparentShardResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectEventsToOccur: true,
			shouldErr:           false,
		},
		{
			name: "nil WaitReplicasTimeout and request takes 31 seconds is error",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
					},
				},
				PopulateReparentJournalDelays: map[string]time.Duration{
					"zone1-0000000200": time.Second * 31,
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						Error: assert.AnError,
					},
					"zone1-0000000200": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
								Position:         "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
					"zone1-0000000200": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
				},
			},
			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: nil,
			},
			expectEventsToOccur: true,
			shouldErr:           true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("tt.EmergencyReparentShardRequest = nil implies test not ready to run")
			}

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster:  true,
				ForceSetShardMaster: true,
				SkipShardCreation:   false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.EmergencyReparentShard(ctx, tt.req)

			// We defer this because we want to check in both error and non-
			// error cases, but after the main set of assertions for those
			// cases.
			defer func() {
				if !tt.expectEventsToOccur {
					testutil.AssertNoLogutilEventsOccurred(t, resp, "expected no events to occur during ERS")

					return
				}

				testutil.AssertLogutilEventsOccurred(t, resp, "expected events to occur during ERS")
			}()

			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertEmergencyReparentShardResponsesEqual(t, *tt.expected, *resp)
		})
	}
}

func TestPlannedReparentShardSlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ts      *topo.Server
		tmc     tmclient.TabletManagerClient
		tablets []*topodatapb.Tablet

		req                 *vtctldatapb.PlannedReparentShardRequest
		expected            *vtctldatapb.PlannedReparentShardResponse
		expectEventsToOccur bool
		shouldErr           bool
	}{
		{
			// Note: this test case and the one below combine to assert that a
			// nil WaitReplicasTimeout in the request results in a default 30
			// second WaitReplicasTimeout.
			name: "nil WaitReplicasTimeout and request takes 29 seconds is ok",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "primary-demotion position",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "doesn't matter",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaPostDelays: map[string]time.Duration{
					"zone1-0000000200": time.Second * 28,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Result: "promotion position",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil, // waiting for master-position during promotion
					// reparent SetMaster calls
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"primary-demotion position": nil,
					},
				},
			},
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: nil,
			},
			expected: &vtctldatapb.PlannedReparentShardResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectEventsToOccur: true,
			shouldErr:           false,
		},
		{
			name: "nil WaitReplicasTimeout and request takes 31 seconds is error",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "primary-demotion position",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "doesn't matter",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaPostDelays: map[string]time.Duration{
					"zone1-0000000200": time.Second * 30,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Result: "promotion position",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil, // waiting for master-position during promotion
					// reparent SetMaster calls
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"primary-demotion position": nil,
					},
				},
			},
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: nil,
			},
			expected: &vtctldatapb.PlannedReparentShardResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectEventsToOccur: true,
			shouldErr:           false,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster:  true,
				ForceSetShardMaster: true,
				SkipShardCreation:   false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.PlannedReparentShard(ctx, tt.req)

			// We defer this because we want to check in both error and non-
			// error cases, but after the main set of assertions for those
			// cases.
			defer func() {
				if !tt.expectEventsToOccur {
					testutil.AssertNoLogutilEventsOccurred(t, resp, "expected no events to occur during ERS")

					return
				}

				testutil.AssertLogutilEventsOccurred(t, resp, "expected events to occur during ERS")
			}()

			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertPlannedReparentShardResponsesEqual(t, *tt.expected, *resp)
		})
	}
}
