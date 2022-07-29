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
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
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
					Type: topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
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
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000200": {},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
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
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
					Type: topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
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
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000200": {},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
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
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
				AlsoSetShardPrimary:  true,
				ForceSetShardPrimary: true,
				SkipShardCreation:    false,
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
			testutil.AssertEmergencyReparentShardResponsesEqual(t, tt.expected, resp)
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
					Type: topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
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
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
							Position: "primary-demotion position",
						},
						Error: nil,
					},
				},
				PrimaryPositionResults: map[string]struct {
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
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000200": nil, // waiting for primary-position during promotion
					// reparent SetReplicationSource calls
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
					Type: topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
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
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
							Position: "primary-demotion position",
						},
						Error: nil,
					},
				},
				PrimaryPositionResults: map[string]struct {
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
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000200": nil, // waiting for primary-position during promotion
					// reparent SetReplicationSource calls
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
				AlsoSetShardPrimary:  true,
				ForceSetShardPrimary: true,
				SkipShardCreation:    false,
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
			testutil.AssertPlannedReparentShardResponsesEqual(t, tt.expected, resp)
		})
	}
}

func TestSleepTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
		Keyspace: "testkeyspace",
		Shard:    "-",
	}, nil)

	tests := []struct {
		name      string
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.SleepTabletRequest
		expected  *vtctldatapb.SleepTabletResponse
		shouldErr bool
	}{
		{
			name: "ok",
			tmc: testutil.TabletManagerClient{
				SleepResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.SleepTabletRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Duration: protoutil.DurationToProto(time.Millisecond),
			},
			expected: &vtctldatapb.SleepTabletResponse{},
		},
		{
			name: "default sleep duration", // this is the slowest test case, and takes 30 seconds. comment this out to go faster.
			tmc: testutil.TabletManagerClient{
				SleepResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.SleepTabletRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtctldatapb.SleepTabletResponse{},
		},
		{
			name: "tablet not found",
			tmc: testutil.TabletManagerClient{
				SleepResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.SleepTabletRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  404,
				},
			},
			shouldErr: true,
		},
		{
			name: "sleep rpc error",
			tmc: testutil.TabletManagerClient{
				SleepResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.SleepTabletRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Duration: protoutil.DurationToProto(time.Millisecond),
			},
			shouldErr: true,
		},
	}

	expectedDur := func(t *testing.T, in *vttime.Duration, defaultDur time.Duration) time.Duration {
		dur, ok, err := protoutil.DurationFromProto(in)
		require.NoError(t, err)

		if !ok {
			return defaultDur
		}

		return dur
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			start := time.Now()
			resp, err := vtctld.SleepTablet(ctx, tt.req)
			sleepDur := time.Since(start)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Nil(t, resp)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
			dur := expectedDur(t, tt.req.Duration, *topo.RemoteOperationTimeout)
			assert.LessOrEqual(t, dur, sleepDur, "sleep should have taken at least %v; took %v", dur, sleepDur)
		})
	}
}
