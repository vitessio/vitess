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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestNewPlannedReparenter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		logger logutil.Logger
	}{
		{
			name:   "default case",
			logger: logutil.NewMemoryLogger(),
		},
		{
			name:   "overrides nil logger with no-op",
			logger: nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			er := NewPlannedReparenter(nil, nil, tt.logger)
			assert.NotNil(t, er.logger, "NewPlannedReparenter should never result in a nil logger instance on the EmergencyReparenter")
		})
	}
}

func TestPlannedReparenter_ReparentShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		ts                  *topo.Server
		tmc                 tmclient.TabletManagerClient
		tablets             []*topodatapb.Tablet
		lockShardBeforeTest bool

		keyspace string
		shard    string
		opts     PlannedReparentOptions

		expectedEvent *events.Reparent
		shouldErr     bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "position1",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
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
			},

			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},

			shouldErr: false,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					KeyRange:        &topodatapb.KeyRange{},
					IsMasterServing: true,
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "cannot lock shard",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockShardBeforeTest: true,

			keyspace: "testkeyspace",
			shard:    "-",
			opts:     PlannedReparentOptions{},

			expectedEvent: nil,
			shouldErr:     true,
		},
		{
			// The simplest setup required to make an overall ReparentShard call
			// fail is to set NewPrimaryAlias = AvoidPrimaryAlias, which will
			// fail the preflight checks. Other functions are unit-tested
			// thoroughly to cover all the cases.
			name: "reparent fails",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_MASTER,
				},
			},

			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},

			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					IsMasterServing: true,
					KeyRange:        &topodatapb.KeyRange{},
				}, nil),
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := ctx

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster: true,
				SkipShardCreation:  false,
			}, tt.tablets...)

			if tt.lockShardBeforeTest {
				lctx, unlock, err := tt.ts.LockShard(ctx, tt.keyspace, tt.shard, "locking for test")
				require.NoError(t, err, "could not lock %s/%s for test case", tt.keyspace, tt.shard)

				defer func() {
					unlock(&err)
					require.NoError(t, err, "could not unlock %s/%s after test case", tt.keyspace, tt.shard)
				}()

				ctx = lctx
			}

			pr := NewPlannedReparenter(tt.ts, tt.tmc, logger)
			ev, err := pr.ReparentShard(ctx, tt.keyspace, tt.shard, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)
				AssertReparentEventsEqual(t, tt.expectedEvent, ev)

				if ev != nil {
					assert.Contains(t, ev.Status, "failed PlannedReparentShard", "expected event status to indicate failed PRS")
				}

				return
			}

			assert.NoError(t, err)
			AssertReparentEventsEqual(t, tt.expectedEvent, ev)
			assert.Contains(t, ev.Status, "finished PlannedReparentShard", "expected event status to indicate successful PRS")
		})
	}
}

func TestPlannedReparenter_getLockAction(t *testing.T) {
	t.Parallel()

	pr := &PlannedReparenter{}
	tests := []struct {
		name     string
		opts     PlannedReparentOptions
		expected string
	}{
		{
			name:     "no options",
			opts:     PlannedReparentOptions{},
			expected: "PlannedReparentShard(<nil>, AvoidPrimary = <nil>)",
		},
		{
			name: "desired primary only",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: "PlannedReparentShard(zone1-0000000100, AvoidPrimary = <nil>)",
		},
		{
			name: "avoid-primary only",
			opts: PlannedReparentOptions{
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  500,
				},
			},
			expected: "PlannedReparentShard(<nil>, AvoidPrimary = zone1-0000000500)",
		},
		{
			name: "all options specified",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  500,
				},
			},
			expected: "PlannedReparentShard(zone1-0000000100, AvoidPrimary = zone1-0000000500)",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := pr.getLockAction(tt.opts)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestPlannedReparenter_preflightChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string

		ts      *topo.Server
		tmc     tmclient.TabletManagerClient
		tablets []*topodatapb.Tablet

		ev        *events.Reparent
		keyspace  string
		shard     string
		tabletMap map[string]*topo.TabletInfo
		opts      *PlannedReparentOptions

		expectedIsNoop bool
		expectedEvent  *events.Reparent
		expectedOpts   *PlannedReparentOptions
		shouldErr      bool
	}{
		{
			name: "invariants hold",
			ev: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  500,
					},
				}, nil),
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
			},
			opts: &PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedIsNoop: false,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  500,
					},
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "invariants hold with primary selection",
			tmc: &testutil.TabletManagerClient{
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000100": { // most advanced position
						Position: &replicationdatapb.Status{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
					},
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
					},
				},
			},
			ev: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  500,
					},
				}, nil),
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000500": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  500,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
			},
			opts: &PlannedReparentOptions{
				// Avoid the current primary.
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  500,
				},
			},
			expectedIsNoop: false,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  500,
					},
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
			},
			expectedOpts: &PlannedReparentOptions{
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  500,
				},
				// NewPrimaryAlias gets populated by the preflightCheck code
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: false,
		},
		{
			name: "new-primary and avoid-primary match",
			opts: &PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedIsNoop: true,
			shouldErr:      true,
		},
		{
			name: "current shard primary is not avoid-primary",
			ev: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				}, nil),
			},
			opts: &PlannedReparentOptions{
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectedIsNoop: true, // nothing to do, but not an error!
			shouldErr:      false,
		},
		{
			// this doesn't cause an actual error from ChooseNewPrimary, because
			// the only way to do that is to set AvoidPrimaryAlias == nil, and
			// that gets checked in preflightChecks before calling
			// ChooseNewPrimary for other reasons. however we do check that we
			// get a non-nil result from ChooseNewPrimary in preflightChecks and
			// bail out if we don't, so we're forcing that case here.
			name: "cannot choose new primary-elect",
			ev: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				}, nil),
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
			},
			opts: &PlannedReparentOptions{
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedIsNoop: true,
			shouldErr:      true,
		},
		{
			name:      "primary-elect is not in tablet map",
			ev:        &events.Reparent{},
			tabletMap: map[string]*topo.TabletInfo{},
			opts: &PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedIsNoop: true,
			shouldErr:      true,
		},
		{
			name: "shard has no current primary",
			ev: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: nil,
				}, nil),
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
			},
			opts: &PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedIsNoop: true,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: nil,
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				if tt.expectedEvent != nil {
					AssertReparentEventsEqualWithMessage(t, tt.expectedEvent, tt.ev, "expected preflightChecks to mutate the passed-in event")
				}

				if tt.expectedOpts != nil {
					assert.Equal(t, tt.expectedOpts, tt.opts, "expected preflightChecks to mutate the passed in PlannedReparentOptions")
				}
			}()

			pr := NewPlannedReparenter(tt.ts, tt.tmc, logger)
			isNoop, err := pr.preflightChecks(ctx, tt.ev, tt.keyspace, tt.shard, tt.tabletMap, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedIsNoop, isNoop, "preflightChecks returned wrong isNoop signal")

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedIsNoop, isNoop, "preflightChecks returned wrong isNoop signal")
		})
	}
}

func TestPlannedReparenter_performGracefulPromotion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ts         *topo.Server
		tmc        tmclient.TabletManagerClient
		unlockTopo bool
		ctxTimeout time.Duration

		ev             *events.Reparent
		keyspace       string
		shard          string
		currentPrimary *topo.TabletInfo
		primaryElect   topodatapb.Tablet
		tabletMap      map[string]*topo.TabletInfo
		opts           PlannedReparentOptions

		expectedPos   string
		expectedEvent *events.Reparent
		shouldErr     bool
		// Optional function to run some additional post-test assertions. Will
		// be run in the main test body before the common assertions are run,
		// regardless of the value of tt.shouldErr for that test case.
		extraAssertions func(t *testing.T, pos string, err error)
	}{
		{
			name: "successful promotion",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// value of Position doesn't strictly matter for
							// this test case, as long as it matches the inner
							// key of the WaitForPositionResults map for the
							// primary-elect.
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Result: "successful reparent journal position",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": nil,
					},
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap:   map[string]*topo.TabletInfo{},
			opts:        PlannedReparentOptions{},
			expectedPos: "successful reparent journal position",
			shouldErr:   false,
		},
		{
			name: "cannot get snapshot of current primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
		{
			name: "primary-elect fails to catch up to current primary snapshot position",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": assert.AnError,
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
		{
			name: "primary-elect times out catching up to current primary snapshot position",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterDelays: map[string]time.Duration{
					"zone1-0000000200": time.Millisecond * 100,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts: PlannedReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 10,
			},
			shouldErr: true,
		},
		{
			name: "lost topology lock",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
			},
			unlockTopo: true,
			ev:         &events.Reparent{},
			keyspace:   "testkeyspace",
			shard:      "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
		{
			name: "failed to demote current primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
		{
			name: "primary-elect fails to catch up to current primary demotion position",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// value of Position doesn't strictly matter for
							// this test case, as long as it matches the inner
							// key of the WaitForPositionResults map for the
							// primary-elect.
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": assert.AnError,
					},
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
		{
			name: "primary-elect times out catching up to current primary demotion position",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// value of Position doesn't strictly matter for
							// this test case, as long as it matches the inner
							// key of the WaitForPositionResults map for the
							// primary-elect.
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000200": time.Millisecond * 100,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": nil,
					},
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts: PlannedReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 10,
			},
			shouldErr: true,
		},
		{
			name: "demotion succeeds but parent context times out",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					// This being present means that if we don't encounter a
					// a case where either WaitForPosition errors, or the parent
					// context times out, then we will fail the test, since it
					// will cause the overall function under test to return no
					// error.
					"zone1-0000000200": {
						Result: "success!",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionPostDelays: map[string]time.Duration{
					"zone1-0000000200": time.Millisecond * 5,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": nil,
					},
				},
			},
			ctxTimeout: time.Millisecond * 4, // WaitForPosition won't return error, but will timeout the parent context
			ev:         &events.Reparent{},
			keyspace:   "testkeyspace",
			shard:      "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
		{
			name: "rollback fails",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// value of Position doesn't strictly matter for
							// this test case, as long as it matches the inner
							// key of the WaitForPositionResults map for the
							// primary-elect.
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": assert.AnError,
					},
				},
				UndoDemoteMasterResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
			extraAssertions: func(t *testing.T, pos string, err error) {
				assert.Contains(t, err.Error(), "UndoDemoteMaster", "expected error to include information about failed demotion rollback")
			},
		},
		{
			name: "rollback succeeds",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// value of Position doesn't strictly matter for
							// this test case, as long as it matches the inner
							// key of the WaitForPositionResults map for the
							// primary-elect.
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": assert.AnError,
					},
				},
				UndoDemoteMasterResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
			extraAssertions: func(t *testing.T, pos string, err error) {
				assert.NotContains(t, err.Error(), "UndoDemoteMaster", "expected error to not include information about failed demotion rollback")
			},
		},
		{
			name: "primary-elect fails to promote",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// value of Position doesn't strictly matter for
							// this test case, as long as it matches the inner
							// key of the WaitForPositionResults map for the
							// primary-elect.
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Error: assert.AnError,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": nil,
					},
				},
			},
			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
		{
			name: "promotion succeeds but parent context times out",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// value of Position doesn't strictly matter for
							// this test case, as long as it matches the inner
							// key of the WaitForPositionResults map for the
							// primary-elect.
							Position: "position1",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
					},
				},
				PromoteReplicaPostDelays: map[string]time.Duration{
					"zone1-0000000200": time.Millisecond * 20, // 2x the parent context timeout
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Error: nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"position1": nil,
					},
				},
			},
			ctxTimeout: time.Millisecond * 10,
			ev:         &events.Reparent{},
			keyspace:   "testkeyspace",
			shard:      "-",
			currentPrimary: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			opts:      PlannedReparentOptions{},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := ctx

			testutil.AddShards(ctx, t, tt.ts, &vtctldatapb.Shard{
				Keyspace: tt.keyspace,
				Name:     tt.shard,
			})

			if !tt.unlockTopo {
				lctx, unlock, err := tt.ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, err, "could not lock %s/%s for testing", tt.keyspace, tt.shard)

				defer func() {
					unlock(&err)
					require.NoError(t, err, "could not unlock %s/%s during testing", tt.keyspace, tt.shard)
				}()

				ctx = lctx
			}

			pr := NewPlannedReparenter(tt.ts, tt.tmc, logger)

			if tt.ctxTimeout > 0 {
				_ctx, cancel := context.WithTimeout(ctx, tt.ctxTimeout)
				defer cancel()

				ctx = _ctx
			}

			pos, err := pr.performGracefulPromotion(
				ctx,
				tt.ev,
				tt.keyspace,
				tt.shard,
				tt.currentPrimary,
				tt.primaryElect,
				tt.tabletMap,
				tt.opts,
			)

			if tt.extraAssertions != nil {
				tt.extraAssertions(t, pos, err)
			}

			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedPos, pos)
		})
	}
}

func TestPlannedReparenter_performPartialPromotionRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tmc          tmclient.TabletManagerClient
		timeout      time.Duration
		primaryElect topodatapb.Tablet
		expectedPos  string
		shouldErr    bool
	}{
		{
			name: "successful recovery",
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "position1",
						Error:    nil,
					},
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expectedPos: "position1",
			shouldErr:   false,
		},
		{
			name: "failed to SetReadWrite",
			tmc: &testutil.TabletManagerClient{
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
		{
			name: "SetReadWrite timed out",
			tmc: &testutil.TabletManagerClient{
				SetReadWriteDelays: map[string]time.Duration{
					"zone1-0000000100": time.Millisecond * 50,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			timeout: time.Millisecond * 10,
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
		{
			name: "failed to get MasterPosition from refreshed primary",
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "",
						Error:    assert.AnError,
					},
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
		{
			name: "MasterPosition timed out",
			tmc: &testutil.TabletManagerClient{
				MasterPositionDelays: map[string]time.Duration{
					"zone1-0000000100": time.Millisecond * 50,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "position1",
						Error:    nil,
					},
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			timeout: time.Millisecond * 10,
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := ctx
			pr := NewPlannedReparenter(nil, tt.tmc, logger)

			if tt.timeout > 0 {
				_ctx, cancel := context.WithTimeout(ctx, tt.timeout)
				defer cancel()

				ctx = _ctx
			}

			rp, err := pr.performPartialPromotionRecovery(ctx, tt.primaryElect)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedPos, rp, "performPartialPromotionRecovery gave unexpected reparent journal position")
		})
	}
}

func TestPlannedReparenter_performPotentialPromotion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ts         *topo.Server
		tmc        tmclient.TabletManagerClient
		timeout    time.Duration
		unlockTopo bool

		keyspace     string
		shard        string
		primaryElect topodatapb.Tablet
		tabletMap    map[string]*topo.TabletInfo

		expectedPos string
		shouldErr   bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000101": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000102": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
						Error: nil,
					},
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Result: "reparent journal position",
						Error:  nil,
					},
				},
			},
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			expectedPos: "reparent journal position",
			shouldErr:   false,
		},
		{
			name: "failed to DemoteMaster on a tablet",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: nil,
						Error:  assert.AnError,
					},
				},
			},
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
			},
			shouldErr: true,
		},
		{
			name: "timed out during DemoteMaster on a tablet",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterDelays: map[string]time.Duration{
					"zone1-0000000100": time.Millisecond * 50,
				},
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
				},
			},
			timeout:    time.Millisecond * 10,
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
			},
			shouldErr: true,
		},
		{
			name: "failed to DecodePosition on a tablet's demote position",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/this-is-nonsense",
						},
						Error: nil,
					},
				},
			},
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
			},
			shouldErr: true,
		},
		{
			name:       "primary-elect not in tablet map",
			ts:         memorytopo.NewServer("zone1"),
			tmc:        &testutil.TabletManagerClient{},
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			shouldErr: true,
		},
		{
			name: "primary-elect not most at most advanced position",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000101": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000102": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10000",
						},
						Error: nil,
					},
				},
			},
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "lost topology lock",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000101": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000102": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
				},
			},
			unlockTopo: true,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "failed to promote primary-elect",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000101": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000102": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
						Error: nil,
					},
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Result: "",
						Error:  assert.AnError,
					},
				},
			},
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "timed out while promoting primary-elect",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000101": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000102": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
						Error: nil,
					},
				},
				PromoteReplicaDelays: map[string]time.Duration{
					"zone1-0000000100": time.Millisecond * 100,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Result: "reparent journal position",
						Error:  nil,
					},
				},
			},
			timeout:    time.Millisecond * 50,
			unlockTopo: false,
			keyspace:   "testkeyspace",
			shard:      "-",
			primaryElect: topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
					},
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := ctx
			pr := NewPlannedReparenter(nil, tt.tmc, logger)

			testutil.AddShards(ctx, t, tt.ts, &vtctldatapb.Shard{
				Keyspace: tt.keyspace,
				Name:     tt.shard,
			})

			if !tt.unlockTopo {
				lctx, unlock, err := tt.ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, err, "could not lock %s/%s for testing", tt.keyspace, tt.shard)

				defer func() {
					unlock(&err)
					require.NoError(t, err, "could not unlock %s/%s during testing", tt.keyspace, tt.shard)
				}()

				ctx = lctx
			}

			if tt.timeout > 0 {
				_ctx, cancel := context.WithTimeout(ctx, tt.timeout)
				defer cancel()

				ctx = _ctx
			}

			rp, err := pr.performPotentialPromotion(ctx, tt.keyspace, tt.shard, tt.primaryElect, tt.tabletMap)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedPos, rp)
		})
	}
}

func TestPlannedReparenter_reparentShardLocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ts         *topo.Server
		tmc        tmclient.TabletManagerClient
		tablets    []*topodatapb.Tablet
		unlockTopo bool

		ev       *events.Reparent
		keyspace string
		shard    string
		opts     PlannedReparentOptions

		shouldErr     bool
		expectedEvent *events.Reparent
	}{
		{
			name: "success: current primary cannot be determined", // "Case (1)"
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
					"zone1-0000000200": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil, // zone1-200 gets promoted
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Result: "reparent journal position",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil, // zone1-100 gets reparented under zone1-200
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds:     1000,
						Nanoseconds: 500,
					},
					Hostname: "primary1", // claims to be MASTER with same term as primary2
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds:     1000,
						Nanoseconds: 500,
					},
					Hostname: "primary2", // claims to be MASTER with same term as primary1
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{ // We want primary2 to be the true primary.
					Cell: "zone1",
					Uid:  200,
				},
			},

			shouldErr: false,
		},
		{
			name: "success: current primary is desired primary", // "Case (2)"
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "position1",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
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
			},

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},

			shouldErr: false,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					KeyRange:        &topodatapb.KeyRange{},
					IsMasterServing: true,
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "success: graceful promotion", // "Case (3)"
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							// a few more transactions happen after waiting for replication
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-8",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Result: "reparent journal position",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil, // called during reparentTablets to make oldPrimary a replica of newPrimary
					"zone1-0000000200": nil, // called during performGracefulPromotion to ensure newPrimary is caught up
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10": nil,
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds:     1000,
						Nanoseconds: 500,
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
			},

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},

			shouldErr: false,
		},
		{
			name:       "shard not found",
			ts:         memorytopo.NewServer("zone1"),
			tmc:        nil,
			tablets:    nil,
			unlockTopo: true,

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts:     PlannedReparentOptions{},

			shouldErr:     true,
			expectedEvent: &events.Reparent{},
		},
		{
			name: "preflight checks fail",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			tablets: []*topodatapb.Tablet{
				// Shard has no current primary, so preflight fails.
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},

			shouldErr: true,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					KeyRange:        &topodatapb.KeyRange{},
					IsMasterServing: true,
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "preflight checks determine PRS is no-op",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
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
			},

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				// This is not the shard primary, so nothing to do.
				AvoidPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},

			shouldErr: false,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					KeyRange:        &topodatapb.KeyRange{},
					IsMasterServing: true,
				}, nil),
			},
		},
		{
			name: "promotion step fails",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
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
			},

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},

			shouldErr: true,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					KeyRange:        &topodatapb.KeyRange{},
					IsMasterServing: true,
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "lost topology lock",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "position1",
						Error:    nil,
					},
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
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
			},
			unlockTopo: true,

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				// This is not the shard primary, so nothing to do.
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},

			shouldErr: true,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					KeyRange:        &topodatapb.KeyRange{},
					IsMasterServing: true,
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "failed to reparent tablets",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "position1",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
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
			},

			ev:       &events.Reparent{},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: PlannedReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},

			shouldErr: true,
			expectedEvent: &events.Reparent{
				ShardInfo: *topo.NewShardInfo("testkeyspace", "-", &topodatapb.Shard{
					MasterAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					KeyRange:        &topodatapb.KeyRange{},
					IsMasterServing: true,
				}, nil),
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
	}

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := ctx

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster:  true,
				ForceSetShardMaster: true, // Some of our test cases count on having multiple primaries, so let the last one "win".
				SkipShardCreation:   false,
			}, tt.tablets...)

			if !tt.unlockTopo {
				lctx, unlock, err := tt.ts.LockShard(ctx, tt.keyspace, tt.shard, "locking for testing")
				require.NoError(t, err, "could not lock %s/%s for testing", tt.keyspace, tt.shard)

				defer func() {
					unlock(&err)
					require.NoError(t, err, "error while unlocking %s/%s after test case", tt.keyspace, tt.shard)
				}()

				ctx = lctx
			}

			if tt.expectedEvent != nil {
				defer func() {
					AssertReparentEventsEqualWithMessage(t, tt.expectedEvent, tt.ev, "expected reparentShardLocked to mutate the passed-in event")
				}()
			}

			pr := NewPlannedReparenter(tt.ts, tt.tmc, logger)

			err := pr.reparentShardLocked(ctx, tt.ev, tt.keyspace, tt.shard, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestPlannedReparenter_reparentTablets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmc  tmclient.TabletManagerClient

		ev                      *events.Reparent
		reparentJournalPosition string
		tabletMap               map[string]*topo.TabletInfo
		opts                    PlannedReparentOptions

		shouldErr bool
	}{
		{
			name: "success",
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
					"zone1-0000000201": nil,
					"zone1-0000000202": nil,
				},
			},
			ev: &events.Reparent{
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"zone1-0000000200": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  200,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000201": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  201,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000202": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  202,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "SetMaster failed on replica",
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
					"zone1-0000000201": assert.AnError,
					"zone1-0000000202": nil,
				},
			},
			ev: &events.Reparent{
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"zone1-0000000200": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  200,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000201": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  201,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000202": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  202,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "SetMaster timed out on replica",
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetMasterDelays: map[string]time.Duration{
					"zone1-0000000201": time.Millisecond * 50,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
					"zone1-0000000201": nil,
					"zone1-0000000202": nil,
				},
			},
			ev: &events.Reparent{
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"zone1-0000000200": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  200,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000201": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  201,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000202": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  202,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			opts: PlannedReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 10,
			},
			shouldErr: true,
		},
		{
			name: "PopulateReparentJournal failed out on new primary",
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
					"zone1-0000000201": nil,
					"zone1-0000000202": nil,
				},
			},
			ev: &events.Reparent{
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"zone1-0000000200": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  200,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000201": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  201,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000202": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  202,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "PopulateReparentJournal timed out on new primary",
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalDelays: map[string]time.Duration{
					"zone1-0000000100": time.Millisecond * 50,
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil,
					"zone1-0000000201": nil,
					"zone1-0000000202": nil,
				},
			},
			ev: &events.Reparent{
				NewMaster: topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type: topodatapb.TabletType_MASTER,
					},
				},
				"zone1-0000000200": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  200,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000201": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  201,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000202": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  202,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			opts: PlannedReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 10,
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pr := NewPlannedReparenter(nil, tt.tmc, logger)
			err := pr.reparentTablets(ctx, tt.ev, tt.reparentJournalPosition, tt.tabletMap, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

// (TODO:@ajm88) when unifying all the mock TMClient implementations (which will
// most likely end up in go/vt/vtctl/testutil), move these to the same testutil
// package.
func AssertReparentEventsEqualWithMessage(t *testing.T, expected *events.Reparent, actual *events.Reparent, msg string) {
	t.Helper()

	if msg != "" && !strings.HasSuffix(msg, " ") {
		msg = msg + ": "
	}

	if expected == nil {
		assert.Nil(t, actual, "%sexpected nil Reparent event", msg)

		return
	}

	if actual == nil {
		// Note: the reason we don't use require.NotNil here is because it would
		// fail the entire test, rather than just this one helper, which is
		// intended to be an atomic assertion. However, we also don't want to
		// have to add a bunch of nil-guards below, as it would complicate the
		// code, so we're going to duplicate the nil check to force a failure
		// and bail early.
		assert.NotNil(t, actual, "%sexpected non-nil Reparent event", msg)

		return
	}

	removeVersion := func(si topo.ShardInfo) topo.ShardInfo {
		return *topo.NewShardInfo(si.Keyspace(), si.ShardName(), si.Shard, nil)
	}

	assert.Equal(t, removeVersion(expected.ShardInfo), removeVersion(actual.ShardInfo), "%sReparent.ShardInfo mismatch", msg)
	assert.Equal(t, expected.NewMaster, actual.NewMaster, "%sReparent.NewMaster mismatch", msg)
	assert.Equal(t, expected.OldMaster, actual.OldMaster, "%sReparent.OldMaster mismatch", msg)
}

func AssertReparentEventsEqual(t *testing.T, expected *events.Reparent, actual *events.Reparent) {
	t.Helper()

	AssertReparentEventsEqualWithMessage(t, expected, actual, "")
}
