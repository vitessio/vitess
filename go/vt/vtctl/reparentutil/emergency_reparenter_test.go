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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestNewEmergencyReparenter(t *testing.T) {
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

			er := NewEmergencyReparenter(nil, nil, tt.logger)
			assert.NotNil(t, er.logger, "NewEmergencyReparenter should never result in a nil logger instance on the EmergencyReparenter")
		})
	}
}

func TestEmergencyReparenter_getLockAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		alias    *topodatapb.TabletAlias
		expected string
		msg      string
	}{
		{
			name: "explicit new primary specified",
			alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			expected: "EmergencyReparentShard(zone1-0000000100)",
			msg:      "lockAction should include tablet alias",
		},
		{
			name:     "user did not specify new primary elect",
			alias:    nil,
			expected: "EmergencyReparentShard",
			msg:      "lockAction should omit parens when no primary elect passed",
		},
	}

	erp := &EmergencyReparenter{}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := erp.getLockAction(tt.alias)
			assert.Equal(t, tt.expected, actual, tt.msg)
		})
	}
}

func TestEmergencyReparenter_reparentShardLocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		emergencyReparentOps EmergencyReparentOptions
		tmc                  *testutil.TabletManagerClient
		// setup
		ts         *topo.Server
		keyspace   string
		shard      string
		unlockTopo bool
		shards     []*vtctldatapb.Shard
		tablets    []*topodatapb.Tablet
		// results
		shouldErr        bool
		errShouldContain string
	}{
		{
			name:                 "success",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {
						Result: "ok",
						Error:  nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26": nil,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "most up-to-date position, wins election",
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
		{
			// Here, all our tablets are tied, so we're going to explicitly pick
			// zone1-101.
			name: "success with requested primary-elect",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			}},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000101": {
						Error: nil,
					},
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000101": {
						Result: "ok",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000102": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
		{
			name:                 "success with existing primary",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
						},
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {
						Result: "ok",
						Error:  nil,
					},
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
					"zone1-0000000100": { // This tablet claims PRIMARY, so is not running replication.
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26": nil,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "most up-to-date position, wins election",
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
		{
			name:                 "shard not found",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc:                  &testutil.TabletManagerClient{},
			unlockTopo:           true, // we shouldn't try to lock the nonexistent shard
			shards:               nil,
			keyspace:             "testkeyspace",
			shard:                "-",
			ts:                   memorytopo.NewServer("zone1"),
			shouldErr:            true,
			errShouldContain:     "node doesn't exist: keyspaces/testkeyspace/shards/-/Shard",
		},
		{
			name:                 "cannot stop replication",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					// We actually need >1 to fail here.
					"zone1-0000000100": {
						Error: assert.AnError,
					},
					"zone1-0000000101": {
						Error: assert.AnError,
					},
					"zone1-0000000102": {
						Error: assert.AnError,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "failed to stop replication and build status maps",
		},
		{
			name:                 "lost topo lock",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{},
					},
				},
			},
			unlockTopo: true,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "lost topology lock, aborting",
		},
		{
			name:                 "cannot get reparent candidates",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{},
						},
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "has a zero relay log position",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "encountered tablet zone1-0000000102 with no relay log position",
		},
		{
			name:                 "zero valid reparent candidates",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc:                  &testutil.TabletManagerClient{},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			name: "error waiting for relay logs to apply",
			// one replica is going to take a minute to apply relay logs
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
				},
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": assert.AnError,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "slow to apply relay logs",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "fails to apply relay logs",
				},
			},
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			errShouldContain: "could not apply all relay logs within the provided waitReplicasTimeout",
		},
		{
			name: "requested primary-elect is not in tablet map",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  200,
			}},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "primary elect zone1-0000000200 has errant GTIDs",
		},
		{
			name: "requested primary-elect is not winning primary-elect",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{ // we're requesting a tablet that's behind in replication
				Cell: "zone1",
				Uid:  102,
			}},
			keyspace: "testkeyspace",
			shard:    "-",
			ts:       memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {
						Result: "ok",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
						Error:    nil,
					},
					"zone1-0000000102": {
						Error: nil,
					},
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20": nil,
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "not most up-to-date position",
				},
			},
			shouldErr: false,
		},
		{
			name: "cannot promote new primary",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			}},
			tmc: &testutil.TabletManagerClient{
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {
						Error: assert.AnError,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "not most up-to-date position",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "failed to be upgraded to primary",
		},
		{
			name:                 "constraint failure - promotion-rule",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {
						Result: "ok",
						Error:  nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26": nil,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_RDONLY,
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
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "most up-to-date position, wins election",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "elected primary does not satisfy promotion rule constraint",
		},
		{
			name:                 "constraint failure - cross-cell",
			emergencyReparentOps: EmergencyReparentOptions{PreventCrossCellPromotion: true},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {
						Result: "ok",
						Error:  nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000101": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26": nil,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  100,
						},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "most up-to-date position, wins election",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "failed previous primary",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1", "zone2"),
			shouldErr:        true,
			errShouldContain: "elected primary does not satisfy geographic constraint",
		},
	}

	_ = SetDurabilityPolicy("none", nil)
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{}

			for i, tablet := range tt.tablets {
				if tablet.Type == topodatapb.TabletType_UNKNOWN {
					tablet.Type = topodatapb.TabletType_REPLICA
				}
				tt.tablets[i] = tablet
			}

			testutil.AddShards(ctx, t, tt.ts, tt.shards...)
			testutil.AddTablets(ctx, t, tt.ts, nil, tt.tablets...)

			if !tt.unlockTopo {
				lctx, unlock, lerr := tt.ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, lerr, "could not lock %s/%s for testing", tt.keyspace, tt.shard)

				defer func() {
					unlock(&lerr)
					require.NoError(t, lerr, "could not unlock %s/%s after test", tt.keyspace, tt.shard)
				}()

				ctx = lctx // make the reparentShardLocked call use the lock ctx
			}

			erp := NewEmergencyReparenter(tt.ts, tt.tmc, logger)

			err := erp.reparentShardLocked(ctx, ev, tt.keyspace, tt.shard, tt.emergencyReparentOps)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestEmergencyReparenter_promoteNewPrimary(t *testing.T) {
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
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.NewString("zone1-0000000404")},
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
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
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
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: fmt.Errorf("primary position error"),
					},
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
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
			emergencyReparentOps: EmergencyReparentOptions{},
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
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
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
			emergencyReparentOps: EmergencyReparentOptions{},
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
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
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
			emergencyReparentOps: EmergencyReparentOptions{WaitReplicasTimeout: time.Millisecond * 10},
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
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
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
			emergencyReparentOps: EmergencyReparentOptions{},
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
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
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

			erp := NewEmergencyReparenter(tt.ts, tt.tmc, logger)
			err := erp.promoteNewPrimary(ctx, ev, tabletInfo.Tablet, tt.emergencyReparentOps, tt.tabletMap, tt.statusMap)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestEmergencyReparenter_waitForAllRelayLogsToApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	waitReplicasTimeout := 50 * time.Millisecond
	tests := []struct {
		name       string
		tmc        *testutil.TabletManagerClient
		candidates map[string]mysql.Position
		tabletMap  map[string]*topo.TabletInfo
		statusMap  map[string]*replicationdatapb.StopReplicationStatus
		shouldErr  bool
	}{
		{
			name: "all tablet pass",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position1": nil,
					},
					"zone1-0000000101": {
						"position1": nil,
					},
				},
			},
			candidates: map[string]mysql.Position{
				"zone1-0000000100": {},
				"zone1-0000000101": {},
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
				"zone1-0000000101": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "one tablet fails",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position1": nil,
					},
					"zone1-0000000101": {
						"position1": nil,
					},
				},
			},
			candidates: map[string]mysql.Position{
				"zone1-0000000100": {},
				"zone1-0000000101": {},
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
				"zone1-0000000101": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position2", // cannot wait for the desired "position1", so we fail
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "multiple tablets fail",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position1": nil,
					},
					"zone1-0000000101": {
						"position2": nil,
					},
					"zone1-0000000102": {
						"position3": nil,
					},
				},
			},
			candidates: map[string]mysql.Position{
				"zone1-0000000100": {},
				"zone1-0000000101": {},
				"zone1-0000000102": {},
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
				"zone1-0000000101": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "one slow tablet",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position1": nil,
					},
					"zone1-0000000101": {
						"position1": nil,
					},
				},
			},
			candidates: map[string]mysql.Position{
				"zone1-0000000100": {},
				"zone1-0000000101": {},
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
				"zone1-0000000101": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			erp := NewEmergencyReparenter(nil, tt.tmc, logger)
			err := erp.waitForAllRelayLogsToApply(ctx, tt.candidates, tt.tabletMap, tt.statusMap, waitReplicasTimeout)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestEmergencyReparenterCounters(t *testing.T) {
	ersCounter.Set(0)
	ersSuccessCounter.Set(0)
	ersFailureCounter.Set(0)
	_ = SetDurabilityPolicy("none", nil)

	emergencyReparentOps := EmergencyReparentOptions{}
	tmc := &testutil.TabletManagerClient{
		PopulateReparentJournalResults: map[string]error{
			"zone1-0000000102": nil,
		},
		PromoteReplicaResults: map[string]struct {
			Result string
			Error  error
		}{
			"zone1-0000000102": {
				Result: "ok",
				Error:  nil,
			},
		},
		MasterPositionResults: map[string]struct {
			Position string
			Error    error
		}{
			"zone1-0000000102": {
				Error: nil,
			},
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
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{},
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
					},
				},
			},
			"zone1-0000000101": {
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{},
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
					},
				},
			},
			"zone1-0000000102": {
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{},
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26",
					},
				},
			},
		},
		WaitForPositionResults: map[string]map[string]error{
			"zone1-0000000100": {
				"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
			},
			"zone1-0000000101": {
				"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
			},
			"zone1-0000000102": {
				"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26": nil,
			},
		},
	}
	shards := []*vtctldatapb.Shard{
		{
			Keyspace: "testkeyspace",
			Name:     "-",
		},
	}
	tablets := []*topodatapb.Tablet{
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Keyspace: "testkeyspace",
			Shard:    "-",
		},
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Keyspace: "testkeyspace",
			Shard:    "-",
		},
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			},
			Keyspace: "testkeyspace",
			Shard:    "-",
			Hostname: "most up-to-date position, wins election",
		},
	}
	keyspace := "testkeyspace"
	shard := "-"
	ts := memorytopo.NewServer("zone1")

	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	for i, tablet := range tablets {
		tablet.Type = topodatapb.TabletType_REPLICA
		tablets[i] = tablet
	}

	testutil.AddShards(ctx, t, ts, shards...)
	testutil.AddTablets(ctx, t, ts, nil, tablets...)

	erp := NewEmergencyReparenter(ts, tmc, logger)

	// run a successful ers
	_, err := erp.ReparentShard(ctx, keyspace, shard, emergencyReparentOps)
	require.NoError(t, err)

	// check the counter values
	require.EqualValues(t, 1, ersCounter.Get())
	require.EqualValues(t, 1, ersSuccessCounter.Get())
	require.EqualValues(t, 0, ersFailureCounter.Get())

	// set emergencyReparentOps to request a non existent tablet
	emergencyReparentOps.NewPrimaryAlias = &topodatapb.TabletAlias{
		Cell: "bogus",
		Uid:  100,
	}

	// run a failing ers
	_, err = erp.ReparentShard(ctx, keyspace, shard, emergencyReparentOps)
	require.Error(t, err)

	// check the counter values
	require.EqualValues(t, 2, ersCounter.Get())
	require.EqualValues(t, 1, ersSuccessCounter.Get())
	require.EqualValues(t, 1, ersFailureCounter.Get())
}

func TestEmergencyReparenter_findMostAdvanced(t *testing.T) {
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

	positionOnly2 := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}
	positionOnly2.GTIDSet = positionOnly2.GTIDSet.AddGTID(mysqlGTID2)

	positionEmpty := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}

	tests := []struct {
		name                 string
		validCandidates      map[string]mysql.Position
		tabletMap            map[string]*topo.TabletInfo
		prevPrimary          *topodatapb.Tablet
		emergencyReparentOps EmergencyReparentOptions
		result               *topodatapb.Tablet
		err                  string
	}{
		{
			name: "choose most advanced - with nil previous primary",
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
			prevPrimary: nil,
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}, {
			name: "choose most advanced in the same cell of previous primary",
			validCandidates: map[string]mysql.Position{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionIntermediate1,
				"zone2-0000000100": positionMostAdvanced,
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
				"zone2-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  100,
						},
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
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}, {
			name: "choose most advanced with the best promotion rule",
			validCandidates: map[string]mysql.Position{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionIntermediate1,
				"zone1-0000000102": positionMostAdvanced,
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
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_RDONLY,
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
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}, {
			name: "choose most advanced with explicit request",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			}},
			validCandidates: map[string]mysql.Position{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionIntermediate1,
				"zone1-0000000102": positionMostAdvanced,
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
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_RDONLY,
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
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			},
		}, {
			name: "split brain detection",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			}},
			validCandidates: map[string]mysql.Position{
				"zone1-0000000100": positionOnly2,
				"zone1-0000000101": positionIntermediate1,
				"zone1-0000000102": positionEmpty,
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
					},
				},
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_RDONLY,
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
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			err: "split brain detected between servers",
		},
	}

	_ = SetDurabilityPolicy("none", nil)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			erp := NewEmergencyReparenter(nil, nil, logutil.NewMemoryLogger())

			winningTablet, _, err := erp.findMostAdvanced(test.prevPrimary, test.validCandidates, test.tabletMap, test.emergencyReparentOps)
			if test.err != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err)
			} else {
				assert.NoError(t, err)
				assert.True(t, topoproto.TabletAliasEqual(test.result.Alias, winningTablet.Alias))
			}
		})
	}
}

func TestEmergencyReparenter_checkIfConstraintsSatisfied(t *testing.T) {
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
			opts: EmergencyReparentOptions{PreventCrossCellPromotion: true},
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
			opts: EmergencyReparentOptions{PreventCrossCellPromotion: true},
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
			opts: EmergencyReparentOptions{PreventCrossCellPromotion: true},
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
			opts: EmergencyReparentOptions{PreventCrossCellPromotion: false},
			err:  "",
		},
	}

	_ = SetDurabilityPolicy("none", nil)
	erp := NewEmergencyReparenter(nil, nil, nil)

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			err := erp.checkIfConstraintsSatisfied(testcase.newPrimary, testcase.prevPrimary, testcase.opts)
			if testcase.err == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testcase.err)
			}
		})
	}
}

func TestEmergencyReparenter_reparentReplicas(t *testing.T) {
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
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.NewString("zone1-0000000404")},
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
			emergencyReparentOps: EmergencyReparentOptions{},
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
			emergencyReparentOps: EmergencyReparentOptions{},
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
			emergencyReparentOps: EmergencyReparentOptions{},
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
			emergencyReparentOps: EmergencyReparentOptions{WaitReplicasTimeout: time.Millisecond * 10},
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
			emergencyReparentOps: EmergencyReparentOptions{},
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

			erp := NewEmergencyReparenter(tt.ts, tt.tmc, logger)
			_, err := erp.reparentReplicas(ctx, ev, tabletInfo.Tablet, tt.tabletMap, tt.statusMap, tt.emergencyReparentOps, false, true)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestEmergencyReparenter_promoteIntermediatePrimary(t *testing.T) {
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
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.NewString("zone1-0000000404")},
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
			emergencyReparentOps: EmergencyReparentOptions{},
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
			emergencyReparentOps: EmergencyReparentOptions{},
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

			erp := NewEmergencyReparenter(tt.ts, tt.tmc, logger)
			res, err := erp.promoteIntermediatePrimary(ctx, ev, tabletInfo.Tablet, tt.tabletMap, tt.statusMap, tt.emergencyReparentOps)
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

func TestEmergencyReparenter_identifyPrimaryCandidate(t *testing.T) {
	tests := []struct {
		name                 string
		emergencyReparentOps EmergencyReparentOptions
		newPrimary           *topodatapb.Tablet
		prevPrimary          *topodatapb.Tablet
		validCandidates      []*topodatapb.Tablet
		tabletMap            map[string]*topo.TabletInfo
		err                  string
		result               *topodatapb.Tablet
	}{
		{
			name: "explicit request for a primary tablet",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			}},
			newPrimary:  nil,
			prevPrimary: nil,
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
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
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}, {
			name: "explicit request for a primary tablet not in valid list",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			}},
			newPrimary:      nil,
			prevPrimary:     nil,
			validCandidates: nil,
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
			err: "requested candidate zone1-0000000100 is not in valid candidates list",
		}, {
			name: "explicit request for a primary tablet not in tablet map",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			}},
			newPrimary:  nil,
			prevPrimary: nil,
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tabletMap: map[string]*topo.TabletInfo{},
			err:       "candidate zone1-0000000100 not found in the tablet map; this an impossible situation",
		}, {
			name:                 "preferred candidate in the same cell same as our replica",
			emergencyReparentOps: EmergencyReparentOptions{},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type: topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  101,
					},
					Type: topodatapb.TabletType_PRIMARY,
				},
			},
			tabletMap: nil,
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}, {
			name:                 "preferred candidate in the same cell different from original replica",
			emergencyReparentOps: EmergencyReparentOptions{},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type: topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  101,
					},
					Type: topodatapb.TabletType_PRIMARY,
				},
			},
			tabletMap: nil,
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
		}, {
			name:                 "preferred candidate in the different cell same as original replica",
			emergencyReparentOps: EmergencyReparentOptions{},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  101,
				},
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  101,
					},
					Type: topodatapb.TabletType_PRIMARY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  102,
					},
					Type: topodatapb.TabletType_PRIMARY,
				},
			},
			tabletMap: nil,
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  101,
				},
			},
		}, {
			name:                 "preferred candidate in the different cell different from original replica",
			emergencyReparentOps: EmergencyReparentOptions{},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  101,
				},
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  101,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  102,
					},
					Type: topodatapb.TabletType_PRIMARY,
				},
			},
			tabletMap: nil,
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  102,
				},
			},
		}, {
			name:                 "prevent cross cell promotion",
			emergencyReparentOps: EmergencyReparentOptions{PreventCrossCellPromotion: true},
			newPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
				},
			},
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  101,
					},
					Type: topodatapb.TabletType_RDONLY,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  102,
					},
					Type: topodatapb.TabletType_PRIMARY,
				},
			},
			tabletMap: nil,
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_ = SetDurabilityPolicy("none", nil)
			logger := logutil.NewMemoryLogger()

			erp := NewEmergencyReparenter(nil, nil, logger)
			res, err := erp.identifyPrimaryCandidate(test.newPrimary, test.newPrimary, test.validCandidates, test.tabletMap, test.emergencyReparentOps)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
				return
			}
			assert.NoError(t, err)
			assert.True(t, topoproto.TabletAliasEqual(res.Alias, test.result.Alias))
		})
	}
}
