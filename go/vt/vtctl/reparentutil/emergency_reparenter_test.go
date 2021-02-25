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
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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
		name string
		// setup
		ts         *topo.Server
		tmc        *testutil.TabletManagerClient
		unlockTopo bool
		shards     []*vtctldatapb.Shard
		tablets    []*topodatapb.Tablet
		// params
		keyspace string
		shard    string
		opts     EmergencyReparentOptions
		// results
		shouldErr bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1"),
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
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
			opts:      EmergencyReparentOptions{},
			shouldErr: false,
		},
		{
			// Here, all our tablets are tied, so we're going to explicitly pick
			// zone1-101.
			name: "success with requested primary-elect",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
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
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
			keyspace: "testkeyspace",
			shard:    "-",
			opts: EmergencyReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
			shouldErr: false,
		},
		{
			name: "success with existing primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
						},
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
					"zone1-0000000100": { // This tablet claims MASTER, so is not running replication.
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
					Type:     topodatapb.TabletType_MASTER,
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
			opts:      EmergencyReparentOptions{},
			shouldErr: false,
		},
		{
			name:       "shard not found",
			ts:         memorytopo.NewServer("zone1"),
			tmc:        &testutil.TabletManagerClient{},
			unlockTopo: true, // we shouldn't try to lock the nonexistent shard
			shards:     nil,
			keyspace:   "testkeyspace",
			shard:      "-",
			opts:       EmergencyReparentOptions{},
			shouldErr:  true,
		},
		{
			name: "cannot stop replication",
			ts:   memorytopo.NewServer("zone1"),
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
			keyspace:  "testkeyspace",
			shard:     "-",
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "lost topo lock",
			ts:   memorytopo.NewServer("zone1"),
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
			keyspace:  "testkeyspace",
			shard:     "-",
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "cannot get reparent candidates",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
			keyspace:  "testkeyspace",
			shard:     "-",
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "zero valid reparent candidates",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  &testutil.TabletManagerClient{},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "error waiting for relay logs to apply",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
			keyspace: "testkeyspace",
			shard:    "-",
			opts: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50, // one replica is going to take a minute to apply relay logs
			},
			shouldErr: true,
		},
		{
			name: "requested primary-elect is not in tablet map",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
			keyspace: "testkeyspace",
			shard:    "-",
			opts: EmergencyReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
		{
			name: "requested primary-elect is not winning primary-elect",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21": nil,
					},
					"zone1-0000000102": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20": nil,
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
			keyspace: "testkeyspace",
			shard:    "-",
			opts: EmergencyReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{ // we're requesting a tablet that's behind in replication
					Cell: "zone1",
					Uid:  102,
				},
			},
			shouldErr: true,
		},
		{
			name: "cannot promote new primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {
						Error: assert.AnError,
					},
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
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
					Hostname: "not most up-to-date position",
				},
			},
			keyspace: "testkeyspace",
			shard:    "-",
			opts: EmergencyReparentOptions{
				// We're explicitly requesting a primary-elect in this test case
				// because we don't care about the correctness of the selection
				// code (it's covered by other test cases), and it simplifies
				// the error mocking.
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{}

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

			err := erp.reparentShardLocked(ctx, ev, tt.keyspace, tt.shard, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)

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
		ts                    *topo.Server
		tmc                   *testutil.TabletManagerClient
		unlockTopo            bool
		keyspace              string
		shard                 string
		newPrimaryTabletAlias string
		tabletMap             map[string]*topo.TabletInfo
		statusMap             map[string]*replicationdatapb.StopReplicationStatus
		opts                  EmergencyReparentOptions
		shouldErr             bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
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
			keyspace:              "testkeyspace",
			shard:                 "-",
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
			opts: EmergencyReparentOptions{
				IgnoreReplicas: sets.NewString("zone1-0000000404"),
			},
			shouldErr: false,
		},
		{
			name:                  "primary not in tablet map",
			ts:                    memorytopo.NewServer("zone1"),
			tmc:                   &testutil.TabletManagerClient{},
			keyspace:              "testkeyspace",
			shard:                 "-",
			newPrimaryTabletAlias: "zone2-0000000200",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {},
				"zone1-0000000101": {},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "PromoteReplica error",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
			},
			keyspace:              "testkeyspace",
			shard:                 "-",
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "lost topology lock",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
			},
			unlockTopo:            true,
			keyspace:              "testkeyspace",
			shard:                 "-",
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "cannot repopulate reparent journal on new primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": assert.AnError,
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
			keyspace:              "testkeyspace",
			shard:                 "-",
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "all replicas failing to SetMaster does fail the promotion",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
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
			keyspace:              "testkeyspace",
			shard:                 "-",
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			opts:      EmergencyReparentOptions{},
			shouldErr: true,
		},
		{
			name: "all replicas slow to SetMaster does fail the promotion",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
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
			keyspace:              "testkeyspace",
			shard:                 "-",
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
			opts: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 10,
			},
			shouldErr: true,
		},
		{
			name: "one replica failing to SetMaster does not fail the promotion",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
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
			keyspace:              "testkeyspace",
			shard:                 "-",
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

			erp := NewEmergencyReparenter(tt.ts, tt.tmc, logger)

			err := erp.promoteNewPrimary(ctx, ev, tt.keyspace, tt.shard, tt.newPrimaryTabletAlias, tt.tabletMap, tt.statusMap, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)

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
	opts := EmergencyReparentOptions{
		WaitReplicasTimeout: time.Millisecond * 50,
	}
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
			err := erp.waitForAllRelayLogsToApply(ctx, tt.candidates, tt.tabletMap, tt.statusMap, opts)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}
