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
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"

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
	tests := []struct {
		name                 string
		durability           string
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
			durability:           "none",
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			name:                 "success - 1 replica and 1 rdonly failure",
			durability:           "semi_sync",
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-26",
							},
						},
					},
					"zone1-0000000103": {
						Error: assert.AnError,
					},
					"zone1-0000000104": {
						Error: assert.AnError,
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
						Cell: "zone1",
						Uid:  103,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  104,
					},
					Type:     topodatapb.TabletType_RDONLY,
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
			// Here, all our tablets are tied, so we're going to explicitly pick
			// zone1-101.
			name:       "success with requested primary-elect",
			durability: "none",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			}},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
				},
				PrimaryPositionResults: map[string]struct {
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
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000102": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			durability:           "none",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
						},
					},
				},
				PrimaryPositionResults: map[string]struct {
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
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": { // This tablet claims PRIMARY, so is not running replication.
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			durability:           "none",
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
			durability:           "none",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
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
			durability:           "none",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)}},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)}},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)}},
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
			durability:           "none",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After:  &replicationdatapb.Status{},
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
			durability:           "none",
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
			name:       "error waiting for relay logs to apply",
			durability: "none",
			// one replica is going to take a minute to apply relay logs
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			name:       "requested primary-elect is not in tablet map",
			durability: "none",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  200,
			}},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			name:       "requested primary-elect is not winning primary-elect",
			durability: "none",
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
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
				PrimaryPositionResults: map[string]struct {
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
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			name:       "cannot promote new primary",
			durability: "none",
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
				PrimaryPositionResults: map[string]struct {
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
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
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
			name:                 "promotion-rule - no valid candidates for emergency reparent",
			durability:           "none",
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			name:       "proposed primary - must not promotion rule",
			durability: "none",
			emergencyReparentOps: EmergencyReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			},
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
					Hostname: "proposed primary",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "proposed primary zone1-0000000102 has a must not promotion rule",
		},
		{
			name:                 "cross cell - no valid candidates",
			durability:           "none",
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			name:       "proposed primary in a different cell",
			durability: "none",
			emergencyReparentOps: EmergencyReparentOptions{
				PreventCrossCellPromotion: true,
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			},
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
					Hostname: "proposed primary",
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
			errShouldContain: "proposed primary zone1-0000000102 is is a different cell as the previous primary",
		},
		{
			name:       "proposed primary cannot make progress",
			durability: "cross_cell",
			emergencyReparentOps: EmergencyReparentOptions{
				NewPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			},
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000102": {
						Error: nil,
					},
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
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
							Cell: "zone1",
							Uid:  101,
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
					Hostname: "proposed primary",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			ts:               memorytopo.NewServer("zone1"),
			shouldErr:        true,
			errShouldContain: "proposed primary zone1-0000000102 will not be able to make forward progress on being promoted",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {

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
			reparenttestutil.SetKeyspaceDurability(ctx, t, tt.ts, tt.keyspace, tt.durability)

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
		initializationTest    bool
	}{
		{
			name:                 "success",
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.NewString("zone1-0000000404")},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
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
				SetReplicationSourceResults: map[string]error{
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
						IoState:  int32(mysql.ReplicationStateStopped),
						SqlState: int32(mysql.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(mysql.ReplicationStateRunning),
						SqlState: int32(mysql.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
		{
			name:                 "PrimaryPosition error",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionResults: map[string]struct {
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
				PrimaryPositionResults: map[string]struct {
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
			name:                 "all replicas failing to SetReplicationSource does fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
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

				SetReplicationSourceResults: map[string]error{
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
			name:                 "all replicas slow to SetReplicationSource does fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{WaitReplicasTimeout: time.Millisecond * 10},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
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

				SetReplicationSourceDelays: map[string]time.Duration{
					// nothing is failing, we're just slow
					"zone1-0000000101": time.Millisecond * 100,
					"zone1-0000000102": time.Millisecond * 75,
				},
				SetReplicationSourceResults: map[string]error{
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
			name:                 "one replica failing to SetReplicationSource does not fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
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

				SetReplicationSourceResults: map[string]error{
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
		{
			name:                 "success in initialization",
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.NewString("zone1-0000000404")},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				InitPrimaryResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
					"zone1-0000000404": assert.AnError, // okay, because we're ignoring it.
				},
			},
			initializationTest:    true,
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
						IoState:  int32(mysql.ReplicationStateStopped),
						SqlState: int32(mysql.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(mysql.ReplicationStateRunning),
						SqlState: int32(mysql.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
	}

	durability, _ := GetDurabilityPolicy("none")
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{ShardInfo: topo.ShardInfo{
				Shard: &topodatapb.Shard{
					PrimaryAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			}}
			if tt.initializationTest {
				ev.ShardInfo.PrimaryAlias = nil
			}

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

			tt.emergencyReparentOps.durability = durability

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
		PrimaryPositionResults: map[string]struct {
			Position string
			Error    error
		}{
			"zone1-0000000102": {
				Error: nil,
			},
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
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
					},
				},
			},
			"zone1-0000000101": {
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
					},
				},
			},
			"zone1-0000000102": {
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
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
			Type:     topodatapb.TabletType_PRIMARY,
			Keyspace: "testkeyspace",
			Shard:    "-",
		},
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Type:     topodatapb.TabletType_REPLICA,
			Keyspace: "testkeyspace",
			Shard:    "-",
		},
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			},
			Type:     topodatapb.TabletType_REPLICA,
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

	testutil.AddShards(ctx, t, ts, shards...)
	testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
		AlsoSetShardPrimary: true,
		SkipShardCreation:   false,
	}, tablets...)

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
		emergencyReparentOps EmergencyReparentOptions
		result               *topodatapb.Tablet
		err                  string
	}{
		{
			name: "choose most advanced",
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
			err: "split brain detected between servers",
		},
	}

	durability, _ := GetDurabilityPolicy("none")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			erp := NewEmergencyReparenter(nil, nil, logutil.NewMemoryLogger())

			test.emergencyReparentOps.durability = durability
			winningTablet, _, err := erp.findMostAdvanced(test.validCandidates, test.tabletMap, test.emergencyReparentOps)
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
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
						IoState:  int32(mysql.ReplicationStateStopped),
						SqlState: int32(mysql.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(mysql.ReplicationStateRunning),
						SqlState: int32(mysql.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			ts:        memorytopo.NewServer("zone1"),
			shouldErr: false,
		},
		{
			name:                 "PrimaryPosition error",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionResults: map[string]struct {
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
				PrimaryPositionResults: map[string]struct {
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
			name:                 "all replicas failing to SetReplicationSource does fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},

				SetReplicationSourceResults: map[string]error{
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
			name:                 "all replicas slow to SetReplicationSource does fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{WaitReplicasTimeout: time.Millisecond * 10},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetReplicationSourceDelays: map[string]time.Duration{
					// nothing is failing, we're just slow
					"zone1-0000000101": time.Millisecond * 100,
					"zone1-0000000102": time.Millisecond * 75,
				},
				SetReplicationSourceResults: map[string]error{
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
			name:                 "one replica failing to SetReplicationSource does not fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
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

	durability, _ := GetDurabilityPolicy("none")
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

			tt.emergencyReparentOps.durability = durability

			erp := NewEmergencyReparenter(tt.ts, tt.tmc, logger)
			_, err := erp.reparentReplicas(ctx, ev, tabletInfo.Tablet, tt.tabletMap, tt.statusMap, tt.emergencyReparentOps, false /* waitForAllReplicas */, true /* populateReparentJournal */)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestEmergencyReparenter_promoteIntermediateSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		emergencyReparentOps  EmergencyReparentOptions
		tmc                   *testutil.TabletManagerClient
		unlockTopo            bool
		newSourceTabletAlias  string
		ts                    *topo.Server
		keyspace              string
		shard                 string
		tablets               []*topodatapb.Tablet
		validCandidateTablets []*topodatapb.Tablet
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
					"zone1-0000000404": assert.AnError, // okay, because we're ignoring it.
				},
			},
			newSourceTabletAlias: "zone1-0000000100",
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
						IoState:  int32(mysql.ReplicationStateStopped),
						SqlState: int32(mysql.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(mysql.ReplicationStateRunning),
						SqlState: int32(mysql.ReplicationStateRunning),
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
			validCandidateTablets: []*topodatapb.Tablet{
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},

				SetReplicationSourceResults: map[string]error{
					// everyone fails, we all fail
					"zone1-0000000101": assert.AnError,
					"zone1-0000000102": assert.AnError,
				},
			},
			newSourceTabletAlias: "zone1-0000000100",
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
			validCandidateTablets: []*topodatapb.Tablet{
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
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil, // this one succeeds, so we're good
					"zone1-0000000102": assert.AnError,
				},
			},
			newSourceTabletAlias: "zone1-0000000100",
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
			validCandidateTablets: []*topodatapb.Tablet{
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
		{
			name:                 "success - filter using valid candidate list",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			newSourceTabletAlias: "zone1-0000000100",
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
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000101": { // forceStart = false
					Before: &replicationdatapb.Status{
						IoState:  int32(mysql.ReplicationStateStopped),
						SqlState: int32(mysql.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(mysql.ReplicationStateRunning),
						SqlState: int32(mysql.ReplicationStateRunning),
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
			},
			validCandidateTablets: []*topodatapb.Tablet{
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
			},
		},
	}

	durability, _ := GetDurabilityPolicy("none")
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
			tabletInfo := tt.tabletMap[tt.newSourceTabletAlias]

			tt.emergencyReparentOps.durability = durability

			erp := NewEmergencyReparenter(tt.ts, tt.tmc, logger)
			res, err := erp.promoteIntermediateSource(ctx, ev, tabletInfo.Tablet, tt.tabletMap, tt.statusMap, tt.validCandidateTablets, tt.emergencyReparentOps)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
			require.Equal(t, len(tt.result), len(res))
			for idx, tablet := range res {
				assert.EqualValues(t, topoproto.TabletAliasString(tt.result[idx].Alias), topoproto.TabletAliasString(tablet.Alias))
			}
		})
	}
}

func TestEmergencyReparenter_identifyPrimaryCandidate(t *testing.T) {
	tests := []struct {
		name                 string
		emergencyReparentOps EmergencyReparentOptions
		intermediateSource   *topodatapb.Tablet
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
			intermediateSource: nil,
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
			name:            "empty valid list",
			validCandidates: nil,
			err:             "no valid candidates for emergency reparent",
		}, {
			name: "explicit request for a primary tablet not in valid list",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			}},
			intermediateSource: nil,
			validCandidates: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
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
			err: "requested candidate zone1-0000000100 is not in valid candidates list",
		}, {
			name: "explicit request for a primary tablet not in tablet map",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			}},
			intermediateSource: nil,
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
			name:                 "preferred candidate in the valid list with the best promote rule",
			emergencyReparentOps: EmergencyReparentOptions{},
			intermediateSource: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
			name:                 "preferred candidate has non optimal promotion rule",
			emergencyReparentOps: EmergencyReparentOptions{},
			intermediateSource: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  101,
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
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			durability, _ := GetDurabilityPolicy("none")
			test.emergencyReparentOps.durability = durability
			logger := logutil.NewMemoryLogger()

			erp := NewEmergencyReparenter(nil, nil, logger)
			res, err := erp.identifyPrimaryCandidate(test.intermediateSource, test.validCandidates, test.tabletMap, test.emergencyReparentOps)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
				return
			}
			assert.NoError(t, err)
			assert.True(t, topoproto.TabletAliasEqual(res.Alias, test.result.Alias))
		})
	}
}

// TestParentContextCancelled tests that even if the parent context of reparentReplicas cancels, we should not cancel the context of
// SetReplicationSource since there could be tablets that are running it even after ERS completes.
func TestParentContextCancelled(t *testing.T) {
	durability, err := GetDurabilityPolicy("none")
	require.NoError(t, err)
	// Setup ERS options with a very high wait replicas timeout
	emergencyReparentOps := EmergencyReparentOptions{IgnoreReplicas: sets.NewString("zone1-0000000404"), WaitReplicasTimeout: time.Minute, durability: durability}
	// Make the replica tablet return its results after 3 seconds
	tmc := &testutil.TabletManagerClient{
		PrimaryPositionResults: map[string]struct {
			Position string
			Error    error
		}{
			"zone1-0000000100": {
				Error: nil,
			},
		},
		SetReplicationSourceResults: map[string]error{
			"zone1-0000000101": nil,
		},
		SetReplicationSourceDelays: map[string]time.Duration{
			"zone1-0000000101": 3 * time.Second,
		},
	}
	newPrimaryTabletAlias := "zone1-0000000100"
	tabletMap := map[string]*topo.TabletInfo{
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
	}
	statusMap := map[string]*replicationdatapb.StopReplicationStatus{
		"zone1-0000000101": {
			Before: &replicationdatapb.Status{
				IoState:  int32(mysql.ReplicationStateRunning),
				SqlState: int32(mysql.ReplicationStateRunning),
			},
		},
	}
	keyspace := "testkeyspace"
	shard := "-"
	ts := memorytopo.NewServer("zone1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := logutil.NewMemoryLogger()
	ev := &events.Reparent{}

	testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{
		Keyspace: keyspace,
		Name:     shard,
	})

	erp := NewEmergencyReparenter(ts, tmc, logger)
	// Cancel the parent context after 1 second. Even though the parent context is cancelled, the command should still succeed
	// We should not be cancelling the context of the RPC call to SetReplicationSource since some tablets may keep on running this even after
	// ERS returns
	go func() {
		time.Sleep(time.Second)
		cancel()
	}()
	_, err = erp.reparentReplicas(ctx, ev, tabletMap[newPrimaryTabletAlias].Tablet, tabletMap, statusMap, emergencyReparentOps, false, false)
	require.NoError(t, err)
}

func TestEmergencyReparenter_filterValidCandidates(t *testing.T) {
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
		name             string
		durability       string
		validTablets     []*topodatapb.Tablet
		tabletsReachable []*topodatapb.Tablet
		prevPrimary      *topodatapb.Tablet
		opts             EmergencyReparentOptions
		filteredTablets  []*topodatapb.Tablet
		errShouldContain string
	}{
		{
			name:             "filter must not",
			durability:       "none",
			validTablets:     allTablets,
			tabletsReachable: allTablets,
			filteredTablets:  []*topodatapb.Tablet{primaryTablet, replicaTablet, replicaCrossCellTablet},
		}, {
			name:             "filter cross cell",
			durability:       "none",
			validTablets:     allTablets,
			tabletsReachable: allTablets,
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone-1",
				},
			},
			opts: EmergencyReparentOptions{
				PreventCrossCellPromotion: true,
			},
			filteredTablets: []*topodatapb.Tablet{primaryTablet, replicaTablet},
		}, {
			name:             "filter establish",
			durability:       "cross_cell",
			validTablets:     []*topodatapb.Tablet{primaryTablet, replicaTablet},
			tabletsReachable: []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, rdonlyCrossCellTablet},
			filteredTablets:  nil,
		}, {
			name:       "filter mixed",
			durability: "cross_cell",
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone-2",
				},
			},
			opts: EmergencyReparentOptions{
				PreventCrossCellPromotion: true,
			},
			validTablets:     allTablets,
			tabletsReachable: allTablets,
			filteredTablets:  []*topodatapb.Tablet{replicaCrossCellTablet},
		}, {
			name:             "error - requested primary must not",
			durability:       "none",
			validTablets:     allTablets,
			tabletsReachable: allTablets,
			opts: EmergencyReparentOptions{
				NewPrimaryAlias: rdonlyTablet.Alias,
			},
			errShouldContain: "proposed primary zone-1-0000000003 has a must not promotion rule",
		}, {
			name:             "error - requested primary not in same cell",
			durability:       "none",
			validTablets:     allTablets,
			tabletsReachable: allTablets,
			prevPrimary:      primaryTablet,
			opts: EmergencyReparentOptions{
				PreventCrossCellPromotion: true,
				NewPrimaryAlias:           replicaCrossCellTablet.Alias,
			},
			errShouldContain: "proposed primary zone-2-0000000002 is is a different cell as the previous primary",
		}, {
			name:             "error - requested primary cannot establish",
			durability:       "cross_cell",
			validTablets:     allTablets,
			tabletsReachable: []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, rdonlyCrossCellTablet},
			opts: EmergencyReparentOptions{
				NewPrimaryAlias: primaryTablet.Alias,
			},
			errShouldContain: "proposed primary zone-1-0000000001 will not be able to make forward progress on being promoted",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			durability, err := GetDurabilityPolicy(tt.durability)
			require.NoError(t, err)
			tt.opts.durability = durability
			logger := logutil.NewMemoryLogger()
			erp := NewEmergencyReparenter(nil, nil, logger)
			tabletList, err := erp.filterValidCandidates(tt.validTablets, tt.tabletsReachable, tt.prevPrimary, tt.opts)
			if tt.errShouldContain != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errShouldContain)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, tt.filteredTablets, tabletList)
			}
		})
	}
}
