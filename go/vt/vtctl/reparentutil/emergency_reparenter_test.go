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
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql/replication"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
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
		cells      []string
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
			durability:           policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:     []string{"zone1"},
			shouldErr: false,
		},
		{
			name:                 "success - 1 replica and 1 rdonly failure",
			durability:           policy.DurabilitySemiSync,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:     []string{"zone1"},
			shouldErr: false,
		},
		{
			// Here, all our tablets are tied, so we're going to explicitly pick
			// zone1-101.
			name:       "success with requested primary-elect",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			}},
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:     []string{"zone1"},
			shouldErr: false,
		},
		{
			name:                 "success with existing primary",
			durability:           policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:     []string{"zone1"},
			shouldErr: false,
		},
		{
			name:                 "shard not found",
			durability:           policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc:                  &testutil.TabletManagerClient{},
			unlockTopo:           true, // we shouldn't try to lock the nonexistent shard
			shards:               nil,
			keyspace:             "testkeyspace",
			shard:                "-",
			cells:                []string{"zone1"},
			shouldErr:            true,
			errShouldContain:     "node doesn't exist: keyspaces/testkeyspace/shards/-/Shard",
		},
		{
			name:                 "cannot stop replication",
			durability:           policy.DurabilityNone,
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "failed to stop replication and build status maps",
		},
		{
			name:                 "lost topo lock",
			durability:           policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)}},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)}},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)}},
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "lost topology lock, aborting",
		},
		{
			name:                 "cannot get reparent candidates",
			durability:           policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "encountered tablet zone1-0000000102 with no relay log position",
		},
		{
			name:                 "zero valid reparent candidates",
			durability:           policy.DurabilityNone,
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
			cells:            []string{"zone1"},
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			name:       "error waiting for relay logs to apply",
			durability: policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:            []string{"zone1"},
			errShouldContain: "could not apply all relay logs within the provided waitReplicasTimeout",
		},
		{
			name:       "requested primary-elect is not in tablet map",
			durability: policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "primary elect zone1-0000000200 has errant GTIDs",
		},
		{
			name:       "requested primary-elect is not winning primary-elect",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{ // we're requesting a tablet that's behind in replication
				Cell: "zone1",
				Uid:  102,
			}},
			keyspace: "testkeyspace",
			shard:    "-",
			cells:    []string{"zone1"},
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
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			durability: policy.DurabilityNone,
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
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "failed to be upgraded to primary",
		},
		{
			name:                 "promotion-rule - no valid candidates for emergency reparent",
			durability:           policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			name:       "proposed primary - must not promotion rule",
			durability: policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "proposed primary zone1-0000000102 has a must not promotion rule",
		},
		{
			name:                 "cross cell - no valid candidates",
			durability:           policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "failed previous primary",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1", "zone2"},
			shouldErr:        true,
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			name:       "proposed primary in a different cell",
			durability: policy.DurabilityNone,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "failed previous primary",
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1", "zone2"},
			shouldErr:        true,
			errShouldContain: "proposed primary zone1-0000000102 is is a different cell as the previous primary",
		},
		{
			name:       "proposed primary cannot make progress",
			durability: policy.DurabilityCrossCell,
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
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "proposed primary zone1-0000000102 will not be able to make forward progress on being promoted",
		},
		{
			name:       "expected primary mismatch",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				ExpectedPrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
			tmc: &testutil.TabletManagerClient{},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						IsPrimaryServing: true,
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
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "primary zone1-0000000100 is not equal to expected alias zone1-0000000101",
		},
		{
			// Regression test: if every candidate has mutually errant GTIDs, findErrantGTIDs
			// returns an empty map, which previously caused findMostAdvanced to panic with
			// "index out of range [0] with length 0" when indexing the empty tablet slice.
			name:                 "all candidates filtered out by errant GTID detection",
			durability:           policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000100": 3,
					"zone1-0000000101": 3,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100", "1-31", "1-50"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-51"),
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-100", "1-31", "1-50"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-100", "1-30", "1-51"): nil,
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
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
			shouldErr:        true,
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			// Regression: a MariaDB candidate (uid 102) is dropped by findErrantGTIDs
			// for an errant GTID, leaving MySQL-only survivors (100 at 8.4, 101 at
			// 8.0) at equal position. The version guard must be scoped to the reduced
			// candidate set — {100, 101}, both MySQL — so version-aware election still
			// runs and promotes the lower version (101). If the guard operated over
			// the full reachable set (which still includes the MariaDB 102), it would
			// see mixed families, disable version ordering, and the tie would fall to
			// the alias tiebreak, promoting 100 — whose PromoteReplica is not mocked,
			// so the reparent would error. In other words, this case only succeeds if
			// ERS passes the post-findErrantGTIDs reduced set to the election.
			name:                 "errant-GTID drop leaves same-family survivors that still get version ordering",
			durability:           policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				// Only 101 (the expected winner) can be promoted; if 100 were elected,
				// PromoteReplica would return an error and the test would fail.
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000101": {Result: "ok", Error: nil},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000102": nil,
				},
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000100": 3,
					"zone1-0000000101": 3,
					"zone1-0000000102": 3,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					// 100: MySQL 8.4, same position as 101.
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning), ServerVersion: "Ver 8.4.0"},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-20"),
							},
						},
					},
					// 101: MySQL 8.0 (lower version), same position as 100 -> should win.
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning), ServerVersion: "Ver 8.0.35"},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-20"),
							},
						},
					},
					// 102: MariaDB, carries an errant GTID (u2:1-5 that no one else has)
					// -> dropped by findErrantGTIDs before the election. Only its flavor
					// (mariadb) matters here — the parsed version is never compared
					// because the tablet is dropped. The version string is the
					// @@global.version form ERS actually sees in production.
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning), ServerVersion: "Ver 10.6.16-MariaDB-1:10.6.16+maria~ubu2004"},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-20", "1-5"),
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {getRelayLogPosition("1-20"): nil},
					"zone1-0000000101": {getRelayLogPosition("1-20"): nil},
					"zone1-0000000102": {getRelayLogPosition("1-20", "1-5"): nil},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, Keyspace: "testkeyspace", Shard: "-"},
				{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Keyspace: "testkeyspace", Shard: "-"},
				{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102}, Keyspace: "testkeyspace", Shard: "-"},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{}

			for i, tablet := range tt.tablets {
				if tablet.Type == topodatapb.TabletType_UNKNOWN {
					tablet.Type = topodatapb.TabletType_REPLICA
				}
				tt.tablets[i] = tablet
			}

			ts := memorytopo.NewServer(ctx, tt.cells...)
			defer ts.Close()
			testutil.AddShards(ctx, t, ts, tt.shards...)
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)
			reparenttestutil.SetKeyspaceDurability(ctx, t, ts, tt.keyspace, tt.durability)

			if !tt.unlockTopo {
				lctx, unlock, lerr := ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, lerr, "could not lock %s/%s for testing", tt.keyspace, tt.shard)

				defer func() {
					unlock(&lerr)
					require.NoError(t, lerr, "could not unlock %s/%s after test", tt.keyspace, tt.shard)
				}()

				ctx = lctx // make the reparentShardLocked call use the lock ctx
			}

			erp := NewEmergencyReparenter(ts, tt.tmc, logger)

			err := erp.reparentShardLocked(ctx, ev, tt.keyspace, tt.shard, tt.emergencyReparentOps)
			if tt.shouldErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
		})
	}
}

// TestEmergencyReparenterRestartsStoppedIOThreadsOnStopReplicationFailure verifies that ERS
// restarts replication on replicas whose IO threads it stopped when encountering a partial
// failure in the stop replication phase.
func TestEmergencyReparenterRestartsStoppedIOThreadsOnStopReplicationFailure(t *testing.T) {
	ctx := t.Context()

	const (
		keyspace           = "testkeyspace"
		shard              = "-"
		primaryAlias       = "zone1-0000000100"
		stoppedIOAlias     = "zone1-0000000101"
		connectingIOAlias  = "zone1-0000000102"
		failedReplicaAlias = "zone1-0000000103"
	)

	stoppedIOStatus := &replicationdatapb.StopReplicationStatus{
		Before: &replicationdatapb.Status{
			IoState:  int32(replication.ReplicationStateRunning),
			SqlState: int32(replication.ReplicationStateRunning),
		},
		After: &replicationdatapb.Status{},
	}

	connectingIOStatus := &replicationdatapb.StopReplicationStatus{
		Before: &replicationdatapb.Status{
			IoState:     int32(replication.ReplicationStateConnecting),
			LastIoError: "",
			SqlState:    int32(replication.ReplicationStateRunning),
		},
		After: &replicationdatapb.Status{},
	}

	mockController := gomock.NewController(t)
	tmc := tmcmock.NewMockTabletManagerClient(mockController)

	// Simulate the TabletManager RPC sequence for one dead primary, two replicas
	// whose IO threads ERS stops successfully, and one replica that fails during
	// the stop-replication phase. That leaves ERS with a partial stop snapshot,
	// then forces haveRevoked to fail before relay log application begins.
	tmc.EXPECT().
		StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(primaryAlias), replicationdatapb.StopReplicationMode_IOTHREADONLY).
		Return(nil, assert.AnError)

	tmc.EXPECT().
		StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(stoppedIOAlias), replicationdatapb.StopReplicationMode_IOTHREADONLY).
		Return(stoppedIOStatus, nil)

	tmc.EXPECT().
		StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(connectingIOAlias), replicationdatapb.StopReplicationMode_IOTHREADONLY).
		Return(connectingIOStatus, nil)

	tmc.EXPECT().
		StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(failedReplicaAlias), replicationdatapb.StopReplicationMode_IOTHREADONLY).
		Return(nil, assert.AnError)

	// ERS should still restart replication on replicas whose IO threads it
	// already stopped before stopReplicationAndBuildStatusMaps returned.
	tmc.EXPECT().
		StartReplication(gomock.Any(), tabletAliasMatcher(stoppedIOAlias), false).
		Return(nil).
		Times(1)

	tmc.EXPECT().
		StartReplication(gomock.Any(), tabletAliasMatcher(connectingIOAlias), false).
		Return(nil).
		Times(1)

	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{
		Keyspace: keyspace,
		Name:     shard,
		Shard: &topodatapb.Shard{
			PrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
		},
	})

	testutil.AddTablets(ctx, t, ts, nil,
		&topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Type:     topodatapb.TabletType_PRIMARY,
			Keyspace: keyspace,
			Shard:    shard,
		},
		&topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Type:     topodatapb.TabletType_REPLICA,
			Keyspace: keyspace,
			Shard:    shard,
		},
		&topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			},
			Type:     topodatapb.TabletType_REPLICA,
			Keyspace: keyspace,
			Shard:    shard,
		},
		&topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  103,
			},
			Type:     topodatapb.TabletType_REPLICA,
			Keyspace: keyspace,
			Shard:    shard,
		},
	)

	reparenttestutil.SetKeyspaceDurability(ctx, t, ts, keyspace, policy.DurabilityNone)

	erp := NewEmergencyReparenter(ts, tmc, logutil.NewMemoryLogger())
	_, err := erp.ReparentShard(ctx, keyspace, shard, EmergencyReparentOptions{})
	require.ErrorContains(t, err, "failed to stop replication and build status maps")
}

// TestEmergencyReparenterRestartsStoppedIOThreadsOnFailure verifies that ERS
// restarts replication on replicas whose IO threads it stopped before
// aborting during relay log application.
func TestEmergencyReparenterRestartsStoppedIOThreadsOnFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		const (
			keyspace         = "testkeyspace"
			shard            = "-"
			primaryAlias     = "zone1-0000000100"
			stoppedIOAlias   = "zone1-0000000101"
			alreadyStoppedIO = "zone1-0000000102"
			relayLogPosition = "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21"
			sourceUUID       = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
		)

		stoppedIOStatus := &replicationdatapb.StopReplicationStatus{
			Before: &replicationdatapb.Status{
				IoState:  int32(replication.ReplicationStateRunning),
				SqlState: int32(replication.ReplicationStateRunning),
			},
			After: &replicationdatapb.Status{
				SourceUuid:       sourceUUID,
				RelayLogPosition: relayLogPosition,
			},
		}

		alreadyStoppedIOStatus := &replicationdatapb.StopReplicationStatus{
			Before: &replicationdatapb.Status{
				IoState:  int32(replication.ReplicationStateStopped),
				SqlState: int32(replication.ReplicationStateRunning),
			},
			After: &replicationdatapb.Status{
				SourceUuid:       sourceUUID,
				RelayLogPosition: relayLogPosition,
			},
		}

		mockController := gomock.NewController(t)
		tmc := tmcmock.NewMockTabletManagerClient(mockController)

		// Simulate the TabletManager RPC sequence for one dead primary, one replica whose IO thread
		// ERS stops, and one replica whose IO thread was already stopped before ERS started.
		tmc.EXPECT().
			StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(primaryAlias), replicationdatapb.StopReplicationMode_IOTHREADONLY).
			Return(nil, assert.AnError)

		tmc.EXPECT().
			StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(stoppedIOAlias), replicationdatapb.StopReplicationMode_IOTHREADONLY).
			Return(stoppedIOStatus, nil)

		tmc.EXPECT().
			StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(alreadyStoppedIO), replicationdatapb.StopReplicationMode_IOTHREADONLY).
			Return(alreadyStoppedIOStatus, nil)

		// Now simulate a replica that takes too long while applying relay logs.
		tmc.EXPECT().
			WaitForPosition(gomock.Any(), tabletAliasMatcher(stoppedIOAlias), relayLogPosition).
			DoAndReturn(func(ctx context.Context, tablet *topodatapb.Tablet, position string) error {
				// Block until the context expires so ERS aborts before it starts repointing replicas.
				<-ctx.Done()
				return ctx.Err()
			}).
			Times(1)

		tmc.EXPECT().
			WaitForPosition(gomock.Any(), tabletAliasMatcher(alreadyStoppedIO), relayLogPosition).
			Return(nil).
			Times(1)

		// We expect the replica whose IO thread was stopped as part of the ERS (and only that replica,
		// not the one that already had its IO thread stopped) to have replication restarted.
		tmc.EXPECT().
			StartReplication(gomock.Any(), tabletAliasMatcher(stoppedIOAlias), false).
			Return(nil).
			Times(1)

		// Build the test topology.
		ts := memorytopo.NewServer(ctx, "zone1")
		defer ts.Close()

		testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{
			Keyspace: keyspace,
			Name:     shard,
			Shard: &topodatapb.Shard{
				PrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		})

		testutil.AddTablets(ctx, t, ts, nil,
			&topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Keyspace: keyspace,
				Shard:    shard,
			},
			&topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
				Keyspace: keyspace,
				Shard:    shard,
			},
			&topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
				Keyspace: keyspace,
				Shard:    shard,
			},
		)

		reparenttestutil.SetKeyspaceDurability(ctx, t, ts, keyspace, policy.DurabilityNone)

		erp := NewEmergencyReparenter(ts, tmc, logutil.NewMemoryLogger())

		// Trigger ERS and verify that the relay log wait timeout causes cleanup to
		// restart replication only on the replica whose IO thread ERS stopped.
		_, err := erp.ReparentShard(ctx, keyspace, shard, EmergencyReparentOptions{
			WaitReplicasTimeout: 50 * time.Millisecond,
		})

		require.ErrorContains(t, err, "could not apply all relay logs within the provided waitReplicasTimeout")
	})
}

// tabletAliasMatcher matches tablets by alias string for gomock expectations.
func tabletAliasMatcher(alias string) gomock.Matcher {
	return gomock.Cond(func(tablet *topodatapb.Tablet) bool {
		return tablet != nil && topoproto.TabletAliasString(tablet.Alias) == alias
	})
}

func TestEmergencyReparenter_promotionOfNewPrimary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		emergencyReparentOps  EmergencyReparentOptions
		tmc                   *testutil.TabletManagerClient
		unlockTopo            bool
		newPrimaryTabletAlias string
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
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.New[string]("zone1-0000000404")},
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
						IoState:  int32(replication.ReplicationStateStopped),
						SqlState: int32(replication.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(replication.ReplicationStateRunning),
						SqlState: int32(replication.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			shouldErr: false,
		},
		{
			name:                 "PromoteReplica error",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Error: errors.New("primary position error"),
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
			errShouldContain: "context deadline exceeded",
		},
		{
			name:                 "one replica failing to SetReplicationSource does not fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{},
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
			shouldErr: false,
		},
		{
			name:                 "success in initialization",
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.New[string]("zone1-0000000404")},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
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
						IoState:  int32(replication.ReplicationStateStopped),
						SqlState: int32(replication.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(replication.ReplicationStateRunning),
						SqlState: int32(replication.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			shouldErr: false,
		},
	}

	durability, _ := policy.GetDurabilityPolicy(policy.DurabilityNone)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
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

			ts := memorytopo.NewServer(ctx, "zone1")
			defer ts.Close()

			testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{
				Keyspace: tt.keyspace,
				Name:     tt.shard,
			})

			if !tt.unlockTopo {
				var (
					unlock func(*error)
					lerr   error
				)

				ctx, unlock, lerr = ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, lerr, "could not lock %s/%s for test", tt.keyspace, tt.shard)

				defer func() {
					unlock(&lerr)
					require.NoError(t, lerr, "could not unlock %s/%s after test", tt.keyspace, tt.shard)
				}()
			}
			tabletInfo := tt.tabletMap[tt.newPrimaryTabletAlias]

			tt.emergencyReparentOps.durability = durability

			erp := NewEmergencyReparenter(ts, tt.tmc, logger)
			_, err := erp.reparentReplicas(ctx, ev, tabletInfo.Tablet, tt.tabletMap, tt.statusMap, tt.emergencyReparentOps, false)
			if tt.shouldErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestEmergencyReparenter_waitForAllRelayLogsToApply(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	logger := logutil.NewMemoryLogger()
	waitReplicasTimeout := 50 * time.Millisecond
	tests := []struct {
		name       string
		tmc        *testutil.TabletManagerClient
		candidates map[string]*RelayLogPositions
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
			candidates: map[string]*RelayLogPositions{
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
			candidates: map[string]*RelayLogPositions{
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
			candidates: map[string]*RelayLogPositions{
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
			candidates: map[string]*RelayLogPositions{
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

func TestEmergencyReparenterStats(t *testing.T) {
	ersCounter.ResetAll()
	reparentShardOpTimings.Reset()

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
					Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
					},
				},
			},
			"zone1-0000000101": {
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
					After: &replicationdatapb.Status{
						SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
						RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21",
					},
				},
			},
			"zone1-0000000102": {
				StopStatus: &replicationdatapb.StopReplicationStatus{
					Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
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

	ctx := t.Context()
	logger := logutil.NewMemoryLogger()

	ts := memorytopo.NewServer(ctx, "zone1")
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
	require.Equal(t, map[string]int64{"testkeyspace.-.success": 1}, ersCounter.Counts())
	require.Equal(t, map[string]int64{"All": 1, "EmergencyReparentShard": 1}, reparentShardOpTimings.Counts())

	// set emergencyReparentOps to request a non existent tablet
	emergencyReparentOps.NewPrimaryAlias = &topodatapb.TabletAlias{
		Cell: "bogus",
		Uid:  100,
	}

	// run a failing ers
	_, err = erp.ReparentShard(ctx, keyspace, shard, emergencyReparentOps)
	require.Error(t, err)

	// check the counter values
	require.Equal(t, map[string]int64{"testkeyspace.-.success": 1, "testkeyspace.-.failure": 1}, ersCounter.Counts())
	require.Equal(t, map[string]int64{"All": 2, "EmergencyReparentShard": 2}, reparentShardOpTimings.Counts())
}

func TestEmergencyReparenter_findMostAdvanced(t *testing.T) {
	sid1 := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	mysqlGTID1 := replication.Mysql56GTID{
		Server:   sid1,
		Sequence: 9,
	}
	mysqlGTID2 := replication.Mysql56GTID{
		Server:   sid1,
		Sequence: 10,
	}
	mysqlGTID3 := replication.Mysql56GTID{
		Server:   sid1,
		Sequence: 11,
	}

	// most advanced gtid set
	positionMostAdvanced := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionMostAdvanced.Combined.GTIDSet = positionMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionMostAdvanced.Combined.GTIDSet = positionMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionMostAdvanced.Combined.GTIDSet = positionMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID3)
	positionMostAdvanced.Executed.GTIDSet = positionMostAdvanced.Executed.GTIDSet.AddGTID(mysqlGTID1)
	positionMostAdvanced.Executed.GTIDSet = positionMostAdvanced.Executed.GTIDSet.AddGTID(mysqlGTID2)

	// same combined gtid set as positionMostAdvanced, but 1 position behind in gtid executed
	positionAlmostMostAdvanced := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionAlmostMostAdvanced.Combined.GTIDSet = positionAlmostMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionAlmostMostAdvanced.Combined.GTIDSet = positionAlmostMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionAlmostMostAdvanced.Combined.GTIDSet = positionAlmostMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID3)
	positionAlmostMostAdvanced.Executed.GTIDSet = positionAlmostMostAdvanced.Executed.GTIDSet.AddGTID(mysqlGTID1)

	positionIntermediate1 := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionIntermediate1.Combined.GTIDSet = positionIntermediate1.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionIntermediate1.Executed.GTIDSet = positionIntermediate1.Executed.GTIDSet.AddGTID(mysqlGTID1)

	positionIntermediate2 := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionIntermediate2.Combined.GTIDSet = positionIntermediate2.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionIntermediate2.Combined.GTIDSet = positionIntermediate2.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionIntermediate2.Executed.GTIDSet = positionIntermediate2.Executed.GTIDSet.AddGTID(mysqlGTID1)

	positionOnly2 := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionOnly2.Combined.GTIDSet = positionOnly2.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionOnly2.Executed.GTIDSet = positionOnly2.Executed.GTIDSet.AddGTID(mysqlGTID2)

	positionEmpty := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}

	tests := []struct {
		name                 string
		validCandidates      map[string]*RelayLogPositions
		tabletMap            map[string]*topo.TabletInfo
		versionMap           map[string]mysqlctl.ServerVersion
		flavorMap            map[string]mysqlctl.MySQLFlavor
		emergencyReparentOps EmergencyReparentOptions
		result               *topodatapb.Tablet
		err                  string
	}{
		{
			name:            "no valid candidates",
			validCandidates: map[string]*RelayLogPositions{},
			tabletMap:       map[string]*topo.TabletInfo{},
			err:             "no valid candidates for emergency reparent",
		}, {
			name: "choose most advanced",
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionIntermediate1,
				"zone1-0000000102": positionIntermediate2,
				"zone1-0000000103": positionAlmostMostAdvanced,
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
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
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
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionIntermediate1,
				"zone1-0000000102": positionMostAdvanced,
				"zone1-0000000103": positionAlmostMostAdvanced,
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
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
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
			name: "choose most advanced with explicit request",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			}},
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionIntermediate1,
				"zone1-0000000102": positionMostAdvanced,
				"zone1-0000000103": positionAlmostMostAdvanced,
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
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
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
					Uid:  102,
				},
			},
		}, {
			name: "split brain detection",
			emergencyReparentOps: EmergencyReparentOptions{NewPrimaryAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			}},
			validCandidates: map[string]*RelayLogPositions{
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
		}, {
			name: "lower MySQL version preferred when positions are equal",
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionMostAdvanced,
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
			},
			versionMap: map[string]mysqlctl.ServerVersion{
				"zone1-0000000100": {Major: 8, Minor: 4, Patch: 0},
				"zone1-0000000101": {Major: 8, Minor: 0, Patch: 35},
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
		}, {
			// Mixed flavor families disable version-aware intermediate-source
			// selection: MariaDB 10.6 (uid 100) and MySQL 8.4 (uid 101) at equal
			// position. Without the flavor guard, findMostAdvanced would compute
			// 8.4 < 10.6 and prefer the MySQL tablet; with it, version ordering is
			// skipped and the tie falls through to the stable alias tiebreak (uid
			// 100 sorts first).
			name: "mixed flavor families skip version-aware selection",
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionMostAdvanced,
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
						Type:  topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000101": {
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
						Type:  topodatapb.TabletType_REPLICA,
					},
				},
			},
			versionMap: map[string]mysqlctl.ServerVersion{
				"zone1-0000000100": {Major: 10, Minor: 6},
				"zone1-0000000101": {Major: 8, Minor: 4, Patch: 0},
			},
			flavorMap: map[string]mysqlctl.MySQLFlavor{
				"zone1-0000000100": mysqlctl.FlavorMariaDB,
				"zone1-0000000101": mysqlctl.FlavorMySQL,
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}, {
			name: "same MySQL version falls through to position",
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionAlmostMostAdvanced,
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
			},
			versionMap: map[string]mysqlctl.ServerVersion{
				"zone1-0000000100": {Major: 8, Minor: 0, Patch: 35},
				"zone1-0000000101": {Major: 8, Minor: 0, Patch: 35},
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}, {
			name: "unknown MySQL version sorts last",
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionMostAdvanced,
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
			},
			versionMap: map[string]mysqlctl.ServerVersion{
				"zone1-0000000101": {Major: 8, Minor: 0, Patch: 35},
			},
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
		},
	}

	durability, _ := policy.GetDurabilityPolicy(policy.DurabilityNone)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			erp := NewEmergencyReparenter(nil, nil, logutil.NewMemoryLogger())

			test.emergencyReparentOps.durability = durability
			winningTablet, _, err := erp.findMostAdvanced(test.validCandidates, test.tabletMap, test.versionMap, test.flavorMap, test.emergencyReparentOps)
			if test.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.err)
			} else {
				require.NoError(t, err)
				assert.True(t, topoproto.TabletAliasEqual(test.result.Alias, winningTablet.Alias))
			}
		})
	}
}

func TestEmergencyReparenter_reparentReplicas(t *testing.T) {
	tests := []struct {
		name                  string
		emergencyReparentOps  EmergencyReparentOptions
		tmc                   *testutil.TabletManagerClient
		unlockTopo            bool
		newPrimaryTabletAlias string
		keyspace              string
		shard                 string
		tablets               []*topodatapb.Tablet
		tabletMap             map[string]*topo.TabletInfo
		statusMap             map[string]*replicationdatapb.StopReplicationStatus
		shouldErr             bool
		errShouldContain      string
		remoteOpTimeout       time.Duration
	}{
		{
			name:                 "success",
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.New[string]("zone1-0000000404")},
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
						IoState:  int32(replication.ReplicationStateStopped),
						SqlState: int32(replication.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(replication.ReplicationStateRunning),
						SqlState: int32(replication.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			shouldErr: false,
		},
		{
			name:                 "PromoteReplica error",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Error: errors.New("primary position error"),
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
			errShouldContain: "context deadline exceeded",
		},
		{
			name:                 "one replica failing to SetReplicationSource does not fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{},
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
			shouldErr: false,
		},
		{
			name:                 "single replica failing to SetReplicationSource does not fail the promotion",
			emergencyReparentOps: EmergencyReparentOptions{},
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
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": assert.AnError,
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:  "testkeyspace",
			shard:     "-",
			shouldErr: false,
		},
		{
			name:                 "primary promotion gets infinitely stuck",
			emergencyReparentOps: EmergencyReparentOptions{},
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
				PromoteReplicaDelays: map[string]time.Duration{
					"zone1-0000000100": 500 * time.Hour,
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			remoteOpTimeout:       100 * time.Millisecond,
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
						IoState:  int32(replication.ReplicationStateStopped),
						SqlState: int32(replication.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(replication.ReplicationStateRunning),
						SqlState: int32(replication.ReplicationStateRunning),
					},
				},
			},
			keyspace:         "testkeyspace",
			shard:            "-",
			shouldErr:        true,
			errShouldContain: "failed to promote zone1-0000000100 to primary: primary-elect tablet zone1-0000000100 failed to be upgraded to primary: context deadline exceeded",
		},
	}

	durability, _ := policy.GetDurabilityPolicy(policy.DurabilityNone)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.remoteOpTimeout != 0 {
				oldTimeout := topo.RemoteOperationTimeout
				topo.RemoteOperationTimeout = tt.remoteOpTimeout
				defer func() {
					topo.RemoteOperationTimeout = oldTimeout
				}()
			}

			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{
				ShardInfo: topo.ShardInfo{
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  0o00,
						},
					},
				},
			}

			ctx := t.Context()
			ts := memorytopo.NewServer(ctx, "zone1")
			defer ts.Close()

			testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{
				Keyspace: tt.keyspace,
				Name:     tt.shard,
			})

			if !tt.unlockTopo {
				var (
					unlock func(*error)
					lerr   error
				)

				ctx, unlock, lerr = ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, lerr, "could not lock %s/%s for test", tt.keyspace, tt.shard)

				defer func() {
					unlock(&lerr)
					require.NoError(t, lerr, "could not unlock %s/%s after test", tt.keyspace, tt.shard)
				}()
			}
			tabletInfo := tt.tabletMap[tt.newPrimaryTabletAlias]

			tt.emergencyReparentOps.durability = durability

			erp := NewEmergencyReparenter(ts, tt.tmc, logger)
			_, err := erp.reparentReplicas(ctx, ev, tabletInfo.Tablet, tt.tabletMap, tt.statusMap, tt.emergencyReparentOps, false /* intermediateReparent */)
			if tt.shouldErr {
				require.Error(t, err)
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
			emergencyReparentOps: EmergencyReparentOptions{IgnoreReplicas: sets.New[string]("zone1-0000000404")},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
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
						IoState:  int32(replication.ReplicationStateStopped),
						SqlState: int32(replication.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(replication.ReplicationStateRunning),
						SqlState: int32(replication.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
			shouldErr: false,
			result: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Hostname: "primary-elect",
				},
				{
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
				},
				{
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
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  404,
					},
				},
			},
		},
		{
			name:                 "success - filter with valid tablets before",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
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
						IoState:  int32(replication.ReplicationStateStopped),
						SqlState: int32(replication.ReplicationStateStopped),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
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
		{
			name:                 "success - only 2 tablets and they error",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": errors.New("An error"),
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{},
			keyspace:  "testkeyspace",
			shard:     "-",
			shouldErr: false,
			result: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Hostname: "primary-elect",
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
		{
			name:                 "all replicas failed",
			emergencyReparentOps: EmergencyReparentOptions{},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
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
				"zone1-0000000102": {
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
				},
				{
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
			shouldErr: false,
			validCandidateTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Hostname: "primary-elect",
				},
				{
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
						IoState:  int32(replication.ReplicationStateStopped),
						SqlState: int32(replication.ReplicationStateStopped),
					},
				},
				"zone1-0000000102": { // forceStart = true
					Before: &replicationdatapb.Status{
						IoState:  int32(replication.ReplicationStateRunning),
						SqlState: int32(replication.ReplicationStateRunning),
					},
				},
			},
			keyspace:  "testkeyspace",
			shard:     "-",
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

	durability, _ := policy.GetDurabilityPolicy(policy.DurabilityNone)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			logger := logutil.NewMemoryLogger()
			ev := &events.Reparent{}

			ts := memorytopo.NewServer(ctx, "zone1")
			defer ts.Close()

			testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{
				Keyspace: tt.keyspace,
				Name:     tt.shard,
			})

			if !tt.unlockTopo {
				var (
					unlock func(*error)
					lerr   error
				)

				ctx, unlock, lerr = ts.LockShard(ctx, tt.keyspace, tt.shard, "test lock")
				require.NoError(t, lerr, "could not lock %s/%s for test", tt.keyspace, tt.shard)

				defer func() {
					unlock(&lerr)
					require.NoError(t, lerr, "could not unlock %s/%s after test", tt.keyspace, tt.shard)
				}()
			}
			tabletInfo := tt.tabletMap[tt.newSourceTabletAlias]

			tt.emergencyReparentOps.durability = durability

			erp := NewEmergencyReparenter(ts, tt.tmc, logger)
			res, err := erp.promoteIntermediateSource(ctx, ev, tabletInfo.Tablet, tt.tabletMap, tt.statusMap, tt.validCandidateTablets, tt.emergencyReparentOps)
			if tt.shouldErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errShouldContain)
				return
			}

			require.NoError(t, err)
			require.Len(t, res, len(tt.result))
			for idx, tablet := range res {
				assert.Equal(t, topoproto.TabletAliasString(tt.result[idx].Alias), topoproto.TabletAliasString(tablet.Alias))
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
		versionMap           map[string]mysqlctl.ServerVersion
		flavorMap            map[string]mysqlctl.MySQLFlavor
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
		}, {
			// Same-family (MySQL) candidates at the same promotion tier: the final
			// election prefers the lower version, promoting zone1-101 (8.0) over the
			// 8.4 intermediate source.
			name:               "lower version preferred among same-family candidates",
			intermediateSource: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}},
			validCandidates: []*topodatapb.Tablet{
				{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}},
				{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}},
				"zone1-0000000101": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}}},
			},
			versionMap: map[string]mysqlctl.ServerVersion{
				"zone1-0000000100": {Major: 8, Minor: 4, Patch: 0},
				"zone1-0000000101": {Major: 8, Minor: 0, Patch: 35},
			},
			result: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}},
		}, {
			// Regression for the mixed-flavor-family case. The intermediate source is
			// the MariaDB tablet (10.6, uid 100); the other candidate is MySQL 8.4
			// (uid 101). identifyPrimaryCandidate is given the raw (unguarded) version
			// and flavor maps, so its internal scopedVersionMap must detect the mixed
			// families and disable version comparison. Without that guard, findCandidate
			// would compute 8.4 < 10.6 and pull the election to the MySQL tablet — the
			// incompatible choice; with it, the MariaDB intermediate source is kept.
			name:               "mixed flavor families disable version comparison in final election",
			intermediateSource: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}},
			validCandidates: []*topodatapb.Tablet{
				{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}, // MariaDB 10.6 (intermediate source)
				{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}}, // MySQL 8.4
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000100": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}},
				"zone1-0000000101": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}}},
			},
			versionMap: map[string]mysqlctl.ServerVersion{
				"zone1-0000000100": {Major: 10, Minor: 6},
				"zone1-0000000101": {Major: 8, Minor: 4, Patch: 0},
			},
			flavorMap: map[string]mysqlctl.MySQLFlavor{
				"zone1-0000000100": mysqlctl.FlavorMariaDB,
				"zone1-0000000101": mysqlctl.FlavorMySQL,
			},
			result: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			durability, _ := policy.GetDurabilityPolicy(policy.DurabilityNone)
			test.emergencyReparentOps.durability = durability
			logger := logutil.NewMemoryLogger()

			erp := NewEmergencyReparenter(nil, nil, logger)
			res, err := erp.identifyPrimaryCandidate(test.intermediateSource, test.validCandidates, test.tabletMap, test.versionMap, test.flavorMap, test.emergencyReparentOps)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
				return
			}
			require.NoError(t, err)
			assert.True(t, topoproto.TabletAliasEqual(res.Alias, test.result.Alias))
		})
	}
}

// TestParentContextCancelled tests that even if the parent context of reparentReplicas cancels, we should not cancel the context of
// SetReplicationSource since there could be tablets that are running it even after ERS completes.
func TestParentContextCancelled(t *testing.T) {
	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)
	// Setup ERS options with a very high wait replicas timeout
	emergencyReparentOps := EmergencyReparentOptions{IgnoreReplicas: sets.New[string]("zone1-0000000404"), WaitReplicasTimeout: time.Minute, durability: durability}
	// Make the replica tablet return its results after 3 seconds
	tmc := &testutil.TabletManagerClient{
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
				IoState:  int32(replication.ReplicationStateRunning),
				SqlState: int32(replication.ReplicationStateRunning),
			},
		},
	}
	keyspace := "testkeyspace"
	shard := "-"

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

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
	_, err = erp.reparentReplicas(ctx, ev, tabletMap[newPrimaryTabletAlias].Tablet, tabletMap, statusMap, emergencyReparentOps, true)
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
	noTabletsTakingBackup := map[string]bool{
		topoproto.TabletAliasString(primaryTablet.Alias): false, topoproto.TabletAliasString(replicaTablet.Alias): false,
		topoproto.TabletAliasString(rdonlyTablet.Alias): false, topoproto.TabletAliasString(replicaCrossCellTablet.Alias): false,
		topoproto.TabletAliasString(rdonlyCrossCellTablet.Alias): false,
	}
	replicaTakingBackup := map[string]bool{
		topoproto.TabletAliasString(primaryTablet.Alias): false, topoproto.TabletAliasString(replicaTablet.Alias): true,
		topoproto.TabletAliasString(rdonlyTablet.Alias): false, topoproto.TabletAliasString(replicaCrossCellTablet.Alias): false,
		topoproto.TabletAliasString(rdonlyCrossCellTablet.Alias): false,
	}
	tests := []struct {
		name                string
		durability          string
		validTablets        []*topodatapb.Tablet
		tabletsReachable    []*topodatapb.Tablet
		tabletsTakingBackup map[string]bool
		prevPrimary         *topodatapb.Tablet
		opts                EmergencyReparentOptions
		filteredTablets     []*topodatapb.Tablet
		errShouldContain    string
	}{
		{
			name:                "filter must not",
			durability:          policy.DurabilityNone,
			validTablets:        allTablets,
			tabletsReachable:    allTablets,
			tabletsTakingBackup: noTabletsTakingBackup,
			filteredTablets:     []*topodatapb.Tablet{primaryTablet, replicaTablet, replicaCrossCellTablet},
		}, {
			name:                "host taking backup must not be on the list when there are other candidates",
			durability:          policy.DurabilityNone,
			validTablets:        allTablets,
			tabletsReachable:    []*topodatapb.Tablet{replicaTablet, replicaCrossCellTablet, rdonlyTablet, rdonlyCrossCellTablet},
			tabletsTakingBackup: replicaTakingBackup,
			filteredTablets:     []*topodatapb.Tablet{replicaCrossCellTablet},
		}, {
			name:                "host taking backup must be the only one on the list when there are no other candidates",
			durability:          policy.DurabilityNone,
			validTablets:        allTablets,
			tabletsReachable:    []*topodatapb.Tablet{replicaTablet, rdonlyTablet, rdonlyCrossCellTablet},
			tabletsTakingBackup: replicaTakingBackup,
			filteredTablets:     []*topodatapb.Tablet{replicaTablet},
		}, {
			name:                "filter cross cell",
			durability:          policy.DurabilityNone,
			validTablets:        allTablets,
			tabletsReachable:    allTablets,
			tabletsTakingBackup: noTabletsTakingBackup,

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
			name:                "filter establish",
			durability:          policy.DurabilityCrossCell,
			validTablets:        []*topodatapb.Tablet{primaryTablet, replicaTablet},
			tabletsReachable:    []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, rdonlyCrossCellTablet},
			tabletsTakingBackup: noTabletsTakingBackup,
			filteredTablets:     []*topodatapb.Tablet{},
		}, {
			name:       "filter mixed",
			durability: policy.DurabilityCrossCell,
			prevPrimary: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone-2",
				},
			},
			opts: EmergencyReparentOptions{
				PreventCrossCellPromotion: true,
			},
			validTablets:        allTablets,
			tabletsReachable:    allTablets,
			tabletsTakingBackup: noTabletsTakingBackup,
			filteredTablets:     []*topodatapb.Tablet{replicaCrossCellTablet},
		}, {
			name:                "error - requested primary must not",
			durability:          policy.DurabilityNone,
			validTablets:        allTablets,
			tabletsReachable:    allTablets,
			tabletsTakingBackup: noTabletsTakingBackup,
			opts: EmergencyReparentOptions{
				NewPrimaryAlias: rdonlyTablet.Alias,
			},
			errShouldContain: "proposed primary zone-1-0000000003 has a must not promotion rule",
		}, {
			name:                "error - requested primary not in same cell",
			durability:          policy.DurabilityNone,
			validTablets:        allTablets,
			tabletsReachable:    allTablets,
			tabletsTakingBackup: noTabletsTakingBackup,
			prevPrimary:         primaryTablet,
			opts: EmergencyReparentOptions{
				PreventCrossCellPromotion: true,
				NewPrimaryAlias:           replicaCrossCellTablet.Alias,
			},
			errShouldContain: "proposed primary zone-2-0000000002 is is a different cell as the previous primary",
		}, {
			name:                "error - requested primary cannot establish",
			durability:          policy.DurabilityCrossCell,
			validTablets:        allTablets,
			tabletsTakingBackup: noTabletsTakingBackup,
			tabletsReachable:    []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, rdonlyCrossCellTablet},
			opts: EmergencyReparentOptions{
				NewPrimaryAlias: primaryTablet.Alias,
			},
			errShouldContain: "proposed primary zone-1-0000000001 will not be able to make forward progress on being promoted",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			durability, err := policy.GetDurabilityPolicy(tt.durability)
			require.NoError(t, err)
			tt.opts.durability = durability
			logger := logutil.NewMemoryLogger()
			erp := NewEmergencyReparenter(nil, nil, logger)
			tabletList, err := erp.filterValidCandidates(tt.validTablets, tt.tabletsReachable, tt.tabletsTakingBackup, tt.prevPrimary, tt.opts)
			if tt.errShouldContain != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errShouldContain)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.filteredTablets, tabletList)
			}
		})
	}
}

// getRelayLogPosition is a helper function that prints out the relay log positions.
func getRelayLogPosition(gtidSets ...string) string {
	u1 := "00000000-0000-0000-0000-000000000001"
	u2 := "00000000-0000-0000-0000-000000000002"
	u3 := "00000000-0000-0000-0000-000000000003"
	u4 := "00000000-0000-0000-0000-000000000004"
	uuids := []string{u1, u2, u3, u4}

	res := "MySQL56/"
	first := true
	var resSb4673 strings.Builder
	for idx, set := range gtidSets {
		if set == "" {
			continue
		}
		if !first {
			resSb4673.WriteString(",")
		}
		first = false
		resSb4673.WriteString(uuids[idx] + ":" + set)
	}
	res += resSb4673.String()
	return res
}

// TestEmergencyReparenterFindErrantGTIDs tests that ERS can find the most advanced replica after marking tablets as errant.
func TestEmergencyReparenterFindErrantGTIDs(t *testing.T) {
	u1 := "00000000-0000-0000-0000-000000000001"
	u2 := "00000000-0000-0000-0000-000000000002"
	tests := []struct {
		name                     string
		tmc                      tmclient.TabletManagerClient
		statusMap                map[string]*replicationdatapb.StopReplicationStatus
		primaryStatusMap         map[string]*replicationdatapb.PrimaryStatus
		tabletMap                map[string]*topo.TabletInfo
		wantedCandidates         []string
		wantMostAdvancedPossible []string
		wantErr                  string
	}{
		{
			name: "Case 1a: No Errant GTIDs. This is the first reparent. A replica is the most advanced.",
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 1,
					"zone1-0000000103": 1,
					"zone1-0000000104": 1,
				},
			},
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-99"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000102", "zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000102"},
		},
		{
			name: "Case 1b: No Errant GTIDs. This is not the first reparent. A replica is the most advanced.",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 2,
					"zone1-0000000103": 2,
					"zone1-0000000104": 2,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-99", "1-30"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000102", "zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000102"},
		},
		{
			name: "Case 1c: No Errant GTIDs. This is not the first reparent. A rdonly is the most advanced.",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 2,
					"zone1-0000000103": 2,
					"zone1-0000000104": 2,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-99", "1-30"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-101", "1-30"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000102", "zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000104"},
		},
		{
			name: "Case 2: Only 1 tablet is recent and all others are severely lagged",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 3,
					"zone1-0000000103": 2,
					"zone1-0000000104": 1,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-100"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-30"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000102", "zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000102"},
		},
		{
			name: "Case 3: All replicas severely lagged (Primary tablet dies with t1: u1-100, u2:1-30, u3:1-100)",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 2,
					"zone1-0000000103": 2,
					"zone1-0000000104": 1,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-30"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000102", "zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000102", "zone1-0000000103"},
		},
		{
			name: "Case 4: Primary dies and comes back, has an extra UUID, right at the point when a new ERS has started.",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_PRIMARY,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 2,
					"zone1-0000000103": 3,
					"zone1-0000000104": 3,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-90", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"zone1-0000000102": {
					Position: getRelayLogPosition("", "1-31", "1-50"),
				},
			},
			wantedCandidates:         []string{"zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000103"},
		},
		{
			name: "Case 5a: Old Primary and a rdonly have errant GTID. Old primary is permanently lost and comes up from backup and ronly comes up during ERS",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 2,
					"zone1-0000000103": 3,
					"zone1-0000000104": 2,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-20", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-31", "1-50"),
						SourceUuid:       u2,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000102", "zone1-0000000103"},
			wantMostAdvancedPossible: []string{"zone1-0000000103"},
		},
		{
			name: "Case 5b: Old Primary and a rdonly have errant GTID. Both come up during ERS",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_PRIMARY,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 2,
					"zone1-0000000103": 3,
					"zone1-0000000104": 2,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-31", "1-50"),
						SourceUuid:       u2,
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"zone1-0000000102": {
					Position: getRelayLogPosition("", "1-31", "1-50"),
				},
			},
			wantedCandidates:         []string{"zone1-0000000103"},
			wantMostAdvancedPossible: []string{"zone1-0000000103"},
		},
		{
			name: "Case 6a: Errant GTID introduced on a replica server by a write that shouldn't happen. The replica with errant GTID is not the most advanced.",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 3,
					"zone1-0000000103": 3,
					"zone1-0000000104": 3,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-99", "1-31", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000103", "zone1-0000000104"},
		},
		{
			name: "Case 6b: Errant GTID introduced on a replica server by a write that shouldn't happen. The replica with errant GTID is the most advanced.",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 3,
					"zone1-0000000103": 3,
					"zone1-0000000104": 2,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-101", "1-31", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000103", "zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000103"},
		},
		{
			name: "Case 6c: Errant GTID introduced on a replica server by a write that shouldn't happen. Only 2 tablets exist.",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 3,
					"zone1-0000000103": 3,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-31", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000103"},
			wantMostAdvancedPossible: []string{"zone1-0000000103"},
		},
		{
			name: "Case 7: Both replicas with errant GTIDs",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 3,
					"zone1-0000000103": 3,
					"zone1-0000000104": 3,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000102": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-31", "1-50"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-51"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-90", "1-30", "1-50"),
						SourceUuid:       u1,
					},
				},
			},
			wantedCandidates:         []string{"zone1-0000000104"},
			wantMostAdvancedPossible: []string{"zone1-0000000104"},
		},
		{
			name: "Case 8a: Old primary and rdonly have errant GTID and come up during ERS and replica has an errant GTID introduced by the user.",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_PRIMARY,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000102": 2,
					"zone1-0000000103": 3,
					"zone1-0000000104": 2,
				},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-51"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-31", "1-50"),
						SourceUuid:       u2,
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"zone1-0000000102": {
					Position: getRelayLogPosition("", "1-31", "1-50"),
				},
			},
			wantedCandidates:         []string{"zone1-0000000103"},
			wantMostAdvancedPossible: []string{"zone1-0000000103"},
		},
		{
			name: "Reading reparent journal fails",
			tabletMap: map[string]*topo.TabletInfo{
				"zone1-0000000102": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000102",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type: topodatapb.TabletType_PRIMARY,
					},
				},
				"zone1-0000000103": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000103",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  103,
						},
						Type: topodatapb.TabletType_REPLICA,
					},
				},
				"zone1-0000000104": {
					Tablet: &topodatapb.Tablet{
						Hostname: "zone1-0000000104",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  104,
						},
						Type: topodatapb.TabletType_RDONLY,
					},
				},
			},
			tmc: &testutil.TabletManagerClient{
				ReadReparentJournalInfoResults: map[string]int32{},
			},
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000103": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("1-100", "1-30", "1-51"),
						SourceUuid:       u1,
					},
				},
				"zone1-0000000104": {
					After: &replicationdatapb.Status{
						RelayLogPosition: getRelayLogPosition("", "1-31", "1-50"),
						SourceUuid:       u2,
					},
				},
			},
			primaryStatusMap: map[string]*replicationdatapb.PrimaryStatus{
				"zone1-0000000102": {
					Position: getRelayLogPosition("", "1-31", "1-50"),
				},
			},
			wantErr: "could not read reparent journal information",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			erp := &EmergencyReparenter{
				tmc: tt.tmc,
			}
			validCandidates, isGtid, err := FindPositionsOfAllCandidates(tt.statusMap, tt.primaryStatusMap)
			require.NoError(t, err)
			require.True(t, isGtid)
			candidates, err := erp.findErrantGTIDs(t.Context(), validCandidates, tt.statusMap, tt.tabletMap, 10*time.Second)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			keys := make([]string, 0, len(candidates))
			for key := range candidates {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			require.ElementsMatch(t, tt.wantedCandidates, keys)

			dp, err := policy.GetDurabilityPolicy(policy.DurabilitySemiSync)
			require.NoError(t, err)
			ers := EmergencyReparenter{logger: logutil.NewCallbackLogger(func(*logutilpb.Event) {})}
			winningPrimary, _, err := ers.findMostAdvanced(candidates, tt.tabletMap, nil, nil, EmergencyReparentOptions{durability: dp})
			require.NoError(t, err)
			require.True(t, slices.Contains(tt.wantMostAdvancedPossible, winningPrimary.Hostname), winningPrimary.Hostname)
		})
	}
}

// TestEmergencyReparenterFindErrantGTIDs_NilPosition is a regression test
// for a bug where a nil *RelayLogPositions entry in validCandidates would
// cause a nil pointer panic. The test includes:
//   - zone1-0000000102: valid candidate with max reparent journal length
//   - zone1-0000000103: nil position with max reparent journal length (exercises the maxLenCandidates loop)
//   - zone1-0000000104: nil position with a lower reparent journal length (exercises the lagged-candidate loop)
func TestEmergencyReparenterFindErrantGTIDs_NilPosition(t *testing.T) {
	u1 := "00000000-0000-0000-0000-000000000001"
	erp := NewEmergencyReparenter(nil, &testutil.TabletManagerClient{
		ReadReparentJournalInfoResults: map[string]int32{
			"zone1-0000000102": 2,
			"zone1-0000000103": 2,
			"zone1-0000000104": 1,
		},
	}, nil)
	tabletMap := map[string]*topo.TabletInfo{
		"zone1-0000000102": {
			Tablet: &topodatapb.Tablet{
				Hostname: "zone1-0000000102",
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
				Type: topodatapb.TabletType_REPLICA,
			},
		},
		"zone1-0000000103": {
			Tablet: &topodatapb.Tablet{
				Hostname: "zone1-0000000103",
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  103,
				},
				Type: topodatapb.TabletType_REPLICA,
			},
		},
		"zone1-0000000104": {
			Tablet: &topodatapb.Tablet{
				Hostname: "zone1-0000000104",
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  104,
				},
				Type: topodatapb.TabletType_REPLICA,
			},
		},
	}
	statusMap := map[string]*replicationdatapb.StopReplicationStatus{
		"zone1-0000000102": {
			After: &replicationdatapb.Status{
				RelayLogPosition: getRelayLogPosition("1-100"),
				SourceUuid:       u1,
			},
		},
		"zone1-0000000103": {
			After: &replicationdatapb.Status{
				RelayLogPosition: getRelayLogPosition("1-99"),
				SourceUuid:       u1,
			},
		},
		"zone1-0000000104": {
			After: &replicationdatapb.Status{
				RelayLogPosition: getRelayLogPosition("1-90"),
				SourceUuid:       u1,
			},
		},
	}
	// Construct validCandidates with nil entries for zone1-0000000103 and
	// zone1-0000000104. zone1-0000000103 has the same reparent journal length
	// as zone1-0000000102 (maxLen), so it exercises the nil guard in the
	// maxLenCandidates loop. zone1-0000000104 has a lower reparent journal
	// length, so it exercises the nil guard in the lagged-candidate loop.
	validCandidates := map[string]*RelayLogPositions{
		"zone1-0000000102": {
			Combined: replication.MustParsePosition(replication.Mysql56FlavorID, u1+":1-100"),
		},
		"zone1-0000000103": nil,
		"zone1-0000000104": nil,
	}

	candidates, err := erp.findErrantGTIDs(t.Context(), validCandidates, statusMap, tabletMap, 10*time.Second)
	require.NoError(t, err)
	require.Contains(t, candidates, "zone1-0000000102")
}

// TestEmergencyReparenter_waitForAllRelayLogsToApply_reconcilesPositions verifies
// that waitForAllRelayLogsToApply advances a candidate's in-memory Executed
// position to its Combined position only when that candidate's SQL thread
// actually caught up. A candidate whose wait succeeds is advanced; a candidate
// absent from the status map (e.g. the former primary) is never waited on and is
// left untouched; and when a candidate's wait fails, its position is left at its
// real (behind) value so a tablet that never caught up cannot look advanced.
func TestEmergencyReparenter_waitForAllRelayLogsToApply_reconcilesPositions(t *testing.T) {
	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	combined := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	combined.GTIDSet = combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 10})
	executedBehind := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	executedBehind.GTIDSet = executedBehind.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 5})

	tests := []struct {
		name string
		// waitErr controls whether the waited replica's WaitForPosition succeeds.
		waitErr bool
		// wantReplicaAdvanced is the expected Executed position for the replica we
		// waited on after the call: Combined on success, behind on failure.
		wantReplicaAdvanced bool
		wantErr             bool
	}{
		{
			name:                "wait succeeds advances waited candidate",
			waitErr:             false,
			wantReplicaAdvanced: true,
			wantErr:             false,
		},
		{
			name:                "wait fails leaves candidate untouched",
			waitErr:             true,
			wantReplicaAdvanced: false,
			wantErr:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Replica with SQL thread behind its relay log; it will be waited on.
			replica := &RelayLogPositions{Combined: combined, Executed: executedBehind}
			// Former primary, absent from the status map, must be left untouched.
			formerPrimary := &RelayLogPositions{Combined: combined, Executed: executedBehind}

			validCandidates := map[string]*RelayLogPositions{
				"zone1-0000000100": replica,
				"zone1-0000000101": formerPrimary,
			}
			tabletMap := map[string]*topo.TabletInfo{
				"zone1-0000000100": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}},
				"zone1-0000000101": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}}},
			}
			combinedStr := replication.EncodePosition(combined)
			statusMap := map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {After: &replicationdatapb.Status{RelayLogPosition: combinedStr}},
			}
			// An empty inner map (no entry for the position) makes WaitForPosition error.
			waitResult := map[string]error{combinedStr: nil}
			if tt.waitErr {
				waitResult = map[string]error{}
			}
			tmc := &testutil.TabletManagerClient{
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": waitResult,
				},
			}

			erp := NewEmergencyReparenter(nil, tmc, logutil.NewMemoryLogger())
			err := erp.waitForAllRelayLogsToApply(t.Context(), validCandidates, tabletMap, statusMap, 30*time.Second)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.wantReplicaAdvanced {
				require.True(t, replica.Executed.Equal(combined), "waited candidate's Executed should advance to Combined")
			} else {
				require.True(t, replica.Executed.Equal(executedBehind), "failed candidate's Executed should stay behind")
			}
			// A candidate absent from the status map is never waited on, so its
			// position must be untouched regardless of the wait outcome.
			require.True(t, formerPrimary.Executed.Equal(executedBehind), "candidate absent from status map should be untouched")
		})
	}
}

// TestEmergencyReparenter_findMostAdvanced_versionTiebreakerAfterCatchUp is a
// regression test for the case where a lower-version candidate has the same
// combined relay-log position as a higher-version candidate but a lower
// pre-wait executed position. After the relay-log wait, both have applied up to
// the same combined position, so the MySQL version tiebreaker must select the
// lower-version tablet rather than the one that merely looked more advanced
// before catch-up.
func TestEmergencyReparenter_findMostAdvanced_versionTiebreakerAfterCatchUp(t *testing.T) {
	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	// The relay log holds GTIDs through sequence 10; executedBehind has applied
	// only through sequence 5, so it is a proper subset of the combined position.
	combined := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	combined.GTIDSet = combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 5})
	combined.GTIDSet = combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 10})
	executedFull := combined
	executedBehind := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	executedBehind.GTIDSet = executedBehind.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 5})

	// olderReplica: lower version, SQL thread behind pre-wait.
	// newerReplica: higher version, fully applied pre-wait.
	// Both have the same combined relay-log position.
	validCandidates := map[string]*RelayLogPositions{
		"zone1-0000000100": {Combined: combined, Executed: executedBehind},
		"zone1-0000000101": {Combined: combined, Executed: executedFull},
	}
	tabletMap := map[string]*topo.TabletInfo{
		"zone1-0000000100": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, Type: topodatapb.TabletType_REPLICA}},
		"zone1-0000000101": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Type: topodatapb.TabletType_REPLICA}},
	}
	statusMap := map[string]*replicationdatapb.StopReplicationStatus{
		"zone1-0000000100": {},
		"zone1-0000000101": {},
	}
	versionMap := map[string]mysqlctl.ServerVersion{
		"zone1-0000000100": {Major: 8, Minor: 0, Patch: 35},
		"zone1-0000000101": {Major: 8, Minor: 4, Patch: 0},
	}

	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)

	// Before the relay-log wait, the newer tablet looks more advanced on Executed
	// and would be chosen as the intermediate source.
	erpBefore := NewEmergencyReparenter(nil, nil, logutil.NewMemoryLogger())
	intermediateSource, _, err := erpBefore.findMostAdvanced(validCandidates, tabletMap, versionMap, nil, EmergencyReparentOptions{durability: durability})
	require.NoError(t, err)
	require.True(t, topoproto.TabletAliasEqual(intermediateSource.Alias, &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}))

	// Both candidates' SQL threads catch up to the combined relay-log position
	// during the wait, which reconciles their positions to that applied position.
	// After that, positions are equal and the version tiebreaker selects the
	// lower-version tablet.
	combinedStr := replication.EncodePosition(combined)
	statusMap["zone1-0000000100"].After = &replicationdatapb.Status{RelayLogPosition: combinedStr}
	statusMap["zone1-0000000101"].After = &replicationdatapb.Status{RelayLogPosition: combinedStr}
	tmc := &testutil.TabletManagerClient{
		WaitForPositionResults: map[string]map[string]error{
			"zone1-0000000100": {combinedStr: nil},
			"zone1-0000000101": {combinedStr: nil},
		},
	}
	erp := NewEmergencyReparenter(nil, tmc, logutil.NewMemoryLogger())
	require.NoError(t, erp.waitForAllRelayLogsToApply(t.Context(), validCandidates, tabletMap, statusMap, 30*time.Second))

	intermediateSource, _, err = erp.findMostAdvanced(validCandidates, tabletMap, versionMap, nil, EmergencyReparentOptions{durability: durability})
	require.NoError(t, err)
	require.True(t, topoproto.TabletAliasEqual(intermediateSource.Alias, &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}))
}

// TestEmergencyReparenter_findMostAdvanced_versionTiebreakerAfterCatchUp_FilePos
// is the file-position (non-GTID) analogue of the test above. For file-position
// replication the candidate positions are stored in Combined as the pre-wait
// executed position, while the wait catches up to the relay-log-equivalent
// position. The reconcile must therefore advance Combined to the actually-waited-for
// position; otherwise two candidates that both caught up would still be compared
// on their stale pre-wait positions and the newer-version tablet would win.
func TestEmergencyReparenter_findMostAdvanced_versionTiebreakerAfterCatchUp_FilePos(t *testing.T) {
	// Pre-wait executed file positions: the newer tablet is slightly ahead.
	// Both relay logs have downloaded up to offset 100 (the equivalent position
	// the wait catches up to).
	olderExecutedStr := "FilePos/mysql-bin.0001:80"
	newerExecutedStr := "FilePos/mysql-bin.0001:90"
	appliedStr := "FilePos/mysql-bin.0001:100"

	olderExecuted, err := replication.DecodePosition(olderExecutedStr)
	require.NoError(t, err)
	newerExecuted, err := replication.DecodePosition(newerExecutedStr)
	require.NoError(t, err)

	// For non-GTID replication FindPositionsOfAllCandidates stores the executed
	// position in Combined and leaves Executed zero.
	validCandidates := map[string]*RelayLogPositions{
		"zone1-0000000100": {Combined: olderExecuted}, // older version, behind pre-wait
		"zone1-0000000101": {Combined: newerExecuted}, // newer version, ahead pre-wait
	}
	tabletMap := map[string]*topo.TabletInfo{
		"zone1-0000000100": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, Type: topodatapb.TabletType_REPLICA}},
		"zone1-0000000101": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Type: topodatapb.TabletType_REPLICA}},
	}
	versionMap := map[string]mysqlctl.ServerVersion{
		"zone1-0000000100": {Major: 8, Minor: 0, Patch: 35},
		"zone1-0000000101": {Major: 8, Minor: 4, Patch: 0},
	}

	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)

	// Before the wait, the newer tablet is ahead on the (stale) executed position
	// and would be chosen as the intermediate source.
	erpBefore := NewEmergencyReparenter(nil, nil, logutil.NewMemoryLogger())
	intermediateSource, _, err := erpBefore.findMostAdvanced(validCandidates, tabletMap, versionMap, nil, EmergencyReparentOptions{durability: durability})
	require.NoError(t, err)
	require.True(t, topoproto.TabletAliasEqual(intermediateSource.Alias, &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}))

	// Both catch up to the relay-log-equivalent position during the wait. For
	// file-position replication that position comes from RelayLogSourceBinlogEquivalentPosition
	// (RelayLogPosition is empty). The reconcile advances Combined to it, so both
	// compare equal and the version tiebreaker selects the lower-version tablet.
	statusMap := map[string]*replicationdatapb.StopReplicationStatus{
		"zone1-0000000100": {After: &replicationdatapb.Status{RelayLogSourceBinlogEquivalentPosition: appliedStr}},
		"zone1-0000000101": {After: &replicationdatapb.Status{RelayLogSourceBinlogEquivalentPosition: appliedStr}},
	}
	tmc := &testutil.TabletManagerClient{
		WaitForPositionResults: map[string]map[string]error{
			"zone1-0000000100": {appliedStr: nil},
			"zone1-0000000101": {appliedStr: nil},
		},
	}
	erp := NewEmergencyReparenter(nil, tmc, logutil.NewMemoryLogger())
	require.NoError(t, erp.waitForAllRelayLogsToApply(t.Context(), validCandidates, tabletMap, statusMap, 30*time.Second))

	intermediateSource, _, err = erp.findMostAdvanced(validCandidates, tabletMap, versionMap, nil, EmergencyReparentOptions{durability: durability})
	require.NoError(t, err)
	require.True(t, topoproto.TabletAliasEqual(intermediateSource.Alias, &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}))
}

// TestEmergencyReparenter_findMostAdvanced_splitBrainSurvivesReconcile guards the
// interaction between the post-wait position reconcile and split-brain
// detection: reconciling each candidate to its own applied position must not
// mask a genuine split brain (two candidates each holding GTIDs the other
// lacks). Because the reconcile advances each candidate only to the position it
// actually applied, the neither-is-a-superset condition is preserved and ERS
// still aborts.
func TestEmergencyReparenter_findMostAdvanced_splitBrainSurvivesReconcile(t *testing.T) {
	sid1 := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}

	// posA has sid1's GTIDs, posB has sid2's — neither is a superset of the other.
	posA := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	posA.GTIDSet = posA.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid1, Sequence: 10})
	posB := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	posB.GTIDSet = posB.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid2, Sequence: 10})

	validCandidates := map[string]*RelayLogPositions{
		"zone1-0000000100": {Combined: posA, Executed: posA},
		"zone1-0000000101": {Combined: posB, Executed: posB},
	}
	tabletMap := map[string]*topo.TabletInfo{
		"zone1-0000000100": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, Type: topodatapb.TabletType_REPLICA}},
		"zone1-0000000101": {Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Type: topodatapb.TabletType_REPLICA}},
	}
	posAStr := replication.EncodePosition(posA)
	posBStr := replication.EncodePosition(posB)
	statusMap := map[string]*replicationdatapb.StopReplicationStatus{
		"zone1-0000000100": {After: &replicationdatapb.Status{RelayLogPosition: posAStr}},
		"zone1-0000000101": {After: &replicationdatapb.Status{RelayLogPosition: posBStr}},
	}
	tmc := &testutil.TabletManagerClient{
		WaitForPositionResults: map[string]map[string]error{
			"zone1-0000000100": {posAStr: nil},
			"zone1-0000000101": {posBStr: nil},
		},
	}

	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)

	erp := NewEmergencyReparenter(nil, tmc, logutil.NewMemoryLogger())
	require.NoError(t, erp.waitForAllRelayLogsToApply(t.Context(), validCandidates, tabletMap, statusMap, 30*time.Second))

	// After the reconcile, the divergent positions remain divergent, so ERS must
	// still detect the split brain.
	_, _, err = erp.findMostAdvanced(validCandidates, tabletMap, nil, nil, EmergencyReparentOptions{durability: durability})
	require.ErrorContains(t, err, "split brain detected between servers")
}
