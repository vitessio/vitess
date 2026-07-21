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
	"maps"
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
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
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
			// all three candidates share the same received position: one of them
			// applying its relay logs is enough for the reparent to proceed, even though
			// another is slow and a third fails outright. this used to fail the whole ERS
			name:       "relay log apply failures are tolerated when a peer at the same position applies",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {
						Result: "ok",
						Error:  nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
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
					Hostname: "applies relay logs, wins election",
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
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// a candidate strictly behind on received relay logs cannot win the election,
			// so ERS does not wait for it to apply: a lagger that would blow the
			// wait-replicas-timeout no longer fails the reparent
			name:       "lagging candidate is excluded from the relay log wait",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {Result: "ok"},
					"zone1-0000000101": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-10"),
							},
						},
					},
				},
				// the lagger would take a minute to apply its relay logs, well past the
				// wait-replicas-timeout
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000102": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-10"): nil,
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
					Hostname: "lagging on received relay logs, excluded from the wait",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// when every candidate at the leading received position fails to apply its
			// relay logs, there is no safe winner and the reparent must fail
			name:       "all leading candidates fail to apply relay logs",
			durability: policy.DurabilityNone,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-10"),
							},
						},
					},
				},
				// both leaders fail their waits: their configured results do not cover
				// the position they are asked to wait for
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-10"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-10"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-10"): nil,
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
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
			errShouldContain: "all candidates failed to apply relay logs",
		},
		{
			// preservation guard: for non-GTID-based shards the received position is not
			// tracked separately from the executed one, so the leading-group filter and
			// race are unsafe. every candidate is waited on and any failure fails the
			// reparent, exactly as before. this cell also passes without the filter/race
			// change; it guards against a future change racing non-GTID shards
			name:       "non-GTID-based shards wait for every candidate",
			durability: policy.DurabilityNone,
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
								RelayLogPosition: "FilePos/mysql-bin.0001:100",
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								RelayLogPosition: "FilePos/mysql-bin.0001:100",
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								RelayLogPosition: "FilePos/mysql-bin.0001:10",
							},
						},
					},
				},
				// one candidate fails its wait even though a peer at the same position
				// succeeds: with requireAll semantics that is fatal
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"FilePos/mysql-bin.0001:100": nil,
					},
					"zone1-0000000101": {
						"FilePos/mysql-bin.0001:10": nil,
					},
					"zone1-0000000102": {
						"FilePos/mysql-bin.0001:10": nil,
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
					Hostname: "fails to apply relay logs",
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
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
			errShouldContain: "could not apply all relay logs",
		},
		{
			// preservation guard: incomparable leading positions are a suspected split
			// brain, so the one-success race is not safe: a failed leader must fail the
			// wait instead of being silently dropped before the split brain check runs
			name:       "incomparable leading positions wait for all leaders",
			durability: policy.DurabilityNone,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21", "1-5"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21", "", "1-5"),
							},
						},
					},
				},
				// one of the incomparable leaders fails its wait
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21", "1-5"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "incomparable position, fails to apply relay logs",
				},
			},
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
			errShouldContain: "could not apply all relay logs",
		},
		{
			// the only candidate that applied its relay logs turns out to have errant
			// GTIDs. the surviving candidates were never waited on, so ERS runs a second
			// relay log wait on them before electing one
			name:       "errant GTIDs on the applied candidate trigger a second relay log wait",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000102": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000102": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					// zone1-0000000100 has GTIDs from a second uuid that no other
					// candidate has seen: errant
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100", "1-5"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100"),
							},
						},
					},
				},
				// in the second wait, zone1-0000000101 is slow, so zone1-0000000102 wins
				// the race and the election
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-100", "1-5"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-100"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-100"): nil,
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
					Hostname: "errant GTIDs, applied first",
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
					Hostname: "wins the second relay log wait",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// same as above, but every survivor of errant GTID detection fails the
			// second relay log wait: the reparent must fail rather than promote a tablet
			// with received-but-unapplied transactions
			name:       "errant GTIDs on the applied candidate and failing survivors fail the reparent",
			durability: policy.DurabilityNone,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100", "1-5"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100"),
							},
						},
					},
				},
				// the survivors' configured results do not cover the position they are
				// asked to wait for, so they both fail the second wait
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-100", "1-5"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-10"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-10"): nil,
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
					Hostname: "errant GTIDs, applied first",
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
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
			errShouldContain: "all candidates failed to apply relay logs",
		},
		{
			// zone1-0000000101 is the only tablet that could send semi-sync ACKs to a
			// promoted zone1-0000000100, and it fails its relay log wait with a real
			// error. It must not be counted as a reachable acker: promoting 100 anyway
			// would wedge it waiting for an ACK that may never come. ERS must refuse
			name:       "sole semi-sync acker failing the relay log wait refuses the promotion",
			durability: policy.DurabilitySemiSync,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				// promotion fixtures are deliberately present: without the fix ERS
				// wrongly promotes zone1-0000000100 and this cell must catch that as a
				// wrong success, not as a missing-fixture error
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				// the rdonly is slow (cancelled by the race); the acker fails outright:
				// its configured result doesn't cover the position it is asked for
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000102": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-10"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "applies relay logs, but has no reachable acker",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "sole acker, fails its relay log wait",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
					Hostname: "rdonly, cannot ack",
				},
			},
			shouldErr:        true,
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
			errShouldContain: "no valid candidates for emergency reparent",
		},
		{
			// zone1-0000000101 had replication fully stopped before the reparent, so ERS
			// repoints it without starting it and it can't send semi-sync ACKs until an
			// operator starts it. It must not count as an acker: with it as
			// zone1-0000000100's only acker, 100 still wins the intermediate election
			// (lowest uid among equals) but must not be the final primary; the promotion
			// must fall to 101 itself, whose acker (100) is actually running
			name:       "acker stopped before the reparent doesn't count for the election",
			durability: policy.DurabilitySemiSync,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000101": {Result: "ok"},
				},
				// 101 catches up to the intermediate source 100 before its promotion
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {Position: getRelayLogPosition("1-21")},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					// fully stopped before the reparent, relay log already drained
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateStopped), SqlState: int32(replication.ReplicationStateStopped)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				// the rdonly is slow (cancelled by the race); the stopped 101 is drained
				// so its wait finishes instantly
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000102": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "running, but its only acker is stopped",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "stopped before the reparent, wins with 100 as its acker",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
					Hostname: "rdonly, cannot ack",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// zone1-0000000100 leads on received position only because of an errant GTID
			// (uuid2), and the clean peers received the latest reparent journal entry
			// without applying it yet, so their journal counts read as lagged and can't
			// corroborate the leader. ERS must wait on the skipped peers so their counts
			// become truthful, re-run errant GTID detection, and convict 100
			name:       "errant sole leader is convicted after rescuing the skipped candidates",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Second * 30,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000101": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000102": nil,
				},
				// 100 applied the latest reparent journal entry; 101 and 102 received it
				// but haven't applied it, so their counts only reach 2 once the rescue
				// wait lets them apply
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000100": 2,
					"zone1-0000000101": 1,
					"zone1-0000000102": 1,
				},
				ReadReparentJournalInfoAfterApplyResults: map[string]int32{
					"zone1-0000000101": 2,
					"zone1-0000000102": 2,
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
								RelayLogPosition: getRelayLogPosition("1-21", "1-1"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				// 102 is slow so the rescue race deterministically finishes on 101
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000102": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21", "1-1"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "errant sole leader, must not be promoted",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "clean peer, applies during the rescue wait and wins",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "clean peer, cancelled during the rescue wait",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// same errant sole leader as the cell above, but the skipped candidates have
			// mixed received positions: zone1-0000000102 is further behind and has no
			// journal entry in its backlog, so a rescue race it wins proves nothing. The
			// rescue must wait for all mixed-position candidates so zone1-0000000101 gets
			// to apply the journal entry that convicts 100
			name:       "errant sole leader is convicted when the rescue candidates have mixed positions",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Second * 30,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000101": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000102": nil,
				},
				// only 101 received the latest reparent journal entry; its count reaches
				// 2 once the rescue wait lets it apply. 102 never received it, so
				// applying its empty backlog adds nothing
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000100": 2,
					"zone1-0000000101": 1,
					"zone1-0000000102": 1,
				},
				ReadReparentJournalInfoAfterApplyResults: map[string]int32{
					"zone1-0000000101": 2,
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
								RelayLogPosition: getRelayLogPosition("1-21", "1-1"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-10"),
							},
						},
					},
				},
				// 101 has the bigger backlog to apply; a rescue that races would see 102
				// finish first and cancel 101
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Second * 2,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21", "1-1"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-10"): nil,
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
					Hostname: "errant sole leader, must not be promoted",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "holds the convicting journal entry, slow to apply",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "most lagged, no journal entry to apply",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// same rescue as the cell above, but the further-behind zone1-0000000102 is
			// stuck and can't apply at all. It is strictly dominated by 101, so it can't
			// hold a journal entry 101 lacks: the rescue must only wait on the
			// most-advanced skipped candidates and a stuck straggler must not abort the
			// reparent
			name:       "stuck dominated candidate doesn't block the errant GTID rescue",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Second * 30,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000101": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000101": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000102": nil,
				},
				// only 101 received the latest reparent journal entry; its count reaches
				// 2 once the rescue wait lets it apply
				ReadReparentJournalInfoResults: map[string]int32{
					"zone1-0000000100": 2,
					"zone1-0000000101": 1,
					"zone1-0000000102": 1,
				},
				ReadReparentJournalInfoAfterApplyResults: map[string]int32{
					"zone1-0000000101": 2,
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
								RelayLogPosition: getRelayLogPosition("1-21", "1-1"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-10"),
							},
						},
					},
				},
				// 102 has no WaitForPosition stub at all: any wait on it fails instantly
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21", "1-1"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "errant sole leader, must not be promoted",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "holds the convicting journal entry",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "stuck and dominated, must not block the rescue",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// zone1-0000000101 is the sole semi-sync acker and lost the relay log race:
			// its applier may still be busy, so its repoint can stall in STOP REPLICA.
			// Promotion must not proceed until its SetReplicationSource is confirmed;
			// here it fails, and ERS must abort before anything is made read-write
			name:       "promotion depending on a cancelled acker aborts when its repoint fails",
			durability: policy.DurabilitySemiSync,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Second * 30,
			},
			tmc: &testutil.TabletManagerClient{
				SetReplicationSourceResults: map[string]error{
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				// 101 loses the relay log race to 100 and is cancelled mid-apply
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "wins the race, must not be promoted unconfirmed",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "sole acker, cancelled mid-apply, repoint fails",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
					Hostname: "rdonly, cannot ack",
				},
			},
			shouldErr:        true,
			errShouldContain: "not enough semi-sync ackers were reachable to guarantee the promotion of zone1-0000000100 can make progress",
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
		},
		{
			// the healthy version of the cell above: the cancelled acker's repoint
			// completes, the quorum forms, and the promotion proceeds
			name:       "promotion depending on a cancelled acker proceeds once its repoint completes",
			durability: policy.DurabilitySemiSync,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Second * 30,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "wins the race, promoted after the acker quorum forms",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "sole acker, cancelled mid-apply, repoint completes",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
					Hostname: "rdonly, cannot ack",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// like the cancelled-acker cell, but the sole acker was skipped from the
			// relay log wait entirely (behind on received position): its applier is
			// just as busy, and the promotion must wait for its repoint the same way
			name:       "promotion depending on a skipped lagging acker aborts when its repoint fails",
			durability: policy.DurabilitySemiSync,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Second * 30,
			},
			tmc: &testutil.TabletManagerClient{
				SetReplicationSourceResults: map[string]error{
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-19"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "leads on received position, must not be promoted unconfirmed",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "sole acker, behind on received, skipped from the wait, repoint fails",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
					Hostname: "rdonly, cannot ack",
				},
			},
			shouldErr:        true,
			errShouldContain: "not enough semi-sync ackers were reachable to guarantee the promotion of zone1-0000000100 can make progress",
			keyspace:         "testkeyspace",
			shard:            "-",
			cells:            []string{"zone1"},
		},
		{
			// the sole acker zone1-0000000101 is a demoted former primary with no
			// replication configured (absent from the replica status map): its repoint
			// always starts replication, so it counts towards the acker quorum and the
			// promotion proceeds
			name:       "former primary acker counts towards the promotion quorum",
			durability: policy.DurabilitySemiSync,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Second * 30,
			},
			tmc: &testutil.TabletManagerClient{
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000101": {
						Status: &replicationdatapb.PrimaryStatus{
							Position: getRelayLogPosition("1-21"),
						},
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					// the former primary fails the replica RPC and is demoted instead
					"zone1-0000000101": {
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000100": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "wins the election, promoted once the former primary is repointed",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
					Hostname: "demoted former primary, sole acker, repoint starts it",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
					Hostname: "rdonly, cannot ack",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// the mirror of the cell above: the sole acker zone1-0000000101 is only
			// slow, so the race cancels its wait rather than failing it. A cancelled
			// wait says nothing about the tablet, so it must still count as a reachable
			// acker and the promotion must succeed
			name:       "sole semi-sync acker cancelled by the race still counts",
			durability: policy.DurabilitySemiSync,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-21"),
							},
						},
					},
				},
				// both peers are slow, so zone1-0000000100 wins the race and the others
				// are cancelled, never failed
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Minute,
					"zone1-0000000102": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-21"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-21"): nil,
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
					Hostname: "applies relay logs, wins election",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "sole acker, slow but healthy",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
					Hostname: "rdonly, cannot ack",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
		},
		{
			// zone1-0000000100 and zone1-0000000101 both received GTIDs from a second
			// uuid that the lagging zone1-0000000102 never saw. 101 fails its relay log
			// wait and is removed from candidacy, but its received position must still
			// corroborate those GTIDs, otherwise 100 would be falsely flagged errant and
			// the behind 102 elected instead
			name:       "failed leading candidate still corroborates errant GTID detection",
			durability: policy.DurabilityNone,
			emergencyReparentOps: EmergencyReparentOptions{
				WaitReplicasTimeout: time.Millisecond * 50,
			},
			tmc: &testutil.TabletManagerClient{
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000100": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000100": {Result: "ok"},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000101": nil,
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
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100", "1-5"),
							},
						},
					},
					"zone1-0000000101": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100", "1-5"),
							},
						},
					},
					"zone1-0000000102": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "00000000-0000-0000-0000-000000000001",
								RelayLogPosition: getRelayLogPosition("1-100"),
							},
						},
					},
				},
				// zone1-0000000101's configured result doesn't cover the position it is
				// asked to wait for, so it fails the wait
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						getRelayLogPosition("1-100", "1-5"): nil,
					},
					"zone1-0000000101": {
						getRelayLogPosition("1-10"): nil,
					},
					"zone1-0000000102": {
						getRelayLogPosition("1-100"): nil,
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
					Hostname: "applies relay logs, wins election",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "fails to apply relay logs, corroborates as evidence",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Hostname: "behind on received relay logs",
				},
			},
			shouldErr: false,
			keyspace:  "testkeyspace",
			shard:     "-",
			cells:     []string{"zone1"},
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

	testutil.AddTablets(
		ctx, t, ts, nil,
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

		// Now simulate every candidate taking too long while applying relay logs. A single
		// candidate applying in time would let the reparent proceed, so all of them have
		// to time out for ERS to abort during relay log application.
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
			DoAndReturn(func(ctx context.Context, tablet *topodatapb.Tablet, position string) error {
				<-ctx.Done()
				return ctx.Err()
			}).
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

		testutil.AddTablets(
			ctx, t, ts, nil,
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

		require.ErrorContains(t, err, "all candidates failed to apply relay logs within the provided waitReplicasTimeout")
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
			ev := &events.Reparent{
				ShardInfo: *topo.NewShardInfo(tt.keyspace, tt.shard, &topodatapb.Shard{
					PrimaryAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				}, nil),
			}
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

func TestEmergencyReparenter_waitForRelayLogsToApply(t *testing.T) {
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
		requireAll bool
		// cancelCtx runs the wait with an already-cancelled parent context.
		cancelCtx bool
		// ctxTimeout runs the wait with a parent context that expires after the given
		// duration, like the shared relay log deadline does.
		ctxTimeout  time.Duration
		shouldErr   bool
		errContains string
		// waitReplicasTimeout overrides the default 50ms timeout. Cells that assert the
		// per-tablet outcome and don't need the deadline to fire use a generous timeout,
		// so a starved CI runner can't turn a cancelled tablet into a deadline failure.
		waitReplicasTimeout time.Duration
		// checkOutcome asserts the per-tablet outcome below. Left false for cells where
		// goroutine scheduling makes the split between the sets nondeterministic.
		checkOutcome  bool
		wantApplied   []string
		wantFailed    []string
		wantCancelled []string
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
			requireAll:   true,
			shouldErr:    false,
			checkOutcome: true,
			wantApplied:  []string{"zone1-0000000100", "zone1-0000000101"},
		},
		{
			// requireAll's first genuine failure cancels the remaining waits: the peer
			// must be classified as cancelled (kept as a candidate), never as failed
			// (removed from candidacy)
			name: "requireAll true, a failure cancels the remaining waits",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position2": nil, // asked to wait for "position1", fails instantly
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
			requireAll:          true,
			waitReplicasTimeout: 30 * time.Second,
			shouldErr:           true,
			errContains:         "could not apply all relay logs",
			checkOutcome:        true,
			wantApplied:         []string{},
			wantFailed:          []string{"zone1-0000000100"},
			wantCancelled:       []string{"zone1-0000000101"},
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
			requireAll:  true,
			shouldErr:   true,
			errContains: "could not apply all relay logs",
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
			requireAll:  true,
			shouldErr:   true,
			errContains: "could not apply all relay logs",
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
			requireAll:    true,
			shouldErr:     true,
			errContains:   "could not apply all relay logs",
			checkOutcome:  true,
			wantApplied:   []string{"zone1-0000000100"},
			wantFailed:    []string{"zone1-0000000101"},
			wantCancelled: []string{},
		},
		{
			// the first candidate to finish applying cancels the remaining waits: the
			// slow peer must end up cancelled, not failed at the group deadline
			name: "requireAll false, first success cancels the remaining waits",
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
			requireAll:          false,
			waitReplicasTimeout: 30 * time.Second,
			shouldErr:           false,
			checkOutcome:        true,
			wantApplied:         []string{"zone1-0000000100"},
			wantFailed:          []string{},
			wantCancelled:       []string{"zone1-0000000101"},
		},
		{
			// a genuine failure before any success must be reported as failed even
			// though a peer later succeeds and the wait as a whole passes
			name: "requireAll false, genuine failure before a success",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000101": 10 * time.Millisecond,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position2": nil, // asked to wait for "position1", fails instantly
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
			requireAll:          false,
			waitReplicasTimeout: 30 * time.Second,
			shouldErr:           false,
			checkOutcome:        true,
			wantApplied:         []string{"zone1-0000000101"},
			wantFailed:          []string{"zone1-0000000100"},
			wantCancelled:       []string{},
		},
		{
			name: "requireAll false, all candidates fail",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position2": nil,
					},
					"zone1-0000000101": {
						"position2": nil,
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
			requireAll:          false,
			waitReplicasTimeout: 30 * time.Second,
			shouldErr:           true,
			errContains:         "all candidates failed to apply relay logs",
			checkOutcome:        true,
			wantApplied:         []string{},
			wantFailed:          []string{"zone1-0000000100", "zone1-0000000101"},
			wantCancelled:       []string{},
		},
		{
			name: "requireAll false, aborted parent context",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000100": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position1": nil,
					},
				},
			},
			candidates: map[string]*RelayLogPositions{
				"zone1-0000000100": {},
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
			},
			requireAll:          false,
			waitReplicasTimeout: 30 * time.Second,
			cancelCtx:           true,
			shouldErr:           true,
			errContains:         "emergency reparent aborted while waiting for relay logs to apply",
			checkOutcome:        true,
			wantApplied:         []string{},
			wantFailed:          []string{},
			wantCancelled:       []string{"zone1-0000000100"},
		},
		{
			// the parent deadline expires while the only wait comes back with a
			// cancellation-coded RPC error (server-side teardown racing the client
			// deadline). a deadline expiry is not an intentional cancellation: the
			// tablet didn't finish in budget and counts as failed, and this must be an
			// error, never a success without a single applied candidate
			name: "requireAll false, deadline expiry with cancellation-coded waits is not a success",
			tmc: &testutil.TabletManagerClient{
				// the result arrives well after the parent deadline below
				WaitForPositionPostDelays: map[string]time.Duration{
					"zone1-0000000100": time.Second,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position1": vterrors.New(vtrpc.Code_CANCELED, "replication wait torn down"),
					},
				},
			},
			candidates: map[string]*RelayLogPositions{
				"zone1-0000000100": {},
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
			statusMap: map[string]*replicationdatapb.StopReplicationStatus{
				"zone1-0000000100": {
					After: &replicationdatapb.Status{
						RelayLogPosition: "position1",
					},
				},
			},
			requireAll:          false,
			waitReplicasTimeout: 30 * time.Second,
			ctxTimeout:          200 * time.Millisecond,
			shouldErr:           true,
			errContains:         "all candidates failed to apply relay logs",
			checkOutcome:        true,
			wantApplied:         []string{},
			wantFailed:          []string{"zone1-0000000100"},
			wantCancelled:       []string{},
		},
		{
			// same shape in requireAll mode with a peer that applied: the
			// cancellation-coded tablet never applied, so treating it as "cancelled"
			// would let requireAll return success without waiting for everyone. it
			// must be classified as failed and fail the wait
			name: "requireAll true, deadline expiry with cancellation-coded waits still fails",
			tmc: &testutil.TabletManagerClient{
				// the failing result arrives well after the parent deadline below
				WaitForPositionPostDelays: map[string]time.Duration{
					"zone1-0000000101": time.Second,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"position1": nil,
					},
					"zone1-0000000101": {
						"position1": vterrors.New(vtrpc.Code_CANCELED, "replication wait torn down"),
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
			requireAll:          true,
			waitReplicasTimeout: 30 * time.Second,
			ctxTimeout:          200 * time.Millisecond,
			shouldErr:           true,
			errContains:         "could not apply all relay logs",
			checkOutcome:        true,
			wantApplied:         []string{"zone1-0000000100"},
			wantFailed:          []string{"zone1-0000000101"},
			wantCancelled:       []string{},
		},
		{
			// a former primary has no relay logs to apply and is skipped: it must not
			// count as a finished candidate or cancel the race between the replicas
			name: "requireAll false, former primary is skipped and does not short-circuit the race",
			tmc: &testutil.TabletManagerClient{
				WaitForPositionDelays: map[string]time.Duration{
					"zone1-0000000102": time.Minute,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000101": {
						"position1": nil,
					},
					"zone1-0000000102": {
						"position1": nil,
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
				// zone1-0000000100 is intentionally absent: it was the primary
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
			requireAll:          false,
			waitReplicasTimeout: 30 * time.Second,
			shouldErr:           false,
			checkOutcome:        true,
			wantApplied:         []string{"zone1-0000000101"},
			wantFailed:          []string{},
			wantCancelled:       []string{"zone1-0000000102"},
		},
		{
			// nothing to wait on at all (e.g. the former primary is the only candidate
			// at the leading position) is a success, not an error
			name: "requireAll false, no candidates need to apply relay logs",
			tmc:  &testutil.TabletManagerClient{},
			candidates: map[string]*RelayLogPositions{
				"zone1-0000000100": {},
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
			statusMap:           map[string]*replicationdatapb.StopReplicationStatus{},
			requireAll:          false,
			waitReplicasTimeout: 30 * time.Second,
			shouldErr:           false,
			checkOutcome:        true,
			wantApplied:         []string{},
			wantFailed:          []string{},
			wantCancelled:       []string{},
		},
		{
			name: "requireAll true, no waiters and an aborted parent context",
			tmc:  &testutil.TabletManagerClient{},
			candidates: map[string]*RelayLogPositions{
				"zone1-0000000100": {},
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
			statusMap:   map[string]*replicationdatapb.StopReplicationStatus{},
			requireAll:  true,
			cancelCtx:   true,
			shouldErr:   true,
			errContains: "emergency reparent aborted while waiting for relay logs to apply",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cellCtx := ctx
			if tt.cancelCtx {
				var cancel context.CancelFunc
				cellCtx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if tt.ctxTimeout != 0 {
				var cancel context.CancelFunc
				cellCtx, cancel = context.WithTimeout(cellCtx, tt.ctxTimeout)
				defer cancel()
			}
			cellTimeout := waitReplicasTimeout
			if tt.waitReplicasTimeout != 0 {
				cellTimeout = tt.waitReplicasTimeout
			}

			erp := NewEmergencyReparenter(nil, tt.tmc, logger)
			result, err := erp.waitForRelayLogsToApply(cellCtx, tt.candidates, tt.tabletMap, tt.statusMap, cellTimeout, tt.requireAll)
			if tt.shouldErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.ErrorContains(t, err, tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}

			if tt.checkOutcome {
				require.NotNil(t, result)
				assert.ElementsMatch(t, tt.wantApplied, result.applied, "applied mismatch")
				assert.ElementsMatch(t, tt.wantFailed, result.failed, "failed mismatch")
				assert.ElementsMatch(t, tt.wantCancelled, result.cancelled, "cancelled mismatch")
			}
		})
	}
}

func TestEmergencyReparenter_applyRelayLogsAndReconcile(t *testing.T) {
	t.Parallel()

	logger := logutil.NewMemoryLogger()
	// generous: no scenario here needs the deadline to fire, and a starved CI runner
	// must not turn a cancelled tablet into a deadline failure
	waitReplicasTimeout := 30 * time.Second

	tabletMap := map[string]*topo.TabletInfo{}
	for uid := 100; uid <= 103; uid++ {
		alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: uint32(uid)}
		tabletMap[topoproto.TabletAliasString(alias)] = &topo.TabletInfo{
			Tablet: &topodatapb.Tablet{Alias: alias},
		}
	}

	// zone1-0000000100 applies its relay logs, zone1-0000000101 fails to,
	// zone1-0000000102 is cancelled after 100 finishes, and zone1-0000000103 is a lagging
	// candidate that is not waited on at all.
	statusMap := map[string]*replicationdatapb.StopReplicationStatus{
		"zone1-0000000100": {After: &replicationdatapb.Status{RelayLogPosition: "position1"}},
		"zone1-0000000101": {After: &replicationdatapb.Status{RelayLogPosition: "position1"}},
		"zone1-0000000102": {After: &replicationdatapb.Status{RelayLogPosition: "position1"}},
	}
	newCandidates := func(t *testing.T) map[string]*RelayLogPositions {
		return map[string]*RelayLogPositions{
			"zone1-0000000100": {
				Combined: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"),
				Executed: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"),
			},
			"zone1-0000000101": {
				Combined: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"),
				Executed: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-4"),
			},
			"zone1-0000000102": {
				Combined: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"),
				Executed: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"),
			},
			"zone1-0000000103": {
				Combined: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3"),
				Executed: mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3"),
			},
		}
	}
	waitCandidatesOf := func(candidates map[string]*RelayLogPositions) map[string]*RelayLogPositions {
		return map[string]*RelayLogPositions{
			"zone1-0000000100": candidates["zone1-0000000100"],
			"zone1-0000000101": candidates["zone1-0000000101"],
			"zone1-0000000102": candidates["zone1-0000000102"],
		}
	}

	t.Run("applied bumped, failed removed, cancelled and unwaited untouched", func(t *testing.T) {
		t.Parallel()

		tmc := &testutil.TabletManagerClient{
			WaitForPositionDelays: map[string]time.Duration{
				"zone1-0000000102": time.Minute,
			},
			WaitForPositionResults: map[string]map[string]error{
				"zone1-0000000100": {"position1": nil},
				"zone1-0000000101": {"position2": nil}, // asked for "position1", fails
				"zone1-0000000102": {"position1": nil},
			},
		}

		candidates := newCandidates(t)
		erp := NewEmergencyReparenter(nil, tmc, logger)
		reconciled, waitResult, err := erp.applyRelayLogsAndReconcile(t.Context(), waitCandidatesOf(candidates), candidates, tabletMap, statusMap, waitReplicasTimeout, false, true)
		require.NoError(t, err)
		require.NotNil(t, waitResult)

		assert.ElementsMatch(t, []string{"zone1-0000000100"}, waitResult.applied)
		assert.ElementsMatch(t, []string{"zone1-0000000101"}, waitResult.failed)
		assert.ElementsMatch(t, []string{"zone1-0000000102"}, waitResult.cancelled)

		// the failed candidate is no longer promotable
		assert.NotContains(t, reconciled, "zone1-0000000101")

		// the applied candidate has fully executed what it received
		assert.True(t, reconciled["zone1-0000000100"].Executed.Equal(reconciled["zone1-0000000100"].Combined))

		// the cancelled and unwaited candidates keep their received positions
		assert.True(t, reconciled["zone1-0000000102"].Executed.Equal(mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6")))
		assert.True(t, reconciled["zone1-0000000103"].Executed.Equal(mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3")))
	})

	t.Run("wait error leaves the candidates unmutated", func(t *testing.T) {
		t.Parallel()

		tmc := &testutil.TabletManagerClient{
			WaitForPositionResults: map[string]map[string]error{
				"zone1-0000000100": {"position1": nil},
				"zone1-0000000101": {"position2": nil}, // asked for "position1", fails
				"zone1-0000000102": {"position1": nil},
			},
		}

		candidates := newCandidates(t)
		erp := NewEmergencyReparenter(nil, tmc, logger)
		reconciled, _, err := erp.applyRelayLogsAndReconcile(t.Context(), waitCandidatesOf(candidates), candidates, tabletMap, statusMap, waitReplicasTimeout, true, true)
		require.ErrorContains(t, err, "could not apply all relay logs")

		// on error the caller's candidates come back as-is: nothing removed, nothing bumped
		assert.Len(t, reconciled, 4)
		assert.True(t, reconciled["zone1-0000000100"].Executed.Equal(mustPosition(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")))
	})

	t.Run("non-GTID candidates never get their Executed position bumped", func(t *testing.T) {
		t.Parallel()

		tmc := &testutil.TabletManagerClient{
			WaitForPositionResults: map[string]map[string]error{
				"zone1-0000000100": {"position1": nil},
				"zone1-0000000101": {"position1": nil},
				"zone1-0000000102": {"position1": nil},
			},
		}

		// non-GTID candidates store their executed position in Combined and leave
		// Executed intentionally zero; the bump must not rewrite it, or every waited
		// replica would sort ahead of an equal-position former primary
		filePos, err := replication.DecodePosition("FilePos/binlog.000001:1000")
		require.NoError(t, err)
		candidates := map[string]*RelayLogPositions{
			"zone1-0000000100": {Combined: filePos},
			"zone1-0000000101": {Combined: filePos},
			"zone1-0000000102": {Combined: filePos},
		}
		erp := NewEmergencyReparenter(nil, tmc, logger)
		reconciled, waitResult, err := erp.applyRelayLogsAndReconcile(t.Context(), maps.Clone(candidates), candidates, tabletMap, statusMap, waitReplicasTimeout, true, false)
		require.NoError(t, err)
		require.NotNil(t, waitResult)
		assert.Len(t, waitResult.applied, 3)

		for alias, pos := range reconciled {
			assert.True(t, pos.Executed.IsZero(), "%s Executed should stay zero, got %v", alias, pos.Executed)
		}
	})
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

	// same combined gtid set as positionMostAdvanced, but with a gap in the executed
	// gtid set, as seen with multi-threaded (parallel) replication applies. the executed
	// sets of positionMtsGaps and positionMostAdvanced are incomparable: neither
	// contains the other
	positionMtsGaps := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionMtsGaps.Combined.GTIDSet = positionMtsGaps.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionMtsGaps.Combined.GTIDSet = positionMtsGaps.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionMtsGaps.Combined.GTIDSet = positionMtsGaps.Combined.GTIDSet.AddGTID(mysqlGTID3)
	positionMtsGaps.Executed.GTIDSet = positionMtsGaps.Executed.GTIDSet.AddGTID(mysqlGTID1)
	positionMtsGaps.Executed.GTIDSet = positionMtsGaps.Executed.GTIDSet.AddGTID(mysqlGTID3)

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
			// incomparable executed gtid sets at an equal combined position happen with
			// multi-threaded replication applies and are not a split brain: split brain
			// is a property of the received (combined) history only
			name: "no split brain false positive on executed gtid skew at equal combined positions",
			validCandidates: map[string]*RelayLogPositions{
				"zone1-0000000100": positionMostAdvanced,
				"zone1-0000000101": positionMtsGaps,
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
			result: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
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
		},
	}

	durability, _ := policy.GetDurabilityPolicy(policy.DurabilityNone)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			erp := NewEmergencyReparenter(nil, nil, logutil.NewMemoryLogger())

			test.emergencyReparentOps.durability = durability
			winningTablet, _, err := erp.findMostAdvanced(test.validCandidates, test.tabletMap, test.emergencyReparentOps)
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
				ShardInfo: *topo.NewShardInfo(tt.keyspace, tt.shard, &topodatapb.Shard{
					PrimaryAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  0o00,
					},
				}, nil),
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
			durability, _ := policy.GetDurabilityPolicy(policy.DurabilityNone)
			test.emergencyReparentOps.durability = durability
			logger := logutil.NewMemoryLogger()

			erp := NewEmergencyReparenter(nil, nil, logger)
			res, err := erp.identifyPrimaryCandidate(test.intermediateSource, test.validCandidates, test.tabletMap, test.emergencyReparentOps)
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
		nonAckers           []string
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
			// a non-acker doesn't count towards another candidate's forward progress
			name:                "filter candidate whose only acker is a non-acker",
			durability:          policy.DurabilitySemiSync,
			validTablets:        []*topodatapb.Tablet{replicaTablet},
			tabletsReachable:    []*topodatapb.Tablet{replicaTablet, replicaCrossCellTablet},
			nonAckers:           []string{topoproto.TabletAliasString(replicaCrossCellTablet.Alias)},
			tabletsTakingBackup: noTabletsTakingBackup,
			filteredTablets:     []*topodatapb.Tablet{},
		}, {
			// a non-acker was still reached, so it remains individually promotable; the
			// other replica loses its only acker (the non-acker) and is filtered instead
			name:                "non-acker candidate remains promotable",
			durability:          policy.DurabilitySemiSync,
			validTablets:        []*topodatapb.Tablet{replicaTablet, replicaCrossCellTablet},
			tabletsReachable:    []*topodatapb.Tablet{replicaTablet, replicaCrossCellTablet},
			nonAckers:           []string{topoproto.TabletAliasString(replicaTablet.Alias)},
			tabletsTakingBackup: noTabletsTakingBackup,
			filteredTablets:     []*topodatapb.Tablet{replicaTablet},
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
			tabletList, err := erp.filterValidCandidates(tt.validTablets, tt.tabletsReachable, tt.nonAckers, tt.tabletsTakingBackup, tt.prevPrimary, tt.opts)
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
		wantStarved              []string
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
			wantStarved:              []string{"zone1-0000000102"},
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
			wantStarved:              []string{"zone1-0000000103"},
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
			wantStarved:              []string{"zone1-0000000103"},
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
			wantStarved:              []string{"zone1-0000000103"},
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
			candidates, starved, err := erp.findErrantGTIDs(t.Context(), validCandidates, tt.statusMap, tt.tabletMap, 10*time.Second, nil)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantStarved, starved)
			keys := make([]string, 0, len(candidates))
			for key := range candidates {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			require.ElementsMatch(t, tt.wantedCandidates, keys)

			dp, err := policy.GetDurabilityPolicy(policy.DurabilitySemiSync)
			require.NoError(t, err)
			ers := EmergencyReparenter{logger: logutil.NewCallbackLogger(func(*logutilpb.Event) {})}
			winningPrimary, _, err := ers.findMostAdvanced(candidates, tt.tabletMap, EmergencyReparentOptions{durability: dp})
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

	candidates, starved, err := erp.findErrantGTIDs(t.Context(), validCandidates, statusMap, tabletMap, 10*time.Second, nil)
	require.NoError(t, err)
	require.Contains(t, candidates, "zone1-0000000102")
	// the nil peer at maxLen contributed no evidence, so the surviving candidate was
	// accepted without any comparison
	assert.ElementsMatch(t, []string{"zone1-0000000102"}, starved)
}
