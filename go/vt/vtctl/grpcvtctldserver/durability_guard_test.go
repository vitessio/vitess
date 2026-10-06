/*
Copyright 2026 The Vitess Authors.

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
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestSetKeyspaceDurabilityPolicyRefusesReplicationModeChange checks that SetKeyspaceDurabilityPolicy
// refuses to switch a keyspace that has an initialized shard between asynchronous replication and
// MySQL Group Replication, which only MigrateReplicationMode does safely, shard by shard, and that it
// refuses any change while a migration is in progress. A keyspace whose shards are not initialized
// yet, and a change that keeps the replication mode, are still allowed.
func TestSetKeyspaceDurabilityPolicyRefusesReplicationModeChange(t *testing.T) {
	primary := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	initialized := &topodatapb.Shard{PrimaryAlias: primary}
	for _, tt := range []struct {
		name     string
		keyspace *topodatapb.Keyspace
		shard    *topodatapb.Shard
		target   string
		// wantErr is a part of the error; empty means that the change is made.
		wantErr string
	}{{
		name:     "semi-sync to group replication, the shard has a primary",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilitySemiSync},
		shard:    initialized,
		target:   policy.DurabilityGroupReplication,
		wantErr:  "use MigrateReplicationMode",
	}, {
		name:     "group replication to semi-sync, the shard has a group",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication},
		shard:    &topodatapb.Shard{GroupReplicationVoters: []*topodatapb.TabletAlias{primary}, GroupReplicationIncarnation: "1790000000"},
		target:   policy.DurabilitySemiSync,
		wantErr:  "use MigrateReplicationMode",
	}, {
		name:     "semi-sync to group replication, the shard has its own policy",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilitySemiSync},
		shard:    &topodatapb.Shard{DurabilityPolicy: policy.DurabilitySemiSync},
		target:   policy.DurabilityGroupReplication,
		wantErr:  "use MigrateReplicationMode",
	}, {
		name:     "a migration is in progress",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication, MigrationSourceDurabilityPolicy: policy.DurabilitySemiSync},
		shard:    &topodatapb.Shard{},
		target:   policy.DurabilityGroupReplicationCrossCell,
		wantErr:  "MigrateReplicationMode is converting keyspace ks",
	}, {
		name:     "a migration source next to an asynchronous policy",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilitySemiSync, MigrationSourceDurabilityPolicy: policy.DurabilitySemiSync},
		shard:    &topodatapb.Shard{},
		target:   policy.DurabilityNone,
		wantErr:  "an older vtctld probably changed it",
	}, {
		// A policy that this vtctld does not know, for example one of a newer version, may run
		// either mode: the change is refused while a shard is initialized.
		name:     "a policy this vtctld does not know, the shard has a primary",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: "a_policy_of_a_newer_version"},
		shard:    initialized,
		target:   policy.DurabilitySemiSync,
		wantErr:  "from an unknown replication mode",
	}, {
		name:     "a policy this vtctld does not know, no shard is initialized",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: "a_policy_of_a_newer_version"},
		shard:    &topodatapb.Shard{},
		target:   policy.DurabilitySemiSync,
	}, {
		name:     "semi-sync to group replication, no shard is initialized",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilitySemiSync},
		shard:    &topodatapb.Shard{},
		target:   policy.DurabilityGroupReplication,
	}, {
		name:     "group replication to group replication, the shard has a group",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication},
		shard:    &topodatapb.Shard{PrimaryAlias: primary, GroupReplicationVoters: []*topodatapb.TabletAlias{primary}, GroupReplicationIncarnation: "1790000000"},
		target:   policy.DurabilityGroupReplicationCrossCell,
	}, {
		name:     "semi-sync to none, the shard has a primary",
		keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilitySemiSync},
		shard:    initialized,
		target:   policy.DurabilityNone,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ts := memorytopo.NewServer(ctx, "zone1")
			t.Cleanup(ts.Close)
			testutil.AddKeyspaces(ctx, t, ts, &vtctldatapb.Keyspace{Name: "ks", Keyspace: tt.keyspace})
			testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{Keyspace: "ks", Name: "-", Shard: tt.shard})
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			_, err := vtctld.SetKeyspaceDurabilityPolicy(ctx, &vtctldatapb.SetKeyspaceDurabilityPolicyRequest{Keyspace: "ks", DurabilityPolicy: tt.target})
			ki, getErr := ts.GetKeyspace(ctx, "ks")
			require.NoError(t, getErr)
			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.target, ki.DurabilityPolicy)
				return
			}
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, tt.wantErr)
			assert.Equal(t, tt.keyspace.DurabilityPolicy, ki.DurabilityPolicy, "the keyspace's policy must not change")
		})
	}
}

// initGateTMC answers FullStatus with a status per tablet, and records the calls that start the
// initialization of a shard.
type initGateTMC struct {
	tmclient.TabletManagerClient

	mu       sync.Mutex
	statuses map[uint32]*replicationdatapb.FullStatus
	calls    []string
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
func (c *initGateTMC) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statuses[tablet.Alias.Uid], nil
}

// ResetReplication is part of the tmclient.TabletManagerClient interface.
func (c *initGateTMC) ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "ResetReplication")
	return nil
}

// InitPrimary is part of the tmclient.TabletManagerClient interface.
func (c *initGateTMC) InitPrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "InitPrimary")
	return "", errors.New("the test stops at InitPrimary")
}

// TestInitShardPrimaryRefusesTabletWithoutGroupReplication checks that InitShardPrimary, under a
// group replication policy, refuses before it changes anything a primary-elect, or a tablet that may
// be a voter, whose vttablet does not run Group Replication (FullStatus field 28) or does not resolve
// the shard's own policy (field 29): it would initialize a writable primary without a group.
func TestInitShardPrimaryRefusesTabletWithoutGroupReplication(t *testing.T) {
	capable := &replicationdatapb.FullStatus{GroupReplicationEnabled: true, ShardDurabilityPolicySupported: true}
	for _, tt := range []struct {
		name     string
		statuses map[uint32]*replicationdatapb.FullStatus
		// wantErr is a part of the error; empty means that the initialization reaches InitPrimary.
		wantErr string
	}{{
		name:     "the primary-elect does not run Group Replication",
		statuses: map[uint32]*replicationdatapb.FullStatus{100: {ShardDurabilityPolicySupported: true}, 200: capable},
		wantErr:  "zone1-0000000100: vttablet does not run Group Replication",
	}, {
		name:     "a replica is an older vttablet",
		statuses: map[uint32]*replicationdatapb.FullStatus{100: capable, 200: {}},
		wantErr:  "zone1-0000000200: vttablet does not run Group Replication",
	}, {
		name:     "both run Group Replication",
		statuses: map[uint32]*replicationdatapb.FullStatus{100: capable, 200: capable},
		wantErr:  "the test stops at InitPrimary",
	}} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ts := memorytopo.NewServer(ctx, "zone1")
			t.Cleanup(ts.Close)
			testutil.AddKeyspaces(ctx, t, ts, &vtctldatapb.Keyspace{Name: "ks", Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication}})
			for _, uid := range []uint32{100, 200} {
				testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: uid}, Keyspace: "ks", Shard: "-",
					Type: topodatapb.TabletType_REPLICA, MysqlHostname: "localhost", MysqlPort: int32(uid),
				}, nil)
			}
			tmc := &initGateTMC{statuses: tt.statuses}
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			_, err := vtctld.InitShardPrimary(ctx, &vtctldatapb.InitShardPrimaryRequest{
				Keyspace: "ks", Shard: "-", PrimaryElectTabletAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, Force: true,
			})
			require.Error(t, err)
			require.ErrorContains(t, err, tt.wantErr)
			tmc.mu.Lock()
			defer tmc.mu.Unlock()
			if tt.wantErr == "the test stops at InitPrimary" {
				assert.Contains(t, tmc.calls, "InitPrimary")
				return
			}
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			assert.Empty(t, tmc.calls, "the initialization must change nothing")
		})
	}
}
