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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"

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
