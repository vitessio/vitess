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

package tabletmanager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// shardVoterIdentities returns the voter identities that the shard record holds, by alias, the
// entries of tablets that are no longer listed included.
func shardVoterIdentities(t *testing.T, ts *topo.Server) map[string]*topodatapb.GroupReplicationVoterIdentity {
	t.Helper()
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	identities := make(map[string]*topodatapb.GroupReplicationVoterIdentity)
	for _, identity := range si.GetGroupReplicationVoterIdentities() {
		identities[topoproto.TabletAliasString(identity.GetTablet().GetAlias())] = identity
	}
	return identities
}

// TestGroupReplicationSyncPublishesVoterIdentity checks that the sync loop of a listed voter writes
// its identity to the shard record, its address and its MySQL's server_uuid, so that VTOrc finds the
// voter after its tablet record was deleted, also after VTOrc restarted. It replaces an identity that is
// outdated, drops the entries of tablets that are no longer listed, and keeps those of the other voters.
// A tablet that is not listed writes nothing.
func TestGroupReplicationSyncPublishesVoterIdentity(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	fmd.ServerUUID = testServerUUID(1)
	fmd.SetGroupReplicationStatus(nonVoterView(2))
	voter2 := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, Keyspace: "ks", Shard: "0", Hostname: "host2"}
	gone := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 9}, Keyspace: "ks", Shard: "0", Hostname: "host9"}
	_, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationVoterIdentities = []*topodatapb.GroupReplicationVoterIdentity{
			policy.NewGroupVoterIdentity(voter2, testServerUUID(2)),
			policy.NewGroupVoterIdentity(gone, testServerUUID(9)),
		}
		return nil
	})
	require.NoError(t, err)
	s := newGroupReplicationSync(tm)

	s.reconcile(ctx)
	identities := shardVoterIdentities(t, ts)
	assert.Len(t, identities, 2, "the entry of tablet 9, which is not listed, is dropped")
	assert.True(t, proto.Equal(policy.NewGroupVoterIdentity(tm.Tablet(), testServerUUID(1)), identities["cell1-0000000001"]), "got %v", identities["cell1-0000000001"])
	assert.True(t, proto.Equal(policy.NewGroupVoterIdentity(voter2, testServerUUID(2)), identities["cell1-0000000002"]), "the identity of another voter is kept")

	// The tablet's MySQL was replaced, with a new server_uuid: the next try replaces the identity.
	fmd.ServerUUID = testServerUUID(11)
	s.identityChecked = time.Time{}
	s.reconcile(ctx)
	assert.Equal(t, testServerUUID(11), shardVoterIdentities(t, ts)["cell1-0000000001"].GetServerUuid())

	// A tablet that is not listed writes nothing.
	setGroupReplicationVoters(t, ts, 2, 3)
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationVoterIdentities = nil
		return nil
	})
	require.NoError(t, err)
	s.identityChecked, s.recordRead = time.Time{}, time.Time{}
	s.reconcile(ctx)
	assert.Empty(t, shardVoterIdentities(t, ts))
}
