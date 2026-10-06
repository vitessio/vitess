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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// weightedGroupReplicationPolicy is a group replication policy whose members have a weight other
// than MySQL's default, so that a test can tell which policy a join used.
const weightedGroupReplicationPolicy = "test_weighted_group_replication"

type weightedGroupReplication struct {
	policy.GroupReplicationDurabler
}

func (weightedGroupReplication) MemberWeight(*topodatapb.Tablet) int { return 75 }

func init() {
	policy.RegisterDurability(weightedGroupReplicationPolicy, func() policy.Durabler {
		d, err := policy.GetDurabilityPolicy(policy.DurabilityGroupReplicationCrossCell)
		if err != nil {
			panic(err)
		}
		grd, _ := policy.AsGroupReplication(d)
		return weightedGroupReplication{GroupReplicationDurabler: grd}
	})
}

// setShardDurabilityPolicy stores the shard's own durability policy in the shard record of ks/0,
// as MigrateReplicationMode does when it converts the shard.
func setShardDurabilityPolicy(t *testing.T, ts *topo.Server, durability string) {
	t.Helper()
	_, err := ts.GetOrCreateShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	_, err = ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.DurabilityPolicy = durability
		return nil
	})
	require.NoError(t, err)
}

// newConvertedShardTestTM starts tablet cell1-1 of ks/0, one of the three voters of the shard's
// group of incarnation 1780000001, with peers cell1-2 and cell1-3. The keyspace has the policy
// keyspacePolicy, and the shard the policy shardPolicy as its own: the state of a shard that
// MigrateReplicationMode converted while its keyspace is half migrated.
func newConvertedShardTestTM(t *testing.T, keyspacePolicy, shardPolicy string) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *grPeersTMC, *topo.Server) {
	t.Helper()
	ts := newGroupReplicationTopo(t, keyspacePolicy)
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	setGroupReplicationIncarnation(t, ts, "1780000001")
	setShardDurabilityPolicy(t, ts, shardPolicy)
	addPeerTablets(t, ts, 2, 3)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	peers := newGRPeersTMC()
	tm.tmc = peers
	for _, uid := range []uint32{2, 3} {
		peers.set(uid, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(int(uid))})
	}
	return tm, fmd, peers, ts
}

// aloneInView returns the status of cell1-1 as the primary of a view of the recorded incarnation
// that holds no other voter.
func aloneInView() *replicationdatapb.GroupReplicationStatus {
	return withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:20")
}

// TestGroupReplicationSyncFailsClosedInConvertedShard checks that the primary of a shard that was
// converted to Group Replication, while its keyspace's policy is still semi_sync, stops serving
// when its view holds fewer than a majority of the voters, as under a group replication keyspace:
// otherwise it acknowledges writes that exist on a single voter.
func TestGroupReplicationSyncFailsClosedInConvertedShard(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newConvertedShardTestTM(t, policy.DurabilitySemiSync, policy.DurabilityGroupReplicationCrossCell)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())

	fmd.SetGroupReplicationStatus(aloneInView())
	newGroupReplicationSync(tm).reconcile(t.Context())
	assert.False(t, qsc.IsServing(), "a primary without the majority of its voters must not serve")
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
}

// TestGroupReplicationSyncServesInShardConvertedBack checks the reverse: the primary of a shard
// that MigrateReplicationMode is converting back to semi-sync, whose own policy is semi_sync while
// its keyspace's is still group_replication, keeps serving as its group shrinks to the primary.
func TestGroupReplicationSyncServesInShardConvertedBack(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newConvertedShardTestTM(t, policy.DurabilityGroupReplicationCrossCell, policy.DurabilitySemiSync)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())

	fmd.SetGroupReplicationStatus(aloneInView())
	newGroupReplicationSync(tm).reconcile(t.Context())
	assert.True(t, qsc.IsServing(), "the primary of a shard converted back serves while its group shrinks")
}

// TestGroupReplicationSyncRejoinsVoterOfConvertedShard checks that a voter of a shard that was
// converted to Group Replication, while its keyspace's policy is still semi_sync, rejoins its group
// once its MySQL left it, for example after mysqld restarted, and that a PRIMARY tablet whose MySQL
// is no longer in the group is demoted.
func TestGroupReplicationSyncRejoinsVoterOfConvertedShard(t *testing.T) {
	t.Run("rejoin", func(t *testing.T) {
		withGroupReplication(t)
		tm, fmd, _, _ := newConvertedShardTestTM(t, policy.DurabilitySemiSync, policy.DurabilityGroupReplicationCrossCell)
		tm.tmc = activeGroupPeersIn("1780000001", 2, 3)
		fmd.StartGroupReplicationError = nil
		fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
		fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, "")))
		start, _, _ := fmd.GroupReplicationCalls()

		newGroupReplicationSync(tm).reconcile(t.Context())
		start2, _, _ := fmd.GroupReplicationCalls()
		assert.Equal(t, start+1, start2, "the voter rejoins its group")
	})

	t.Run("stale primary", func(t *testing.T) {
		withGroupReplication(t)
		tm, fmd, _, ts := newConvertedShardTestTM(t, policy.DurabilitySemiSync, policy.DurabilityGroupReplicationCrossCell)
		setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
		fmd.SetSuperReadOnlyError = errors.New("MySQL must be left alone")
		fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, "")))

		newGroupReplicationSync(tm).reconcile(t.Context())
		assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
		ti, err := ts.GetTablet(t.Context(), tm.tabletAlias)
		require.NoError(t, err)
		assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	})
}

// TestGroupReplicationSyncFollowsShardPolicyChange checks that the sync loop does not wait for its
// cached durability policy to expire once a shard record it reads sets another policy for the
// shard: MigrateReplicationMode changes it when it converts the shard, and from then on the primary
// must fail closed without the majority of its voters.
func TestGroupReplicationSyncFollowsShardPolicyChange(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, ts := newConvertedShardTestTM(t, policy.DurabilitySemiSync, "")
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SetGroupReplicationStatus(aloneInView())
	s := newGroupReplicationSync(tm)
	s.reconcile(t.Context())
	require.True(t, qsc.IsServing(), "a group that is being formed under a semi-sync policy does not need its voters")

	setShardDurabilityPolicy(t, ts, policy.DurabilityGroupReplicationCrossCell)
	// The loop reads the shard record again, as it does every few seconds, well before its cached
	// policy expires.
	s.recordRead = time.Time{}
	require.Less(t, time.Since(s.durabilityRead), groupReplicationDurabilityCacheTTL)
	s.reconcile(t.Context())
	assert.False(t, qsc.IsServing(), "the primary of the converted shard fails closed without the majority of its voters")
}

// TestGroupReplicationConfigUsesShardPolicy checks that a join derives the member weight from the
// shard's own policy: a voter of a converted shard that rejoins while its keyspace's policy is
// still semi_sync gets the weight of its group replication policy, not MySQL's default.
func TestGroupReplicationConfigUsesShardPolicy(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, _, _, _ := newConvertedShardTestTM(t, policy.DurabilitySemiSync, weightedGroupReplicationPolicy)

	durability, err := tm.shardDurability(ctx)
	require.NoError(t, err)
	cfg, err := tm.groupReplicationConfig(ctx, durability)
	require.NoError(t, err)
	assert.Equal(t, 75, cfg.MemberWeight)

	isVoter, err := tm.isGroupReplicationVoter(ctx)
	require.NoError(t, err)
	assert.True(t, isVoter)
}

// TestFullStatusReportsShardDurabilityPolicySupport checks that FullStatus tells that the tablet
// applies its shard's own durability policy: MigrateReplicationMode refuses voters that do not.
func TestFullStatusReportsShardDurabilityPolicySupport(t *testing.T) {
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, _ := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.FullStatusData = &replicationdatapb.FullStatus{}
	})
	status, err := tm.FullStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, status.ShardDurabilityPolicySupported)
}
