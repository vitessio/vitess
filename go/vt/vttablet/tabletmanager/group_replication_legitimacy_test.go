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
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// grPeersTMC answers FullStatus for the other tablets of the shard.
type grPeersTMC struct {
	tmclient.TabletManagerClient

	mu       sync.Mutex
	statuses map[string]*replicationdatapb.FullStatus
	calls    int
}

func newGRPeersTMC() *grPeersTMC {
	return &grPeersTMC{statuses: make(map[string]*replicationdatapb.FullStatus)}
}

func (c *grPeersTMC) set(uid uint32, status *replicationdatapb.FullStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statuses[topoproto.TabletAliasString(&topodatapb.TabletAlias{Cell: "cell1", Uid: uid})] = status
}

func (c *grPeersTMC) fullStatusCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
func (c *grPeersTMC) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	status, ok := c.statuses[topoproto.TabletAliasString(tablet.Alias)]
	if !ok {
		return nil, errors.New("tablet unreachable")
	}
	return status.CloneVT(), nil
}

// addPeerTablets creates the tablet records cell1-<uid> of ks/0, which publish a group
// replication port.
func addPeerTablets(t *testing.T, ts *topo.Server, uids ...uint32) {
	t.Helper()
	for _, uid := range uids {
		require.NoError(t, ts.CreateTablet(t.Context(), &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: uid}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA,
			Hostname: fmt.Sprintf("tablet%d", uid), MysqlHostname: fmt.Sprintf("mysql%d", uid), MysqlPort: 3306,
			PortMap: map[string]int32{"gr": 33060 + int32(uid)},
		}))
	}
}

// setGroupReplicationIncarnation records the incarnation of the group of ks/0 in the shard record.
func setGroupReplicationIncarnation(t *testing.T, ts *topo.Server, incarnation string) {
	t.Helper()
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = incarnation
		return nil
	})
	require.NoError(t, err)
}

// withViewID sets the view id of a status.
func withViewID(status *replicationdatapb.GroupReplicationStatus, viewID string) *replicationdatapb.GroupReplicationStatus {
	status.ViewId = viewID
	return status
}

// newLegitimacyTestTM starts tablet cell1-1 of ks/0 under a group replication policy, with peers
// cell1-2 and cell1-3, the three voters of the shard's group of incarnation 1780000001. Its own
// joins fail, so that only the sync loop changes its state.
func newLegitimacyTestTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *grPeersTMC, *topo.Server) {
	t.Helper()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	setGroupReplicationIncarnation(t, ts, "1780000001")
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

// TestGroupReplicationSyncLeavesForeignGroup reproduces S7d of the Group Replication failover
// audit: after the partitions healed, a member that was made to join again ended up alone in a
// new group incarnation, ONLINE and PRIMARY with quorum in its view of one. The sync loop must not
// promote its tablet: that group lacks the transactions the other members acknowledged. It makes
// MySQL leave the group and suspends its own rejoins instead.
func TestGroupReplicationSyncLeavesForeignGroup(t *testing.T) {
	enableGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "17907858161940982:1"))
	require.True(t, mysql.IsGroupPrimary(tmStatus(t, fmd)))
	_, stopsBefore, _ := fmd.GroupReplicationCalls()

	newGroupReplicationSync(tm).reconcile(ctx)

	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type, "the primary of a foreign group must not become the shard primary")
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, stopsBefore+1, stops, "MySQL must leave the foreign group")
	assert.Equal(t, mysql.GroupMemberStateOffline, tmStatus(t, fmd).MemberState)
	assert.True(t, fmd.SuperReadOnly.Load())
	assert.True(t, tm.groupReplicationRejoinSuspended.Load(), "the tablet must not rejoin on its own")
}

// TestGroupReplicationSyncPromotesOnlyWithVoterMajority checks that the sync loop only promotes the
// primary of a group view that holds a majority of the shard's listed voters. The voters are found
// in the view by the server_uuids that their tablets report: MySQL reports its own hostname, which
// need not match the tablet record.
func TestGroupReplicationSyncPromotesOnlyWithVoterMajority(t *testing.T) {
	enableGroupReplication(t)
	ctx := t.Context()
	tm, fmd, peers, _ := newLegitimacyTestTM(t)
	s := newGroupReplicationSync(tm)

	// Alone in the recorded incarnation: the others left the group, which shrank to one member.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:12"))
	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, stops, "a member of the shard's group is not made to leave it")

	// Non-voters in the view do not count.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(7), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:13"))
	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)

	// Voter cell1-3 is ONLINE in the view: two of the three voters.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:14"))
	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.Positive(t, peers.fullStatusCalls(), "the voters' server_uuids come from their FullStatus")
}

// TestGroupReplicationSyncTrustsOwnBootstrap checks that a tablet does not take the group it just
// bootstrapped for a foreign one before the shard record lists its incarnation.
func TestGroupReplicationSyncTrustsOwnBootstrap(t *testing.T) {
	enableGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	status, err := tm.StartGroupReplication(ctx, true)
	require.NoError(t, err)
	require.NotEqual(t, "1780000001", policy.GroupIncarnation(status.ViewId))

	newGroupReplicationSync(tm).reconcile(ctx)
	assert.True(t, mysql.IsGroupMemberActive(tmStatus(t, fmd)), "the tablet must not leave the group it bootstrapped")
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, stops)
}

// TestGroupReplicationSyncStopsServingWithoutVoterMajority checks the graceful-leave shrink of the
// Group Replication failover audit: once the other voters left the group (an expulsion, a clean
// shutdown, unreachable_majority_timeout), MySQL's view of one still has quorum and commits, but a
// transaction would then exist on a single voter. The PRIMARY tablet stops serving, keeps its
// type so that vtgate buffers, leaves MySQL alone, and serves again once a majority of the
// voters is back in the view.
func TestGroupReplicationSyncStopsServingWithoutVoterMajority(t *testing.T) {
	enableGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	fmd.SetSuperReadOnlyError = errors.New("MySQL must be left alone")
	s := newGroupReplicationSync(tm)

	withVoters := func(uids ...int) *replicationdatapb.GroupReplicationStatus {
		members := []*replicationdatapb.GroupReplicationMember{groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)}
		for _, uid := range uids {
			members = append(members, groupMember(testServerUUID(uid), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary))
		}
		return withViewID(groupStatus(testServerUUID(1), members...), "1780000001:20")
	}

	fmd.SetGroupReplicationStatus(withVoters())
	s.reconcile(ctx)
	assert.False(t, qsc.IsServing(), "a primary without the majority of its voters must not serve")
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type, "the tablet keeps its type so that vtgate buffers")
	start, stop, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, stop)

	// Something else made the query service serve again: the loop enforces its decision.
	require.NoError(t, qsc.SetServingType(topodatapb.TabletType_PRIMARY, time.Now(), true, ""))
	s.reconcile(ctx)
	assert.False(t, qsc.IsServing())

	fmd.SetGroupReplicationStatus(withVoters(3))
	s.reconcile(ctx)
	assert.True(t, qsc.IsServing(), "the primary serves again once the majority is back")
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	start2, stop2, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, start, start2)
	assert.Equal(t, stop, stop2)
}

// TestGroupReplicationSyncServesDuringMigration checks that the voter majority does not apply
// while the keyspace policy is not a group replication policy: during MigrateReplicationMode the
// primary bootstraps a group of one and keeps serving with semi-sync while the voters join.
func TestGroupReplicationSyncServesDuringMigration(t *testing.T) {
	enableGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	addPeerTablets(t, ts, 2, 3)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	tm.tmc = newGRPeersTMC()
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))

	newGroupReplicationSync(tm).reconcile(ctx)
	assert.True(t, qsc.IsServing())
}

// tmStatus returns the group replication status of the fake daemon.
func tmStatus(t *testing.T, fmd *mysqlctl.FakeMysqlDaemon) *replicationdatapb.GroupReplicationStatus {
	t.Helper()
	status, err := fmd.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	return status
}
