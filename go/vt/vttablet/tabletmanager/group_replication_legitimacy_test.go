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
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// grPeersTMC answers FullStatus for the other tablets of the shard.
type grPeersTMC struct {
	tmclient.TabletManagerClient

	mu       sync.Mutex
	statuses map[string]*replicationdatapb.FullStatus
	calls    int
	// frozen tablets do not answer until the caller gives up, like a SIGSTOPped vttablet.
	frozen map[string]bool
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
	frozen := c.frozen[topoproto.TabletAliasString(tablet.Alias)]
	c.mu.Unlock()
	if frozen {
		<-ctx.Done()
		return nil, ctx.Err()
	}
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
// MySQL leave the group instead.
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
}

// TestGroupReplicationSyncRejoinsAfterLeavingForeignGroup checks that a tablet whose MySQL left a
// group of another incarnation rejoins the shard's legitimate group on its own, as soon as another
// tablet reports it active. In the S7d chaos runs, such tablets suspended their rejoins until
// VTOrc's GroupMemberNotOnline, which only ran once the group had a primary tablet again: the group
// could not get one while these voters were missing from its majority, and writes stopped for up
// to 15s longer.
func TestGroupReplicationSyncRejoinsAfterLeavingForeignGroup(t *testing.T) {
	enableGroupReplication(t)
	ctx := t.Context()
	tm, fmd, peers, _ := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "17907858161940982:1"))
	s := newGroupReplicationSync(tm)

	s.reconcile(ctx)
	require.Equal(t, mysql.GroupMemberStateOffline, tmStatus(t, fmd).MemberState, "MySQL must leave the foreign group")
	startsBefore, _, _ := fmd.GroupReplicationCalls()

	// No other tablet is an active member of the shard's group yet: the tablet does not join.
	s.reconcile(ctx)
	starts, _, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, startsBefore, starts)

	// The shard's group was bootstrapped again on a peer: the tablet joins it without being told.
	legitimate := activeGroupPeersIn("1780000001", 2)
	peers.set(2, legitimate.statuses["cell1-0000000002"])
	s.nextRejoin = time.Time{}
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	s.reconcile(ctx)
	starts, _, _ = fmd.GroupReplicationCalls()
	assert.Equal(t, startsBefore+1, starts, "the tablet must rejoin the shard's group")
	assert.False(t, fmd.GroupReplicationBootstrapped)
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

// TestGroupReplicationSyncPromotesWithoutWaitingForFailedVoter checks that the promotion of the
// group's new primary does not wait for the failed primary's tablet, which cannot tell its
// server_uuid, once the voters that answer make a majority (S2: the old primary is frozen).
func TestGroupReplicationSyncPromotesWithoutWaitingForFailedVoter(t *testing.T) {
	enableGroupReplication(t)
	oldPeerTimeout := groupReplicationPeerTimeout
	groupReplicationPeerTimeout = 10 * time.Second
	t.Cleanup(func() { groupReplicationPeerTimeout = oldPeerTimeout })
	ctx := t.Context()
	tm, fmd, peers, _ := newLegitimacyTestTM(t)
	peers.mu.Lock()
	peers.frozen = map[string]bool{"cell1-0000000002": true}
	peers.mu.Unlock()
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:5"))

	start := time.Now()
	newGroupReplicationSync(tm).reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.Less(t, time.Since(start), 5*time.Second, "the promotion must not wait for the frozen voter")
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

// activeGroupPeers returns a client through which the tablets cell1-<uid> report that their MySQL
// is an ONLINE member of the group of ks/0, with quorum, in a view of the given incarnation.
func activeGroupPeersIn(incarnation string, uids ...uint32) *grPeersTMC {
	peers := newGRPeersTMC()
	for _, uid := range uids {
		uuid := testServerUUID(int(uid))
		gs := groupStatus(uuid, groupMember(uuid, mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary))
		if incarnation != "" {
			gs.ViewId = incarnation + ":3"
		}
		peers.set(uid, &replicationdatapb.FullStatus{ServerUuid: uuid, GroupReplicationStatus: gs})
	}
	return peers
}

// activeGroupPeers is activeGroupPeersIn without a view id.
func activeGroupPeers(uids ...uint32) *grPeersTMC {
	return activeGroupPeersIn("", uids...)
}

// TestGroupReplicationSyncRejoinsOnlyAnActiveGroup reproduces the bootstrap starvation of the Group
// Replication failover audit (S7d, G11): when no member of the group is active, a START
// GROUP_REPLICATION cannot join anything and blocks until MySQL's join timeout, during which
// VTOrc's bootstrap on the same member fails. The sync loop only starts a join while another
// tablet reports an active member of the shard's legitimate group with quorum.
func TestGroupReplicationSyncRejoinsOnlyAnActiveGroup(t *testing.T) {
	enableGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	setGroupReplicationIncarnation(t, ts, "1780000001")
	addPeerTablets(t, ts, 2, 3)
	peers := newGRPeersTMC()
	// Both peers are reachable, but their MySQL is not in any group.
	for _, uid := range []uint32{2, 3} {
		peers.set(uid, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(int(uid)), GroupReplicationStatus: groupStatus(testServerUUID(int(uid)))})
	}
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, peers, nil)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start, "the tablet must not start a join at startup while no group is active")
	s := newGroupReplicationSync(tm)

	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Zero(t, start, "the sync loop must not start a join while no group is active")

	// A peer is ONLINE, but in a group of another incarnation.
	foreign := activeGroupPeersIn("1799999999", 2)
	peers.set(2, foreign.statuses["cell1-0000000002"])
	s.nextRejoin = time.Time{}
	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Zero(t, start, "the sync loop must not join a group of another incarnation")

	// The shard's group is active on a peer.
	legitimate := activeGroupPeersIn("1780000001", 3)
	peers.set(3, legitimate.statuses["cell1-0000000003"])
	s.nextRejoin = time.Time{}
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Equal(t, 1, start)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	// The joining member contacts the active member of the shard's group first.
	assert.Equal(t, []string{"mysql3:33063", "mysql2:33062"}, fmd.GroupReplicationConfig.Seeds)
}

// TestStartGroupReplicationJoinStopsOngoingStart checks that a join on a member on which an
// earlier START GROUP_REPLICATION is still in progress stops it and starts again. MySQL keeps
// running a START whose client gave up, refuses any change meanwhile (errno 3724), and such a
// START has been seen to end in a group of its own (S7d after the legitimacy fixes).
func TestStartGroupReplicationJoinStopsOngoingStart(t *testing.T) {
	enableGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.ConfigureGroupReplicationErrors = []error{
		sqlerror.NewSQLError(mysqlErrGroupReplicationCommandOngoing, sqlerror.SSUnknownSQLState, "This option cannot be set while START or STOP GROUP_REPLICATION is ongoing."),
	}
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	status, err := tm.StartGroupReplication(t.Context(), false)
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 1, stops, "the ongoing START must be stopped first")
}

func TestPreferSeeds(t *testing.T) {
	seeds := []string{"a:1", "b:1", "c:1"}
	assert.Equal(t, []string{"c:1", "a:1", "b:1"}, preferSeeds(seeds, []string{"c:1"}))
	assert.Equal(t, []string{"b:1", "c:1", "a:1"}, preferSeeds(seeds, []string{"c:1", "b:1", "x:1"}))
	assert.Equal(t, seeds, preferSeeds(seeds, nil))
}

// TestStartGroupReplicationBootstrapStopsOngoingStart checks that a bootstrap succeeds on a member
// on which a START GROUP_REPLICATION is still in progress, for example one whose RPC timed out:
// MySQL refuses to change the configuration with errno 3724 until that START ends, which can take
// minutes when no group exists. The tablet stops it and bootstraps.
func TestStartGroupReplicationBootstrapStopsOngoingStart(t *testing.T) {
	enableGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	setGroupReplicationVoters(t, ts, 1)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.ConfigureGroupReplicationErrors = []error{
		sqlerror.NewSQLError(mysqlErrGroupReplicationCommandOngoing, sqlerror.SSUnknownSQLState, "This option cannot be set while START or STOP GROUP_REPLICATION is ongoing."),
	}
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	_, stopsBefore, _ := fmd.GroupReplicationCalls()

	status, err := tm.StartGroupReplication(t.Context(), true)
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, fmd.GroupReplicationBootstrapped)
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, stopsBefore+1, stops, "the ongoing START must be stopped first")
	assert.False(t, tm.groupReplicationRejoinSuspended.Load(), "the rejoin loop is only suspended during the bootstrap")
}

// TestStartGroupReplicationBootstrapStopsJoinWithoutGroup checks that a bootstrap stops a member
// that is RECOVERING without a group, a join that found no member, instead of refusing it.
func TestStartGroupReplicationBootstrapStopsJoinWithoutGroup(t *testing.T) {
	enableGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	setGroupReplicationVoters(t, ts, 1)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	recovering := groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateRecovering, ""))
	fmd.SetGroupReplicationStatus(recovering)
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	status, err := tm.StartGroupReplication(t.Context(), true)
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 1, stops)

	// A member that is RECOVERING in a group with an ONLINE member is not stopped.
	tm2, fmd2 := newGroupReplicationTestTM(t, ts, 2, nil)
	fmd2.SetGroupReplicationStatus(groupStatus(testServerUUID(2),
		groupMember(testServerUUID(2), mysql.GroupMemberStateRecovering, ""),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	_, err = tm2.StartGroupReplication(t.Context(), true)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	_, stops, _ = fmd2.GroupReplicationCalls()
	assert.Zero(t, stops)
}

// TestGroupReplicationSyncDemotesStalePrimary reproduces NEW-6 of the Group Replication failover
// audit: the old primary's mysqld restarted, its member is OFFLINE, and its tablet stayed PRIMARY,
// so vtgate kept routing to it (S7/S7b) and the tablet's own rejoin skipped it. Under a group
// replication policy, a PRIMARY tablet that is a listed voter and whose MySQL is not in any group
// is demoted to REPLICA, without touching MySQL. During a migration (a policy that does not use
// Group Replication, or no voter listed), the primary serves outside of a group and stays PRIMARY.
func TestGroupReplicationSyncDemotesStalePrimary(t *testing.T) {
	tests := []struct {
		name        string
		durability  string
		voters      []uint32
		pluginOff   bool
		wantDemoted bool
	}{
		{name: "voter restarted", durability: policy.DurabilityGroupReplication, voters: []uint32{1, 2, 3}, wantDemoted: true},
		{name: "voter restarted without the plugin", durability: policy.DurabilityGroupReplication, voters: []uint32{1, 2, 3}, pluginOff: true, wantDemoted: true},
		{name: "migration: policy is not group replication", durability: policy.DurabilitySemiSync, voters: []uint32{1, 2, 3}},
		{name: "migration: no voter listed", durability: policy.DurabilityGroupReplication},
		{name: "not a voter", durability: policy.DurabilityGroupReplication, voters: []uint32{2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enableGroupReplication(t)
			ts := newGroupReplicationTopo(t, tt.durability)
			setGroupReplicationVoters(t, ts, tt.voters...)
			addPeerTablets(t, ts, 2, 3)
			tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, newGRPeersTMC(), nil)
			setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
			fmd.SetSuperReadOnlyError = errors.New("MySQL must be left alone")
			status := groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, ""))
			status.PluginActive = !tt.pluginOff
			fmd.SetGroupReplicationStatus(status)

			newGroupReplicationSync(tm).reconcile(t.Context())
			if tt.wantDemoted {
				assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
				ti, err := ts.GetTablet(t.Context(), tm.tabletAlias)
				require.NoError(t, err)
				assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
			} else {
				assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
			}
			start, stop, _ := fmd.GroupReplicationCalls()
			assert.Zero(t, stop)
			assert.Zero(t, start, "no other member is active, so the demoted tablet does not join yet")
		})
	}
}

// tmStatus returns the group replication status of the fake daemon.
func tmStatus(t *testing.T, fmd *mysqlctl.FakeMysqlDaemon) *replicationdatapb.GroupReplicationStatus {
	t.Helper()
	status, err := fmd.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	return status
}
