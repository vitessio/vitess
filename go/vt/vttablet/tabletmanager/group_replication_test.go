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
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// resetDefaultChannel is the statement a tablet runs once its MySQL is an active member.
var resetDefaultChannel = mysql.ResetDefaultReplicationChannelCommand()

// withGroupReplication sets the group replication flags for the duration of the test. The
// sync loop and fence check intervals are long enough for them never to run on their own: tests
// call reconcile and checkFence.
func withGroupReplication(t *testing.T) {
	t.Helper()
	oldEnabled, oldInterval, oldPoll, oldFence := enableGroupReplication, groupReplicationSyncInterval, groupReplicationPollInterval, groupReplicationFenceCheckInterval
	t.Cleanup(func() {
		enableGroupReplication, groupReplicationSyncInterval, groupReplicationPollInterval, groupReplicationFenceCheckInterval = oldEnabled, oldInterval, oldPoll, oldFence
	})
	enableGroupReplication = true
	groupReplicationSyncInterval = time.Hour
	groupReplicationPollInterval = 5 * time.Millisecond
	groupReplicationFenceCheckInterval = time.Hour
}

func testServerUUID(uid int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", uid)
}

// newGroupReplicationTopo returns a topo server with keyspace ks, whose durability policy is
// the given one.
func newGroupReplicationTopo(t *testing.T, durability string) *topo.Server {
	t.Helper()
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: durability}))
	return ts
}

// newGroupReplicationTestTM starts a tablet manager for tablet ks/0 cell1-<uid>, backed by a
// FakeMysqlDaemon that configure can prepare before the tablet starts.
func newGroupReplicationTestTM(t *testing.T, ts *topo.Server, uid int, configure func(fmd *mysqlctl.FakeMysqlDaemon)) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
	t.Helper()
	return newGroupReplicationTestTMWithPeers(t, ts, uid, nil, configure)
}

// newGroupReplicationTestTMWithPeers is newGroupReplicationTestTM with a client through which the
// tablet reaches the other tablets of its shard from the start.
func newGroupReplicationTestTMWithPeers(t *testing.T, ts *topo.Server, uid int, peers tmclient.TabletManagerClient, configure func(fmd *mysqlctl.FakeMysqlDaemon)) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
	t.Helper()
	fmd := newTestMysqlDaemon(t, 3306)
	fmd.ServerUUID = testServerUUID(uid)
	if configure != nil {
		configure(fmd)
	}
	tm := &TabletManager{
		BatchCtx:            t.Context(),
		TopoServer:          ts,
		MysqlDaemon:         fmd,
		DBConfigs:           &dbconfigs.DBConfigs{},
		SemiSyncMonitor:     semisyncmonitor.CreateTestSemiSyncMonitor(fmd.DB(), exporter),
		QueryServiceControl: tabletservermock.NewController(),
		tmc:                 peers,
	}
	require.NoError(t, tm.Start(newTestTablet(t, uid, "ks", "0", nil), nil))
	t.Cleanup(tm.Stop)
	return tm, fmd
}

func groupMember(uuid, state, role string) *replicationdatapb.GroupReplicationMember {
	return &replicationdatapb.GroupReplicationMember{MemberUuid: uuid, State: state, Role: role}
}

// groupStatus returns the status of the member uuid of the group of ks/0, whose view is members.
func groupStatus(uuid string, members ...*replicationdatapb.GroupReplicationMember) *replicationdatapb.GroupReplicationStatus {
	status := &replicationdatapb.GroupReplicationStatus{
		PluginActive:      true,
		GroupName:         policy.GroupName("ks", "0"),
		SinglePrimaryMode: true,
		MemberState:       mysql.GroupMemberStateOffline,
		Members:           members,
	}
	reachable := 0
	for _, m := range members {
		if m.MemberUuid == uuid {
			status.MemberState = m.State
			if m.State == mysql.GroupMemberStateOnline {
				status.MemberRole = m.Role
			}
		}
		if m.State == mysql.GroupMemberStateOnline && m.Role == mysql.GroupMemberRolePrimary {
			status.PrimaryUuid = m.MemberUuid
		}
		if m.State == mysql.GroupMemberStateOnline || m.State == mysql.GroupMemberStateRecovering {
			reachable++
		}
	}
	status.HasQuorum = mysql.IsGroupMemberActive(status) && reachable > len(members)/2
	return status
}

// setGroupReplicationVoters records the tablets cell1-<uid> as the voters of the group of ks/0
// in the shard record, creating the shard if needed.
func setGroupReplicationVoters(t *testing.T, ts *topo.Server, uids ...uint32) {
	t.Helper()
	ctx := t.Context()
	_, err := ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationVoters = nil
		for _, uid := range uids {
			si.GroupReplicationVoters = append(si.GroupReplicationVoters, &topodatapb.TabletAlias{Cell: "cell1", Uid: uid})
		}
		return nil
	})
	require.NoError(t, err)
}

// setTabletType changes the type of the tablet without going through the group replication
// checks, the way the tablet was before its MySQL joined a group.
func setTabletType(t *testing.T, tm *TabletManager, tabletType topodatapb.TabletType) {
	t.Helper()
	require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), tabletType, DBActionNone))
}

func requireCode(t *testing.T, err error, code vtrpcpb.Code) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, code, vterrors.Code(err), "unexpected error: %v", err)
}

func TestBuildTabletFromInputPublishesGroupReplicationPort(t *testing.T) {
	oldHostname, oldKeyspace, oldShard, oldType := tabletHostname, initKeyspace, initShard, initTabletType
	t.Cleanup(func() {
		tabletHostname, initKeyspace, initShard, initTabletType = oldHostname, oldKeyspace, oldShard, oldType
	})
	tabletHostname = "host1"
	initKeyspace = "ks"
	initShard = "0"
	initTabletType = "replica"
	alias := &topodatapb.TabletAlias{Cell: "cell1", Uid: 1}

	// Members reach each other through the MySQL port of their tablet records: the tablet does not
	// publish a port of its own for group replication.
	withGroupReplication(t)
	tablet, err := BuildTabletFromInput(alias, 1, 2, nil, collations.MySQL8())
	require.NoError(t, err)
	assert.Equal(t, map[string]int32{"vt": 1, "grpc": 2}, tablet.PortMap)
}

func TestValidateGroupReplicationFlags(t *testing.T) {
	withGroupReplication(t)
	require.NoError(t, validateFlags())

	groupReplicationSyncInterval = 0
	require.ErrorContains(t, validateFlags(), "--group-replication-sync-interval must be positive")

	// The interval does not matter while group replication is disabled.
	enableGroupReplication = false
	require.NoError(t, validateFlags())
}

func TestGroupReplicationConfig(t *testing.T) {
	withGroupReplication(t)
	oldTries := groupReplicationAutorejoinTries
	t.Cleanup(func() {
		groupReplicationAutorejoinTries = oldTries
	})
	groupReplicationAutorejoinTries = 3

	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	peers := []*topodatapb.Tablet{{
		// Other tablets reach MySQL at its mysql hostname and port.
		Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA,
		Hostname: "tablet2", MysqlHostname: "mysql2", MysqlPort: 3312,
	}, {
		Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 3}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA,
		Hostname: "tablet3", MysqlPort: 3313,
	}, {
		// A tablet whose MySQL port is not known yet is not a seed.
		Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 4}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_RDONLY,
		Hostname: "tablet4", MysqlHostname: "mysql4",
	}}
	for _, peer := range peers {
		require.NoError(t, ts.CreateTablet(ctx, peer))
	}
	tm, _ := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		// The tablet joins its group at startup; let it fail so that the test starts clean.
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})

	durability, err := tm.shardDurability(ctx)
	require.NoError(t, err)
	cfg, err := tm.groupReplicationConfig(ctx, durability)
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupReplicationConfig{
		GroupName:       policy.GroupName("ks", "0"),
		LocalAddress:    "localhost:3306",
		Seeds:           []string{"mysql2:3312", "tablet3:3313"},
		MemberWeight:    50,
		Consistency:     "BEFORE_ON_PRIMARY_FAILOVER",
		ExitStateAction: "READ_ONLY",
		AutorejoinTries: 3,
	}, cfg)
}

// TestGroupReplicationDisablesAutorejoinByDefault checks that the tablet turns MySQL's own
// auto-rejoin off unless told otherwise: an auto-rejoin attempt does not check whether the shard's
// group is active, blocks the member for about a minute (S7d in
// doc/failover-audit/GroupReplication.md) and can end in a group of its own. The tablet rejoins
// expelled members itself.
func TestGroupReplicationDisablesAutorejoinByDefault(t *testing.T) {
	fs := pflag.NewFlagSet("vttablet", pflag.ContinueOnError)
	registerGroupReplicationFlags(fs)
	flag := fs.Lookup("group-replication-autorejoin-tries")
	require.NotNil(t, flag)
	assert.Equal(t, "0", flag.DefValue)
	assert.Contains(t, mysql.ConfigureGroupReplicationCommands(mysql.GroupReplicationConfig{AutorejoinTries: groupReplicationAutorejoinTries}),
		"SET GLOBAL group_replication_autorejoin_tries = 0")
}

func TestGroupReplicationRPCsRequireFlag(t *testing.T) {
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, _ := newGroupReplicationTestTM(t, ts, 1, nil)

	_, err := tm.StartGroupReplication(t.Context(), startRequest(false))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	_, err = tm.StopGroupReplication(t.Context())
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
}

// TestStartGroupReplicationJoinsAsSecondary checks that a replica stops its asynchronous
// replication before it joins, clears the default channel afterwards and disables semi-sync.
func TestStartGroupReplicationJoinsAsSecondary(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.Replicating = true
	fmd.SemiSyncReplicaEnabled = true
	fmd.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", resetDefaultChannel}

	status, err := tm.StartGroupReplication(t.Context(), startRequest(false))
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
	assert.Equal(t, mysql.GroupMemberRoleSecondary, status.MemberRole)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	assert.Equal(t, policy.GroupName("ks", "0"), fmd.GroupReplicationConfig.GroupName)
	assert.False(t, fmd.Replicating, "asynchronous replication must be stopped")
	assert.False(t, fmd.SemiSyncReplicaEnabled, "a secondary must not acknowledge semi-sync transactions")
	require.NoError(t, fmd.CheckSuperQueryList())
}

// TestStartGroupReplicationBootstrapOnPrimary checks that a primary that bootstraps a group
// stays writable and keeps semi-sync, which its asynchronous replicas still acknowledge.
func TestStartGroupReplicationBootstrapOnPrimary(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SemiSyncPrimaryEnabled = true
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	status, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, fmd.GroupReplicationBootstrapped)
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
	assert.True(t, fmd.SemiSyncPrimaryEnabled, "bootstrapping must keep primary semi-sync")
	require.NoError(t, fmd.CheckSuperQueryList())
	// The primary serves while it bootstraps the group: redoing prepared transactions would
	// restart the transaction engine and fail in-flight queries.
	assert.False(t, tm.QueryServiceControl.(*tabletservermock.Controller).MethodCalled["RedoPreparedTransactions"])

	// Bootstrapping an active member would create a second group.
	_, err = tm.StartGroupReplication(t.Context(), startRequest(true))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 1, start)
}

// TestStartGroupReplicationOnActiveMember checks that the RPC can be retried on a member that
// already joined: it does not start group replication again, and it disables primary semi-sync
// once the group supersedes it.
func TestStartGroupReplicationOnActiveMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	fmd.SemiSyncPrimaryEnabled = true
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	status, err := tm.StartGroupReplication(t.Context(), startRequest(false))
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start)
	assert.False(t, fmd.SemiSyncPrimaryEnabled, "a group with two ONLINE members supersedes semi-sync")
	require.NoError(t, fmd.CheckSuperQueryList())
}

func TestStartGroupReplicationRefusesOtherGroup(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	status := groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary))
	status.GroupName = policy.GroupName("ks", "-80")
	fmd.SetGroupReplicationStatus(status)

	_, err := tm.StartGroupReplication(t.Context(), startRequest(false))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
}

// TestStartGroupReplicationRestartsReplicationAfterFailedJoin checks that a replica that cannot
// join keeps replicating asynchronously, so that it keeps acknowledging semi-sync transactions.
func TestStartGroupReplicationRestartsReplicationAfterFailedJoin(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.Replicating = true
	fmd.StartGroupReplicationError = errors.New("no seed reachable")
	fmd.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	_, err := tm.StartGroupReplication(t.Context(), startRequest(false))
	require.ErrorContains(t, err, "no seed reachable")
	assert.True(t, fmd.Replicating)
	require.NoError(t, fmd.CheckSuperQueryList())
}

func TestStartGroupReplicationWaitsForOnline(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	recovering := groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateRecovering, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary))
	fmd.SetGroupReplicationStatus(recovering)
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel, resetDefaultChannel}

	// A member that stays RECOVERING times out.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := tm.StartGroupReplication(ctx, startRequest(false))
	requireCode(t, err, vtrpcpb.Code_DEADLINE_EXCEEDED)

	// An ONLINE member succeeds.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	status, err := tm.StartGroupReplication(t.Context(), startRequest(false))
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
}

// TestStopGroupReplicationRestoresReadWriteOnPrimary checks that a primary that leaves its
// group, the last step of a migration back to asynchronous replication, serves writes again.
func TestStopGroupReplicationRestoresReadWriteOnPrimary(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	_, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	for len(qsc.StateChanges) > 0 {
		<-qsc.StateChanges
	}

	status, err := tm.StopGroupReplication(t.Context())
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOffline, status.MemberState)
	// MySQL rejects commits while the primary leaves its group, so the tablet stops serving
	// first, which makes vtgate buffer writes, and serves again once MySQL is writable.
	require.Len(t, qsc.StateChanges, 2)
	assert.Equal(t, &tabletservermock.StateChange{Serving: false, TabletType: topodatapb.TabletType_PRIMARY}, <-qsc.StateChanges)
	assert.Equal(t, &tabletservermock.StateChange{Serving: true, TabletType: topodatapb.TabletType_PRIMARY}, <-qsc.StateChanges)
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
	assert.True(t, fmd.SemiSyncPrimaryEnabled, "the primary applies the semi-sync setting of the policy")
	assert.True(t, tm.groupReplicationRejoinSuspended.Load())
}

func TestStopGroupReplicationKeepsSecondaryReadOnly(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	// Even a PRIMARY tablet stays read-only if its MySQL was not the group primary.
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true

	_, err := tm.StopGroupReplication(t.Context())
	require.NoError(t, err)
	assert.True(t, fmd.SuperReadOnly.Load())
	assert.True(t, fmd.ReadOnly)
}

// TestGroupSecondaryIsNeverMadeWritable checks that no RPC clears super_read_only on an active
// member that is not the group primary: such a member would accept writes and replicate them to
// the whole group.
func TestGroupSecondaryIsNeverMadeWritable(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	secondary := groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary))
	fmd.SetGroupReplicationStatus(secondary)
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	ctx := t.Context()

	requireCode(t, tm.SetReadOnly(ctx, false), vtrpcpb.Code_FAILED_PRECONDITION)
	requireCode(t, tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false), vtrpcpb.Code_FAILED_PRECONDITION)
	_, err := tm.InitPrimary(ctx, false)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	requireCode(t, tm.UndoDemotePrimary(ctx, false), vtrpcpb.Code_FAILED_PRECONDITION)
	requireCode(t, tm.redoPreparedTransactionsAndSetReadWrite(ctx), vtrpcpb.Code_FAILED_PRECONDITION)

	assert.True(t, fmd.SuperReadOnly.Load())
	assert.True(t, fmd.ReadOnly)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	ti, err := ts.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)

	// A member in a partition without quorum cannot be made writable either.
	partitioned := groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateUnreachable, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateUnreachable, mysql.GroupMemberRoleSecondary))
	fmd.SetGroupReplicationStatus(partitioned)
	requireCode(t, tm.SetReadOnly(ctx, false), vtrpcpb.Code_FAILED_PRECONDITION)

	// The group primary can be made writable.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	require.NoError(t, tm.SetReadOnly(ctx, false))
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
}

// TestPromoteReplicaSwitchesGroupPrimary checks that PromoteReplica on a member asks the group
// to switch primaries, instead of resetting its replication.
func TestPromoteReplicaSwitchesGroupPrimary(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	// Promote resets the replication of an asynchronous replica. It must not run on a member.
	fmd.PromoteError = errors.New("RESET REPLICA ALL must not run on a group member")
	pos, err := replication.ParsePosition(gtidFlavor, gtidPosition)
	require.NoError(t, err)
	fmd.SetPrimaryPositionLocked(pos)

	gotPos, err := tm.PromoteReplica(t.Context(), true)
	require.NoError(t, err)
	assert.Equal(t, replication.EncodePosition(pos), gotPos)
	_, _, setPrimary := fmd.GroupReplicationCalls()
	assert.Equal(t, 1, setPrimary)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
	// The group of two ONLINE members supersedes the semi-sync that the policy asks for.
	assert.False(t, fmd.SemiSyncPrimaryEnabled)

	// Promoting the group primary again only changes the tablet type: MySQL fails to switch to
	// the member that already is the primary.
	_, err = tm.PromoteReplica(t.Context(), true)
	require.NoError(t, err)
	_, _, setPrimary = fmd.GroupReplicationCalls()
	assert.Equal(t, 1, setPrimary)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
}

func TestPromoteReplicaRefusesRecoveringMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateRecovering, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))

	_, err := tm.PromoteReplica(t.Context(), false)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

// TestDemotePrimarySkipsSemiSyncOnGroupMember checks that DemotePrimary makes a group primary
// read-only without touching semi-sync.
func TestDemotePrimarySkipsSemiSyncOnGroupMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	fmd.SemiSyncPrimaryEnabled = true

	_, err := tm.DemotePrimary(t.Context(), false)
	require.NoError(t, err)
	assert.True(t, fmd.SuperReadOnly.Load())
	assert.True(t, fmd.SemiSyncPrimaryEnabled)
	assert.False(t, fmd.SemiSyncReplicaEnabled)
}

// TestSetReplicationSourceOnGroupMember checks that a member keeps replicating through its
// group: it does not configure the default channel, and still waits for the position.
func TestSetReplicationSourceOnGroupMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	fmd.SemiSyncReplicaEnabled = true
	fmd.SetReplicationSourceFunc = func(context.Context, string, int32, float64, bool, bool) error {
		return errors.New("the default channel must not be configured on a group member")
	}
	pos, err := replication.ParsePosition(gtidFlavor, gtidPosition)
	require.NoError(t, err)
	fmd.WaitPrimaryPositions = []replication.Position{pos}
	parent := &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}

	err = tm.SetReplicationSource(t.Context(), parent, 0, replication.EncodePosition(pos), false, true, 0)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	assert.False(t, fmd.SemiSyncReplicaEnabled)

	// The position wait still applies.
	otherPos, err := replication.ParsePosition(gtidFlavor, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-9")
	require.NoError(t, err)
	err = tm.SetReplicationSource(t.Context(), parent, 0, replication.EncodePosition(otherPos), false, true, 0)
	require.ErrorContains(t, err, "wrong input for WaitSourcePos")
}

// TestSetReplicationSourceRefusedOnGroupPrimary checks that a stale SetReplicationSource, for
// example from a VTOrc acting on an old view, does not demote the tablet whose MySQL is the
// primary of a group with quorum.
func TestSetReplicationSourceRefusedOnGroupPrimary(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	parent := &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}

	err := tm.SetReplicationSource(t.Context(), parent, 0, "", false, false, 0)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
}

// TestSetReplicationSourceWaitsForGroupPrimarySwitch checks that a reparent's SetReplicationSource
// on the old primary succeeds although its member is still the group primary when the request
// arrives: PlannedReparentShard repoints the tablets while the primary-elect's PromoteReplica
// switches the group's primary, which the old primary's member applies a moment later.
func TestSetReplicationSourceWaitsForGroupPrimarySwitch(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	parent := &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}

	switched := make(chan struct{})
	go func() {
		defer close(switched)
		time.Sleep(50 * time.Millisecond)
		fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
			groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
			groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, tm.SetReplicationSource(ctx, parent, time.Now().UnixNano(), "", false, false, 0))
	<-switched
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

func TestStopAndStartReplicationOnGroupMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 1, 2)
	// The shard record lists the incarnation of the group the peer is active in.
	setGroupReplicationIncarnation(t, ts, "1780000001")
	addPeerTablets(t, ts, 2)
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, activeGroupPeersIn("1780000001", 2), func(fmd *mysqlctl.FakeMysqlDaemon) {
		// The tablet joins its group at startup.
		fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel, resetDefaultChannel}
	})
	status, err := fmd.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	require.True(t, mysql.IsGroupMemberActive(status))

	// StopReplication leaves the group, and keeps the sync loop from rejoining it.
	require.NoError(t, tm.StopReplication(t.Context()))
	status, err = fmd.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOffline, status.MemberState)
	assert.True(t, tm.groupReplicationRejoinSuspended.Load())
	assert.False(t, fmd.Replicating)

	// StartReplication joins the group again, instead of starting the default channel.
	require.NoError(t, tm.StartReplication(t.Context(), false))
	status, err = fmd.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupMemberActive(status))
	assert.False(t, tm.groupReplicationRejoinSuspended.Load())
	assert.False(t, fmd.Replicating)
	start, stop, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 2, start)
	assert.Equal(t, 1, stop)
	require.NoError(t, fmd.CheckSuperQueryList())
}

func TestResetReplicationParametersOnGroupMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	// RESET REPLICA ALL fails on a member (ERROR 3139): only the default channel is reset.
	fmd.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", resetDefaultChannel}

	require.NoError(t, tm.ResetReplicationParameters(t.Context()))
	require.NoError(t, fmd.CheckSuperQueryList())
}

// TestInitPrimaryBootstrapsGroup checks that InitPrimary in a keyspace that uses group
// replication bootstraps the shard's group on the new primary.
func TestInitPrimaryBootstrapsGroup(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		// No other member is reachable when the tablet starts.
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	fmd.StartGroupReplicationError = nil
	fmd.SuperReadOnly.Store(true)
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel, mysqlctl.GenerateInitialBinlogEntry()}

	_, err := tm.InitPrimary(t.Context(), false)
	require.NoError(t, err)
	assert.True(t, fmd.GroupReplicationBootstrapped)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
	require.NoError(t, fmd.CheckSuperQueryList())

	// A member of a group with other members is not bootstrapped again.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	_, err = tm.InitPrimary(t.Context(), false)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
}

func TestFixSemiSyncSupersededByGroup(t *testing.T) {
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	ctx := t.Context()

	// A group of one does not make transactions durable anywhere else.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	require.NoError(t, tm.fixSemiSync(ctx, topodatapb.TabletType_PRIMARY, SemiSyncActionSet))
	assert.True(t, fmd.SemiSyncPrimaryEnabled)
	assert.True(t, fmd.SemiSyncReplicaEnabled)

	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	require.NoError(t, tm.fixSemiSync(ctx, topodatapb.TabletType_PRIMARY, SemiSyncActionSet))
	assert.False(t, fmd.SemiSyncPrimaryEnabled)
	assert.False(t, fmd.SemiSyncReplicaEnabled)
}

// TestStartJoinsGroup checks that a tablet that the shard record lists as a voter joins its
// group when it starts, and never bootstraps one.
func TestStartJoinsGroup(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	// The shard has a primary, which an asynchronous replica would replicate from.
	primary := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY,
		Hostname: "tablet2", MysqlHostname: "mysql2", MysqlPort: 3306,
	}
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	require.NoError(t, ts.CreateTablet(ctx, primary))
	_, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primary.Alias
		return nil
	})
	require.NoError(t, err)
	setGroupReplicationVoters(t, ts, 1, 2)
	// The shard record lists the incarnation of the group the peer is active in.
	setGroupReplicationIncarnation(t, ts, "1780000001")

	_, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, activeGroupPeersIn("1780000001", 2), func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
		fmd.SetReplicationSourceFunc = func(context.Context, string, int32, float64, bool, bool) error {
			return errors.New("the default channel must not be configured on a group member")
		}
	})
	assert.Equal(t, []string{"mysql2:3306"}, fmd.GroupReplicationConfig.Seeds)
	status, err := fmd.GroupReplicationStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	require.NoError(t, fmd.CheckSuperQueryList())
}

func TestStartSucceedsWhenGroupJoinFails(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 1, 2)
	// The shard record lists the incarnation of the group the peer is active in.
	setGroupReplicationIncarnation(t, ts, "1780000001")
	addPeerTablets(t, ts, 2)
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, activeGroupPeersIn("1780000001", 2), func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 1, start)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

// setShardPrimary creates the PRIMARY tablet cell1-2 of ks/0 and records it as the primary of
// the shard. It makes the tablet manager reach other tablets through a fake client, which
// reports the primary at the position of the fake MySQL, so that the tablet can replicate from
// it without errant transactions.
func setShardPrimary(t *testing.T, ts *topo.Server, tm *TabletManager, fmd *mysqlctl.FakeMysqlDaemon) {
	t.Helper()
	ctx := t.Context()
	primary := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY,
		Hostname: "tablet2", MysqlHostname: "mysql2", MysqlPort: 3306,
	}
	require.NoError(t, ts.CreateTablet(ctx, primary))
	_, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primary.Alias
		return nil
	})
	require.NoError(t, err)
	tm.tmc = newFakeTMClient()
	pos, err := replication.ParsePosition(gtidFlavor, gtidPosition)
	require.NoError(t, err)
	fmd.SetPrimaryPositionLocked(pos)
}

// TestStartReplicatesAsynchronouslyWhenNotVoter checks that a REPLICA that the shard record does
// not list as a voter of its group does not join the group when it starts, although the
// durability policy uses group replication, and replicates asynchronously from the primary
// instead, like any replica.
func TestStartReplicatesAsynchronouslyWhenNotVoter(t *testing.T) {
	testCases := []struct {
		name   string
		voters []uint32
	}{{
		name:   "tablet is not a voter",
		voters: []uint32{2},
	}, {
		name:   "voters are not selected yet",
		voters: []uint32{},
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withGroupReplication(t)
			ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
			setGroupReplicationVoters(t, ts, tc.voters...)
			tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
			start, _, _ := fmd.GroupReplicationCalls()
			assert.Zero(t, start, "a tablet that is not a voter must not join the group")

			// Replication is initialized again once the shard has a primary, for example at the
			// end of a restore.
			setShardPrimary(t, ts, tm, fmd)
			fmd.SetReplicationSourceInputs = []string{"mysql2:3306"}
			fmd.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE SET SOURCE", "START REPLICA"}
			pos, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("%s/%s", gtidFlavor, gtidPosition), pos)
			assert.Equal(t, "mysql2", fmd.CurrentSourceHost)
			assert.EqualValues(t, 3306, fmd.CurrentSourcePort)
			require.NoError(t, fmd.CheckSuperQueryList())
			start, _, _ = fmd.GroupReplicationCalls()
			assert.Zero(t, start)
		})
	}
}

// TestStartLeavesActiveNonVoterAlone checks that a tablet whose MySQL is still an active member
// of its group, but that the shard record does not list as a voter, neither leaves the group nor
// configures asynchronous replication when it starts: removing a member is VTOrc's decision.
func TestStartLeavesActiveNonVoterAlone(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 2, 3)
	superQueries := 0
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
			groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
			groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
		fmd.ExecuteSuperQueryListCallback = func() { superQueries++ }
		fmd.SetReplicationSourceFunc = func(context.Context, string, int32, float64, bool, bool) error {
			return errors.New("the default channel must not be configured on a group member")
		}
	})
	assert.Zero(t, superQueries)

	setShardPrimary(t, ts, tm, fmd)
	pos, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.Empty(t, pos)

	assert.Zero(t, superQueries)
	start, stop, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start)
	assert.Zero(t, stop)
	status, err := fmd.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
}

// TestStartReplicationOnNonVoter checks that StartReplication and StopReplication keep their
// asynchronous meaning on a tablet that is neither an active member of its group nor a voter.
func TestStartReplicationOnNonVoter(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 2)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.ExpectedExecuteSuperQueryList = []string{"START REPLICA", "STOP REPLICA"}

	require.NoError(t, tm.StartReplication(t.Context(), false))
	require.NoError(t, tm.StopReplication(t.Context()))
	require.NoError(t, fmd.CheckSuperQueryList())
	start, stop, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start)
	assert.Zero(t, stop)
}

// TestGroupReplicationSyncRejoinsOnlyVoters checks that the sync loop makes MySQL rejoin its
// group only while the shard record lists the tablet as a voter, and that it never makes a
// member that is no longer a voter leave the group.
func TestGroupReplicationSyncRejoinsOnlyVoters(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 2)
	// The shard record lists the incarnation of the group the peer is active in.
	setGroupReplicationIncarnation(t, ts, "1780000001")
	addPeerTablets(t, ts, 2)
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, activeGroupPeersIn("1780000001", 2), nil)
	s := newGroupReplicationSync(tm)

	s.reconcile(ctx)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start, "a tablet that is not a voter must not join the group")

	// The loop caches the voters for a while.
	setGroupReplicationVoters(t, ts, 1, 2)
	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Zero(t, start)

	// Once the cached voters expired, the new voter joins.
	s.votersRead = time.Time{}
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Equal(t, 1, start)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	require.NoError(t, fmd.CheckSuperQueryList())

	// A member that is no longer a voter leaves the group (TestGroupReplicationSyncLeavesAsNonVoter),
	// when the group keeps a majority of its members without it: here it would not.
	setGroupReplicationVoters(t, ts, 2)
	s.votersRead = time.Time{}
	s.reconcile(ctx)
	_, stop, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, stop)
	status, err := fmd.GroupReplicationStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
}

// TestCheckPrimaryShipUnderGroupReplication checks that a restarted tablet does not trust a
// PRIMARY record in a keyspace that uses group replication: the group decides.
func TestCheckPrimaryShipUnderGroupReplication(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tablet := newTestTablet(t, 1, "ks", "0", nil)
	tablet.Type = topodatapb.TabletType_PRIMARY
	tablet.PrimaryTermStartTime = protoutil.TimeToProto(time.Now())
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	require.NoError(t, ts.CreateTablet(ctx, tablet))
	_, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		si.PrimaryTermStartTime = tablet.PrimaryTermStartTime
		return nil
	})
	require.NoError(t, err)

	tm, _ := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	ti, err := ts.GetTablet(ctx, tablet.Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
}

func TestEndPrimaryTermOnGroupMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	// The group already made MySQL read-only: the tablet must not demote it again.
	fmd.SetSuperReadOnlyError = errors.New("super_read_only must not be changed")

	require.NoError(t, tm.endPrimaryTerm(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}))
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

func TestGroupReplicationSyncPromotesGroupPrimary(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	// The group elected this member after its primary failed.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	s := newGroupReplicationSync(tm)

	// The loop does not wait for the action lock.
	require.NoError(t, tm.lock(ctx))
	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	tm.unlock()

	s.reconcile(ctx)
	tablet := tm.Tablet()
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tablet.Type)
	require.NotNil(t, tablet.PrimaryTermStartTime)
	ti, err := ts.GetTablet(ctx, tablet.Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.True(t, proto.Equal(tablet.PrimaryTermStartTime, ti.PrimaryTermStartTime))

	// The shard record follows the new primary term.
	assert.Eventually(t, func() bool {
		si, err := ts.GetShard(ctx, "ks", "0")
		return err == nil && si.PrimaryAlias != nil && si.PrimaryAlias.Uid == 1
	}, 30*time.Second, 10*time.Millisecond)
}

func TestGroupReplicationSyncDemotesFormerPrimary(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	s := newGroupReplicationSync(tm)
	fmd.SetSuperReadOnlyError = errors.New("super_read_only must not be changed")

	// A PRIMARY whose MySQL left its group on purpose, or never joined one, stays PRIMARY.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, "")))
	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)

	// The group elected another primary.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	ti, err := ts.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
}

func TestGroupReplicationSyncDemotesPrimaryWithoutQuorum(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateUnreachable, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateUnreachable, mysql.GroupMemberRoleSecondary)))

	newGroupReplicationSync(tm).reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

func TestGroupReplicationSyncRejoinsWithBackoff(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 1, 2)
	// The shard record lists the incarnation of the group the peer is active in.
	setGroupReplicationIncarnation(t, ts, "1780000001")
	addPeerTablets(t, ts, 2)
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, activeGroupPeersIn("1780000001", 2), func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	s := newGroupReplicationSync(tm)

	// A failed attempt is not retried before the backoff expires.
	s.reconcile(ctx)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 2, start, "the tablet tried to join at startup and in the loop")
	assert.Equal(t, groupReplicationSyncInterval, s.rejoinBackoff)
	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Equal(t, 2, start)

	// The backoff doubles, up to its cap.
	s.nextRejoin = time.Time{}
	s.reconcile(ctx)
	assert.Equal(t, groupReplicationMaxRejoinBackoff, s.rejoinBackoff)

	// A successful attempt resets the backoff.
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	s.nextRejoin = time.Time{}
	s.reconcile(ctx)
	status, err := fmd.GroupReplicationStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
	assert.Zero(t, s.rejoinBackoff)
	assert.False(t, fmd.GroupReplicationBootstrapped, "the loop never bootstraps a group")
	require.NoError(t, fmd.CheckSuperQueryList())
}

func TestGroupReplicationSyncDoesNotRejoin(t *testing.T) {
	testCases := []struct {
		name       string
		durability string
		tabletType topodatapb.TabletType
		voters     []uint32
		suspended  bool
	}{{
		name:       "policy does not use group replication",
		durability: policy.DurabilitySemiSync,
		tabletType: topodatapb.TabletType_REPLICA,
	}, {
		name:       "tablet is not a voter",
		durability: policy.DurabilityGroupReplicationCrossCell,
		tabletType: topodatapb.TabletType_REPLICA,
		voters:     []uint32{2},
	}, {
		name:       "voters are not selected yet",
		durability: policy.DurabilityGroupReplicationCrossCell,
		tabletType: topodatapb.TabletType_REPLICA,
		voters:     []uint32{},
	}, {
		name:       "tablet is being backed up",
		durability: policy.DurabilityGroupReplicationCrossCell,
		tabletType: topodatapb.TabletType_BACKUP,
	}, {
		name:       "tablet is drained",
		durability: policy.DurabilityGroupReplicationCrossCell,
		tabletType: topodatapb.TabletType_DRAINED,
	}, {
		name:       "primary serves on its own",
		durability: policy.DurabilityGroupReplicationCrossCell,
		tabletType: topodatapb.TabletType_PRIMARY,
	}, {
		name:       "group replication was stopped explicitly",
		durability: policy.DurabilityGroupReplicationCrossCell,
		tabletType: topodatapb.TabletType_REPLICA,
		suspended:  true,
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withGroupReplication(t)
			ts := newGroupReplicationTopo(t, tc.durability)
			voters := tc.voters
			if voters == nil {
				voters = []uint32{1}
			}
			setGroupReplicationVoters(t, ts, voters...)
			tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
				fmd.StartGroupReplicationError = errors.New("no seed reachable")
			})
			setTabletType(t, tm, tc.tabletType)
			tm.groupReplicationRejoinSuspended.Store(tc.suspended)
			startBefore, _, _ := fmd.GroupReplicationCalls()

			newGroupReplicationSync(tm).reconcile(t.Context())
			start, _, _ := fmd.GroupReplicationCalls()
			assert.Equal(t, startBefore, start)
		})
	}
}

func TestGroupReplicationSyncEnforcesSemiSync(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	s := newGroupReplicationSync(tm)

	// A group with two ONLINE members supersedes semi-sync.
	fmd.SemiSyncPrimaryEnabled = true
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	s.reconcile(ctx)
	assert.False(t, fmd.SemiSyncPrimaryEnabled)

	// Once the group has shrunk, semi-sync is not enabled while no replica acknowledges: it
	// would block every commit.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	s.reconcile(ctx)
	assert.False(t, fmd.SemiSyncPrimaryEnabled)

	// It is enabled as soon as a semi-sync replica is connected.
	fmd.GlobalStatusVars = map[string]string{"Rpl_semi_sync_source_clients": "1"}
	s.reconcile(ctx)
	assert.True(t, fmd.SemiSyncPrimaryEnabled)
}

// TestGroupReplicationSyncLoop checks that the loop runs in the background once the tablet
// started, and stops with the tablet.
func TestGroupReplicationSyncLoop(t *testing.T) {
	withGroupReplication(t)
	groupReplicationSyncInterval = 10 * time.Millisecond
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	tm, _ := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
		fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
			groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
			groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	})
	assert.Eventually(t, func() bool {
		return tm.Tablet().Type == topodatapb.TabletType_PRIMARY
	}, 30*time.Second, 10*time.Millisecond)

	tm.stopGroupReplicationSync()
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	assert.Nil(t, tm._groupReplicationSyncCancel)
}

// TestFullStatusReportsGroupReplicationEnabled checks that FullStatus tells whether the tablet runs
// with --enable-group-replication: the migration to Group Replication refuses voters without it.
func TestFullStatusReportsGroupReplicationEnabled(t *testing.T) {
	ts := newGroupReplicationTopo(t, policy.DurabilityNone)
	tm, _ := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.FullStatusData = &replicationdatapb.FullStatus{}
	})
	status, err := tm.FullStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, status.GroupReplicationEnabled)

	withGroupReplication(t)
	status, err = tm.FullStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, status.GroupReplicationEnabled)
}
