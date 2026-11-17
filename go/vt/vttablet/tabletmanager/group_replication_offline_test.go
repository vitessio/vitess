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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// With group_replication_exit_state_action=OFFLINE_MODE, MySQL sets offline_mode when a member
// leaves its group involuntarily, and never clears it: not when the member rejoins, nor when it
// becomes the primary (verified on MySQL 8.4.11). While it is set, MySQL refuses vttablet's app
// and replication users, so the tablet cannot serve and the member cannot be a recovery donor.

// TestGroupReplicationSyncLiftsOfflineModeInLegitimateGroup checks that the sync loop clears
// offline_mode once the member is ONLINE in the shard's legitimate group with a majority of the
// voters in its view, and not before: not while it recovers, not in a view without the majority
// of the voters, and not in a group of another incarnation.
func TestGroupReplicationSyncLiftsOfflineModeInLegitimateGroup(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.OfflineMode.Store(true)
	s := newGroupReplicationSync(tm)

	// Out of any group, after the exit state action.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateError, "")))
	s.reconcile(ctx)
	assert.True(t, fmd.OfflineMode.Load(), "a member out of its group stays fenced")

	// Still catching up with the group.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateRecovering, ""),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:7"))
	s.reconcile(ctx)
	assert.True(t, fmd.OfflineMode.Load(), "a RECOVERING member stays fenced")

	// ONLINE, but only one of the three voters is in the view (the other member is not a voter).
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(7), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:8"))
	s.reconcile(ctx)
	assert.True(t, fmd.OfflineMode.Load(), "a member of a view without the majority of the voters stays fenced")

	// Alone in a group of another incarnation, ONLINE and PRIMARY in its own view: a join that the
	// loss of the group's majority turned into a group of its own (S7d, NEW-1 of the audit). The
	// tablet makes MySQL leave that group, and MySQL stays fenced.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1790000000:1"))
	s.reconcile(ctx)
	assert.True(t, fmd.OfflineMode.Load(), "a member of a foreign group stays fenced")
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 1, stops, "the member leaves the foreign group")

	// ONLINE secondary of the legitimate group: two of the three voters are in its view.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:9"))
	s.reconcile(ctx)
	assert.False(t, fmd.OfflineMode.Load(), "an ONLINE member of the legitimate group must serve again")
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

// TestGroupReplicationSyncLiftsOfflineModeOnBootstrappedPrimary checks that the primary of the
// recorded incarnation is unfenced before the other voters joined: they recover from it, which
// MySQL refuses while offline_mode is ON. Its PRIMARY tablet still does not serve without the
// majority of the voters.
func TestGroupReplicationSyncLiftsOfflineModeOnBootstrappedPrimary(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.OfflineMode.Store(true)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:1"))

	newGroupReplicationSync(tm).reconcile(ctx)
	assert.False(t, fmd.OfflineMode.Load())
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type, "a primary without the majority of the voters is not followed")
}

// TestStartGroupReplicationBootstrapLiftsOfflineMode checks that a bootstrap clears offline_mode
// right away. After the group lost its majority every member left it, and with the OFFLINE_MODE
// exit state action every member is fenced; the member that VTOrc bootstraps is the only donor
// of the others.
func TestStartGroupReplicationBootstrapLiftsOfflineMode(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	fmd.OfflineMode.Store(true)
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	status, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.False(t, fmd.OfflineMode.Load())
	require.NoError(t, fmd.CheckSuperQueryList())
}

// TestPromoteReplicaLiftsOfflineMode checks that PromoteReplica clears offline_mode on the member
// it makes the group primary: the tablet could not serve as the primary otherwise.
func TestPromoteReplicaLiftsOfflineMode(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	fmd.SuperReadOnly.Store(true)
	fmd.OfflineMode.Store(true)
	pos, err := replication.ParsePosition(gtidFlavor, gtidPosition)
	require.NoError(t, err)
	fmd.SetPrimaryPositionLocked(pos)

	_, err = tm.PromoteReplica(t.Context(), true)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.False(t, fmd.OfflineMode.Load())
}

// TestPromoteReplicaFailsWhenOfflineModeStays checks that a promotion that cannot clear
// offline_mode fails before it changes the tablet type.
func TestPromoteReplicaFailsWhenOfflineModeStays(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))
	fmd.OfflineMode.Store(true)
	fmd.SetOfflineModeError = errors.New("access denied")

	_, err := tm.PromoteReplica(t.Context(), true)
	require.Error(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	assert.True(t, fmd.OfflineMode.Load())
}

// TestSetReplicationSourceLiftsOfflineModeOnFormerMember checks that a former group member that
// Vitess makes an asynchronous replica of the shard primary, for example after VTOrc replaced it
// as a voter, is unfenced: nothing else would clear the offline_mode that its exit from the group
// left.
func TestSetReplicationSourceLiftsOfflineModeOnFormerMember(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 2)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setShardPrimary(t, ts, tm, fmd)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, "")))
	fmd.OfflineMode.Store(true)
	fmd.SetReplicationSourceInputs = []string{"mysql2:3306"}
	fmd.ExpectedExecuteSuperQueryList = []string{"FAKE SET SOURCE", "START REPLICA"}

	require.NoError(t, tm.SetReplicationSource(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, 0, "", true, false, 0))
	assert.Equal(t, "mysql2", fmd.CurrentSourceHost)
	assert.False(t, fmd.OfflineMode.Load())
	require.NoError(t, fmd.CheckSuperQueryList())
}
