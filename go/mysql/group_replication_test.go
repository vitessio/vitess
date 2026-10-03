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

package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

func member(uuid, state, role string) *replicationdatapb.GroupReplicationMember {
	return &replicationdatapb.GroupReplicationMember{MemberUuid: uuid, State: state, Role: role}
}

func TestFillGroupReplicationMemberStatus(t *testing.T) {
	testCases := []struct {
		name        string
		members     []*replicationdatapb.GroupReplicationMember
		wantState   string
		wantRole    string
		wantPrimary string
		wantQuorum  bool
	}{{
		name:      "not started",
		members:   []*replicationdatapb.GroupReplicationMember{member("a", GroupMemberStateOffline, "")},
		wantState: GroupMemberStateOffline,
	}, {
		name:      "plugin loaded but no member row",
		wantState: GroupMemberStateOffline,
	}, {
		name: "secondary of a healthy group",
		members: []*replicationdatapb.GroupReplicationMember{
			member("a", GroupMemberStateOnline, GroupMemberRoleSecondary),
			member("b", GroupMemberStateOnline, GroupMemberRolePrimary),
			member("c", GroupMemberStateRecovering, GroupMemberRoleSecondary),
		},
		wantState:   GroupMemberStateOnline,
		wantRole:    GroupMemberRoleSecondary,
		wantPrimary: "b",
		wantQuorum:  true,
	}, {
		name: "primary in the minority of a partition",
		members: []*replicationdatapb.GroupReplicationMember{
			member("a", GroupMemberStateOnline, GroupMemberRolePrimary),
			member("b", GroupMemberStateUnreachable, GroupMemberRoleSecondary),
			member("c", GroupMemberStateUnreachable, GroupMemberRoleSecondary),
		},
		wantState:   GroupMemberStateOnline,
		wantRole:    GroupMemberRolePrimary,
		wantPrimary: "a",
		wantQuorum:  false,
	}, {
		name: "member in error state reports no primary",
		members: []*replicationdatapb.GroupReplicationMember{
			member("a", GroupMemberStateError, ""),
		},
		wantState: GroupMemberStateError,
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status := &replicationdatapb.GroupReplicationStatus{PluginActive: true, Members: tc.members}
			fillGroupReplicationMemberStatus(status, "a")
			assert.Equal(t, tc.wantState, status.MemberState)
			assert.Equal(t, tc.wantRole, status.MemberRole)
			assert.Equal(t, tc.wantPrimary, status.PrimaryUuid)
			assert.Equal(t, tc.wantQuorum, status.HasQuorum)
		})
	}
}

func TestIsGroupPrimary(t *testing.T) {
	status := &replicationdatapb.GroupReplicationStatus{
		PluginActive: true,
		MemberState:  GroupMemberStateOnline,
		MemberRole:   GroupMemberRolePrimary,
		HasQuorum:    true,
	}
	assert.True(t, IsGroupPrimary(status))
	assert.True(t, IsGroupMemberActive(status))

	status.HasQuorum = false
	assert.False(t, IsGroupPrimary(status), "a primary without quorum cannot commit")

	assert.False(t, IsGroupPrimary(nil))
	assert.False(t, IsGroupMemberActive(&replicationdatapb.GroupReplicationStatus{MemberState: GroupMemberStateOnline}),
		"the member state is meaningless without the plugin")
}

// TestGroupReplicationUnreachableMajorityTimeoutOutlastsJoinerExpulsion checks that a member that
// lost the majority of its view waits long enough for the group to expel the lost member with the
// vote of a member being admitted: up to 1 second while the joiner's group communication thread is
// blocked connecting to the lost member, plus MySQL's one-second check of the restored majority.
// With 1 second the group lost about half of those races in the chaos tests.
func TestGroupReplicationUnreachableMajorityTimeoutOutlastsJoinerExpulsion(t *testing.T) {
	const joinerConnectTimeout = time.Second
	const majorityCheckInterval = time.Second
	assert.GreaterOrEqual(t, GroupReplicationUnreachableMajorityTimeout, joinerConnectTimeout+majorityCheckInterval)
	cmds := ConfigureGroupReplicationCommands(GroupReplicationConfig{GroupName: "g", LocalAddress: "h1:3306", AutorejoinTries: -1})
	assert.Contains(t, cmds, "SET GLOBAL group_replication_unreachable_majority_timeout = 2")
}

func TestGroupReplicationCommands(t *testing.T) {
	assert.Equal(t, []string{"START GROUP_REPLICATION"}, StartGroupReplicationCommands(false))
	assert.Equal(t, []string{
		"SET GLOBAL group_replication_bootstrap_group = ON",
		"START GROUP_REPLICATION",
		"SET GLOBAL group_replication_bootstrap_group = OFF",
	}, StartGroupReplicationCommands(true))

	cmd, err := SetGroupPrimaryCommand("uuid-1")
	require.NoError(t, err)
	assert.Equal(t, "SELECT group_replication_set_as_primary('uuid-1')", cmd)
	_, err = SetGroupPrimaryCommand("")
	require.Error(t, err)

	cmds := ConfigureGroupReplicationCommands(GroupReplicationConfig{
		GroupName:       "g",
		LocalAddress:    "h1:3306",
		Seeds:           []string{"h2:3306", "h3:3306"},
		MemberWeight:    75,
		Consistency:     "BEFORE_ON_PRIMARY_FAILOVER",
		AutorejoinTries: -1,
	})
	assert.Contains(t, cmds, "SET GLOBAL group_replication_member_expel_timeout = 0")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_paxos_single_leader = ON")
	assert.Contains(t, cmds, "SET PERSIST group_replication_start_on_boot = OFF")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_communication_stack = 'MYSQL'")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_local_address = 'h1:3306'")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_group_seeds = 'h2:3306,h3:3306'")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_member_weight = 75")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_consistency = 'BEFORE_ON_PRIMARY_FAILOVER'")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_unreachable_majority_timeout = 2")

	assert.Equal(t, "CHANGE REPLICATION SOURCE TO\n"+
		"  SOURCE_PASSWORD = 'p\\'w',\n"+
		"  SOURCE_USER = 'vt_repl'\n"+
		"  FOR CHANNEL 'group_replication_recovery'", GroupReplicationCredentialsCommand("vt_repl", "p'w"))
}

// TestGroupReplicationStatusProgressRoundTrip checks the new fields of GroupReplicationStatus through
// the generated marshalling code: the vtprotobuf functions and the reflection-based ones agree, and a
// status without them encodes as before, so that readers that do not know them skip them.
func TestGroupReplicationStatusProgressRoundTrip(t *testing.T) {
	status := &replicationdatapb.GroupReplicationStatus{
		PluginActive:      true,
		GroupName:         "f2758c3b-42d2-5bef-9eb7-01b4c747900d",
		MemberState:       GroupMemberStateOffline,
		PaxosSingleLeader: true,
		StartInProgress:   true,
	}
	vt, err := status.MarshalVT()
	require.NoError(t, err)
	assert.Len(t, vt, status.SizeVT())
	reflected, err := proto.Marshal(status)
	require.NoError(t, err)

	fromVT := &replicationdatapb.GroupReplicationStatus{}
	require.NoError(t, proto.Unmarshal(vt, fromVT))
	assert.True(t, proto.Equal(status, fromVT), "vtprotobuf encoding, reflection decoding")
	fromReflected := &replicationdatapb.GroupReplicationStatus{}
	require.NoError(t, fromReflected.UnmarshalVT(reflected))
	assert.True(t, proto.Equal(status, fromReflected), "reflection encoding, vtprotobuf decoding")
	assert.True(t, proto.Equal(status, status.CloneVT()))

	without := status.CloneVT()
	without.StartInProgress = false
	old, err := without.MarshalVT()
	require.NoError(t, err)
	assert.Equal(t, old, vt[:len(old)], "the new fields are encoded after the existing ones")
}
