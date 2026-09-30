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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestGroupReplicationCommands(t *testing.T) {
	assert.Equal(t, []string{"START GROUP_REPLICATION USER='vt_repl', PASSWORD='p\\'w'"}, StartGroupReplicationCommands(false, "vt_repl", "p'w"))
	assert.Equal(t, []string{
		"SET GLOBAL group_replication_bootstrap_group = ON",
		"START GROUP_REPLICATION",
		"SET GLOBAL group_replication_bootstrap_group = OFF",
	}, StartGroupReplicationCommands(true, "", ""))

	cmd, err := SetGroupPrimaryCommand("uuid-1")
	require.NoError(t, err)
	assert.Equal(t, "SELECT group_replication_set_as_primary('uuid-1')", cmd)
	_, err = SetGroupPrimaryCommand("")
	require.Error(t, err)

	cmds := ConfigureGroupReplicationCommands(GroupReplicationConfig{
		GroupName:                         "g",
		LocalAddress:                      "h1:33061",
		Seeds:                             []string{"h2:33061", "h3:33061"},
		MemberWeight:                      75,
		Consistency:                       "BEFORE_ON_PRIMARY_FAILOVER",
		UnreachableMajorityTimeoutSeconds: -1,
		AutorejoinTries:                   -1,
	})
	assert.Contains(t, cmds, "SET PERSIST group_replication_start_on_boot = OFF")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_group_seeds = 'h2:33061,h3:33061'")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_member_weight = 75")
	assert.Contains(t, cmds, "SET GLOBAL group_replication_consistency = 'BEFORE_ON_PRIMARY_FAILOVER'")
	assert.NotContains(t, cmds, "SET GLOBAL group_replication_unreachable_majority_timeout = -1")
}
