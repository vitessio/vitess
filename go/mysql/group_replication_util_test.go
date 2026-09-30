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

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

func TestGroupSupersedesSemiSync(t *testing.T) {
	testCases := []struct {
		name       string
		status     *replicationdatapb.GroupReplicationStatus
		wantOnline int
		want       bool
	}{{
		name: "no status",
	}, {
		name:   "plugin not loaded",
		status: &replicationdatapb.GroupReplicationStatus{},
	}, {
		name: "bootstrapped group of one",
		status: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true,
			MemberState:  GroupMemberStateOnline,
			Members:      []*replicationdatapb.GroupReplicationMember{member("a", GroupMemberStateOnline, GroupMemberRolePrimary)},
		},
		wantOnline: 1,
	}, {
		name: "second member still recovering",
		status: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true,
			MemberState:  GroupMemberStateOnline,
			Members: []*replicationdatapb.GroupReplicationMember{
				member("a", GroupMemberStateOnline, GroupMemberRolePrimary),
				member("b", GroupMemberStateRecovering, GroupMemberRoleSecondary),
			},
		},
		wantOnline: 1,
	}, {
		name: "two online members",
		status: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true,
			MemberState:  GroupMemberStateOnline,
			Members: []*replicationdatapb.GroupReplicationMember{
				member("a", GroupMemberStateOnline, GroupMemberRolePrimary),
				member("b", GroupMemberStateOnline, GroupMemberRoleSecondary),
			},
		},
		wantOnline: 2,
		want:       true,
	}, {
		name: "member that left keeps a stale view",
		status: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true,
			MemberState:  GroupMemberStateError,
			Members: []*replicationdatapb.GroupReplicationMember{
				member("a", GroupMemberStateOnline, GroupMemberRolePrimary),
				member("b", GroupMemberStateOnline, GroupMemberRoleSecondary),
			},
		},
		wantOnline: 2,
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantOnline, OnlineGroupMembers(tc.status))
			assert.Equal(t, tc.want, GroupSupersedesSemiSync(tc.status))
		})
	}
}
