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

package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func grMember(uuid, host string, port int32, state, role string) *replicationdatapb.GroupReplicationMember {
	return &replicationdatapb.GroupReplicationMember{MemberUuid: uuid, Host: host, Port: port, State: state, Role: role}
}

// groupPrimaryView returns the view of an ONLINE primary with quorum in its own view.
func groupPrimaryView(viewID string, members ...*replicationdatapb.GroupReplicationMember) *replicationdatapb.GroupReplicationStatus {
	return &replicationdatapb.GroupReplicationStatus{
		PluginActive: true,
		MemberState:  mysql.GroupMemberStateOnline,
		MemberRole:   mysql.GroupMemberRolePrimary,
		HasQuorum:    true,
		ViewId:       viewID,
		Members:      members,
	}
}

func TestGroupIncarnation(t *testing.T) {
	assert.Equal(t, "17907858161940982", GroupIncarnation("17907858161940982:1"))
	assert.Equal(t, "17907858161940982", GroupIncarnation("17907858161940982"))
	assert.Empty(t, GroupIncarnation(""))
}

// TestLegitimatePrimary covers the rule that makes Vitess follow a group's primary only within the
// shard's legitimate group.
func TestLegitimatePrimary(t *testing.T) {
	a := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	b := &topodatapb.TabletAlias{Cell: "zone2", Uid: 200}
	c := &topodatapb.TabletAlias{Cell: "zone3", Uid: 300}
	voters := []*topodatapb.TabletAlias{a, b, c}
	// Tablets report their MySQL as localhost, but MySQL reports its own hostname ("vm"): only the
	// server_uuids identify the members, as in the chaos harness.
	tablets := map[string]*topodatapb.Tablet{
		"zone1-0000000100": {Alias: a, MysqlHostname: "localhost", MysqlPort: 13715},
		"zone2-0000000200": {Alias: b, MysqlHostname: "localhost", MysqlPort: 13718},
		"zone3-0000000300": {Alias: c, MysqlHostname: "localhost", MysqlPort: 13721},
	}
	uuids := map[string]string{"zone1-0000000100": "uuid-a", "zone2-0000000200": "uuid-b", "zone3-0000000300": "uuid-c"}
	online := func(uuid string, port int32) *replicationdatapb.GroupReplicationMember {
		return grMember(uuid, "vm", port, mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)
	}

	g := NewLegitimateGroup("1790785744160779", voters, tablets, uuids)
	assert.Equal(t, 2, g.VoterMajority())

	// S7d-ar0: zone3 alone, ONLINE and PRIMARY, with quorum in a view of one, in a new incarnation.
	aloneInNewIncarnation := groupPrimaryView("17907858161940982:1", online("uuid-c", 13721))
	assert.True(t, mysql.IsGroupPrimary(aloneInNewIncarnation), "MySQL alone sees quorum")
	assert.False(t, g.IsLegitimatePrimary(aloneInNewIncarnation))
	assert.True(t, g.IsForeignIncarnation(aloneInNewIncarnation))
	assert.False(t, g.IsLegitimateMember(aloneInNewIncarnation))

	// The same member alone in the recorded incarnation: the group shrank to one member (a graceful
	// leave, or unreachable_majority_timeout leaves). It is not foreign, but lacks the voter majority.
	aloneInGroup := groupPrimaryView("1790785744160779:8", online("uuid-c", 13721))
	assert.False(t, g.IsForeignIncarnation(aloneInGroup))
	assert.True(t, g.IsLegitimateMember(aloneInGroup))
	assert.False(t, g.HasVoterMajority(aloneInGroup))
	assert.False(t, g.IsLegitimatePrimary(aloneInGroup))

	// Two of three voters ONLINE in the recorded incarnation.
	healthy := groupPrimaryView("1790785744160779:9", online("uuid-a", 13715), online("uuid-c", 13721),
		grMember("uuid-b", "vm", 13718, mysql.GroupMemberStateUnreachable, mysql.GroupMemberRoleSecondary))
	assert.Equal(t, 2, g.OnlineVoters(healthy))
	assert.True(t, g.IsLegitimatePrimary(healthy))

	// Members that are not voters do not count towards the majority.
	withNonVoters := groupPrimaryView("1790785744160779:10", online("uuid-c", 13721), online("uuid-x", 13730), online("uuid-y", 13733))
	assert.False(t, g.IsLegitimatePrimary(withNonVoters))

	// A secondary is never the primary, and a primary without quorum in its own view neither.
	secondary := groupPrimaryView("1790785744160779:9", online("uuid-a", 13715), online("uuid-c", 13721))
	secondary.MemberRole = mysql.GroupMemberRoleSecondary
	assert.False(t, g.IsLegitimatePrimary(secondary))
	noQuorum := groupPrimaryView("1790785744160779:9", online("uuid-a", 13715), online("uuid-c", 13721))
	noQuorum.HasQuorum = false
	assert.False(t, g.IsLegitimatePrimary(noQuorum))
}

func TestLegitimateGroupMatchesVotersByAddress(t *testing.T) {
	a := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	b := &topodatapb.TabletAlias{Cell: "zone2", Uid: 200}
	c := &topodatapb.TabletAlias{Cell: "zone3", Uid: 300}
	tablets := map[string]*topodatapb.Tablet{
		"zone1-0000000100": {Alias: a, MysqlHostname: "db1.zone1", MysqlPort: 3306},
		"zone2-0000000200": {Alias: b, MysqlHostname: "db2.zone2", MysqlPort: 3306},
		"zone3-0000000300": {Alias: c, MysqlHostname: "db3.zone3", MysqlPort: 3306},
	}
	// No server_uuid is known: the members are matched by the MySQL address of the tablet records.
	g := NewLegitimateGroup("", []*topodatapb.TabletAlias{a, b, c}, tablets, nil)
	view := groupPrimaryView("1:3",
		grMember("uuid-a", "DB1.zone1", 3306, mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		grMember("uuid-b", "db2.zone2", 3306, mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		grMember("uuid-c", "db3.zone3", 3307, mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary))
	assert.Equal(t, 2, g.OnlineVoters(view), "the port of zone3's member differs from its tablet record")
	assert.True(t, g.IsLegitimatePrimary(view), "no incarnation is recorded: only the voter majority applies")
}

func TestLegitimateGroupWithoutRecords(t *testing.T) {
	// Without voters and incarnation, MySQL's view quorum decides, as before.
	var nilGroup *LegitimateGroup
	empty := NewLegitimateGroup("", nil, nil, nil)
	alone := groupPrimaryView("5:1", grMember("uuid-c", "vm", 1, mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary))
	for _, g := range []*LegitimateGroup{nilGroup, empty} {
		assert.True(t, g.IsLegitimatePrimary(alone))
		assert.False(t, g.IsForeignIncarnation(alone))
		assert.True(t, g.IsLegitimateMember(alone))
	}
	alone.HasQuorum = false
	assert.False(t, empty.IsLegitimatePrimary(alone))

	// An inactive member is never foreign: it is not in any group.
	g := NewLegitimateGroup("1", nil, nil, nil)
	assert.False(t, g.IsForeignIncarnation(&replicationdatapb.GroupReplicationStatus{PluginActive: true, MemberState: mysql.GroupMemberStateError, ViewId: "2:1"}))
}
