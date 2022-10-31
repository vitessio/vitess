/*
Copyright 2021 The Vitess Authors.

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

package controller

import (
	"math"
	"testing"

	"vitess.io/vitess/go/vt/vtgr/log"

	"vitess.io/vitess/go/vt/vtgr/db"
	"vitess.io/vitess/go/vt/vtgr/inst"

	"github.com/stretchr/testify/assert"
)

func TestSQLGroupToString(t *testing.T) {
	group := NewSQLGroup(2, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group_name"
	var l1 []*db.GroupMember
	var l2 []*db.GroupMember
	m1 := db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false)
	m2 := db.NewGroupMember("ONLINE", "SECONDARY", "host2", 10, true)
	m3 := db.NewGroupMember("OFFLINE", "SECONDARY", "host3", 10, true)
	l1 = append(l1, m1)
	l1 = append(l1, m2)
	v1.UnresolvedMembers = l1
	l2 = append(l2, m3)
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group_name"
	v2.UnresolvedMembers = l2
	group.recordView(v2)
	group.recordView(v1)
	assert.Equal(t, `[v2] SQLGroup group=group_name | host3 SECONDARY OFFLINE readonly=true
[v1] SQLGroup group=group_name | host1 PRIMARY ONLINE readonly=false | host2 SECONDARY ONLINE readonly=true
`, group.ToString())
	group.Resolve()
	assert.Equal(t, `[v2] SQLGroup group=group_name | host3 SECONDARY OFFLINE readonly=true
[v1] SQLGroup group=group_name | host1 PRIMARY ONLINE readonly=false | host2 SECONDARY ONLINE readonly=true
[resolved_view]
group_name=group_name
[host1] state=ONLINE role=PRIMARY readonly=false
[host2] state=ONLINE role=SECONDARY readonly=true
[host3] state=OFFLINE role=SECONDARY readonly=true
`, group.ToString())
}

func TestGetGroupName(t *testing.T) {
	group := NewSQLGroup(3, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host1", 10, true),
	}
	group.recordView(v1)
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "", 0, true),
	}
	group.recordView(v2)
	err := group.Resolve()
	assert.NoError(t, err)
	name := group.GetGroupName()
	assert.Equal(t, "group", name)
	v3 := db.NewGroupView("v3", "host3", 10)
	v3.GroupName = "group_foo"
	group.recordView(v3)
	err = group.Resolve()
	assert.Errorf(t, err, "group has split brain")
	name = group.GetGroupName()
	// group keeps the group name before finding a divergent group name
	assert.Equal(t, "group", name)
}

func TestIsActiveWithMultiplePrimary(t *testing.T) {
	group := NewSQLGroup(2, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false),
		db.NewGroupMember("ONLINE", "SECONDARY", "host2", 10, true),
	}
	group.recordView(v1)
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "SECONDARY", "host1", 10, true),
		db.NewGroupMember("ONLINE", "PRIMARY", "host2", 10, false),
	}
	group.recordView(v2)
	err := group.Resolve()
	assert.Errorf(t, err, "group network partition")
}

func TestIsSafeToBootstrap(t *testing.T) {
	group := NewSQLGroup(1, true, "ks", "0")
	isSafe := group.IsSafeToBootstrap()
	assert.False(t, isSafe)
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "", 0, true),
		db.NewGroupMember("OFFLINE", "", "", 0, true),
	}
	group.recordView(v1)
	group.Resolve()
	isSafe = group.IsSafeToBootstrap()
	assert.True(t, isSafe)
}

func TestIsSafeToBootstrapWithPrimary(t *testing.T) {
	group := NewSQLGroup(1, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	// it is not safe to bootstrap if we see a primary node in group
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 0, false),
		db.NewGroupMember("OFFLINE", "", "", 0, true),
	}
	group.recordView(v1)
	group.Resolve()
	isSafe := group.IsSafeToBootstrap()
	assert.False(t, isSafe)
}

func TestIsUnconnectedReplica(t *testing.T) {
	group := NewSQLGroup(1, true, "ks", "0")
	isSafe := group.IsSafeToBootstrap()
	assert.False(t, isSafe)
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false),
		db.NewGroupMember("ONLINE", "SECONDARY", "host2", 10, true),
	}
	group.recordView(v1)
	group.Resolve()
	isUnconnected := group.IsUnconnectedReplica(&inst.InstanceKey{Hostname: "host2", Port: 10})
	assert.False(t, isUnconnected)
}

func TestGetOnlineGroupSizeFromPrimary(t *testing.T) {
	group := NewSQLGroup(1, true, "ks", "0")
	isSafe := group.IsSafeToBootstrap()
	assert.False(t, isSafe)
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false),
		db.NewGroupMember("ONLINE", "SECONDARY", "host2", 10, true),
		db.NewGroupMember("RECOVERING", "SECONDARY", "host3", 10, true),
	}
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.UnresolvedMembers = []*db.GroupMember{}
	group.recordView(v1)
	group.recordView(v2)
	group.Resolve()
	size, readOnly := group.GetOnlineGroupInfo()
	assert.Equal(t, 2, size)
	assert.False(t, readOnly)
}

func TestNetworkPartition(t *testing.T) {
	group := NewSQLGroup(3, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false),
		db.NewGroupMember("UNREACHABLE", "SECONDARY", "host2", 10, true),
		db.NewGroupMember("UNREACHABLE", "SECONDARY", "host3", 10, true),
	}
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host2", 10, true),
	}
	v3 := db.NewGroupView("v3", "host3", 10)
	v3.GroupName = "group"
	v3.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host3", 10, true),
	}
	group.recordView(v1)
	group.recordView(v2)
	group.recordView(v3)
	err := group.Resolve()
	assert.EqualErrorf(t, err, "group backoff error", err.Error())
	rv := group.resolvedView
	assert.Equal(t, "group", rv.groupName)
	assert.Equal(t, map[inst.InstanceKey]db.GroupMember{
		{Hostname: "host1", Port: 10}: {HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
		{Hostname: "host2", Port: 10}: {HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.UNREACHABLE, ReadOnly: true},
		{Hostname: "host3", Port: 10}: {HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.UNREACHABLE, ReadOnly: true},
	}, rv.view)
}

func TestInconsistentState(t *testing.T) {
	group := NewSQLGroup(3, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.HeartbeatStaleness = 11
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false),
		db.NewGroupMember("ONLINE", "SECONDARY", "host2", 10, true),
		db.NewGroupMember("ONLINE", "SECONDARY", "host3", 10, true),
	}
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.HeartbeatStaleness = 11
	v2.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host2", 10, true),
	}
	v3 := db.NewGroupView("v3", "host3", 10)
	v3.GroupName = "group"
	v3.HeartbeatStaleness = 13
	v3.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host3", 10, true),
	}
	group.recordView(v1)
	group.recordView(v2)
	group.recordView(v3)
	group.heartbeatThreshold = 10
	err := group.Resolve()
	assert.EqualErrorf(t, err, "group backoff error", err.Error())
	rv := group.resolvedView
	assert.Equal(t, "group", rv.groupName)
	assert.Nil(t, rv.view)
}

func TestInconsistentStateWithInvalidStaleResult(t *testing.T) {
	group := NewSQLGroup(3, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.HeartbeatStaleness = math.MaxInt32
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false),
		db.NewGroupMember("ONLINE", "SECONDARY", "host2", 10, true),
		db.NewGroupMember("ONLINE", "SECONDARY", "host3", 10, true),
	}
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.HeartbeatStaleness = math.MaxInt32
	v2.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host2", 10, true),
	}
	v3 := db.NewGroupView("v3", "host3", 10)
	v3.GroupName = "group"
	v3.HeartbeatStaleness = math.MaxInt32
	v3.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host3", 10, true),
	}
	group.recordView(v1)
	group.recordView(v2)
	group.recordView(v3)
	group.heartbeatThreshold = 10
	err := group.Resolve()
	// Same setup as TestInconsistentState but because HeartbeatStaleness are all MaxInt32
	// the backoff is not triggered
	assert.NoError(t, err)
	rv := group.resolvedView
	assert.Equal(t, "group", rv.groupName)
}

func TestInconsistentUnknownState(t *testing.T) {
	group := NewSQLGroup(3, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "PRIMARY", "host1", 10, false),
		db.NewGroupMember("RECOVERING", "SECONDARY", "host2", 10, true),
		db.NewGroupMember("ONLINE", "SECONDARY", "host3", 10, true),
	}
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("", "", "host2", 10, true),
	}
	v3 := db.NewGroupView("v3", "host3", 10)
	v3.GroupName = "group"
	v3.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("ONLINE", "SECONDARY", "host3", 10, true),
	}
	group.recordView(v1)
	group.recordView(v2)
	group.recordView(v3)
	err := group.Resolve()
	// host 2 reports itself with empty state
	// therefore we shouldn't raise error even with inconsistent state
	assert.NoError(t, err)
	rv := group.resolvedView
	assert.Equal(t, "group", rv.groupName)
	assert.Equal(t, map[inst.InstanceKey]db.GroupMember{
		{Hostname: "host1", Port: 10}: {HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
		{Hostname: "host2", Port: 10}: {HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.RECOVERING, ReadOnly: true},
		{Hostname: "host3", Port: 10}: {HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
	}, rv.view)
}

func TestIsBootstrapInProcess(t *testing.T) {
	group := NewSQLGroup(3, true, "ks", "0")
	v1 := db.NewGroupView("v1", "host1", 10)
	v1.GroupName = "group"
	v1.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("RECOVERING", "SECONDARY", "host1", 10, false),
	}
	v2 := db.NewGroupView("v2", "host2", 10)
	v2.GroupName = "group"
	v2.UnresolvedMembers = []*db.GroupMember{
		db.NewGroupMember("OFFLINE", "", "host2", 10, false),
	}
	v3 := db.NewGroupView("v3", "host", 10)
	v3.GroupName = "group"
	v3.UnresolvedMembers = []*db.GroupMember{}
	group.recordView(v1)
	group.recordView(v2)
	group.recordView(v3)
	err := group.Resolve()
	assert.Errorf(t, err, "group transient error")
}

func TestResolve(t *testing.T) {
	healthyView := []*db.GroupMember{
		{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
		{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
		{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
	}
	var testCases = []struct {
		testName string
		views    []*db.GroupView
		expected *ResolvedView
		errorMsg string
	}{
		{"test healthy shard", []*db.GroupView{
			{MySQLHost: "host1", MySQLPort: 10, GroupName: "group", UnresolvedMembers: healthyView},
			{MySQLHost: "host2", MySQLPort: 10, GroupName: "group", UnresolvedMembers: healthyView},
			{MySQLHost: "host3", MySQLPort: 10, GroupName: "group", UnresolvedMembers: healthyView},
		}, &ResolvedView{"group", map[inst.InstanceKey]db.GroupMember{
			{Hostname: "host1", Port: 10}: {HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
			{Hostname: "host2", Port: 10}: {HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
			{Hostname: "host3", Port: 10}: {HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
		}, nil}, ""},
		{"test readonly with unreachable primary", []*db.GroupView{ // host1 is unreachable
			{MySQLHost: "host2", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
				{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
				{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: false},
			}},
			{MySQLHost: "host3", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
				{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: false},
				{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
			}},
		}, &ResolvedView{"group", map[inst.InstanceKey]db.GroupMember{
			{Hostname: "host1", Port: 10}: {HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
			{Hostname: "host2", Port: 10}: {HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
			{Hostname: "host3", Port: 10}: {HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
		}, nil}, ""},
		{"test split brain by group name", []*db.GroupView{
			{MySQLHost: "host1", MySQLPort: 10, GroupName: "group", UnresolvedMembers: healthyView},
			{MySQLHost: "host2", MySQLPort: 10, GroupName: "group1", UnresolvedMembers: healthyView},
			{MySQLHost: "host3", MySQLPort: 10, GroupName: "group", UnresolvedMembers: healthyView},
		}, nil, "group has split brain"},
		{"test empty hostname", []*db.GroupView{
			{MySQLHost: "host1", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "", Port: 0, Role: db.UNKNOWNROLE, State: db.OFFLINE, ReadOnly: true},
			}},
			{MySQLHost: "host2", MySQLPort: 10, GroupName: "", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host2", Port: 10, Role: db.UNKNOWNROLE, State: db.OFFLINE, ReadOnly: true},
			}},
			{MySQLHost: "host3", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host3", Port: 10, Role: db.UNKNOWNROLE, State: db.OFFLINE, ReadOnly: true},
			}},
		}, &ResolvedView{"group", map[inst.InstanceKey]db.GroupMember{
			{Hostname: "host1", Port: 10}: {HostName: "host1", Port: 10, Role: db.UNKNOWNROLE, State: db.OFFLINE, ReadOnly: true},
			{Hostname: "host2", Port: 10}: {HostName: "host2", Port: 10, Role: db.UNKNOWNROLE, State: db.OFFLINE, ReadOnly: true},
			{Hostname: "host3", Port: 10}: {HostName: "host3", Port: 10, Role: db.UNKNOWNROLE, State: db.OFFLINE, ReadOnly: true},
		}, nil}, ""},
		{"test network partition by majority unreachable", []*db.GroupView{
			{MySQLHost: "host1", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.UNREACHABLE, ReadOnly: false},
				{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
				{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.UNREACHABLE, ReadOnly: true},
			}},
		}, nil, "group backoff error"},
		{"test no network partition with less then majority unreachable", []*db.GroupView{
			{MySQLHost: "host1", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
				{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
				{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.UNREACHABLE, ReadOnly: false},
			}},
			{MySQLHost: "host2", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
				{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
				{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.UNREACHABLE, ReadOnly: false},
			}},
		}, &ResolvedView{"group", map[inst.InstanceKey]db.GroupMember{
			{Hostname: "host1", Port: 10}: {HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.ONLINE, ReadOnly: false},
			{Hostname: "host2", Port: 10}: {HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE, ReadOnly: true},
			{Hostname: "host3", Port: 10}: {HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.UNREACHABLE, ReadOnly: false},
		}, nil}, "group backoff error"},
		{"test network partition by unreachable primary", []*db.GroupView{
			{MySQLHost: "host2", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.UNREACHABLE},
				{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE},
				{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE},
			}},
			{MySQLHost: "host3", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "host1", Port: 10, Role: db.PRIMARY, State: db.UNREACHABLE},
				{HostName: "host2", Port: 10, Role: db.SECONDARY, State: db.ONLINE},
				{HostName: "host3", Port: 10, Role: db.SECONDARY, State: db.ONLINE},
			}},
		}, nil, "group backoff error"},
		{"test bootstrap ongoing", []*db.GroupView{
			{MySQLHost: "host1", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{
				{HostName: "", Port: 0, Role: db.SECONDARY, State: db.RECOVERING, ReadOnly: true},
			}},
			{MySQLHost: "host2", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{}},
			{MySQLHost: "host3", MySQLPort: 10, GroupName: "group", UnresolvedMembers: []*db.GroupMember{}},
		}, nil, "group ongoing bootstrap"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			group := SQLGroup{views: testCase.views, statsTags: []string{"ks", "0"}, logger: log.NewVTGRLogger("ks", "0")}
			err := group.Resolve()
			if testCase.errorMsg != "" {
				assert.EqualError(t, err, testCase.errorMsg)
			} else {
				assert.NoError(t, err)
			}
			if testCase.expected != nil {
				rv := group.resolvedView
				expected := testCase.expected
				assert.Equal(t, expected.view, rv.view)
				assert.Equal(t, expected.groupName, rv.groupName)
			}
		})
	}
}
