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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

// fenceStatusFields are the columns of readGroupReplicationFenceStatus.
var fenceStatusFields = sqltypes.MakeTestFields(
	"plugin_status|server_uuid|super_read_only|view_id|member_id|member_host|member_port|member_state|member_role",
	"varchar|varchar|int64|varchar|varchar|varchar|int64|varchar|varchar")

// TestParseGroupReplicationFenceStatus checks the parsing of the single query with which the tablet
// checks whether its MySQL must be fenced, with the rows MySQL 8.4.11 returns: one row per member of
// the view, or one row with NULL member columns when the member is not in a group.
func TestParseGroupReplicationFenceStatus(t *testing.T) {
	const self = "2d1fa1eb-beb8-11f1-9fd0-02fc00000001"
	const other = "2f2fe988-beb8-11f1-bc60-02fc00000001"

	t.Run("primary of a view of one, writable", func(t *testing.T) {
		qr := sqltypes.MakeTestResult(fenceStatusFields,
			"ACTIVE|"+self+"|0|17909833879041995:1|"+self+"|127.0.0.1|23301|ONLINE|PRIMARY")
		status := parseGroupReplicationFenceStatus(qr)
		require.NotNil(t, status.Status)
		assert.True(t, IsGroupPrimary(status.Status))
		assert.Equal(t, "17909833879041995:1", status.Status.ViewId)
		assert.Equal(t, self, status.ServerUUID)
		assert.False(t, status.SuperReadOnly)
		require.Len(t, status.Status.Members, 1)
		assert.Equal(t, "127.0.0.1", status.Status.Members[0].Host)
		assert.EqualValues(t, 23301, status.Status.Members[0].Port)
	})

	t.Run("fenced primary of a view of two", func(t *testing.T) {
		qr := sqltypes.MakeTestResult(fenceStatusFields,
			"ACTIVE|"+self+"|1|17909833879041995:3|"+self+"|127.0.0.1|23301|ONLINE|PRIMARY",
			"ACTIVE|"+self+"|1|17909833879041995:3|"+other+"|127.0.0.1|23302|ONLINE|SECONDARY")
		status := parseGroupReplicationFenceStatus(qr)
		assert.True(t, IsGroupPrimary(status.Status))
		assert.True(t, status.SuperReadOnly)
		assert.Equal(t, 2, OnlineGroupMembers(status.Status))
	})

	t.Run("not in a group", func(t *testing.T) {
		qr := sqltypes.MakeTestResult(fenceStatusFields, "ACTIVE|"+self+"|1|null|null|null|null|null|null")
		status := parseGroupReplicationFenceStatus(qr)
		assert.True(t, status.Status.PluginActive)
		assert.Equal(t, GroupMemberStateOffline, status.Status.MemberState)
		assert.False(t, IsGroupMemberActive(status.Status))
		assert.Empty(t, status.Status.Members)
		assert.True(t, status.SuperReadOnly)
	})

	t.Run("primary of the group of its own that a START is forming, without the view id", func(t *testing.T) {
		qr := sqltypes.MakeTestResult(fenceStatusFields, "ACTIVE|"+self+"|0|null|"+self+"|127.0.0.1|23301|ONLINE|PRIMARY")
		status := parseGroupReplicationFenceStatus(qr)
		assert.True(t, IsGroupPrimary(status.Status))
		assert.Empty(t, status.Status.ViewId)
		assert.Equal(t, 1, OnlineGroupMembers(status.Status))
	})

	t.Run("plugin not loaded", func(t *testing.T) {
		qr := sqltypes.MakeTestResult(fenceStatusFields, "null|"+self+"|0|null|null|null|null|null|null")
		status := parseGroupReplicationFenceStatus(qr)
		assert.False(t, status.Status.PluginActive)
		assert.False(t, IsGroupMemberActive(status.Status))
		assert.False(t, status.SuperReadOnly)
	})
}

// TestGroupReplicationFenceStatusQueries checks that the query read while START GROUP_REPLICATION
// runs leaves out performance_schema.replication_group_member_stats, which MySQL can hold back for
// about a second while a START forms or joins a group, and that both queries return the same columns.
func TestGroupReplicationFenceStatusQueries(t *testing.T) {
	assert.Contains(t, readGroupReplicationFenceStatus, "replication_group_member_stats")
	assert.NotContains(t, readGroupReplicationFenceStatusWithoutView, "replication_group_member_stats")
	assert.Contains(t, readGroupReplicationFenceStatusWithoutView, "NULL AS view_id")
	assert.Equal(t, strings.Replace(readGroupReplicationFenceStatus,
		"(SELECT VIEW_ID FROM performance_schema.replication_group_member_stats WHERE MEMBER_ID = @@global.server_uuid)", "NULL", 1),
		readGroupReplicationFenceStatusWithoutView)
}
