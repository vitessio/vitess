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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

// readGroupReplicationFenceStatus reads, in one round trip, what a tablet needs to decide whether
// its MySQL must be fenced: whether the Group Replication plugin is active, the member's
// server_uuid, super_read_only and view id, and the members of its view, one row each. A member that
// is not in a group lists no member (performance_schema then shows its own row with an empty
// MEMBER_ID), so the LEFT JOIN returns a single row whose member columns are NULL.
//
// %s is the view id's expression: performance_schema.replication_group_member_stats does not
// answer while START GROUP_REPLICATION runs, for about a second after the member is ONLINE
// (measured on MySQL 8.4.11), while replication_group_members already lists it as the primary of
// the group it formed. readGroupReplicationFenceStatusWithoutView leaves it out.
const readGroupReplicationFenceStatusFormat = "SELECT " +
	"(SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'group_replication') AS plugin_status, " +
	"@@global.server_uuid AS server_uuid, @@global.super_read_only AS super_read_only, " +
	"%s AS view_id, " +
	"m.MEMBER_ID AS member_id, m.MEMBER_HOST AS member_host, m.MEMBER_PORT AS member_port, m.MEMBER_STATE AS member_state, m.MEMBER_ROLE AS member_role " +
	"FROM (SELECT 1) AS self LEFT JOIN performance_schema.replication_group_members AS m ON m.MEMBER_ID != '' ORDER BY m.MEMBER_ID"

var (
	readGroupReplicationFenceStatus = fmt.Sprintf(readGroupReplicationFenceStatusFormat,
		"(SELECT VIEW_ID FROM performance_schema.replication_group_member_stats WHERE MEMBER_ID = @@global.server_uuid)")
	readGroupReplicationFenceStatusWithoutView = fmt.Sprintf(readGroupReplicationFenceStatusFormat, "NULL")
)

// GroupReplicationFenceStatus is a member's Group Replication state and whether MySQL is
// super_read_only, read together with a single query (see readGroupReplicationFenceStatus).
type GroupReplicationFenceStatus struct {
	// Status is the member's state, role, view id and view, with the quorum and the group's primary
	// derived from the view as GroupReplicationStatus does. The group's variables and the received
	// transaction set are not read.
	Status *replicationdatapb.GroupReplicationStatus
	// ViewKnown is whether the view id was read: Status.ViewId is empty, whatever the view, when it
	// was not.
	ViewKnown bool
	// ServerUUID is the member's server_uuid.
	ServerUUID string
	// SuperReadOnly is whether super_read_only is ON.
	SuperReadOnly bool
}

// GroupReplicationFenceStatus reads the member's Group Replication state and super_read_only with a
// single query, and the view id if withView is set: the view id does not answer while START
// GROUP_REPLICATION runs. Status.PluginActive is false, and there is no error, when the plugin is
// not loaded.
func (c *Conn) GroupReplicationFenceStatus(withView bool) (*GroupReplicationFenceStatus, error) {
	query := readGroupReplicationFenceStatusWithoutView
	if withView {
		query = readGroupReplicationFenceStatus
	}
	qr, err := c.ExecuteFetch(query, 100, true)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication fence status")
	}
	status := parseGroupReplicationFenceStatus(qr)
	status.ViewKnown = withView
	return status, nil
}

// parseGroupReplicationFenceStatus parses the result of readGroupReplicationFenceStatus.
func parseGroupReplicationFenceStatus(qr *sqltypes.Result) *GroupReplicationFenceStatus {
	status := &GroupReplicationFenceStatus{Status: &replicationdatapb.GroupReplicationStatus{MemberState: GroupMemberStateOffline}}
	if len(qr.Rows) == 0 {
		return status
	}
	rows := qr.Named().Rows
	first := rows[0]
	status.ServerUUID = first.AsString("server_uuid", "")
	status.SuperReadOnly = first.AsString("super_read_only", "") == "1" || first.AsString("super_read_only", "") == "ON"
	if first.AsString("plugin_status", "") != "ACTIVE" {
		return status
	}
	status.Status.PluginActive = true
	status.Status.ViewId = first.AsString("view_id", "")
	for _, row := range rows {
		uuid := row.AsString("member_id", "")
		if uuid == "" {
			continue
		}
		status.Status.Members = append(status.Status.Members, &replicationdatapb.GroupReplicationMember{
			MemberUuid: uuid,
			Host:       row.AsString("member_host", ""),
			Port:       int32(row.AsInt64("member_port", 0)),
			State:      row.AsString("member_state", ""),
			Role:       row.AsString("member_role", ""),
		})
	}
	fillGroupReplicationMemberStatus(status.Status, status.ServerUUID)
	return status
}
