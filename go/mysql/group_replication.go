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
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Group Replication member states, as reported by performance_schema.replication_group_members.
const (
	GroupMemberStateOnline      = "ONLINE"
	GroupMemberStateRecovering  = "RECOVERING"
	GroupMemberStateOffline     = "OFFLINE"
	GroupMemberStateError       = "ERROR"
	GroupMemberStateUnreachable = "UNREACHABLE"

	GroupMemberRolePrimary   = "PRIMARY"
	GroupMemberRoleSecondary = "SECONDARY"

	// GroupReplicationApplierChannel and GroupReplicationRecoveryChannel are the replication
	// channels that the Group Replication plugin creates on every member.
	GroupReplicationApplierChannel  = "group_replication_applier"
	GroupReplicationRecoveryChannel = "group_replication_recovery"

	groupReplicationPluginName = "group_replication"

	// GroupReplicationCommandRunningMessage is how MySQL explains its errno 3663
	// (ER_GROUP_REPLICATION_COMMAND_FAILURE) when it refuses a START or STOP GROUP_REPLICATION
	// because another one is running. A START that finds no group runs for about a minute, and a STOP
	// is refused for as long (verified on MySQL 8.4.11).
	GroupReplicationCommandRunningMessage = "Another instance of START/STOP GROUP_REPLICATION command is executing"
)

const (
	readGroupReplicationPlugin = "SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'group_replication'"
	// The member's group_replication_paxos_single_leader is reported rather than the group's
	// WRITE_CONSENSUS_SINGLE_LEADER_CAPABLE: reading performance_schema.
	// replication_group_communication_information waits on the group communication engine, and
	// on a member that was expelled after a freeze it waited forever while holding a lock that the
	// member's own rejoin needed, which wedged the member in ERROR (S2 in
	// doc/failover-audit/GroupReplication.md). Vitess sets the variable before every start, and
	// MySQL refuses a joiner whose setting differs from its group's, so on the members of a group
	// that Vitess bootstrapped both agree.
	readGroupReplicationVars = "SELECT @@global.server_uuid AS server_uuid, @@global.group_replication_group_name AS group_name, " +
		"@@global.group_replication_single_primary_mode AS single_primary_mode, @@global.group_replication_member_weight AS member_weight, " +
		"@@global.group_replication_paxos_single_leader AS paxos_single_leader"
	readGroupReplicationMembers = "SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION " +
		"FROM performance_schema.replication_group_members WHERE MEMBER_ID != '' ORDER BY MEMBER_ID"
	readGroupReplicationViewID = "SELECT VIEW_ID FROM performance_schema.replication_group_member_stats WHERE MEMBER_ID = @@global.server_uuid"
	// The received transaction set of the applier channel includes the transactions this
	// member has received from the group but not applied yet.
	readGroupReplicationReceived = "SELECT RECEIVED_TRANSACTION_SET FROM performance_schema.replication_connection_status " +
		"WHERE CHANNEL_NAME = 'group_replication_applier'"
	// readGroupReplicationProgress reads whether a START GROUP_REPLICATION runs on the member. MySQL
	// lists the statement in performance_schema.processlist for as long as it runs, also after its
	// client connection was killed (verified on MySQL 8.4.11), and a START that finds no group runs
	// for about a minute while the member reports OFFLINE. The query's own text starts with SELECT,
	// so it does not count itself.
	readGroupReplicationProgress = "SELECT (SELECT COUNT(*) FROM performance_schema.processlist " +
		"WHERE INFO LIKE 'START GROUP_REPLICATION%') AS starts"
)

// GroupReplicationStatus reads the Group Replication state of the server. It returns a
// status with PluginActive set to false, and no error, if the plugin is not loaded.
func (c *Conn) GroupReplicationStatus() (*replicationdatapb.GroupReplicationStatus, error) {
	status := &replicationdatapb.GroupReplicationStatus{}

	qr, err := c.ExecuteFetch(readGroupReplicationPlugin, 1, false)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication plugin status")
	}
	if len(qr.Rows) == 0 || qr.Rows[0][0].ToString() != "ACTIVE" {
		return status, nil
	}
	status.PluginActive = true

	qr, err = c.ExecuteFetch(readGroupReplicationVars, 1, true)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication variables")
	}
	var serverUUID string
	if len(qr.Rows) == 1 {
		row := qr.Named().Row()
		serverUUID = row.AsString("server_uuid", "")
		status.GroupName = row.AsString("group_name", "")
		status.SinglePrimaryMode = row.AsBool("single_primary_mode", false)
		status.MemberWeight = int32(row.AsInt64("member_weight", 0))
		status.PaxosSingleLeader = row.AsBool("paxos_single_leader", false)
	}

	qr, err = c.ExecuteFetch(readGroupReplicationMembers, 100, false)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication members")
	}
	status.Members = parseGroupReplicationMembers(qr)
	fillGroupReplicationMemberStatus(status, serverUUID)

	qr, err = c.ExecuteFetch(readGroupReplicationViewID, 1, false)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication view")
	}
	if len(qr.Rows) == 1 {
		status.ViewId = qr.Rows[0][0].ToString()
	}

	qr, err = c.ExecuteFetch(readGroupReplicationReceived, 1, false)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication received transactions")
	}
	if len(qr.Rows) == 1 {
		status.ReceivedTransactionSet = strings.ReplaceAll(qr.Rows[0][0].ToString(), "\n", "")
	}

	qr, err = c.ExecuteFetch(readGroupReplicationProgress, 1, true)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read whether group replication is starting")
	}
	if len(qr.Rows) == 1 {
		status.StartInProgress = qr.Named().Row().AsInt64("starts", 0) > 0
	}
	return status, nil
}

func parseGroupReplicationMembers(qr *sqltypes.Result) []*replicationdatapb.GroupReplicationMember {
	members := make([]*replicationdatapb.GroupReplicationMember, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		port, _ := row[2].ToInt32()
		members = append(members, &replicationdatapb.GroupReplicationMember{
			MemberUuid: row[0].ToString(),
			Host:       row[1].ToString(),
			Port:       port,
			State:      row[3].ToString(),
			Role:       row[4].ToString(),
			Version:    row[5].ToString(),
		})
	}
	return members
}

// fillGroupReplicationMemberStatus derives this member's own state and role, the group's
// primary and whether the member's view has quorum from the membership list.
func fillGroupReplicationMemberStatus(status *replicationdatapb.GroupReplicationStatus, serverUUID string) {
	status.MemberState = GroupMemberStateOffline
	reachable := 0
	for _, m := range status.Members {
		if m.MemberUuid == serverUUID {
			status.MemberState = m.State
			if m.State == GroupMemberStateOnline {
				status.MemberRole = m.Role
			}
		}
		if m.Role == GroupMemberRolePrimary && m.State == GroupMemberStateOnline {
			status.PrimaryUuid = m.MemberUuid
		}
		if m.State == GroupMemberStateOnline || m.State == GroupMemberStateRecovering {
			reachable++
		}
	}
	// A member that is not part of a group sees only itself, in state OFFLINE or ERROR. It
	// does not have quorum.
	if status.MemberState != GroupMemberStateOnline && status.MemberState != GroupMemberStateRecovering {
		status.PrimaryUuid = ""
		return
	}
	status.HasQuorum = reachable > len(status.Members)/2
}

// IsGroupMemberActive returns whether the server is currently part of a replication group,
// either serving (ONLINE) or catching up (RECOVERING).
func IsGroupMemberActive(status *replicationdatapb.GroupReplicationStatus) bool {
	if status == nil || !status.PluginActive {
		return false
	}
	return status.MemberState == GroupMemberStateOnline || status.MemberState == GroupMemberStateRecovering
}

// IsGroupPrimary returns whether the server is the writable primary of a group that has quorum.
func IsGroupPrimary(status *replicationdatapb.GroupReplicationStatus) bool {
	return status != nil && status.PluginActive && status.MemberState == GroupMemberStateOnline &&
		status.MemberRole == GroupMemberRolePrimary && status.HasQuorum
}

// GroupReplicationUnreachableMajorityTimeout is group_replication_unreachable_majority_timeout:
// how long a member that cannot reach the majority of its group waits before it rolls back its
// pending transactions and leaves the group. A member in a minority cannot commit meanwhile:
// MySQL blocks its transactions until they are certified by a majority.
//
// It must outlast the expulsion that a member being admitted to the group makes possible. When a
// group of two loses one member while a third one is joining, the remaining member suspects the
// lost one 5 seconds later and, since the joiner is not in its view yet, finds itself without a
// majority. The group survives only if it expels the lost member, with the joiner's vote, before
// this timeout; otherwise the remaining member leaves and the joiner ends up alone in the group's
// configuration. The joiner's group communication engine runs on a single thread, which blocks for
// up to 1 second each time it tries to connect to the lost member, and MySQL reports a majority
// that is back on its next one-second check. On MySQL 8.4.11, in chaos tests where the primary's
// cell was cut off while another member rejoined, the group kept its majority in 10 of 18 such
// joins with 1 second, 19 of 22 with 2 seconds and 17 of 18 with 3 seconds; 2 and 3 seconds
// cannot be told apart (doc/failover-audit/GroupReplication.md, "Unreachable majority timeout
// sweep"). Each extra second delays the fencing of a partitioned primary by a second: its clients
// wait that much longer for their commits to fail, and it becomes super_read_only (and, under
// OFFLINE_MODE, offline_mode) that much later. The election of a new primary does not depend on it.
const GroupReplicationUnreachableMajorityTimeout = 2 * time.Second

// GroupReplicationConfig is the configuration Vitess applies to a member before it starts
// Group Replication.
type GroupReplicationConfig struct {
	// GroupName is the UUID that identifies the group. All members of a shard use the same name.
	GroupName string
	// LocalAddress is this member's MySQL host:port. The group uses the MySQL communication
	// stack, so members reach each other through the port on which MySQL serves clients.
	LocalAddress string
	// Seeds are the MySQL host:port addresses of the other members.
	Seeds []string
	// MemberWeight is the member's weight in primary elections, 0-100.
	MemberWeight int
	// Consistency is group_replication_consistency. Empty keeps the server's setting.
	Consistency string
	// ExitStateAction is group_replication_exit_state_action. Empty keeps the server's setting.
	ExitStateAction string
	// AutorejoinTries is group_replication_autorejoin_tries. A negative value keeps the
	// server's setting.
	AutorejoinTries int
}

// InstallGroupReplicationPluginCommand returns the statement that installs the Group
// Replication plugin. INSTALL PLUGIN is not written to the binary log and survives restarts.
func InstallGroupReplicationPluginCommand() string {
	return fmt.Sprintf("INSTALL PLUGIN %s SONAME 'group_replication.so'", groupReplicationPluginName)
}

// ConfigureGroupReplicationCommands returns the statements that apply the configuration
// to a member that is not running Group Replication. Group Replication never starts on boot:
// vttablet decides when a member joins a group, like it does for asynchronous replication.
func ConfigureGroupReplicationCommands(cfg GroupReplicationConfig) []string {
	cmds := []string{
		"SET PERSIST group_replication_start_on_boot = OFF",
		"SET GLOBAL group_replication_single_primary_mode = ON",
		"SET GLOBAL group_replication_enforce_update_everywhere_checks = OFF",
		// Members connect to each other through MySQL's own port, authenticated as the
		// replication user and secured like any other client connection, rather than through
		// XCom's separate port, allowlist and TLS settings. MySQL deprecates XCom in 9.7.2 and
		// makes this the default in 26.7. Every member of a group must use the same stack.
		"SET GLOBAL group_replication_communication_stack = 'MYSQL'",
		"SET GLOBAL group_replication_group_name = " + sqltypes.EncodeStringSQL(cfg.GroupName),
		"SET GLOBAL group_replication_local_address = " + sqltypes.EncodeStringSQL(cfg.LocalAddress),
		"SET GLOBAL group_replication_group_seeds = " + sqltypes.EncodeStringSQL(strings.Join(cfg.Seeds, ",")),
		fmt.Sprintf("SET GLOBAL group_replication_member_weight = %d", cfg.MemberWeight),
		// The recovery channel authenticates as the replication user, which uses
		// caching_sha2_password. Without TLS it needs the source's public key.
		"SET GLOBAL group_replication_recovery_get_public_key = ON",
		// Expel an unreachable member as soon as the fixed 5 second detection period ends. On
		// MySQL 8.4 any timeout from 1 to 10 seconds delays the expulsion, and so the election
		// of a new primary, by about 16 seconds. vttablet rejoins an expelled member on its own,
		// so expelling a member that only stalled is cheap.
		"SET GLOBAL group_replication_member_expel_timeout = 0",
		// Make the primary the group's only consensus leader, so that a slow or failed
		// secondary does not delay commits. MySQL applies the setting when a group is
		// bootstrapped and refuses a member whose setting differs from its group's, so it must
		// be the same everywhere.
		"SET GLOBAL group_replication_paxos_single_leader = ON",
		// A member that lost contact with the majority of its group rolls back its pending
		// transactions and leaves the group GroupReplicationUnreachableMajorityTimeout after the
		// others became unreachable, so that a partitioned primary stops blocking its clients
		// and becomes super_read_only. With 0 it would stay in its minority forever.
		fmt.Sprintf("SET GLOBAL group_replication_unreachable_majority_timeout = %d", int(GroupReplicationUnreachableMajorityTimeout/time.Second)),
	}
	if cfg.Consistency != "" {
		cmds = append(cmds, "SET GLOBAL group_replication_consistency = "+sqltypes.EncodeStringSQL(cfg.Consistency))
	}
	if cfg.ExitStateAction != "" {
		cmds = append(cmds, "SET GLOBAL group_replication_exit_state_action = "+sqltypes.EncodeStringSQL(cfg.ExitStateAction))
	}
	if cfg.AutorejoinTries >= 0 {
		cmds = append(cmds, fmt.Sprintf("SET GLOBAL group_replication_autorejoin_tries = %d", cfg.AutorejoinTries))
	}
	return cmds
}

// GroupReplicationCredentialsCommand returns the statement that sets the user as which a member
// connects to the other members of its group. With the MySQL communication stack, MySQL uses the
// credentials of the recovery channel both for distributed recovery and for the group
// communication connections, so they must be stored on the channel: credentials given to START
// GROUP_REPLICATION are only used for recovery. The user needs REPLICATION SLAVE,
// GROUP_REPLICATION_STREAM and CONNECTION_ADMIN.
//
// The password is formatted like in SetReplicationSourceCommand, so that logs redact it.
func GroupReplicationCredentialsCommand(user, password string) string {
	return "CHANGE REPLICATION SOURCE TO\n" +
		"  SOURCE_PASSWORD = " + sqltypes.EncodeStringSQL(password) + ",\n" +
		"  SOURCE_USER = " + sqltypes.EncodeStringSQL(user) + "\n" +
		"  FOR CHANNEL '" + GroupReplicationRecoveryChannel + "'"
}

// GroupReplicationPrivileges are the privileges the replication user needs, in addition to
// REPLICATION SLAVE, to connect members of a group that uses the MySQL communication stack:
// GROUP_REPLICATION_STREAM for the group communication connections, and CONNECTION_ADMIN so that
// MySQL keeps them when a member is placed in offline_mode, rather than expelling it.
var GroupReplicationPrivileges = []string{"GROUP_REPLICATION_STREAM", "CONNECTION_ADMIN"}

// StartGroupReplicationCommands returns the statements that make the member join its group.
// With bootstrap set, the member instead creates a new group of which it is the only member
// and the primary. Bootstrapping a group that already exists elsewhere splits the shard's
// data, so callers must only bootstrap while they hold the shard lock and have verified that
// no member of the group is active.
//
// The member authenticates as the user stored on the recovery channel by
// GroupReplicationCredentialsCommand.
func StartGroupReplicationCommands(bootstrap bool) []string {
	start := "START GROUP_REPLICATION"
	if !bootstrap {
		return []string{start}
	}
	return []string{
		"SET GLOBAL group_replication_bootstrap_group = ON",
		start,
		"SET GLOBAL group_replication_bootstrap_group = OFF",
	}
}

// GroupReplicationStartInProgress returns whether a START GROUP_REPLICATION runs on the server (see
// readGroupReplicationProgress).
func (c *Conn) GroupReplicationStartInProgress() (bool, error) {
	qr, err := c.ExecuteFetch(readGroupReplicationProgress, 1, true)
	if err != nil {
		return false, vterrors.Wrapf(err, "failed to read whether group replication is starting")
	}
	return len(qr.Rows) == 1 && qr.Named().Row().AsInt64("starts", 0) > 0, nil
}

// StopGroupReplicationCommand returns the statement that makes the member leave its group.
// The member stays super_read_only afterwards.
func StopGroupReplicationCommand() string {
	return "STOP GROUP_REPLICATION"
}

// SetGroupPrimaryCommand returns the statement that makes the member with the given
// server_uuid the group's primary. The statement can run on any ONLINE member. It waits for
// transactions that are running on the current primary to finish.
func SetGroupPrimaryCommand(memberUUID string) (string, error) {
	if memberUUID == "" {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "empty member uuid")
	}
	return fmt.Sprintf("SELECT group_replication_set_as_primary(%s)", sqltypes.EncodeStringSQL(memberUUID)), nil
}
