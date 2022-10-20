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

package db

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	gouuid "github.com/google/uuid"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgr/config"
	"vitess.io/vitess/go/vt/vtgr/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/vtgr/inst"
)

var (
	configFilePath       string
	dbFlavor             = "MySQL56"
	mysqlGroupPort       = 33061
	enableHeartbeatCheck bool

	// ErrGroupSplitBrain is the error when mysql group is split-brain
	ErrGroupSplitBrain = errors.New("group has split brain")
	// ErrGroupBackoffError is either the transient error or network partition from the group
	ErrGroupBackoffError = errors.New("group backoff error")
	// ErrGroupOngoingBootstrap is the error when a bootstrap is in progress
	ErrGroupOngoingBootstrap = errors.New("group ongoing bootstrap")
	// ErrGroupInactive is the error when mysql group is inactive unexpectedly
	ErrGroupInactive = errors.New("group is inactive")
	// ErrInvalidInstance is the error when the instance key has empty hostname
	ErrInvalidInstance = errors.New("invalid mysql instance key")
)

func init() {
	servenv.OnParseFor("vtgr", func(fs *pflag.FlagSet) {
		fs.StringVar(&configFilePath, "db_config", "", "Full path to db config file that will be used by VTGR.")
		fs.StringVar(&dbFlavor, "db_flavor", "MySQL56", "MySQL flavor override.")
		fs.IntVar(&mysqlGroupPort, "gr_port", 33061, "Port to bootstrap a MySQL group.")
		fs.BoolVar(&enableHeartbeatCheck, "enable_heartbeat_check", false, "Enable heartbeat checking, set together with --group_heartbeat_threshold.")
	})
}

// Agent is used by vtgr to interact with Mysql
type Agent interface {
	// BootstrapGroupLocked bootstraps a mysql group
	// the caller should grab a lock before
	BootstrapGroupLocked(instanceKey *inst.InstanceKey) error

	// RebootstrapGroupLocked rebootstrap a group with an existing name
	RebootstrapGroupLocked(instanceKey *inst.InstanceKey, name string) error

	// StopGroupLocked stops a mysql group
	StopGroupLocked(instanceKey *inst.InstanceKey) error

	// JoinGroupLocked puts an instance into a mysql group based on primary instance
	// the caller should grab a lock before
	JoinGroupLocked(instanceKey *inst.InstanceKey, primaryKey *inst.InstanceKey) error

	// SetReadOnly set super_read_only variable
	// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_super_read_only
	SetReadOnly(instanceKey *inst.InstanceKey, readOnly bool) error

	// FetchApplierGTIDSet fetches the GTID set from group_replication_applier channel
	FetchApplierGTIDSet(instanceKey *inst.InstanceKey) (mysql.GTIDSet, error)

	// Failover move the mysql primary to the node defined by memberUUID
	Failover(instance *inst.InstanceKey) error

	// FetchGroupView fetches group related information
	FetchGroupView(alias string, instanceKey *inst.InstanceKey) (*GroupView, error)
}

// MemberState is member state
type MemberState int

// MemberRole is member role
type MemberRole int

const (
	UNKNOWNSTATE MemberState = iota
	OFFLINE
	UNREACHABLE
	RECOVERING
	ONLINE
	ERROR
)

const (
	UNKNOWNROLE MemberRole = iota
	SECONDARY
	PRIMARY
)

// GroupMember represents a ROW we get from performance_schema
type GroupMember struct {
	HostName string
	Port     int
	Role     MemberRole
	State    MemberState
	ReadOnly bool
}

// GroupView is an instance's view for the group
type GroupView struct {
	TabletAlias        string
	MySQLHost          string
	MySQLPort          int
	GroupName          string
	HeartbeatStaleness int
	UnresolvedMembers  []*GroupMember
}

// SQLAgentImpl implements Agent
type SQLAgentImpl struct {
	config          *config.Configuration
	dbFlavor        string
	enableHeartbeat bool
}

// NewGroupView creates a new GroupView
func NewGroupView(alias, host string, port int) *GroupView {
	return &GroupView{TabletAlias: alias, MySQLHost: host, MySQLPort: port}
}

// NewGroupMember creates a new GroupMember
func NewGroupMember(state, role, host string, port int, readonly bool) *GroupMember {
	return &GroupMember{
		State:    toMemberState(state),
		Role:     toMemberRole(role),
		HostName: host,
		Port:     port,
		ReadOnly: readonly,
	}
}

// NewVTGRSqlAgent creates a SQLAgentImpl
func NewVTGRSqlAgent() *SQLAgentImpl {
	var conf *config.Configuration
	if (configFilePath) != "" {
		log.Infof("use config from %v", configFilePath)
		conf = config.ForceRead(configFilePath)
	} else {
		log.Warningf("use default config")
		conf = config.Config
	}
	agent := &SQLAgentImpl{
		config:          conf,
		dbFlavor:        dbFlavor,
		enableHeartbeat: enableHeartbeatCheck,
	}
	return agent
}

// BootstrapGroupLocked implements Agent interface
func (agent *SQLAgentImpl) BootstrapGroupLocked(instanceKey *inst.InstanceKey) error {
	if instanceKey == nil {
		return errors.New("nil instance key for bootstrap")
	}
	// Before bootstrap a group, double check locally there is really nothing running locally
	uuid, state, err := agent.getGroupNameAndMemberState(instanceKey)
	if err != nil {
		return err
	}
	if state != "" && state != inst.GroupReplicationMemberStateOffline {
		return fmt.Errorf("%v not OFFLINE mode %v [group_name=%v]", instanceKey.Hostname, state, uuid)
	}
	// If there is a group name stored locally, we should try to reuse it
	// for port, we will override with a new one
	if uuid == "" {
		uuid = gouuid.New().String()
		log.Infof("Try to bootstrap with a new uuid")
	}
	log.Infof("Bootstrap group on %v with %v", instanceKey.Hostname, uuid)
	return agent.bootstrapInternal(instanceKey, uuid)
}

func (agent *SQLAgentImpl) RebootstrapGroupLocked(instanceKey *inst.InstanceKey, name string) error {
	log.Infof("Rebootstrapping group on %v with %v", instanceKey.Hostname, name)
	return agent.bootstrapInternal(instanceKey, name)
}

func (agent *SQLAgentImpl) bootstrapInternal(instanceKey *inst.InstanceKey, uuid string) error {
	// Use persist to set group_replication_group_name
	// so that the instance will persist the name after restart
	cmds := []string{
		"set global offline_mode=0",
		fmt.Sprintf("set @@persist.group_replication_group_name=\"%s\"", uuid),
		fmt.Sprintf("set global group_replication_local_address=\"%s:%d\"", instanceKey.Hostname, mysqlGroupPort),
		fmt.Sprintf("set global group_replication_group_seeds=\"%s:%d\"", instanceKey.Hostname, mysqlGroupPort),
		"set global group_replication_bootstrap_group=ON",
		fmt.Sprintf("start group_replication user='%s', password='%s'", agent.config.MySQLReplicaUser, agent.config.MySQLReplicaPassword),
		"set global group_replication_bootstrap_group=OFF",
	}
	for _, cmd := range cmds {
		if err := execInstanceWithTopo(instanceKey, cmd); err != nil {
			log.Errorf("Failed to execute: %v: %v", cmd, err)
			return err
		}
	}
	return nil
}

// StopGroupLocked implements Agent interface
func (agent *SQLAgentImpl) StopGroupLocked(instanceKey *inst.InstanceKey) error {
	cmd := "stop group_replication"
	return execInstanceWithTopo(instanceKey, cmd)
}

// SetReadOnly implements Agent interface
func (agent *SQLAgentImpl) SetReadOnly(instanceKey *inst.InstanceKey, readOnly bool) error {
	// Setting super_read_only ON implicitly forces read_only ON
	// Setting read_only OFF implicitly forces super_read_only OFF
	// https://www.perconaicom/blog/2016/09/27/using-the-super_read_only-system-variable/
	if readOnly {
		return execInstance(instanceKey, "set @@global.super_read_only=1")
	}
	return execInstance(instanceKey, "set @@global.read_only=0")
}

// JoinGroupLocked implements Agent interface
// Note: caller should grab the lock before calling this
func (agent *SQLAgentImpl) JoinGroupLocked(instanceKey *inst.InstanceKey, primaryInstanceKey *inst.InstanceKey) error {
	var numExistingMembers int
	var uuid string
	query := `select count(*) as count, @@group_replication_group_name as group_name
		from performance_schema.replication_group_members where member_state='ONLINE'`
	err := fetchInstance(primaryInstanceKey, query, func(m sqlutils.RowMap) error {
		numExistingMembers = m.GetInt("count")
		uuid = m.GetString("group_name")
		return nil
	})
	if err != nil {
		return err
	}
	if numExistingMembers == 0 {
		return fmt.Errorf("there is no group members found on %v:%v", primaryInstanceKey.Hostname, primaryInstanceKey.Port)
	}
	// The queries above are executed on the primary instance
	// now let's do one more check with local information to make sure it's OK to join the primary
	localGroup, state, err := agent.getGroupNameAndMemberState(instanceKey)
	if err != nil {
		return err
	}
	if localGroup != "" && localGroup != uuid {
		return fmt.Errorf("%v has a different group name (%v) than primary %v (%v)", instanceKey.Hostname, localGroup, primaryInstanceKey.Hostname, uuid)
	}
	if state == inst.GroupReplicationMemberStateOnline || state == inst.GroupReplicationMemberStateRecovering {
		return fmt.Errorf("%v [%v] is alredy in a group %v", instanceKey.Hostname, state, localGroup)
	}
	var primaryGrPort int
	query = `select @@group_replication_local_address as address`
	err = fetchInstance(primaryInstanceKey, query, func(m sqlutils.RowMap) error {
		address := m.GetString("address")
		arr := strings.Split(address, ":")
		primaryGrPort, err = strconv.Atoi(arr[1])
		if err != nil {
			log.Errorf("Failed to parse primary GR port: %v", err)
			return err
		}
		return nil
	})
	if primaryGrPort == 0 {
		return fmt.Errorf("cannot find group replication port on %v", primaryInstanceKey.Hostname)
	}
	// Now it's safe to join the group
	cmds := []string{
		"set global offline_mode=0",
		fmt.Sprintf("set @@persist.group_replication_group_name=\"%s\"", uuid),
		fmt.Sprintf("set global group_replication_group_seeds=\"%s:%d\"", primaryInstanceKey.Hostname, primaryGrPort),
		fmt.Sprintf("set global group_replication_local_address=\"%s:%d\"", instanceKey.Hostname, mysqlGroupPort),
		fmt.Sprintf("start group_replication user='%s', password='%s'", agent.config.MySQLReplicaUser, agent.config.MySQLReplicaPassword),
	}
	for _, cmd := range cmds {
		if err := execInstanceWithTopo(instanceKey, cmd); err != nil {
			return err
		}
	}
	return nil
}

// Failover implements Agent interface
func (agent *SQLAgentImpl) Failover(instance *inst.InstanceKey) error {
	var memberUUID string
	query := `select member_id
		from performance_schema.replication_group_members
		where member_host=convert(@@hostname using ascii) and member_port=@@port and member_state='ONLINE'`
	err := fetchInstance(instance, query, func(m sqlutils.RowMap) error {
		memberUUID = m.GetString("member_id")
		if memberUUID == "" {
			return fmt.Errorf("unable to find member_id on %v", instance.Hostname)
		}
		return nil
	})
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf(`select group_replication_set_as_primary('%s')`, memberUUID)
	if err := execInstance(instance, cmd); err != nil {
		return err
	}
	return nil
}

// heartbeatCheck returns heartbeat check freshness result
func (agent *SQLAgentImpl) heartbeatCheck(instanceKey *inst.InstanceKey) (int, error) {
	query := `select timestampdiff(SECOND, from_unixtime(truncate(ts * 0.000000001, 0)), NOW()) as diff from _vt.heartbeat;`
	var result int
	err := fetchInstance(instanceKey, query, func(m sqlutils.RowMap) error {
		result = m.GetInt("diff")
		return nil
	})
	return result, err
}

// FetchGroupView implements Agent interface
func (agent *SQLAgentImpl) FetchGroupView(alias string, instanceKey *inst.InstanceKey) (*GroupView, error) {
	view := NewGroupView(alias, instanceKey.Hostname, instanceKey.Port)
	var groupName string
	var isReadOnly bool
	query := `select
		@@group_replication_group_name as group_name,
		@@super_read_only as read_only,
		member_host, member_port, member_state, member_role
	from performance_schema.replication_group_members`
	err := fetchInstance(instanceKey, query, func(m sqlutils.RowMap) error {
		if groupName == "" {
			groupName = m.GetString("group_name")
		}
		host := m.GetString("member_host")
		port := m.GetInt("member_port")
		isReadOnly = m.GetBool("read_only")
		unresolvedMember := NewGroupMember(
			m.GetString("member_state"),
			m.GetString("member_role"),
			host,
			port,
			false)
		// readOnly is used to re-enable write after we set primary to read_only to protect the shard when there is
		// less than desired number of nodes
		// the default value is false because if the node is reachable and read_only, it will get override by the OR op
		// if the host is unreachable, we don't need to trigger the protection for it therefore assume the it's writable
		if host == instanceKey.Hostname && port == instanceKey.Port && isReadOnly {
			unresolvedMember.ReadOnly = true
		}
		view.UnresolvedMembers = append(view.UnresolvedMembers, unresolvedMember)
		return nil
	})
	view.GroupName = groupName
	if err != nil {
		return nil, err
	}
	view.HeartbeatStaleness = math.MaxInt32
	if agent.enableHeartbeat {
		heartbeatStaleness, err := agent.heartbeatCheck(instanceKey)
		if err != nil {
			// We can run into Error 1146: Table '_vt.heartbeat' doesn't exist on new provisioned shard:
			//   vtgr is checking heartbeat table
			//   -> heartbeat table is waiting primary tablet
			//   -> primary tablet needs vtgr.
			//
			// Therefore if we run into error, HeartbeatStaleness will
			// remain to be max int32, which is 2147483647 sec
			log.Errorf("Failed to check heartbeatCheck: %v", err)
		} else {
			view.HeartbeatStaleness = heartbeatStaleness
		}
	}
	return view, nil
}

// GetPrimaryView returns the view of primary member
func (view *GroupView) GetPrimaryView() (string, int, bool) {
	for _, member := range view.UnresolvedMembers {
		if member.Role == PRIMARY {
			return member.HostName, member.Port, member.State == ONLINE
		}
	}
	return "", 0, false
}

func (agent *SQLAgentImpl) getGroupNameAndMemberState(instanceKey *inst.InstanceKey) (string, string, error) {
	// If there is an instance that is unreachable but we still have quorum, GR will remove it from
	// the replication_group_members and Failover if it is the primary node
	// If the state becomes UNREACHABLE it indicates there is a network partition inside the group
	// https://dev.mysql.com/doc/refman/8.0/en/group-replication-network-partitioning.html
	// And then eventually if the node does not recover, the group will transit into ERROR state
	// VTGR cannot handle this case, therefore we raise error here
	var name, state string
	query := `select @@group_replication_group_name as group_name`
	err := fetchInstance(instanceKey, query, func(m sqlutils.RowMap) error {
		name = m.GetString("group_name")
		return nil
	})
	if err != nil {
		return "", "", err
	}
	query = `select member_state
		from performance_schema.replication_group_members
		where member_host=convert(@@hostname using ascii) and member_port=@@port`
	err = fetchInstance(instanceKey, query, func(m sqlutils.RowMap) error {
		state = m.GetString("member_state")
		if state == "" {
			state = inst.GroupReplicationMemberStateOffline
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return name, state, nil
}

// FetchApplierGTIDSet implements Agent interface
func (agent *SQLAgentImpl) FetchApplierGTIDSet(instanceKey *inst.InstanceKey) (mysql.GTIDSet, error) {
	var gtidSet string
	// TODO: should we also take group_replication_recovery as well?
	query := `select gtid_subtract(concat(received_transaction_set, ',', @@global.gtid_executed), '') as gtid_set
		from performance_schema.replication_connection_status
		where channel_name='group_replication_applier'`
	err := fetchInstance(instanceKey, query, func(m sqlutils.RowMap) error {
		// If the instance has no committed transaction, gtidSet will be empty string
		gtidSet = m.GetString("gtid_set")
		return nil
	})
	if err != nil {
		return nil, err
	}
	pos, err := mysql.ParsePosition(agent.dbFlavor, gtidSet)
	if err != nil {
		return nil, err
	}
	return pos.GTIDSet, nil
}

// execInstance executes a given query on the given MySQL discovery instance
func execInstance(instanceKey *inst.InstanceKey, query string, args ...any) error {
	if err := verifyInstance(instanceKey); err != nil {
		return err
	}
	sqlDb, err := OpenDiscovery(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		log.Errorf("error exec %v: %v", query, err)
		return err
	}
	_, err = sqlutils.ExecNoPrepare(sqlDb, query, args...)
	return err
}

// execInstanceWithTopo executes a given query on the given MySQL topology instance
func execInstanceWithTopo(instanceKey *inst.InstanceKey, query string, args ...any) error {
	if err := verifyInstance(instanceKey); err != nil {
		return err
	}
	sqlDb, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		log.Errorf("error exec %v: %v", query, err)
		return err
	}
	_, err = sqlutils.ExecNoPrepare(sqlDb, query, args...)
	return err
}

// fetchInstance fetches result from mysql
func fetchInstance(instanceKey *inst.InstanceKey, query string, onRow func(sqlutils.RowMap) error) error {
	if err := verifyInstance(instanceKey); err != nil {
		return err
	}
	sqlDb, err := OpenDiscovery(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	return sqlutils.QueryRowsMap(sqlDb, query, onRow)
}

// The hostname and port can be empty if a tablet crashed and did not populate them in
// the topo server. We treat them as if the host is unreachable when we calculate the
// quorum for the shard.
func verifyInstance(instanceKey *inst.InstanceKey) error {
	if instanceKey.Hostname == "" || instanceKey.Port == 0 {
		return ErrInvalidInstance
	}
	return nil
}

// CreateInstanceKey returns an InstanceKey based on group member input
// When the group is init for the first time, the hostname and port are not set, e.g.,
// +---------------------------+-----------+-------------+-------------+--------------+-------------+
// | CHANNEL_NAME              | MEMBER_ID | MEMBER_HOST | MEMBER_PORT | MEMBER_STATE | MEMBER_ROLE |
// +---------------------------+-----------+-------------+-------------+--------------+-------------+
// | group_replication_applier |           |             |        NULL | OFFLINE      |             |
// +---------------------------+-----------+-------------+-------------+--------------+-------------+
// therefore we substitute with view's local hostname and port
func (view *GroupView) CreateInstanceKey(member *GroupMember) inst.InstanceKey {
	if member.HostName == "" && member.Port == 0 {
		return inst.InstanceKey{
			Hostname: view.MySQLHost,
			Port:     view.MySQLPort,
		}
	}
	return inst.InstanceKey{
		Hostname: member.HostName,
		Port:     member.Port,
	}
}

// ToString make string for group view
func (view *GroupView) ToString() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("group_name:%v\n", view.GroupName))
	for _, m := range view.UnresolvedMembers {
		sb.WriteString(fmt.Sprintf("host:%v:%v | role:%v | state:%v\n", m.HostName, m.Port, m.Role, m.State))
	}
	return sb.String()
}

func (state MemberState) String() string {
	switch state {
	case ONLINE:
		return inst.GroupReplicationMemberStateOnline
	case ERROR:
		return inst.GroupReplicationMemberStateError
	case RECOVERING:
		return inst.GroupReplicationMemberStateRecovering
	case OFFLINE:
		return inst.GroupReplicationMemberStateOffline
	case UNREACHABLE:
		return inst.GroupReplicationMemberStateUnreachable
	}
	return "UNKNOWN"
}

func toMemberState(state string) MemberState {
	switch state {
	case inst.GroupReplicationMemberStateOnline:
		return ONLINE
	case inst.GroupReplicationMemberStateError:
		return ERROR
	case inst.GroupReplicationMemberStateRecovering:
		return RECOVERING
	case inst.GroupReplicationMemberStateOffline:
		return OFFLINE
	case inst.GroupReplicationMemberStateUnreachable:
		return UNREACHABLE
	default:
		return UNKNOWNSTATE
	}
}

func (role MemberRole) String() string {
	switch role {
	case PRIMARY:
		return inst.GroupReplicationMemberRolePrimary
	case SECONDARY:
		return inst.GroupReplicationMemberRoleSecondary
	}
	return "UNKNOWN"
}

func toMemberRole(role string) MemberRole {
	switch role {
	case inst.GroupReplicationMemberRolePrimary:
		return PRIMARY
	case inst.GroupReplicationMemberRoleSecondary:
		return SECONDARY
	default:
		return UNKNOWNROLE
	}
}
