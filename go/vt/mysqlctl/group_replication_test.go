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

package mysqlctl

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const testGroupMemberUUID = "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9"

// addGroupReplicationStatusQueries makes the fake server answer the status queries of an ONLINE
// primary of a group of one.
func addGroupReplicationStatusQueries(db *fakesqldb.DB) {
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'group_replication'",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar"), "ACTIVE"))
	db.AddQueryPattern(`SELECT @@global\.server_uuid AS server_uuid, .*`, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("server_uuid|group_name|single_primary_mode|member_weight|paxos_single_leader", "varchar|varchar|int64|int64|int64"),
		testGroupMemberUUID+"|f2758c3b-42d2-5bef-9eb7-01b4c747900d|1|50|1"))
	db.AddQueryPattern(`SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION FROM performance_schema\.replication_group_members .*`,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("MEMBER_ID|MEMBER_HOST|MEMBER_PORT|MEMBER_STATE|MEMBER_ROLE|MEMBER_VERSION", "varchar|varchar|int32|varchar|varchar|varchar"),
			testGroupMemberUUID+"|vm|3306|ONLINE|PRIMARY|8.4.11"))
	db.AddQueryPattern(`SELECT VIEW_ID FROM performance_schema\.replication_group_member_stats .*`,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("VIEW_ID", "varchar"), "17907857441607796:3"))
	db.AddQueryPattern(`SELECT RECEIVED_TRANSACTION_SET FROM performance_schema\.replication_connection_status .*`,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("RECEIVED_TRANSACTION_SET", "varchar"), ""))
	setGroupReplicationStartsInProgress(db, 0)
}

// groupReplicationProgressPattern matches the query that reads whether a START GROUP_REPLICATION
// runs.
const groupReplicationProgressPattern = `SELECT \(SELECT COUNT\(\*\) FROM performance_schema\.processlist WHERE INFO LIKE 'START GROUP_REPLICATION%'\) AS starts.*`

// setGroupReplicationStartsInProgress makes the fake server list the given number of START
// GROUP_REPLICATION statements in its processlist, and no primary election thread.
func setGroupReplicationStartsInProgress(db *fakesqldb.DB, starts int) {
	setGroupReplicationProgress(db, starts, 0)
}

// setGroupReplicationProgress makes the fake server list the given number of START
// GROUP_REPLICATION statements in its processlist, and of primary election threads.
func setGroupReplicationProgress(db *fakesqldb.DB, starts, elections int) {
	db.AddQueryPattern(groupReplicationProgressPattern,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("starts|elections", "int64|int64"), fmt.Sprintf("%d|%d", starts, elections)))
}

// memberActionsQuery is the query that reads the member action mysql_disable_super_read_only_if_primary.
const memberActionsPattern = `SELECT \(SELECT ENABLED FROM performance_schema\.replication_group_member_actions WHERE NAME = 'mysql_disable_super_read_only_if_primary' AND EVENT = 'AFTER_PRIMARY_ELECTION'\) AS enabled, .*replication_group_configuration_version.*`

// setSuperReadOnlyActionEnabled makes the fake server report the member action
// mysql_disable_super_read_only_if_primary as enabled or disabled.
func setSuperReadOnlyActionEnabled(db *fakesqldb.DB, enabled bool) {
	value := "0"
	if enabled {
		value = "1"
	}
	db.AddQueryPattern(memberActionsPattern, sqltypes.MakeTestResult(sqltypes.MakeTestFields("enabled|version", "int64|int64"), value+"|3"))
}

// TestGroupReplicationStatusReportsPrimaryElection checks that the status reports the primary
// election that runs on the member it elected (THD_primary_election_primary_process): the member is
// already PRIMARY, and Group Replication sets super_read_only when the election ends (sro-eval case B).
func TestGroupReplicationStatusReportsPrimaryElection(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	addGroupReplicationStatusQueries(db)

	status, err := mysqld.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, status.PrimaryElectionInProgress)

	setGroupReplicationProgress(db, 0, 1)
	status, err = mysqld.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, status.PrimaryElectionInProgress)
	assert.False(t, status.StartInProgress)
}

// TestConfigureGroupReplicationDisablesSuperReadOnlyAction checks that the configuration that precedes
// every START GROUP_REPLICATION disables the member action mysql_disable_super_read_only_if_primary in
// the member's own configuration when it is enabled, lifting super_read_only for the change, as MySQL
// requires, and setting it again: a group that the member forms on its own uses that configuration,
// and its primary must stay read-only (sro-eval case B). An action that is disabled already is left
// alone, and so is a writable member's super_read_only.
func TestConfigureGroupReplicationDisablesSuperReadOnlyAction(t *testing.T) {
	const (
		sroOff  = "SET GLOBAL super_read_only = OFF"
		sroOn   = "SET GLOBAL super_read_only = ON"
		disable = "SELECT group_replication_disable_member_action('mysql_disable_super_read_only_if_primary', 'AFTER_PRIMARY_ELECTION')"
	)
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	addGroupReplicationStatusQueries(db)
	db.AddQueryPattern(`SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION FROM performance_schema\.replication_group_members .*`,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("MEMBER_ID|MEMBER_HOST|MEMBER_PORT|MEMBER_STATE|MEMBER_ROLE|MEMBER_VERSION", "varchar|varchar|int32|varchar|varchar|varchar"),
			testGroupMemberUUID+"|vm|3306|OFFLINE||8.4.11"))
	db.AddQuery("SELECT HOST, PRIV FROM mysql.global_grants WHERE USER = '"+cp.Uname+"'",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("HOST|PRIV", "varchar|varchar"), "%|GROUP_REPLICATION_STREAM", "%|CONNECTION_ADMIN"))
	db.AddQueryPattern("SET GLOBAL group_replication.*", &sqltypes.Result{})
	db.AddQueryPattern("SET PERSIST group_replication.*", &sqltypes.Result{})
	db.AddQueryPattern("CHANGE REPLICATION SOURCE TO.*", &sqltypes.Result{})
	db.AddQuery(sroOff, &sqltypes.Result{})
	db.AddQuery(sroOn, &sqltypes.Result{})
	db.AddQuery(disable, &sqltypes.Result{})
	cfg := mysql.GroupReplicationConfig{GroupName: "g", LocalAddress: "h1:3306", Seeds: []string{"h2:3306"}, AutorejoinTries: -1}
	superReadOnly := func(on bool) {
		value := "0"
		if on {
			value = "1"
		}
		db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.super_read_only", "int64"), value))
	}

	// A read-only member whose configuration has the action enabled, as MySQL's default.
	superReadOnly(true)
	setSuperReadOnlyActionEnabled(db, true)
	db.ResetQueryLog()
	require.NoError(t, mysqld.ConfigureGroupReplication(t.Context(), cfg))
	assert.Equal(t, 1, db.GetQueryCalledNum(disable))
	assert.Equal(t, 1, db.GetQueryCalledNum(sroOff))
	assert.Equal(t, 1, db.GetQueryCalledNum(sroOn), "super_read_only is set again")
	log := strings.ToLower(db.QueryLog())
	off, dis, on := strings.Index(log, strings.ToLower(sroOff)), strings.Index(log, strings.ToLower(disable)), strings.LastIndex(log, strings.ToLower(sroOn))
	assert.True(t, off >= 0 && off < dis && dis < on, "super_read_only is lifted for the change only: %s", log)

	// The action is disabled already: nothing changes.
	setSuperReadOnlyActionEnabled(db, false)
	require.NoError(t, mysqld.ConfigureGroupReplication(t.Context(), cfg))
	assert.Equal(t, 1, db.GetQueryCalledNum(disable))
	assert.Equal(t, 1, db.GetQueryCalledNum(sroOff))

	// A writable member, such as a primary that bootstraps the group during a migration, keeps
	// super_read_only off.
	superReadOnly(false)
	setSuperReadOnlyActionEnabled(db, true)
	require.NoError(t, mysqld.ConfigureGroupReplication(t.Context(), cfg))
	assert.Equal(t, 2, db.GetQueryCalledNum(disable))
	assert.Equal(t, 1, db.GetQueryCalledNum(sroOff))
	assert.Equal(t, 1, db.GetQueryCalledNum(sroOn))
}

// TestGroupReplicationStatusReadsSingleLeaderFromVariables checks that the status of an active
// member reports paxos_single_leader from the member's variables, and does not query
// performance_schema.replication_group_communication_information: on a member expelled after a
// freeze, that query never returned and held a lock that the member's rejoin needed, which
// wedged the member in ERROR (S2 in doc/failover-audit/GroupReplication.md). The fake server
// fails every query it does not know.
func TestGroupReplicationStatusReadsSingleLeaderFromVariables(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	addGroupReplicationStatusQueries(db)

	status, err := mysqld.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "ONLINE", status.MemberState)
	assert.Equal(t, "PRIMARY", status.MemberRole)
	assert.Equal(t, "17907857441607796:3", status.ViewId)
	assert.True(t, status.PaxosSingleLeader)
	assert.False(t, status.StartInProgress)
}

// TestGroupReplicationStatusReportsStartInProgress checks that the status reports a START
// GROUP_REPLICATION that MySQL lists in its processlist: such a START reports the member OFFLINE for
// up to about a minute, also after its client gave up (sro-eval case A).
func TestGroupReplicationStatusReportsStartInProgress(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	addGroupReplicationStatusQueries(db)
	db.AddQueryPattern(`SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION FROM performance_schema\.replication_group_members .*`,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("MEMBER_ID|MEMBER_HOST|MEMBER_PORT|MEMBER_STATE|MEMBER_ROLE|MEMBER_VERSION", "varchar|varchar|int32|varchar|varchar|varchar"),
			testGroupMemberUUID+"|vm|3306|OFFLINE||8.4.11"))
	setGroupReplicationStartsInProgress(db, 1)

	status, err := mysqld.GroupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOffline, status.MemberState)
	assert.True(t, status.StartInProgress)
}

// TestStartGroupReplicationBootstrapRefusedWhileStartRuns checks that a bootstrap does not set
// group_replication_bootstrap_group while a START GROUP_REPLICATION runs: MySQL accepts the flag then,
// and could complete that START as a bootstrap (sro-eval case A). The refusal is UNAVAILABLE, and
// says that a START runs, so that the tablet waits for it to end.
func TestStartGroupReplicationBootstrapRefusedWhileStartRuns(t *testing.T) {
	const bootstrapOn = "SET GLOBAL group_replication_bootstrap_group = ON"
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(bootstrapOn, &sqltypes.Result{})
	db.AddQuery("SET GLOBAL group_replication_bootstrap_group = OFF", &sqltypes.Result{})
	db.AddQuery("START GROUP_REPLICATION", &sqltypes.Result{})
	setGroupReplicationStartsInProgress(db, 1)

	err := mysqld.StartGroupReplication(t.Context(), true)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	require.ErrorContains(t, err, mysql.GroupReplicationCommandRunningMessage)
	assert.Zero(t, db.GetQueryCalledNum(bootstrapOn))
	assert.Zero(t, db.GetQueryCalledNum("START GROUP_REPLICATION"))

	setGroupReplicationStartsInProgress(db, 0)
	require.NoError(t, mysqld.StartGroupReplication(t.Context(), true))
	assert.Equal(t, 1, db.GetQueryCalledNum(bootstrapOn))
	assert.Equal(t, 1, db.GetQueryCalledNum("START GROUP_REPLICATION"))
}

const groupReplicationApplierStatusPattern = `SELECT \(SELECT PLUGIN_STATUS FROM information_schema\.PLUGINS WHERE PLUGIN_NAME = 'group_replication'\) AS plugin_status, .*performance_schema\.replication_applier_status_by_worker .*performance_schema\.replication_applier_status_by_coordinator .*`

var groupReplicationApplierStatusFields = sqltypes.MakeTestFields(
	"plugin_status|member_state|members|reachable_members|view_id|queued_transactions|applier_lag_seconds",
	"varchar|varchar|int64|int64|varchar|uint64|decimal")

// TestGroupReplicationApplierStatus checks that the member's state, quorum and applier lag are
// read with one query, which the fake server answers. It fails every other query.
func TestGroupReplicationApplierStatus(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	db.AddQuery("SELECT 1", &sqltypes.Result{})

	db.AddQueryPattern(groupReplicationApplierStatusPattern, sqltypes.MakeTestResult(groupReplicationApplierStatusFields,
		"ACTIVE|ONLINE|3|1|17907857441607796:3|5|1.250000"))
	status, err := mysqld.GroupReplicationApplierStatus(t.Context())
	require.NoError(t, err)
	assert.Equal(t, &mysql.GroupReplicationApplierStatus{
		PluginActive: true, MemberState: "ONLINE", Members: 3, ReachableMembers: 1, ViewID: "17907857441607796:3",
		QueuedTransactions: 5, Applying: true, OldestApplying: 1250 * time.Millisecond,
	}, status)
	assert.False(t, status.HasQuorum())

	// Nothing being applied: the age is NULL.
	db.AddQueryPattern(groupReplicationApplierStatusPattern, sqltypes.MakeTestResult(groupReplicationApplierStatusFields,
		"ACTIVE|ONLINE|3|3|17907857441607796:3|0|NULL"))
	status, err = mysqld.GroupReplicationApplierStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, status.Applying)
	lag, known := status.ApplierLag()
	assert.True(t, known)
	assert.Zero(t, lag)

	// Not in a group: MySQL lists no member with its server_uuid.
	db.AddQueryPattern(groupReplicationApplierStatusPattern, sqltypes.MakeTestResult(groupReplicationApplierStatusFields,
		"ACTIVE|NULL|0|0|NULL|NULL|NULL"))
	status, err = mysqld.GroupReplicationApplierStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, status.PluginActive)
	assert.Equal(t, mysql.GroupMemberStateOffline, status.MemberState)

	// The plugin is not loaded.
	db.AddQueryPattern(groupReplicationApplierStatusPattern, sqltypes.MakeTestResult(groupReplicationApplierStatusFields,
		"NULL|NULL|0|0|NULL|NULL|NULL"))
	status, err = mysqld.GroupReplicationApplierStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, status.PluginActive)
}

// TestGroupReplicationStatusIsBoundedByContext checks that a status query that the server does
// not answer does not hold the caller beyond its context.
func TestGroupReplicationStatusIsBoundedByContext(t *testing.T) {
	oldKillGrace := killGraceTimeout
	killGraceTimeout = 100 * time.Millisecond
	t.Cleanup(func() { killGraceTimeout = oldKillGrace })
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	addGroupReplicationStatusQueries(db)
	db.AddQueryPattern("kill .*", &sqltypes.Result{})
	// The member list never comes back in time.
	db.AddQueryPatternWithCallback(`SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION FROM performance_schema\.replication_group_members .*`,
		&sqltypes.Result{}, func(string) { time.Sleep(5 * time.Second) })

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := mysqld.GroupReplicationStatus(ctx)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 3*time.Second, "the status read must end with its context")
}

// TestStartGroupReplicationBootstrapTimeoutResetsFlag checks that a bootstrap whose START
// GROUP_REPLICATION outlives the caller's context still turns group_replication_bootstrap_group
// off. The killed statement, or any later START GROUP_REPLICATION, would otherwise create a new
// group.
func TestStartGroupReplicationBootstrapTimeoutResetsFlag(t *testing.T) {
	const (
		bootstrapOn  = "SET GLOBAL group_replication_bootstrap_group = ON"
		bootstrapOff = "SET GLOBAL group_replication_bootstrap_group = OFF"
	)
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(bootstrapOn, &sqltypes.Result{})
	db.AddQuery(bootstrapOff, &sqltypes.Result{})
	db.AddQueryPattern("kill .*", &sqltypes.Result{})
	setGroupReplicationStartsInProgress(db, 0)
	// The join outlives the caller's context, as when no seed answers.
	db.AddQueryPatternWithCallback("START GROUP_REPLICATION.*", &sqltypes.Result{}, func(string) {
		time.Sleep(500 * time.Millisecond)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := mysqld.StartGroupReplication(ctx, true)
	require.Error(t, err)
	assert.Equal(t, 1, db.GetQueryCalledNum(bootstrapOn))
	assert.Equal(t, 1, db.GetQueryCalledNum(bootstrapOff), "group_replication_bootstrap_group must be reset after a failed bootstrap")
}

// TestConfigureGroupReplicationRequiresStreamPrivileges checks that the configuration refuses a
// replication user without the privileges of the MySQL communication stack, and that otherwise it
// selects that stack and stores the replication user's credentials on the recovery channel, which
// MySQL uses for the connections between members. Credentials given to START GROUP_REPLICATION
// would only be used for distributed recovery.
func TestConfigureGroupReplicationRequiresStreamPrivileges(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	cp.Pass = "secret"
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	addGroupReplicationStatusQueries(db)
	// A member that is not in a group.
	db.AddQueryPattern(`SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION FROM performance_schema\.replication_group_members .*`,
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("MEMBER_ID|MEMBER_HOST|MEMBER_PORT|MEMBER_STATE|MEMBER_ROLE|MEMBER_VERSION", "varchar|varchar|int32|varchar|varchar|varchar"),
			testGroupMemberUUID+"|vm|3306|OFFLINE||8.4.11"))
	grantsQuery := "SELECT HOST, PRIV FROM mysql.global_grants WHERE USER = '" + cp.Uname + "'"
	grantFields := sqltypes.MakeTestFields("HOST|PRIV", "varchar|varchar")
	db.AddQueryPattern("SET .*", &sqltypes.Result{})
	db.AddQueryPattern("CHANGE REPLICATION SOURCE TO.*", &sqltypes.Result{})
	setSuperReadOnlyActionEnabled(db, false)
	cfg := mysql.GroupReplicationConfig{GroupName: "g", LocalAddress: "h1:3306", Seeds: []string{"h2:3306"}, AutorejoinTries: -1}

	// GROUP_REPLICATION_STREAM on one account and CONNECTION_ADMIN on another are not enough.
	db.AddQuery(grantsQuery, sqltypes.MakeTestResult(grantFields, "%|GROUP_REPLICATION_STREAM", "localhost|CONNECTION_ADMIN"))
	err := mysqld.ConfigureGroupReplication(t.Context(), cfg)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, "lacks GROUP_REPLICATION_STREAM, CONNECTION_ADMIN")
	assert.Zero(t, db.GetQueryCalledNum("SET GLOBAL group_replication_communication_stack = 'MYSQL'"), "nothing is configured")

	db.AddQuery(grantsQuery, sqltypes.MakeTestResult(grantFields, "%|GROUP_REPLICATION_STREAM", "%|connection_admin", "%|BACKUP_ADMIN"))
	require.NoError(t, mysqld.ConfigureGroupReplication(t.Context(), cfg))
	assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL group_replication_communication_stack = 'MYSQL'"))
	assert.Equal(t, 1, db.GetQueryCalledNum(mysql.GroupReplicationCredentialsCommand(cp.Uname, "secret")))
}

// TestOfflineMode checks that the tablet reads offline_mode, which Group Replication's
// OFFLINE_MODE exit state action sets, and clears it through the dba connection.
func TestOfflineMode(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.offline_mode", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.offline_mode", "int64"), "1"))
	db.AddQuery("SET GLOBAL offline_mode = OFF", &sqltypes.Result{})

	on, err := mysqld.IsOfflineMode(t.Context())
	require.NoError(t, err)
	assert.True(t, on)

	require.NoError(t, mysqld.SetOfflineMode(t.Context(), false))
	assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL offline_mode = OFF"))

	db.AddQuery("SELECT @@global.offline_mode", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.offline_mode", "int64"), "0"))
	on, err = mysqld.IsOfflineMode(t.Context())
	require.NoError(t, err)
	assert.False(t, on)
}

// TestApplyGroupReplicationRelayLog checks that the relay log of a member out of its group is
// applied with the applier thread of the group_replication_applier channel, until the executed GTID
// set holds the required transactions, and that the thread is stopped again, also when they are not
// applied in time.
func TestApplyGroupReplicationRelayLog(t *testing.T) {
	const gtidExecuted = "SELECT @@global.gtid_executed"
	gtidFields := sqltypes.MakeTestFields("@@global.gtid_executed", "varchar")
	until, err := replication.ParseMysql56GTIDSet(testGroupMemberUUID + ":1-15")
	require.NoError(t, err)
	startApplier, stopApplier := mysql.StartGroupReplicationApplierCommand(), mysql.StopGroupReplicationApplierCommand()
	assert.Equal(t, "START REPLICA SQL_THREAD FOR CHANNEL 'group_replication_applier'", startApplier)
	assert.Equal(t, "STOP REPLICA SQL_THREAD FOR CHANNEL 'group_replication_applier'", stopApplier)

	newMysqld := func(t *testing.T) (*fakesqldb.DB, *Mysqld) {
		db := fakesqldb.New(t)
		t.Cleanup(db.Close)
		cp := *db.ConnParams()
		mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
		t.Cleanup(mysqld.Close)
		db.AddQuery("SELECT 1", &sqltypes.Result{})
		db.AddQuery(stopApplier, &sqltypes.Result{})
		return db, mysqld
	}

	t.Run("applied", func(t *testing.T) {
		db, mysqld := newMysqld(t)
		db.AddQuery(gtidExecuted, sqltypes.MakeTestResult(gtidFields, testGroupMemberUUID+":1-10"))
		// The applier applies the relay log once it starts.
		starts := 0
		db.AddQueryPatternWithCallback(regexp.QuoteMeta(startApplier), &sqltypes.Result{}, func(string) {
			starts++
			db.AddQuery(gtidExecuted, sqltypes.MakeTestResult(gtidFields, testGroupMemberUUID+":1-15"))
		})
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		require.NoError(t, mysqld.ApplyGroupReplicationRelayLog(ctx, until))
		assert.Equal(t, 1, starts)
		assert.Equal(t, 1, db.GetQueryCalledNum(stopApplier), "the applier must be stopped again")
	})

	t.Run("not applied in time", func(t *testing.T) {
		db, mysqld := newMysqld(t)
		db.AddQuery(startApplier, &sqltypes.Result{})
		db.AddQuery(gtidExecuted, sqltypes.MakeTestResult(gtidFields, testGroupMemberUUID+":1-10"))
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		err := mysqld.ApplyGroupReplicationRelayLog(ctx, until)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_DEADLINE_EXCEEDED, vterrors.Code(err))
		assert.Equal(t, 1, db.GetQueryCalledNum(startApplier))
		assert.Equal(t, 1, db.GetQueryCalledNum(stopApplier), "the applier must be stopped again")
	})
}
