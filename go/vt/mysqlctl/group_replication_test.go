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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
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
