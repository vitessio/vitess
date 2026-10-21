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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestCollectFullStatusData(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQueryPattern(
		"SELECT @@global.server_id AS server_id,.*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image|gtid_purged|replica_net_timeout", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|int64"),
			"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5|9",
		),
	)
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("Last_SQL_Error|Last_IO_Error", "varchar|varchar"), "|"))
	db.AddQuery("SHOW BINARY LOG STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("File|Position|Binlog_Do_DB|Binlog_Ignore_DB|Executed_Gtid_Set", "varchar|int64|varchar|varchar|varchar"),
		"binlog.000001|154|||8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
	))
	db.AddQuery("SELECT @@global.gtid_purged", sqltypes.MakeTestResult(sqltypes.MakeTestFields("gtid_purged", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5"))
	db.AddQueryPattern(
		"SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"rpl_semi_sync_source_enabled|ON",
			"rpl_semi_sync_replica_enabled|OFF",
			"rpl_semi_sync_source_timeout|10000",
			"rpl_semi_sync_source_wait_for_replica_count|2",
		),
	)
	db.AddQueryPattern(
		"SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_status|ON",
			"Rpl_semi_sync_replica_status|OFF",
			"Rpl_semi_sync_source_clients|3",
		),
	)
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"),
		"4.5",
	))

	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)
	conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
	require.NoError(t, err)
	conn.Recycle()
	selectOneCalls := db.GetQueryCalledNum("SELECT 1")
	db.ResetQueryLog()

	status, err := testMysqld.CollectFullStatusData(t.Context())
	require.NoError(t, err)
	require.NotNil(t, status)

	assert.Equal(t, uint32(42), status.ServerId)
	assert.Equal(t, "test-uuid", status.ServerUuid)
	assert.Equal(t, "8.0.35", status.Version)
	assert.Equal(t, "MySQL Community Server - GPL", status.VersionComment)
	assert.True(t, status.ReadOnly)
	assert.True(t, status.SuperReadOnly)
	assert.Equal(t, "ON", status.GtidMode)
	assert.Equal(t, "ROW", status.BinlogFormat)
	assert.Equal(t, "FULL", status.BinlogRowImage)
	assert.True(t, status.LogBinEnabled)
	assert.True(t, status.LogReplicaUpdates)
	assert.True(t, status.SemiSyncPrimaryEnabled)
	assert.False(t, status.SemiSyncReplicaEnabled)
	assert.True(t, status.SemiSyncPrimaryStatus)
	assert.False(t, status.SemiSyncReplicaStatus)
	assert.Equal(t, uint32(3), status.SemiSyncPrimaryClients)
	assert.Equal(t, uint64(10000), status.SemiSyncPrimaryTimeout)
	assert.Equal(t, uint32(2), status.SemiSyncWaitForReplicaCount)
	require.NotNil(t, status.ReplicationStatus)
	require.NotNil(t, status.PrimaryStatus)
	assert.Equal(t, "test-uuid", status.PrimaryStatus.ServerUuid)
	assert.Equal(t, "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", status.GtidPurged)
	require.NotNil(t, status.ReplicationConfiguration)
	assert.Equal(t, int32(9), status.ReplicationConfiguration.ReplicaNetTimeout)
	// gtid_purged and replica_net_timeout now ride the mandatory batch instead of
	// costing their own queries.
	assert.Zero(t, db.GetQueryCalledNum("SELECT @@global.gtid_purged"))
	assert.Zero(t, db.GetQueryCalledNum("select @@global.replica_net_timeout"))
	assert.Len(t, strings.Split(db.QueryLog(), ";"), 7)
	assert.Equal(t, selectOneCalls+1, db.GetQueryCalledNum("SELECT 1"))

	db.AddQuery("SHOW REPLICA STATUS", &sqltypes.Result{})
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"),
	))
	db.ResetQueryLog()

	status, err = testMysqld.CollectFullStatusData(t.Context())
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Nil(t, status.ReplicationStatus)
	assert.Nil(t, status.ReplicationConfiguration)
	// A primary reads the same batch, so the query count does not move.
	assert.Len(t, strings.Split(db.QueryLog(), ";"), 7)
}

func newCollectFullStatusDataTestMysqld(t *testing.T) (*fakesqldb.DB, *Mysqld) {
	t.Helper()

	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQueryPattern(
		"SELECT @@global.server_id AS server_id,.*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image|gtid_purged|replica_net_timeout", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|int64"),
			"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5|9",
		),
	)
	db.AddQuery("SHOW REPLICA STATUS", &sqltypes.Result{})
	db.AddQuery("SHOW BINARY LOG STATUS", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_purged", sqltypes.MakeTestResult(sqltypes.MakeTestFields("gtid_purged", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5"))
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", &sqltypes.Result{})
	db.AddQueryPattern("SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*", &sqltypes.Result{})
	db.AddQueryPattern("SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*", &sqltypes.Result{})

	mysqld := NewMysqld(dbc)
	t.Cleanup(mysqld.Close)
	return db, mysqld
}

func TestCollectFullStatusDataStopsAfterCancellation(t *testing.T) {
	db, mysqld := newCollectFullStatusDataTestMysqld(t)
	ctx, cancel := context.WithCancel(t.Context())
	db.SetBeforeFunc("SHOW REPLICA STATUS", cancel)

	result, err := mysqld.CollectFullStatusData(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, result)
	assert.Zero(t, db.GetQueryCalledNum("SHOW BINARY LOG STATUS"))
	assert.Zero(t, db.GetQueryCalledNum("SELECT * FROM performance_schema.replication_connection_configuration"))
}

func TestCollectFullStatusDataRetriesLostConnectionOnce(t *testing.T) {
	t.Run("successful retry", func(t *testing.T) {
		db, mysqld := newCollectFullStatusDataTestMysqld(t)
		var connectionClosed atomic.Bool
		db.SetBeforeFunc("SHOW REPLICA STATUS", func() {
			if connectionClosed.CompareAndSwap(false, true) {
				db.CloseAllConnections()
			}
		})

		result, err := mysqld.CollectFullStatusData(t.Context())

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 2, db.GetQueryCalledNum("SHOW REPLICA STATUS"))
	})

	t.Run("retry failure", func(t *testing.T) {
		db, mysqld := newCollectFullStatusDataTestMysqld(t)
		db.SetBeforeFunc("SHOW REPLICA STATUS", db.CloseAllConnections)

		result, err := mysqld.CollectFullStatusData(t.Context())

		require.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, 2, db.GetQueryCalledNum("SHOW REPLICA STATUS"))
	})
}

// TestCollectFullStatusDataRetriesVariablesAfterLostConnection covers a
// connection lost while reading the mandatory variables. Nothing collects
// FullStatus behind this batch, so without a retry a dropped connection turns
// into a failed FullStatus rather than a reconnect.
func TestCollectFullStatusDataRetriesVariablesAfterLostConnection(t *testing.T) {
	db, mysqld := newCollectFullStatusDataTestMysqld(t)

	// Register the batch as an exact query so it can carry a before func.
	conn, err := mysqld.GetDbaConnection(t.Context())
	require.NoError(t, err)
	variablesQuery := conn.FullStatusVariablesQuery()
	conn.Close()
	db.AddQuery(variablesQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image|gtid_purged|replica_net_timeout", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|int64"),
		"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5|9",
	))

	var connectionClosed atomic.Bool
	db.SetBeforeFunc(variablesQuery, func() {
		if connectionClosed.CompareAndSwap(false, true) {
			db.CloseAllConnections()
		}
	})

	status, err := mysqld.CollectFullStatusData(t.Context())

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, 2, db.GetQueryCalledNum(variablesQuery))
	assert.Equal(t, uint32(42), status.ServerId)
	assert.Equal(t, "test-uuid", status.ServerUuid)
}

// fullStatusSemiSyncQuery builds the bound semi-sync query for the given names
// so tests can register it as an exact query.
func fullStatusSemiSyncQuery(t *testing.T, queryTemplate string, names []string) string {
	t.Helper()

	bv, err := sqltypes.BuildBindVariable(names)
	require.NoError(t, err)
	query, err := sqlparser.ParseAndBind(queryTemplate, bv)
	require.NoError(t, err)
	return query
}

// TestCollectFullStatusDataRetriesSemiSyncAfterLostConnection covers a
// connection lost while reading semi-sync data. Without a retry the collector
// reports semi-sync as disabled and off while returning success, which VTOrc
// consumes as the truth and can act on.
func TestCollectFullStatusDataRetriesSemiSyncAfterLostConnection(t *testing.T) {
	variablesQuery := fullStatusSemiSyncQuery(t, fullStatusGlobalVariablesQuery, fullStatusSemiSyncVariables)
	statusQuery := fullStatusSemiSyncQuery(t, fullStatusGlobalStatusQuery, fullStatusSemiSyncStatuses)

	testcases := []struct {
		name  string
		query string
	}{
		{name: "variables query", query: variablesQuery},
		{name: "status query", query: statusQuery},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			db, mysqld := newCollectFullStatusDataTestMysqld(t)
			db.AddQuery(variablesQuery, sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"rpl_semi_sync_source_enabled|ON",
				"rpl_semi_sync_replica_enabled|ON",
				"rpl_semi_sync_source_timeout|10000",
				"rpl_semi_sync_source_wait_for_replica_count|2",
			))
			db.AddQuery(statusQuery, sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_source_status|ON",
				"Rpl_semi_sync_replica_status|ON",
				"Rpl_semi_sync_source_clients|3",
			))

			var connectionClosed atomic.Bool
			db.SetBeforeFunc(testcase.query, func() {
				if connectionClosed.CompareAndSwap(false, true) {
					db.CloseAllConnections()
				}
			})

			status, err := mysqld.CollectFullStatusData(t.Context())

			require.NoError(t, err)
			require.NotNil(t, status)
			assert.Equal(t, 2, db.GetQueryCalledNum(testcase.query))

			assert.True(t, status.SemiSyncPrimaryEnabled)
			assert.True(t, status.SemiSyncReplicaEnabled)
			assert.Equal(t, uint64(10000), status.SemiSyncPrimaryTimeout)
			assert.Equal(t, uint32(2), status.SemiSyncWaitForReplicaCount)
			assert.True(t, status.SemiSyncPrimaryStatus)
			assert.True(t, status.SemiSyncReplicaStatus)
			assert.Equal(t, uint32(3), status.SemiSyncPrimaryClients)
		})
	}
}

func TestCollectFullStatusDataFailsWhenMySQLStaysDownDuringSemiSync(t *testing.T) {
	db, mysqld := newCollectFullStatusDataTestMysqld(t)
	variablesQuery := fullStatusSemiSyncQuery(t, fullStatusGlobalVariablesQuery, fullStatusSemiSyncVariables)
	db.AddQuery(variablesQuery, &sqltypes.Result{})
	db.SetBeforeFunc(variablesQuery, func() {
		db.EnableConnFail()
		db.CloseAllConnections()
	})
	t.Cleanup(db.DisableConnFail)

	status, err := mysqld.CollectFullStatusData(t.Context())

	require.ErrorContains(t, err, "semi-sync variables")
	assert.Nil(t, status)
}

// TestCollectFullStatusDataFailsWhenCoreBatchFails covers a mandatory variable
// missing from the batch result. There is no second collection path, so the
// caller has to see the failure instead of a partially populated status.
func TestCollectFullStatusDataFailsWhenCoreBatchFails(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQueryPattern(
		"SELECT @@global.server_id AS server_id,.*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("server_id|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image", "uint64|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar"),
			"42|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL",
		),
	)

	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)

	status, err := testMysqld.CollectFullStatusData(t.Context())
	require.ErrorContains(t, err, "failed to read server_uuid")
	assert.Nil(t, status)
	assert.Zero(t, db.GetQueryCalledNum("SHOW REPLICA STATUS"))
}

func TestFakeMysqlDaemonCollectFullStatusData(t *testing.T) {
	mysqld := NewFakeMysqlDaemon(nil)

	status, err := mysqld.CollectFullStatusData(t.Context())
	require.NoError(t, err)
	assert.Nil(t, status)
}

// TestCollectFullStatusDataFailsForMariaDB pins the unsupported-flavor error.
// The batch reads server_uuid, gtid_mode and super_read_only, none of which
// MariaDB has, and one unknown variable fails the whole statement.
func TestCollectFullStatusDataFailsForMariaDB(t *testing.T) {
	env, err := vtenv.New(vtenv.Options{MySQLServerVersion: "10.11.14-MariaDB"})
	require.NoError(t, err)
	db := fakesqldb.NewWithEnv(t, env)
	t.Cleanup(db.Close)
	db.AddQuery("SELECT 1", &sqltypes.Result{})

	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)

	result, err := mysqld.CollectFullStatusData(t.Context())
	require.ErrorContains(t, err, "not supported on MariaDB")
	assert.Nil(t, result)
}

// TestCollectFullStatusDataOmitsGTIDPurgedForFilePos covers the FilePos flavor,
// which tracks positions by binlog file and offset. The batch still reads
// gtid_purged from the server, but reporting it would put a MySQL56 position in
// a status whose other positions are FilePos, so it has to stay empty.
func TestCollectFullStatusDataOmitsGTIDPurgedForFilePos(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	cp.Flavor = replication.FilePosFlavorID
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQueryPattern(
		"SELECT @@global.server_id AS server_id,.*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image|gtid_purged|replica_net_timeout", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|int64"),
			"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|OFF|ROW|1|1|FULL|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5|9",
		),
	)
	db.AddQuery("SHOW SLAVE STATUS", &sqltypes.Result{})
	db.AddQuery("SHOW BINARY LOG STATUS", &sqltypes.Result{})
	db.AddQuery("SHOW MASTER STATUS", &sqltypes.Result{})
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", &sqltypes.Result{})
	db.AddQueryPattern("SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*", &sqltypes.Result{})
	db.AddQueryPattern("SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*", &sqltypes.Result{})

	status, err := mysqld.CollectFullStatusData(t.Context())

	require.NoError(t, err)
	require.NotNil(t, status)
	// The server answered with a real GTID set, so an empty value here is the
	// FilePos guard rather than the server having nothing to report.
	assert.Empty(t, status.GtidPurged)
	assert.Equal(t, uint32(42), status.ServerId)
}

func TestFetchFullStatusVariablesHonorsCanceledContext(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)
	conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
	require.NoError(t, err)
	t.Cleanup(conn.Recycle)

	query := conn.Conn.FullStatusVariablesQuery()
	db.AddQuery(query, &sqltypes.Result{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = testMysqld.fetchFullStatusVariables(ctx, conn)

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, db.GetQueryCalledNum(query))
}

func TestCollectFullStatusSemiSync(t *testing.T) {
	newTestMysqld := func(t *testing.T) (*fakesqldb.DB, *Mysqld, *dbconnpool.PooledDBConnection) {
		t.Helper()
		db := fakesqldb.New(t)
		t.Cleanup(db.Close)
		params := db.ConnParams()
		cp := *params
		dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
		db.AddQuery("SELECT 1", &sqltypes.Result{})
		mysqld := NewMysqld(dbc)
		t.Cleanup(mysqld.Close)
		require.NoError(t, mysqld.dbaPool.SetCapacity(t.Context(), 1))
		conn, err := getPoolReconnect(t.Context(), mysqld.dbaPool)
		require.NoError(t, err)
		t.Cleanup(conn.Recycle)
		return db, mysqld, conn
	}

	t.Run("legacy names", func(t *testing.T) {
		db, mysqld, conn := newTestMysqld(t)
		db.AddQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*",
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"rpl_semi_sync_master_enabled|ON",
				"rpl_semi_sync_slave_enabled|ON",
				"rpl_semi_sync_master_timeout|10000",
				"rpl_semi_sync_master_wait_for_slave_count|2",
			),
		)
		db.AddQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*",
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_master_status|ON",
				"Rpl_semi_sync_slave_status|OFF",
				"Rpl_semi_sync_master_clients|3",
			),
		)

		status := &replicationdatapb.FullStatus{}
		err := mysqld.collectFullStatusSemiSync(t.Context(), conn, status)
		require.NoError(t, err)

		assert.True(t, status.SemiSyncPrimaryEnabled)
		assert.True(t, status.SemiSyncReplicaEnabled)
		assert.Equal(t, uint64(10000), status.SemiSyncPrimaryTimeout)
		assert.Equal(t, uint32(2), status.SemiSyncWaitForReplicaCount)
		assert.True(t, status.SemiSyncPrimaryStatus)
		assert.False(t, status.SemiSyncReplicaStatus)
		assert.Equal(t, uint32(3), status.SemiSyncPrimaryClients)
	})

	// A server without the semi-sync plugin answers both reads with an empty
	// result and no error, which is how "off" is meant to be reported.
	t.Run("plugin absent reports off", func(t *testing.T) {
		db, mysqld, conn := newTestMysqld(t)
		db.AddQueryPattern("SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*", &sqltypes.Result{})
		db.AddQueryPattern("SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*", &sqltypes.Result{})

		status := &replicationdatapb.FullStatus{}
		err := mysqld.collectFullStatusSemiSync(t.Context(), conn, status)
		require.NoError(t, err)

		assert.False(t, status.SemiSyncPrimaryEnabled)
		assert.False(t, status.SemiSyncReplicaEnabled)
		assert.False(t, status.SemiSyncPrimaryStatus)
		assert.False(t, status.SemiSyncReplicaStatus)
	})

	// A malformed value means the setting is unknown, not zero. Reporting zero
	// would hand VTOrc a timeout the server never set.
	t.Run("malformed value fails collection", func(t *testing.T) {
		db, mysqld, conn := newTestMysqld(t)
		db.AddQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*",
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"rpl_semi_sync_master_enabled|ON",
				"rpl_semi_sync_slave_enabled|ON",
				"rpl_semi_sync_master_timeout|invalid",
				"rpl_semi_sync_master_wait_for_slave_count|2",
			),
		)

		status := &replicationdatapb.FullStatus{}
		err := mysqld.collectFullStatusSemiSync(t.Context(), conn, status)

		require.ErrorContains(t, err, "rpl_semi_sync_master_timeout")
	})

	// Since an absent plugin already reports off without an error, a failed read
	// means the values are unknown. Reporting them as disabled and off is a
	// state VTOrc acts on, so the read has to fail instead.
	t.Run("variable query failure fails collection", func(t *testing.T) {
		db, mysqld, conn := newTestMysqld(t)
		db.RejectQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*",
			"semi-sync variables unavailable",
		)
		db.AddQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*",
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_source_status|ON",
				"Rpl_semi_sync_replica_status|OFF",
				"Rpl_semi_sync_source_clients|3",
			),
		)

		status := &replicationdatapb.FullStatus{}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		err := mysqld.collectFullStatusSemiSync(ctx, conn, status)

		require.ErrorContains(t, err, "semi-sync variables unavailable")
	})

	t.Run("status query failure fails collection", func(t *testing.T) {
		db, mysqld, conn := newTestMysqld(t)
		db.AddQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*",
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"rpl_semi_sync_source_enabled|ON",
				"rpl_semi_sync_replica_enabled|OFF",
				"rpl_semi_sync_source_timeout|10000",
				"rpl_semi_sync_source_wait_for_replica_count|2",
			),
		)
		db.RejectQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*",
			"semi-sync status unavailable",
		)

		status := &replicationdatapb.FullStatus{}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		err := mysqld.collectFullStatusSemiSync(ctx, conn, status)

		require.ErrorContains(t, err, "semi-sync status unavailable")
	})

	t.Run("context cancellation remains fatal", func(t *testing.T) {
		_, mysqld, conn := newTestMysqld(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := mysqld.collectFullStatusSemiSync(ctx, conn, &replicationdatapb.FullStatus{})

		assert.ErrorIs(t, err, context.Canceled)
	})
}
