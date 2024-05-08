/*
Copyright 2019 The Vitess Authors.

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
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

func testRedacted(t *testing.T, source, expected string) {
	assert.Equal(t, expected, redactPassword(source))
}

func TestRedactSourcePassword(t *testing.T) {

	// regular test case
	testRedacted(t, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = 'AAA',
  SOURCE_CONNECT_RETRY = 1
`,
		`CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = '****',
  SOURCE_CONNECT_RETRY = 1
`)

	// empty password
	testRedacted(t, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = '',
  SOURCE_CONNECT_RETRY = 1
`,
		`CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = '****',
  SOURCE_CONNECT_RETRY = 1
`)

	// no beginning match
	testRedacted(t, "aaaaaaaaaaaaaa", "aaaaaaaaaaaaaa")

	// no end match
	testRedacted(t, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = 'AAA`, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = 'AAA`)
}

func TestRedactMasterPassword(t *testing.T) {

	// regular test case
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '****',
  MASTER_CONNECT_RETRY = 1
`)

	// empty password
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = '',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '****',
  MASTER_CONNECT_RETRY = 1
`)

	// no beginning match
	testRedacted(t, "aaaaaaaaaaaaaa", "aaaaaaaaaaaaaa")

	// no end match
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`)
}

func TestRedactPassword(t *testing.T) {
	// regular case
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = 'AAA'`,
		`START xxx USER = 'vt_repl', PASSWORD = '****'`)

	// empty password
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = ''`,
		`START xxx USER = 'vt_repl', PASSWORD = '****'`)

	// no end match
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = 'AAA`,
		`START xxx USER = 'vt_repl', PASSWORD = 'AAA`)

	// both primary password and password
	testRedacted(t, `START xxx
  SOURCE_PASSWORD = 'AAA',
  PASSWORD = 'BBB'
`,
		`START xxx
  SOURCE_PASSWORD = '****',
  PASSWORD = '****'
`)
}

func TestWaitForReplicationStart(t *testing.T) {
	db := fakesqldb.New(t)
	fakemysqld := NewFakeMysqlDaemon(db)

	defer func() {
		db.Close()
		fakemysqld.Close()
	}()

	err := WaitForReplicationStart(context.Background(), fakemysqld, 2)
	assert.NoError(t, err)

	fakemysqld.ReplicationStatusError = fmt.Errorf("test error")
	err = WaitForReplicationStart(context.Background(), fakemysqld, 2)
	assert.ErrorContains(t, err, "test error")

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("Last_SQL_Error|Last_IO_Error", "varchar|varchar"), "test sql error|test io error"))

	err = WaitForReplicationStart(context.Background(), testMysqld, 2)
	assert.ErrorContains(t, err, "Last_SQL_Error: test sql error, Last_IO_Error: test io error")
}

func TestGetMysqlPort(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'port'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|test_field2", "varchar|uint64"), "test_port|12"))
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := testMysqld.GetMysqlPort(ctx)
	assert.Equal(t, int32(12), res)
	assert.NoError(t, err)

	db.AddQuery("SHOW VARIABLES LIKE 'port'", &sqltypes.Result{})
	res, err = testMysqld.GetMysqlPort(ctx)
	assert.ErrorContains(t, err, "no port variable in mysql")
	assert.Equal(t, int32(0), res)
}

func TestGetServerID(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("select @@global.server_id", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "uint64"), "12"))
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	res, err := testMysqld.GetServerID(ctx)
	assert.Equal(t, uint32(12), res)
	assert.NoError(t, err)

	db.AddQuery("select @@global.server_id", &sqltypes.Result{})
	res, err = testMysqld.GetServerID(ctx)
	assert.ErrorContains(t, err, "no server_id in mysql")
	assert.Equal(t, uint32(0), res)
}

func TestGetServerUUID(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	uuid := "test_uuid"
	db.AddQuery("SELECT @@global.server_uuid", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), uuid))

	ctx := context.Background()
	res, err := testMysqld.GetServerUUID(ctx)
	assert.Equal(t, uuid, res)
	assert.NoError(t, err)

	db.AddQuery("SELECT @@global.server_uuid", &sqltypes.Result{})
	res, err = testMysqld.GetServerUUID(ctx)
	assert.Error(t, err)
	assert.Equal(t, "", res)
}

func TestWaitSourcePos(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	err := testMysqld.WaitSourcePos(ctx, replication.Position{GTIDSet: replication.Mysql56GTIDSet{}})
	assert.NoError(t, err)

	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "invalid_id"))
	err = testMysqld.WaitSourcePos(ctx, replication.Position{GTIDSet: replication.Mysql56GTIDSet{}})
	assert.ErrorContains(t, err, "invalid MySQL 5.6 GTID set")
}

func TestReplicationStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_status"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.True(t, res.ReplicationLagUnknown)

	db.AddQuery("SHOW REPLICA STATUS", &sqltypes.Result{})
	res, err = testMysqld.ReplicationStatus(context.Background())
	assert.Error(t, err)
	assert.False(t, res.ReplicationLagUnknown)
}

func TestPrimaryStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW MASTER STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_status"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	res, err := testMysqld.PrimaryStatus(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	db.AddQuery("SHOW MASTER STATUS", &sqltypes.Result{})
	_, err = testMysqld.PrimaryStatus(ctx)
	assert.ErrorContains(t, err, "no master status")
}

func TestReplicationConfiguration(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|HEARTBEAT_INTERVAL|field2", "varchar|float64|varchar"), "test_status|4.5000|test"))
	db.AddQuery("select @@global.replica_net_timeout", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.replica_net_timeout", "int64"), "9"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	replConfig, err := testMysqld.ReplicationConfiguration(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, replConfig)
	require.EqualValues(t, math.Round(replConfig.HeartbeatInterval*2), replConfig.ReplicaNetTimeout)

	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|HEARTBEAT_INTERVAL|field2", "varchar|float64|varchar")))
	replConfig, err = testMysqld.ReplicationConfiguration(ctx)
	assert.NoError(t, err)
	assert.Nil(t, replConfig)
}

func TestGetGTIDPurged(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_purged", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	res, err := testMysqld.GetGTIDPurged(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17", res.String())
}

func TestPrimaryPosition(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.PrimaryPosition(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17", res.String())
}

func TestSetReplicationPosition(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("RESET MASTER", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()

	pos := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	pos.GTIDSet = pos.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 1})

	err := testMysqld.SetReplicationPosition(ctx, pos)
	assert.Error(t, err)

	// We expect this query to be executed
	db.AddQuery("SET GLOBAL gtid_purged = '00010203-0405-0607-0809-0a0b0c0d0e0f:1'", &sqltypes.Result{})

	err = testMysqld.SetReplicationPosition(ctx, pos)
	assert.NoError(t, err)
}

func TestSetReplicationSource(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("RESET MASTER", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()

	// We expect query containing passed host and port to be executed
	err := testMysqld.SetReplicationSource(ctx, "test_host", 2, 0, true, true)
	assert.ErrorContains(t, err, `SOURCE_HOST = 'test_host'`)
	assert.ErrorContains(t, err, `SOURCE_PORT = 2`)
	assert.ErrorContains(t, err, `CHANGE REPLICATION SOURCE TO`)
}

func TestResetReplication(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync%'", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	err := testMysqld.ResetReplication(ctx)
	assert.ErrorContains(t, err, "RESET REPLICA ALL")

	// We expect this query to be executed
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})
	err = testMysqld.ResetReplication(ctx)
	assert.ErrorContains(t, err, "RESET MASTER")

	// We expect this query to be executed
	db.AddQuery("RESET MASTER", &sqltypes.Result{})
	err = testMysqld.ResetReplication(ctx)
	assert.NoError(t, err)
}

func TestResetReplicationParameters(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync%'", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	err := testMysqld.ResetReplicationParameters(ctx)
	assert.ErrorContains(t, err, "RESET REPLICA ALL")

	// We expect this query to be executed
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})
	err = testMysqld.ResetReplicationParameters(ctx)
	assert.NoError(t, err)
}

func TestFindReplicas(t *testing.T) {
	db := fakesqldb.New(t)
	fakemysqld := NewFakeMysqlDaemon(db)

	defer func() {
		db.Close()
		fakemysqld.Close()
	}()

	fakemysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW PROCESSLIST": sqltypes.MakeTestResult(sqltypes.MakeTestFields("Id|User|Host|db|Command|Time|State|Info", "varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar"), "1|user1|localhost:12|db1|Binlog Dump|54|Has sent all binlog to replica|NULL"),
	}

	res, err := FindReplicas(context.Background(), fakemysqld)
	assert.NoError(t, err)

	want, err := net.LookupHost("localhost")
	require.NoError(t, err)

	assert.Equal(t, want, res)
}

func TestGetBinlogInformation(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.binlog_format, @@global.log_bin, @@global.log_replica_updates, @@global.binlog_row_image", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.binlog_format|@@global.log_bin|@@global.log_replica_updates|@@global.binlog_row_image", "varchar|int64|int64|varchar"), "binlog|1|2|row_image"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	bin, logBin, replicaUpdate, rowImage, err := testMysqld.GetBinlogInformation(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "binlog", bin)
	assert.Equal(t, "row_image", rowImage)
	assert.True(t, logBin)
	assert.False(t, replicaUpdate)
}

func TestGetGTIDMode(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	in := "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("select @@global.gtid_mode", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), in))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	res, err := testMysqld.GetGTIDMode(ctx)
	assert.NoError(t, err)
	assert.Equal(t, in, res)
}

func TestFlushBinaryLogs(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// We expect this query to be executed
	err := testMysqld.FlushBinaryLogs(context.Background())
	assert.ErrorContains(t, err, "FLUSH BINARY LOGS")
}

func TestGetBinaryLogs(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	db.AddQuery("SHOW BINARY LOGS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field", "varchar"), "binlog1", "binlog2"))

	res, err := testMysqld.GetBinaryLogs(context.Background())
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Contains(t, res, "binlog1")
	assert.Contains(t, res, "binlog2")
}

func TestGetPreviousGTIDs(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW BINLOG EVENTS IN 'binlog' LIMIT 2", sqltypes.MakeTestResult(sqltypes.MakeTestFields("Event_type|Info", "varchar|varchar"), "Previous_gtids|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := context.Background()
	res, err := testMysqld.GetPreviousGTIDs(ctx, "binlog")
	assert.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8", res)
}

func TestSetSemiSyncEnabled(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// We expect this query to be executed
	err := testMysqld.SetSemiSyncEnabled(context.Background(), true, true)
	assert.ErrorIs(t, err, ErrNoSemiSync)

	// We expect this query to be executed
	err = testMysqld.SetSemiSyncEnabled(context.Background(), true, false)
	assert.ErrorIs(t, err, ErrNoSemiSync)

	// We expect this query to be executed
	err = testMysqld.SetSemiSyncEnabled(context.Background(), false, true)
	assert.ErrorIs(t, err, ErrNoSemiSync)
}

func TestSemiSyncEnabled(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|OFF", "rpl_semi_sync_replica_enabled|ON"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	p, r := testMysqld.SemiSyncEnabled(context.Background())
	assert.False(t, p)
	assert.True(t, r)
}

func TestSemiSyncStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW STATUS LIKE 'Rpl_semi_sync_%_status'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "Rpl_semi_sync_source_status|ON", "Rpl_semi_sync_replica_status|OFF"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	p, r := testMysqld.SemiSyncStatus(context.Background())
	assert.True(t, p)
	assert.False(t, r)
}

func TestSemiSyncClients(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW STATUS LIKE 'Rpl_semi_sync_source_clients'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "val1|12"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res := testMysqld.SemiSyncClients(context.Background())
	assert.Equal(t, uint32(12), res)
}

func TestSemiSyncSettings(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "rpl_semi_sync_source_timeout|123", "rpl_semi_sync_source_wait_for_replica_count|80"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	timeout, replicas := testMysqld.SemiSyncSettings(context.Background())
	assert.Equal(t, uint64(123), timeout)
	assert.Equal(t, uint32(80), replicas)
}

func TestSemiSyncReplicationStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW STATUS LIKE 'rpl_semi_sync_replica_status'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "rpl_semi_sync_replica_status|ON"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.SemiSyncReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.True(t, res)

	db.AddQuery("SHOW STATUS LIKE 'rpl_semi_sync_replica_status'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "rpl_semi_sync_replica_status|OFF"))

	res, err = testMysqld.SemiSyncReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.False(t, res)
}

func TestSemiSyncExtensionLoaded(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.SemiSyncExtensionLoaded(ctx)
	assert.NoError(t, err)
	assert.Contains(t, []mysql.SemiSyncType{mysql.SemiSyncTypeSource, mysql.SemiSyncTypeMaster}, res)

	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", &sqltypes.Result{})

	res, err = testMysqld.SemiSyncExtensionLoaded(ctx)
	assert.NoError(t, err)
	assert.Equal(t, mysql.SemiSyncTypeOff, res)
}
