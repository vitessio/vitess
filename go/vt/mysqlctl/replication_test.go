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
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
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

func TestRedactIdentifiedByPassword(t *testing.T) {
	testRedacted(t, "CLONE INSTANCE FROM 'user'@'host':3306 IDENTIFIED BY 'secret' REQUIRE SSL",
		"CLONE INSTANCE FROM 'user'@'host':3306 IDENTIFIED BY '****' REQUIRE SSL")
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

	err := WaitForReplicationStart(t.Context(), fakemysqld, 2)
	require.NoError(t, err)

	fakemysqld.ReplicationStatusError = errors.New("test error")
	err = WaitForReplicationStart(t.Context(), fakemysqld, 2)
	require.ErrorContains(t, err, "test error")

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("Last_SQL_Error|Last_IO_Error", "varchar|varchar"), "test sql error|test io error"))

	err = WaitForReplicationStart(t.Context(), testMysqld, 2)
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

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := testMysqld.GetMysqlPort(ctx)
	assert.Equal(t, int32(12), res)
	require.NoError(t, err)

	db.AddQuery("SHOW VARIABLES LIKE 'port'", &sqltypes.Result{})
	res, err = testMysqld.GetMysqlPort(ctx)
	require.ErrorContains(t, err, "no port variable in mysql")
	assert.Equal(t, int32(0), res)
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

	ctx := t.Context()
	res, err := testMysqld.GetServerUUID(ctx)
	assert.Equal(t, uuid, res)
	require.NoError(t, err)

	db.AddQuery("SELECT @@global.server_uuid", &sqltypes.Result{})
	res, err = testMysqld.GetServerUUID(ctx)
	require.Error(t, err)
	assert.Empty(t, res)
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

	ctx := t.Context()
	err := testMysqld.WaitSourcePos(ctx, replication.Position{GTIDSet: replication.Mysql56GTIDSet{}})
	require.NoError(t, err)

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

	res, err := testMysqld.ReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, res.ReplicationLagUnknown)

	db.AddQuery("SHOW REPLICA STATUS", &sqltypes.Result{})
	res, err = testMysqld.ReplicationStatus(t.Context())
	require.Error(t, err)
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
	db.AddQuery("SHOW BINARY LOG STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_status"))
	db.AddQuery("SELECT @@global.server_uuid", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_uuid"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	res, err := testMysqld.PrimaryStatus(ctx)
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, "test_uuid", res.ServerUUID)

	db.AddQuery("SHOW MASTER STATUS", &sqltypes.Result{})
	db.AddQuery("SHOW BINARY LOG STATUS", &sqltypes.Result{})
	_, err = testMysqld.PrimaryStatus(ctx)
	assert.ErrorContains(t, err, "no master status")
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

	ctx := t.Context()
	res, err := testMysqld.GetGTIDPurged(ctx)
	require.NoError(t, err)
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

	res, err := testMysqld.PrimaryPosition(t.Context())
	require.NoError(t, err)
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
	db.AddQuery("RESET BINARY LOGS AND GTIDS", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()

	pos := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	pos.GTIDSet = pos.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 1})

	err := testMysqld.SetReplicationPosition(ctx, pos)
	require.Error(t, err)

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
	db.AddQuery("RESET BINARY LOGS AND GTIDS", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()

	// We expect query containing passed host and port to be executed
	err := testMysqld.SetReplicationSource(ctx, "test_host", 2, 0, true, true)
	require.ErrorContains(t, err, `SOURCE_HOST = 'test_host'`)
	require.ErrorContains(t, err, `SOURCE_PORT = 2`)
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

	ctx := t.Context()
	err := testMysqld.ResetReplication(ctx)
	require.ErrorContains(t, err, "RESET REPLICA ALL")

	// We expect this query to be executed
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})
	err = testMysqld.ResetReplication(ctx)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "RESET MASTER") || strings.Contains(err.Error(), "RESET BINARY LOGS AND GTIDS"))

	// We expect this query to be executed
	db.AddQuery("RESET MASTER", &sqltypes.Result{})
	db.AddQuery("RESET BINARY LOGS AND GTIDS", &sqltypes.Result{})
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

	ctx := t.Context()
	err := testMysqld.ResetReplicationParameters(ctx)
	require.ErrorContains(t, err, "RESET REPLICA ALL")

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

	res, err := FindReplicas(t.Context(), fakemysqld)
	require.NoError(t, err)

	want, err := net.LookupHost("localhost")
	require.NoError(t, err)

	assert.Equal(t, want, res)
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
	err := testMysqld.FlushBinaryLogs(t.Context())
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

	res, err := testMysqld.GetBinaryLogs(t.Context())
	require.NoError(t, err)
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

	ctx := t.Context()
	res, err := testMysqld.GetPreviousGTIDs(ctx, "binlog")
	require.NoError(t, err)
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
	err := testMysqld.SetSemiSyncEnabled(t.Context(), true, true)
	require.ErrorIs(t, err, ErrNoSemiSync)

	// We expect this query to be executed
	err = testMysqld.SetSemiSyncEnabled(t.Context(), true, false)
	require.ErrorIs(t, err, ErrNoSemiSync)

	// We expect this query to be executed
	err = testMysqld.SetSemiSyncEnabled(t.Context(), false, true)
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

	p, r := testMysqld.SemiSyncEnabled(t.Context())
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

	p, r := testMysqld.SemiSyncStatus(t.Context())
	assert.True(t, p)
	assert.False(t, r)
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

	res, err := testMysqld.SemiSyncReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, res)

	db.AddQuery("SHOW STATUS LIKE 'rpl_semi_sync_replica_status'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "rpl_semi_sync_replica_status|OFF"))

	res, err = testMysqld.SemiSyncReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, res)
}

func TestSemiSyncExtensionLoaded(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	ctx := t.Context()

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.SemiSyncExtensionLoaded(ctx)
	require.NoError(t, err)
	assert.Contains(t, []mysql.SemiSyncType{mysql.SemiSyncTypeSource, mysql.SemiSyncTypeMaster}, res)

	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", &sqltypes.Result{})

	res, err = testMysqld.SemiSyncExtensionLoaded(ctx)
	require.NoError(t, err)
	assert.Equal(t, mysql.SemiSyncTypeOff, res)
}

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
			sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar"),
			"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL",
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
	db.AddQuery("select @@global.replica_net_timeout", sqltypes.MakeTestResult(sqltypes.MakeTestFields("replica_net_timeout", "int64"), "9"))

	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)
	conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
	require.NoError(t, err)
	conn.Recycle()
	selectOneCalls := db.GetQueryCalledNum("SELECT 1")
	db.ResetQueryLog()

	result, err := testMysqld.CollectFullStatusData(t.Context())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Status)

	status := result.Status
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
	assert.Empty(t, result.SoftErrors)
	assert.Len(t, strings.Split(db.QueryLog(), ";"), 9)
	assert.Equal(t, selectOneCalls+1, db.GetQueryCalledNum("SELECT 1"))

	db.AddQuery("SHOW REPLICA STATUS", &sqltypes.Result{})
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"),
	))
	netTimeoutCalls := db.GetQueryCalledNum("select @@global.replica_net_timeout")
	db.ResetQueryLog()

	result, err = testMysqld.CollectFullStatusData(t.Context())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Status)
	assert.Nil(t, result.Status.ReplicationStatus)
	assert.Nil(t, result.Status.ReplicationConfiguration)
	assert.Equal(t, netTimeoutCalls, db.GetQueryCalledNum("select @@global.replica_net_timeout"))
	assert.Len(t, strings.Split(db.QueryLog(), ";"), 8)
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
			sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar"),
			"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL",
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
	assert.Zero(t, db.GetQueryCalledNum("SELECT @@global.gtid_purged"))
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
		sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar"),
		"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL",
	))

	var connectionClosed atomic.Bool
	db.SetBeforeFunc(variablesQuery, func() {
		if connectionClosed.CompareAndSwap(false, true) {
			db.CloseAllConnections()
		}
	})

	result, err := mysqld.CollectFullStatusData(t.Context())

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2, db.GetQueryCalledNum(variablesQuery))
	assert.Equal(t, uint32(42), result.Status.ServerId)
	assert.Equal(t, "test-uuid", result.Status.ServerUuid)
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

			result, err := mysqld.CollectFullStatusData(t.Context())

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Empty(t, result.SoftErrors)
			assert.Equal(t, 2, db.GetQueryCalledNum(testcase.query))

			status := result.Status
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

	result, err := mysqld.CollectFullStatusData(t.Context())

	require.ErrorContains(t, err, "replication configuration")
	assert.Nil(t, result)
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

	result, err := testMysqld.CollectFullStatusData(t.Context())
	require.ErrorContains(t, err, "failed to read server_uuid")
	assert.Nil(t, result)
	assert.Zero(t, db.GetQueryCalledNum("SHOW REPLICA STATUS"))
}

func TestFakeMysqlDaemonCollectFullStatusData(t *testing.T) {
	mysqld := NewFakeMysqlDaemon(nil)

	result, err := mysqld.CollectFullStatusData(t.Context())
	require.NoError(t, err)
	assert.Nil(t, result)
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

	t.Run("legacy names and malformed optional value", func(t *testing.T) {
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
		db.AddQueryPattern(
			"SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*",
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_master_status|ON",
				"Rpl_semi_sync_slave_status|OFF",
				"Rpl_semi_sync_master_clients|3",
			),
		)

		result := &FullStatusResult{Status: &replicationdatapb.FullStatus{}}
		err := mysqld.collectFullStatusSemiSync(t.Context(), conn, result)
		require.NoError(t, err)

		status := result.Status
		assert.True(t, status.SemiSyncPrimaryEnabled)
		assert.True(t, status.SemiSyncReplicaEnabled)
		assert.Zero(t, status.SemiSyncPrimaryTimeout)
		assert.Equal(t, uint32(2), status.SemiSyncWaitForReplicaCount)
		assert.True(t, status.SemiSyncPrimaryStatus)
		assert.False(t, status.SemiSyncReplicaStatus)
		assert.Equal(t, uint32(3), status.SemiSyncPrimaryClients)
		require.Len(t, result.SoftErrors, 1)
		assert.ErrorContains(t, result.SoftErrors[0], "rpl_semi_sync_master_timeout")
	})

	t.Run("variable query failure does not hide status", func(t *testing.T) {
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

		result := &FullStatusResult{Status: &replicationdatapb.FullStatus{}}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		err := mysqld.collectFullStatusSemiSync(ctx, conn, result)
		require.NoError(t, err)

		status := result.Status
		assert.False(t, status.SemiSyncPrimaryEnabled)
		assert.True(t, status.SemiSyncPrimaryStatus)
		assert.Equal(t, uint32(3), status.SemiSyncPrimaryClients)
		require.Len(t, result.SoftErrors, 1)
		assert.ErrorContains(t, result.SoftErrors[0], "semi-sync variables unavailable")
	})

	t.Run("status query failure retains variables", func(t *testing.T) {
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

		result := &FullStatusResult{Status: &replicationdatapb.FullStatus{}}
		err := mysqld.collectFullStatusSemiSync(t.Context(), conn, result)
		require.NoError(t, err)

		status := result.Status
		assert.True(t, status.SemiSyncPrimaryEnabled)
		assert.False(t, status.SemiSyncPrimaryStatus)
		require.Len(t, result.SoftErrors, 1)
		assert.ErrorContains(t, result.SoftErrors[0], "semi-sync status unavailable")
	})

	t.Run("context cancellation remains fatal", func(t *testing.T) {
		_, mysqld, conn := newTestMysqld(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		result := &FullStatusResult{Status: &replicationdatapb.FullStatus{}}
		err := mysqld.collectFullStatusSemiSync(ctx, conn, result)

		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestSetSuperReadOnlyLockWaitTimeout(t *testing.T) {
	newTestMysqld := func(t *testing.T) (*fakesqldb.DB, *Mysqld) {
		db := fakesqldb.New(t)
		t.Cleanup(db.Close)

		params := db.ConnParams()
		cp := *params
		dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

		db.AddQuery("SELECT 1", &sqltypes.Result{})
		db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.super_read_only", "int64"), "0"))
		db.AddQuery("SET SESSION lock_wait_timeout = 1", &sqltypes.Result{})
		db.AddQuery("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout", &sqltypes.Result{})
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})

		testMysqld := NewMysqld(dbc)
		t.Cleanup(testMysqld.Close)
		return db, testMysqld
	}

	t.Run("applies the session lock_wait_timeout before enabling", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.NoError(t, err)
		assert.NotNil(t, resetFunc)

		queryLog := db.QueryLog()
		setIdx := strings.Index(queryLog, "set session lock_wait_timeout = 1")
		enableIdx := strings.Index(queryLog, "set global super_read_only = 'on'")
		require.NotEqual(t, -1, setIdx, "expected the session lock_wait_timeout to be set, got queries: %s", queryLog)
		require.NotEqual(t, -1, enableIdx, "expected super_read_only to be enabled, got queries: %s", queryLog)
		assert.Less(t, setIdx, enableIdx, "lock_wait_timeout must be set before enabling super_read_only")
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"), "the session lock_wait_timeout must be restored on success")
	})

	t.Run("rounds the timeout up to whole seconds", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddQuery("SET SESSION lock_wait_timeout = 2", &sqltypes.Result{})

		_, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(500*time.Millisecond))
		require.NoError(t, err)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = 1"))

		_, err = testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(1500*time.Millisecond))
		require.NoError(t, err)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = 2"))
	})

	t.Run("default leaves lock_wait_timeout untouched", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true)
		require.NoError(t, err)
		assert.NotNil(t, resetFunc)

		assert.NotContains(t, db.QueryLog(), "lock_wait_timeout")
	})

	t.Run("enabling failure still surfaces the error", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddRejectedQuery("SET GLOBAL super_read_only = 'ON'", sqlerror.NewSQLError(sqlerror.ERLockWaitTimeout, sqlerror.SSUnknownSQLState, "Lock wait timeout exceeded; try restarting transaction"))

		_, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.ErrorContains(t, err, "Lock wait timeout exceeded")
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"), "the session must be restored after a clean statement failure")
	})

	t.Run("reset function does not apply the lock_wait_timeout", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.NoError(t, err)
		require.NotNil(t, resetFunc)

		require.NoError(t, resetFunc())

		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = 1"), "the reset must not bound its lock wait")
	})

	t.Run("unknown lock_wait_timeout proceeds without a bound", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddRejectedQuery("SET SESSION lock_wait_timeout = 1", sqlerror.NewSQLError(sqlerror.ERUnknownSystemVariable, sqlerror.SSUnknownSQLState, "Unknown system variable 'lock_wait_timeout'"))

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.NoError(t, err)
		assert.NotNil(t, resetFunc)

		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
		assert.Equal(t, 0, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"), "must not restore a lock_wait_timeout that was never set")
	})
}
