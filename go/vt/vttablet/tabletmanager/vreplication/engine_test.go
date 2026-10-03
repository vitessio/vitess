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

package vreplication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestEngineOpen(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)
	require.False(t, vre.IsOpen())

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source|tablet_types|options",
			"int64|varchar|varchar|varbinary|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:{end:"\x80"}|PRIMARY,REPLICA|{}`, env.KeyspaceName),
	), nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	vre.Open(t.Context())
	defer vre.Close()
	assert.True(t, vre.IsOpen())

	// Verify stats
	assert.Equal(t, globalStats.controllers, vre.controllers)

	ct := vre.controllers[1]
	assert.True(t, ct != nil && ct.id == 1)
}

func TestEngineOpenRetry(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer func(saved int64) { openRetryInterval.Store(saved) }(openRetryInterval.Load())
	openRetryInterval.Store((10 * time.Millisecond).Nanoseconds())

	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	// Fail twice to ensure the retry retries at least once.
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source|options",
			"int64|varchar|varchar|varchar",
		),
	), nil)

	isRetrying := func() bool {
		vre.mu.Lock()
		defer vre.mu.Unlock()
		return vre.cancelRetry != nil
	}

	vre.Open(t.Context())

	assert.True(t, isRetrying())
	func() {
		for range 10 {
			time.Sleep(10 * time.Millisecond)
			if !isRetrying() {
				return
			}
		}
		assert.Fail(t, "retrying did not become false")
	}()

	// Open is idempotent.
	assert.True(t, vre.IsOpen())
	vre.Open(t.Context())

	vre.Close()
	assert.False(t, vre.IsOpen())

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	vre.Open(t.Context())

	// A second Open should cancel the existing retry and start a new one.
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	vre.Open(t.Context())

	start := time.Now()
	// Close should cause the retry to exit.
	vre.Close()
	elapsed := time.Since(start)
	assert.Greater(t, openRetryInterval.Load(), elapsed.Nanoseconds())
}

func TestEngineExec(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	// Test Insert

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(t.Context())
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values(null)", &sqltypes.Result{InsertID: 1}, nil)
	dbClient.ExpectRequest("select @@session.auto_increment_increment", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source|tablet_types|options",
			"int64|varchar|varchar|varbinary|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:{end:"\x80"}|PRIMARY,REPLICA|{}`, env.KeyspaceName),
	), nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	qr, err := vre.Exec("insert into _vt.vreplication values(null)")
	require.NoError(t, err)
	wantqr := &sqltypes.Result{InsertID: 1}
	assert.Truef(t, qr.Equal(wantqr), "Exec: %v, want %v", qr, wantqr)
	dbClient.Wait()

	ct := vre.controllers[1]
	if ct == nil || ct.id != 1 {
		assert.Failf(t, "controller mismatch", "ct: %v, id should be 1", ct)
		return
	}

	// Verify stats
	assert.Equalf(t, vre.controllers, globalStats.controllers, "stats are mismatched")

	// Test Update

	savedBlp := ct.blpStats

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 1", testSelectorResponse1, nil)
	dbClient.ExpectRequest("update _vt.vreplication set pos = 'MariaDB/0-1-1084', state = 'Running' where id in (1)", testDMLResponse, nil)
	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source|options",
			"int64|varchar|varchar|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:{end:"\x80"}|{}`, env.KeyspaceName),
	), nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	qr, err = vre.Exec("update _vt.vreplication set pos = 'MariaDB/0-1-1084', state = 'Running' where id = 1")
	require.NoError(t, err)
	wantqr = &sqltypes.Result{RowsAffected: 1}
	assert.Truef(t, qr.Equal(wantqr), "Exec: %v, want %v", qr, wantqr)
	dbClient.Wait()

	ct = vre.controllers[1]

	// Verify that the new controller has reused the previous blpStats.
	assert.Samef(t, savedBlp, ct.blpStats, "BlpStats must be same")

	// Verify stats
	assert.Equalf(t, vre.controllers, globalStats.controllers, "stats are mismatched")

	// Test no update
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)
	_, err = vre.Exec("update _vt.vreplication set pos = 'MariaDB/0-1-1084', state = 'Running' where id = 2")
	require.NoError(t, err)
	dbClient.Wait()

	// Test Delete

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 1", testSelectorResponse1, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("delete from _vt.vreplication where id in (1)", testDMLResponse, nil)
	dbClient.ExpectRequest("delete from _vt.copy_state where vrepl_id in (1)", nil, nil)
	dbClient.ExpectRequest("delete from _vt.post_copy_action where vrepl_id in (1)", nil, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	qr, err = vre.Exec("delete from _vt.vreplication where id = 1")
	require.NoError(t, err)
	wantqr = &sqltypes.Result{RowsAffected: 1}
	assert.Truef(t, qr.Equal(wantqr), "Exec: %v, want %v", qr, wantqr)
	dbClient.Wait()

	ct = vre.controllers[1]
	assert.Nilf(t, ct, "ct: %v, want nil", ct)

	// Verify stats
	assert.Equalf(t, vre.controllers, globalStats.controllers, "stats are mismatched")

	// Test simple delete.
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 3", &sqltypes.Result{}, nil)
	_, err = vre.Exec("delete from _vt.vreplication where id = 3")
	require.NoError(t, err)
	dbClient.Wait()

	// Test unsafe writes of multiple rows, which we want to prevent.
	unsafeQueries := []string{
		"delete from _vt.vreplication",
		"delete from _vt.vreplication where id > 1",
		"delete from _vt.vreplication where message != 'FROZEN'",
		"update _vt.vreplication set workflow = 'bad'",
		"update _vt.vreplication set state = 'Stopped' where id > 1",
		"update _vt.vreplication set message = '' where state == 'Running'",
	}
	for _, unsafeQuery := range unsafeQueries {
		_, err = vre.Exec(unsafeQuery)
		require.Error(t, err, "%s should fail", unsafeQuery)
		dbClient.Wait()
	}
}

func TestEngineBadInsert(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer deleteTablet(addTablet(100))
	resetBinlogClient()

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(t.Context())
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values(null)", &sqltypes.Result{}, nil)
	_, err := vre.Exec("insert into _vt.vreplication values(null)")
	require.EqualError(t, err, "insert failed to generate an id", "vre.Exec")

	// Verify stats
	assert.Equalf(t, vre.controllers, globalStats.controllers, "stats are mismatched")
}

func TestEngineSelect(t *testing.T) {
	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(t.Context())
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	wantQuery := "select * from _vt.vreplication where workflow = 'x'"
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source|pos",
			"int64|varchar|varchar|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:{end:"\x80"}|MariaDB/0-1-1083`, env.KeyspaceName),
	)
	dbClient.ExpectRequest(wantQuery, wantResult, nil)
	qr, err := vre.Exec(wantQuery)
	require.NoError(t, err)
	assert.Truef(t, qr.Equal(wantResult), "Exec: %v, want %v", qr, wantResult)
}

func TestWaitForPos(t *testing.T) {
	savedRetryTime := waitRetryTime
	defer func() { waitRetryTime = savedRetryTime }()
	waitRetryTime = 10 * time.Millisecond

	dbClient := binlogplayer.NewMockDBClient(t)
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(t.Context())

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
		sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1084"),
		sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	start := time.Now()
	require.NoError(t, vre.WaitForPos(t.Context(), 1, "MariaDB/0-1-1084"))
	duration := time.Since(start)
	assert.GreaterOrEqualf(t, duration, 10*time.Microsecond, "duration: %v, want >= 10us", duration)
}

func TestWaitForPosError(t *testing.T) {
	dbClient := binlogplayer.NewMockDBClient(t)
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	err := vre.WaitForPos(t.Context(), 1, "MariaDB/0-1-1084")
	require.EqualError(t, err, `vreplication engine is closed`, "WaitForPos")

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(t.Context())

	err = vre.WaitForPos(t.Context(), 1, "BadFlavor/0-1-1084")
	require.EqualError(t, err, `parse error: unknown GTIDSet flavor "BadFlavor"`, "WaitForPos")

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{}}}, nil)
	err = vre.WaitForPos(t.Context(), 1, "MariaDB/0-1-1084")
	require.EqualError(t, err, "vreplication stream received an unexpected number of columns, got 0 instead of 3", "WaitForPos")

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}, {
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}}}, nil)
	err = vre.WaitForPos(t.Context(), 1, "MariaDB/0-1-1084")
	assert.EqualError(t, err, "vreplication stream received more rows than expected, got 2 instead of 1", "WaitForPos")
}

func TestWaitForPosCancel(t *testing.T) {
	dbClient := binlogplayer.NewMockDBClient(t)
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(t.Context())

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
		sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := vre.WaitForPos(ctx, 1, "MariaDB/0-1-1084")
	require.ErrorContains(t, err, "error waiting for pos: MariaDB/0-1-1084, last pos: MariaDB/0-1-1083: context canceled", "WaitForPos")
	dbClient.Wait()

	go func() {
		time.Sleep(5 * time.Millisecond)
		vre.Close()
	}()
	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
		sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	err = vre.WaitForPos(t.Context(), 1, "MariaDB/0-1-1084")
	assert.EqualError(t, err, "vreplication is closing: context canceled", "WaitForPos")
}

func TestGetDBClient(t *testing.T) {
	dbClientDba := binlogplayer.NewMockDbaClient(t)
	dbClientFiltered := binlogplayer.NewMockDBClient(t)
	dbClientFactoryDba := func() binlogplayer.DBClient { return dbClientDba }
	dbClientFactoryFiltered := func() binlogplayer.DBClient { return dbClientFiltered }

	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactoryFiltered, dbClientFactoryDba, dbClientDba.DBName(), nil)

	shouldBeDbaClient := vre.getDBClient(true /*runAsAdmin*/)
	assert.Equal(t, shouldBeDbaClient, dbClientDba)

	shouldBeFilteredClient := vre.getDBClient(false /*runAsAdmin*/)
	assert.Equal(t, shouldBeFilteredClient, dbClientFiltered)
}

// failingDbaDaemon simulates DBA connections being unavailable, which makes
// the KILL of the connection executing a stream's post copy actions fail.
// attempted is closed on the first attempt.
type failingDbaDaemon struct {
	mysqlctl.MysqlDaemon
	attempted chan struct{}
	once      sync.Once
}

func (d *failingDbaDaemon) GetDbaConnection(ctx context.Context) (*dbconnpool.DBConnection, error) {
	d.once.Do(func() { close(d.attempted) })
	return nil, errors.New("dba connections are unavailable")
}

// TestExecStopWhilePostCopyActionRunning drives a stream stop and a stream
// delete -- what Workflow stop and Workflow delete issue to the tablet --
// through the engine while the stream's controller is executing a post copy
// action: the ALTER TABLE which re-adds deferred secondary keys and can run
// for hours. The engine stops the controller while holding its lock, so the
// operation, and with it every other vreplication operation on the tablet,
// only completes once the controller has stopped. The operation must
// therefore complete promptly, whether the KILL of the connection executing
// the action succeeds or fails; in the latter case the action runs on as an
// orphan and is reconciled when the stream is restarted.
func TestExecStopWhilePostCopyActionRunning(t *testing.T) {
	ctx := t.Context()
	defer deleteTablet(addTablet(100))

	// Like execStatements, but bound to this test's context rather than
	// the calling subtest's, which is done by the time its cleanups run.
	execSuper := func(t *testing.T, queries ...string) {
		t.Helper()
		require.NoError(t, env.Mysqld.ExecuteSuperQueryList(ctx, queries))
		for _, query := range queries {
			updateMockSchemaForQuery(query)
		}
	}
	execSuper(t,
		"create table src1(id int, val varbinary(128), primary key(id))",
		"insert into src1 values(1, 'aaa'), (2, 'bbb')",
	)
	defer execSuper(t, "drop table src1")

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "dst1",
				Filter: "select * from src1",
			}},
		},
	}
	alter := fmt.Sprintf("alter table %s.dst1 add key val (val)", vrepldb)
	action, err := json.Marshal(PostCopyAction{Type: PostCopyActionSQL, Task: alter})
	require.NoError(t, err)

	// superQueryCount returns the count that the query selects, or -1 on
	// error, so that it can be polled from Eventually conditions.
	superQueryCount := func(query string) int64 {
		qr, err := env.Mysqld.FetchSuperQuery(ctx, query)
		if err != nil || len(qr.Rows) != 1 {
			return -1
		}
		n, err := qr.Rows[0][0].ToInt64()
		if err != nil {
			return -1
		}
		return n
	}
	alterRunning := func() bool {
		return superQueryCount(fmt.Sprintf("select count(*) from information_schema.processlist where info = %s", encodeString(alter))) == 1
	}
	alterBlocked := func() bool {
		return superQueryCount(fmt.Sprintf("select count(*) from information_schema.processlist where info = %s and state = 'Waiting for table metadata lock'", encodeString(alter))) == 1
	}
	hasKey := func() bool {
		return superQueryCount(fmt.Sprintf("select count(*) from information_schema.statistics where table_schema = '%s' and table_name = 'dst1' and index_name = 'val'", vrepldb)) > 0
	}
	postCopyActions := func(id int32) int64 {
		return superQueryCount(fmt.Sprintf("select count(*) from _vt.post_copy_action where vrepl_id = %d", id))
	}
	streamState := func(id int32) string {
		qr, err := env.Mysqld.FetchSuperQuery(ctx, fmt.Sprintf("select state from _vt.vreplication where id = %d", id))
		if err != nil || len(qr.Rows) == 0 {
			return ""
		}
		return qr.Rows[0][0].ToString()
	}
	controller := func(id int32) *controller {
		playerEngine.mu.Lock()
		defer playerEngine.mu.Unlock()
		return playerEngine.controllers[id]
	}
	controllerStopped := func(id int32) bool {
		ct := controller(id)
		if ct == nil {
			return false
		}
		select {
		case <-ct.done:
			return true
		default:
			return false
		}
	}

	// startStream creates and starts a stream whose copy of dst1 is followed
	// by the ALTER, and returns once the ALTER is running but blocked. The
	// returned connection holds the lock which blocks it until it ends its
	// transaction or is closed.
	startStream := func(t *testing.T) (int32, *dbconnpool.DBConnection) {
		execSuper(t, fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), primary key(id))", vrepldb))
		t.Cleanup(func() { execSuper(t, fmt.Sprintf("drop table %s.dst1", vrepldb)) })
		// The stream's queries are captured for the tests which assert on
		// them; this one doesn't, so drain them once done.
		t.Cleanup(drainDBQueries)

		// Create the stream stopped so that the post copy action can be
		// recorded, for the stream's id, before the copy of dst1 completes
		// and the actions run. Deferring secondary keys would record the
		// same action; recording it directly keeps the ALTER that drops the
		// keys before the copy out of the way.
		qr, err := playerEngine.Exec(binlogplayer.CreateVReplicationState("test", bls, "", binlogdatapb.VReplicationWorkflowState_Stopped, vrepldb, 0, 0))
		require.NoError(t, err)
		id := int32(qr.InsertID)
		t.Cleanup(func() {
			// A no-op when the test deleted the stream.
			_, err := playerEngine.Exec(fmt.Sprintf("delete from _vt.vreplication where id = %d", id))
			require.NoError(t, err)
		})
		insert, err := sqlparser.ParseAndBind(sqlCreatePostCopyAction, sqltypes.Int32BindVariable(id),
			sqltypes.StringBindVariable("dst1"), sqltypes.StringBindVariable(string(action)))
		require.NoError(t, err)
		require.NoError(t, env.Mysqld.ExecuteSuperQuery(ctx, insert))

		// Block the ALTER, but not the copy's inserts, with the shared
		// metadata lock that an open transaction which has read the table
		// holds until it ends: the ALTER waits for its exclusive lock.
		blocker, err := env.Mysqld.GetDbaConnection(ctx)
		require.NoError(t, err)
		t.Cleanup(blocker.Close)
		_, err = blocker.ExecuteFetch("begin", 1, false)
		require.NoError(t, err)
		_, err = blocker.ExecuteFetch(fmt.Sprintf("select * from %s.dst1", vrepldb), 10, false)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = blocker.ExecuteFetch("rollback", 1, false) })

		_, err = playerEngine.Exec(fmt.Sprintf("update _vt.vreplication set state = 'Running' where id = %d", id))
		require.NoError(t, err)
		require.Eventually(t, alterBlocked, 30*time.Second, 50*time.Millisecond, "the post copy ALTER did not start")
		return id, blocker
	}

	// exec runs the query through the engine in the background and returns
	// its error once it completes, failing the test if it does not complete
	// within the timeout: the engine lock is held meanwhile, so a blocked
	// operation would otherwise hang the test.
	exec := func(t *testing.T, query string, timeout time.Duration) error {
		t.Helper()
		errCh := make(chan error, 1)
		go func() {
			_, err := playerEngine.Exec(query)
			errCh <- err
		}()
		var err error
		require.Eventually(t, func() bool {
			select {
			case err = <-errCh:
				return true
			default:
				return false
			}
		}, timeout, 10*time.Millisecond, "the operation did not complete: %s", query)
		return err
	}

	for _, tc := range []struct {
		name  string
		query string // with a %d for the stream id
		// wantState is the stream's expected state afterwards; "" for no
		// stream. wantActions is the expected number of post copy action
		// records, and wantController whether a (stopped) controller is
		// expected to remain.
		wantState      string
		wantActions    int64
		wantController bool
	}{
		{
			name:  "stop",
			query: "update _vt.vreplication set state = 'Stopped' where id = %d",
			// The action remains recorded, to be retried when the stream
			// is restarted.
			wantState:      "Stopped",
			wantActions:    1,
			wantController: true,
		},
		{
			name:  "delete",
			query: "delete from _vt.vreplication where id = %d",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, _ := startStream(t)

			// The operation completes promptly -- the engine kills the
			// connection executing the action -- rather than waiting for
			// the ALTER to complete.
			require.NoError(t, exec(t, fmt.Sprintf(tc.query, id), 30*time.Second))

			// The killed ALTER did not add the key.
			require.Eventually(t, func() bool { return !alterRunning() }, 30*time.Second, 50*time.Millisecond)
			assert.False(t, hasKey())
			assert.Equal(t, tc.wantState, streamState(id))
			assert.Equal(t, tc.wantActions, postCopyActions(id))
			if !tc.wantController {
				assert.Nil(t, controller(id))
				return
			}
			assert.True(t, controllerStopped(id), "the stream's controller is missing or still running")
		})
	}

	t.Run("stop with failed kill", func(t *testing.T) {
		// Make the kill fail. The daemon is captured by the controllers
		// created from here on.
		daemon := &failingDbaDaemon{MysqlDaemon: playerEngine.mysqld, attempted: make(chan struct{})}
		setMysqld := func(mysqld mysqlctl.MysqlDaemon) {
			playerEngine.mu.Lock()
			defer playerEngine.mu.Unlock()
			playerEngine.mysqld = mysqld
		}
		setMysqld(daemon)
		t.Cleanup(func() { setMysqld(daemon.MysqlDaemon) })

		id, blocker := startStream(t)

		// The stop still completes promptly: the kill fails and the
		// connection executing the action is abandoned instead.
		require.NoError(t, exec(t, fmt.Sprintf("update _vt.vreplication set state = 'Stopped' where id = %d", id), 30*time.Second))
		select {
		case <-daemon.attempted:
		default:
			t.Fatal("the kill was never attempted")
		}
		assert.Equal(t, "Stopped", streamState(id))
		assert.Equal(t, int64(1), postCopyActions(id))
		assert.True(t, controllerStopped(id), "the stream's controller is missing or still running")

		// The ALTER runs on as an orphan, still blocked...
		assert.True(t, alterBlocked(), "the orphaned ALTER should still be running")

		// ...and completes once unblocked.
		_, err = blocker.ExecuteFetch("rollback", 1, false)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return hasKey() && !alterRunning() }, 30*time.Second, 50*time.Millisecond, "the orphaned ALTER did not complete")

		// Restarting the stream re-runs the action, which finds the key in
		// place and reconciles, and the stream completes its copy.
		require.NoError(t, exec(t, fmt.Sprintf("update _vt.vreplication set state = 'Running' where id = %d", id), 30*time.Second))
		require.Eventually(t, func() bool {
			return postCopyActions(id) == 0 && streamState(id) == "Running"
		}, 30*time.Second, 50*time.Millisecond, "the restarted stream did not reconcile the action and start running")

		require.NoError(t, exec(t, fmt.Sprintf("update _vt.vreplication set state = 'Stopped' where id = %d", id), 30*time.Second))
		assert.Equal(t, "Stopped", streamState(id))
	})
}
