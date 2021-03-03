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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
)

func TestEngineOpen(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)
	require.False(t, vre.IsOpen())

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
	), nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	vre.Open(context.Background())
	defer vre.Close()
	assert.True(t, vre.IsOpen())

	// Verify stats
	assert.Equal(t, globalStats.controllers, vre.controllers)

	ct := vre.controllers[1]
	assert.True(t, ct != nil && ct.id == 1)
}

func TestEngineOpenRetry(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer func(saved time.Duration) { openRetryInterval.Set(saved) }(openRetryInterval.Get())
	openRetryInterval.Set(10 * time.Millisecond)

	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	// Fail twice to ensure the retry retries at least once.
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
	), nil)

	isRetrying := func() bool {
		vre.mu.Lock()
		defer vre.mu.Unlock()
		return vre.cancelRetry != nil
	}

	vre.Open(context.Background())

	assert.True(t, isRetrying())
	func() {
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			if !isRetrying() {
				return
			}
		}
		t.Error("retrying did not become false")
	}()

	// Open is idempotent.
	assert.True(t, vre.IsOpen())
	vre.Open(context.Background())

	vre.Close()
	assert.False(t, vre.IsOpen())

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	vre.Open(context.Background())

	// A second Open should cancel the existing retry and start a new one.
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, errors.New("err"))
	vre.Open(context.Background())

	start := time.Now()
	// Close should cause the retry to exit.
	vre.Close()
	elapsed := time.Since(start)
	assert.Greater(t, int64(openRetryInterval.Get()), int64(elapsed))
}

func TestEngineExec(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}

	// Test Insert

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(context.Background())
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values(null)", &sqltypes.Result{InsertID: 1}, nil)
	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
	), nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	qr, err := vre.Exec("insert into _vt.vreplication values(null)")
	if err != nil {
		t.Fatal(err)
	}
	wantqr := &sqltypes.Result{InsertID: 1}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("Exec: %v, want %v", qr, wantqr)
	}
	dbClient.Wait()

	ct := vre.controllers[1]
	if ct == nil || ct.id != 1 {
		t.Errorf("ct: %v, id should be 1", ct)
	}

	// Verify stats
	if !reflect.DeepEqual(globalStats.controllers, vre.controllers) {
		t.Errorf("stats are mismatched: %v, want %v", globalStats.controllers, vre.controllers)
	}

	// Test Update

	savedBlp := ct.blpStats

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 1", testSelectorResponse1, nil)
	dbClient.ExpectRequest("update _vt.vreplication set pos = 'MariaDB/0-1-1084', state = 'Running' where id in (1)", testDMLResponse, nil)
	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
	), nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	qr, err = vre.Exec("update _vt.vreplication set pos = 'MariaDB/0-1-1084', state = 'Running' where id = 1")
	if err != nil {
		t.Fatal(err)
	}
	wantqr = &sqltypes.Result{RowsAffected: 1}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("Exec: %v, want %v", qr, wantqr)
	}
	dbClient.Wait()

	ct = vre.controllers[1]

	// Verify that the new controller has reused the previous blpStats.
	if ct.blpStats != savedBlp {
		t.Errorf("BlpStats: %v and %v, must be same", ct.blpStats, savedBlp)
	}

	// Verify stats
	if !reflect.DeepEqual(globalStats.controllers, vre.controllers) {
		t.Errorf("stats are mismatched: %v, want %v", globalStats.controllers, vre.controllers)
	}

	// Test no update
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)
	_, err = vre.Exec("update _vt.vreplication set pos = 'MariaDB/0-1-1084', state = 'Running' where id = 2")
	if err != nil {
		t.Fatal(err)
	}
	dbClient.Wait()

	// Test Delete

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 1", testSelectorResponse1, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("delete from _vt.vreplication where id in (1)", testDMLResponse, nil)
	dbClient.ExpectRequest("delete from _vt.copy_state where vrepl_id in (1)", nil, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	qr, err = vre.Exec("delete from _vt.vreplication where id = 1")
	if err != nil {
		t.Fatal(err)
	}
	wantqr = &sqltypes.Result{RowsAffected: 1}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("Exec: %v, want %v", qr, wantqr)
	}
	dbClient.Wait()

	ct = vre.controllers[1]
	if ct != nil {
		t.Errorf("ct: %v, want nil", ct)
	}

	// Verify stats
	if !reflect.DeepEqual(globalStats.controllers, vre.controllers) {
		t.Errorf("stats are mismatched: %v, want %v", globalStats.controllers, vre.controllers)
	}

	// Test Delete of multiple rows

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id > 1", testSelectorResponse2, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("delete from _vt.vreplication where id in (1, 2)", testDMLResponse, nil)
	dbClient.ExpectRequest("delete from _vt.copy_state where vrepl_id in (1, 2)", nil, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	_, err = vre.Exec("delete from _vt.vreplication where id > 1")
	if err != nil {
		t.Fatal(err)
	}
	dbClient.Wait()

	// Test no delete
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("select id from _vt.vreplication where id = 3", &sqltypes.Result{}, nil)
	_, err = vre.Exec("delete from _vt.vreplication where id = 3")
	if err != nil {
		t.Fatal(err)
	}
	dbClient.Wait()
}

func TestEngineBadInsert(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer deleteTablet(addTablet(100))
	resetBinlogClient()

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(context.Background())
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values(null)", &sqltypes.Result{}, nil)
	_, err := vre.Exec("insert into _vt.vreplication values(null)")
	want := "insert failed to generate an id"
	if err == nil || err.Error() != want {
		t.Errorf("vre.Exec err: %v, want %v", err, want)
	}

	// Verify stats
	if !reflect.DeepEqual(globalStats.controllers, vre.controllers) {
		t.Errorf("stats are mismatched: %v, want %v", globalStats.controllers, vre.controllers)
	}
}

func TestEngineSelect(t *testing.T) {
	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(context.Background())
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	wantQuery := "select * from _vt.vreplication where workflow = 'x'"
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source|pos",
			"int64|varchar|varchar|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:<end:"\200" > |MariaDB/0-1-1083`, env.KeyspaceName),
	)
	dbClient.ExpectRequest(wantQuery, wantResult, nil)
	qr, err := vre.Exec(wantQuery)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Exec: %v, want %v", qr, wantResult)
	}
}

func TestWaitForPos(t *testing.T) {
	savedRetryTime := waitRetryTime
	defer func() { waitRetryTime = savedRetryTime }()
	waitRetryTime = 10 * time.Millisecond

	dbClient := binlogplayer.NewMockDBClient(t)
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(context.Background())

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
		sqltypes.NewVarBinary("Running"),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1084"),
		sqltypes.NewVarBinary("Running"),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	start := time.Now()
	if err := vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084"); err != nil {
		t.Fatal(err)
	}
	if duration := time.Since(start); duration < 10*time.Microsecond {
		t.Errorf("duration: %v, want < 10ms", duration)
	}
}

func TestWaitForPosError(t *testing.T) {
	dbClient := binlogplayer.NewMockDBClient(t)
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	err := vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want := `vreplication engine is closed`
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(context.Background())

	err = vre.WaitForPos(context.Background(), 1, "BadFlavor/0-1-1084")
	want = `parse error: unknown GTIDSet flavor "BadFlavor"`
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{}}}, nil)
	err = vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want = "unexpected result: &{[] 0 0 [[]]  0}"
	assert.EqualError(t, err, want, "WaitForPos:")

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}, {
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}}}, nil)
	err = vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want = `unexpected result: &{[] 0 0 [[VARBINARY("MariaDB/0-1-1083")] [VARBINARY("MariaDB/0-1-1083")]]  0}`
	assert.EqualError(t, err, want, "WaitForPos:")
}

func TestWaitForPosCancel(t *testing.T) {
	dbClient := binlogplayer.NewMockDBClient(t)
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	vre.Open(context.Background())

	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
		sqltypes.NewVarBinary("Running"),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := vre.WaitForPos(ctx, 1, "MariaDB/0-1-1084")
	want := "error waiting for pos: MariaDB/0-1-1084, last pos: MariaDB/0-1-1083: context canceled"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("WaitForPos: %v, must contain %v", err, want)
	}
	dbClient.Wait()

	go func() {
		time.Sleep(5 * time.Millisecond)
		vre.Close()
	}()
	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
		sqltypes.NewVarBinary("Running"),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	err = vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want = "vreplication is closing: context canceled"
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}
}

func TestCreateDBAndTable(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	defer deleteTablet(addTablet(100))
	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(3306)}

	// Test Insert

	vre := NewTestEngine(env.TopoServ, env.Cells[0], mysqld, dbClientFactory, dbClient.DBName(), nil)

	tableNotFound := mysql.SQLError{Num: 1146, Message: "table not found"}
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", nil, &tableNotFound)
	vre.Open(context.Background())
	defer vre.Close()

	// Missing db. Statement should get retried after creating everything.
	dbNotFound := mysql.SQLError{Num: 1049, Message: "db not found"}
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, &dbNotFound)

	expectDDLs := func() {
		t.Helper()
		dbClient.ExpectRequest("CREATE DATABASE IF NOT EXISTS _vt", &sqltypes.Result{}, nil)
		dbClient.ExpectRequest("DROP TABLE IF EXISTS _vt.blp_checkpoint", &sqltypes.Result{}, nil)
		dbClient.ExpectRequestRE("CREATE TABLE IF NOT EXISTS _vt.vreplication.*", &sqltypes.Result{}, nil)
		dbClient.ExpectRequestRE("ALTER TABLE _vt.vreplication ADD COLUMN db_name.*", &sqltypes.Result{}, nil)
		dbClient.ExpectRequestRE("ALTER TABLE _vt.vreplication MODIFY source.*", &sqltypes.Result{}, nil)
		dbClient.ExpectRequestRE("ALTER TABLE _vt.vreplication ADD KEY.*", &sqltypes.Result{}, nil)
		dbClient.ExpectRequestRE("create table if not exists _vt.resharding_journal.*", &sqltypes.Result{}, nil)
		dbClient.ExpectRequestRE("create table if not exists _vt.copy_state.*", &sqltypes.Result{}, nil)
	}
	expectDDLs()
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)

	// Non-recoverable error.
	unrecoverableError := &mysql.SQLError{Num: 1234, Message: "random error"}
	dbClient.ExpectRequest("select fail_query from _vt.vreplication", &sqltypes.Result{}, unrecoverableError)

	// Missing table. Statement should get retried after creating everything.
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values(null)", &sqltypes.Result{}, &tableNotFound)
	expectDDLs()
	dbClient.ExpectRequest("insert into _vt.vreplication values(null)", &sqltypes.Result{InsertID: 1}, nil)

	// The rest of this test is normal with no db errors or extra queries.

	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		fmt.Sprintf(`1|Running|keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
	), nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	_, err := vre.Exec("select fail_query from _vt.vreplication")
	if err != unrecoverableError {
		t.Errorf("Want: %v, Got: %v", unrecoverableError, err)
	}

	qr, err := vre.Exec("insert into _vt.vreplication values(null)")
	if err != nil {
		t.Fatal(err)
	}
	wantqr := &sqltypes.Result{InsertID: 1}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("Exec: %v, want %v", qr, wantqr)
	}
	dbClient.Wait()
}
