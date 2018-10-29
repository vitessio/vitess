/*
Copyright 2018 The Vitess Authors.

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
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestEngineOpen(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	ts := createTopo()
	_ = addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)
	_ = newFakeBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	// Test Insert

	vre := NewEngine(ts, testCell, mysqld, dbClientFactory)
	if vre.IsOpen() {
		t.Errorf("IsOpen: %v, want false", vre.IsOpen())
	}

	dbClient.ExpectRequest("select * from _vt.vreplication", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		`1|Running|keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
	), nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer vre.Close()
	if !vre.IsOpen() {
		t.Errorf("IsOpen: %v, want true", vre.IsOpen())
	}

	// Verify stats
	if !reflect.DeepEqual(globalStats.controllers, vre.controllers) {
		t.Errorf("stats are mismatched: %v, wnat %v", globalStats.controllers, vre.controllers)
	}

	ct := vre.controllers[1]
	if ct == nil || ct.id != 1 {
		t.Errorf("ct: %v, id should be 1", ct)
	}
}

func TestEngineExec(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	ts := createTopo()
	_ = addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)
	_ = newFakeBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	// Test Insert

	vre := NewEngine(ts, testCell, mysqld, dbClientFactory)

	dbClient.ExpectRequest("select * from _vt.vreplication", &sqltypes.Result{}, nil)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values (null)", &sqltypes.Result{InsertID: 1}, nil)
	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		`1|Running|keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
	), nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
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
		t.Errorf("stats are mismatched: %v, wnat %v", globalStats.controllers, vre.controllers)
	}

	// Test Update

	savedBlp := ct.blpStats

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("update _vt.vreplication set pos = 'MariaDB/0-1-1084', state = 'Running' where id = 1", testDMLResponse, nil)
	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		`1|Running|keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
	), nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
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
		t.Errorf("stats are mismatched: %v, wnat %v", globalStats.controllers, vre.controllers)
	}

	// Test Delete

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	delQuery := "delete from _vt.vreplication where id = 1"
	dbClient.ExpectRequest(delQuery, testDMLResponse, nil)

	qr, err = vre.Exec(delQuery)
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
}

func TestEngineBadInsert(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	ts := createTopo()
	_ = addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)
	_ = newFakeBinlogClient()

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	vre := NewEngine(ts, testCell, mysqld, dbClientFactory)

	dbClient.ExpectRequest("select * from _vt.vreplication", &sqltypes.Result{}, nil)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values (null)", &sqltypes.Result{}, nil)
	_, err := vre.Exec("insert into _vt.vreplication values(null)")
	want := "insert failed to generate an id"
	if err == nil || err.Error() != want {
		t.Errorf("vre.Exec err: %v, want %v", err, want)
	}

	// Verify stats
	if !reflect.DeepEqual(globalStats.controllers, vre.controllers) {
		t.Errorf("stats are mismatched: %v, wnat %v", globalStats.controllers, vre.controllers)
	}
}

func TestEngineSelect(t *testing.T) {
	ts := createTopo()
	_ = addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)
	_ = newFakeBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	vre := NewEngine(ts, testCell, mysqld, dbClientFactory)

	dbClient.ExpectRequest("select * from _vt.vreplication", &sqltypes.Result{}, nil)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer vre.Close()

	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	wantQuery := "select * from _vt.vreplication where workflow = 'x'"
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source|pos",
			"int64|varchar|varchar|varchar",
		),
		`1|Running|keyspace:"ks" shard:"0" key_range:<end:"\200" > |MariaDB/0-1-1083`,
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
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewEngine(createTopo(), testCell, mysqld, dbClientFactory)

	dbClient.ExpectRequest("select * from _vt.vreplication", &sqltypes.Result{}, nil)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	dbClient.ExpectRequest("select pos from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}}}, nil)
	dbClient.ExpectRequest("select pos from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1084"),
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
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewEngine(createTopo(), testCell, mysqld, dbClientFactory)

	err := vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want := `vreplication engine is closed`
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}

	dbClient.ExpectRequest("select * from _vt.vreplication", &sqltypes.Result{}, nil)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	err = vre.WaitForPos(context.Background(), 1, "BadFlavor/0-1-1084")
	want = `parse error: unknown GTIDSet flavor "BadFlavor"`
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}

	dbClient.ExpectRequest("select pos from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{}}}, nil)
	err = vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want = "unexpected result: &{[] 0 0 [[]] <nil>}"
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}

	dbClient.ExpectRequest("select pos from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}, {
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}}}, nil)
	err = vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want = `unexpected result: &{[] 0 0 [[VARBINARY("MariaDB/0-1-1083")] [VARBINARY("MariaDB/0-1-1083")]] <nil>}`
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}
}

func TestWaitForPosCancel(t *testing.T) {
	dbClient := binlogplayer.NewMockDBClient(t)
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	vre := NewEngine(createTopo(), testCell, mysqld, dbClientFactory)

	dbClient.ExpectRequest("select * from _vt.vreplication", &sqltypes.Result{}, nil)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	dbClient.ExpectRequest("select pos from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}}}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := vre.WaitForPos(ctx, 1, "MariaDB/0-1-1084")
	if err == nil || err != context.Canceled {
		t.Errorf("WaitForPos: %v, want %v", err, context.Canceled)
	}
	dbClient.Wait()

	go func() {
		time.Sleep(5 * time.Millisecond)
		vre.Close()
	}()
	dbClient.ExpectRequest("select pos from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/0-1-1083"),
	}}}, nil)
	err = vre.WaitForPos(context.Background(), 1, "MariaDB/0-1-1084")
	want := "vreplication is closing: context canceled"
	if err == nil || err.Error() != want {
		t.Errorf("WaitForPos: %v, want %v", err, want)
	}
}

func TestCreateDBAndTable(t *testing.T) {
	defer func() { globalStats = &vrStats{} }()

	ts := createTopo()
	_ = addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)
	_ = newFakeBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	// Test Insert

	vre := NewEngine(ts, testCell, mysqld, dbClientFactory)

	notFound := mysql.SQLError{Num: 1146, Message: "not found"}
	dbClient.ExpectRequest("select * from _vt.vreplication", nil, &notFound)
	if err := vre.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer vre.Close()

	dbClient.ExpectRequest("CREATE DATABASE IF NOT EXISTS _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("DROP TABLE IF EXISTS _vt.blp_checkpoint", &sqltypes.Result{}, nil)
	dbClient.ExpectRequestRE("CREATE TABLE IF NOT EXISTS _vt.vreplication.*", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	dbClient.ExpectRequest("insert into _vt.vreplication values (null)", &sqltypes.Result{InsertID: 1}, nil)
	dbClient.ExpectRequest("select * from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|state|source",
			"int64|varchar|varchar",
		),
		`1|Running|keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
	), nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
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
}
