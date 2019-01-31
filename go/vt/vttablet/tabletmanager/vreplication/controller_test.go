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
	"errors"
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	testSettingsResponse = &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"), // pos
				sqltypes.NULL, // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
			},
		},
	}
	testDMLResponse = &sqltypes.Result{RowsAffected: 1}
	testPos         = "MariaDB/0-1-1083"
)

func TestControllerKeyRange(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "replica", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop()
	}()

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}

func TestControllerTables(t *testing.T) {
	wantTablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(wantTablet)
	resetBinlogClient()

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" tables:"table1" tables:"/funtables_/" `, env.KeyspaceName),
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{
		MysqlPort: 3306,
		Schema: &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              "funtables_one",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              "excluded_table",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
			},
		},
	}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "replica", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop()
	}()

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, []string{"table1", "funtables_one"}, nil)
}

func TestControllerBadID(t *testing.T) {
	params := map[string]string{
		"id": "bad",
	}
	_, err := newController(context.Background(), params, nil, nil, nil, "", "", nil)
	want := `strconv.Atoi: parsing "bad": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("newController err: %v, want %v", err, want)
	}
}

func TestControllerStopped(t *testing.T) {
	params := map[string]string{
		"id":    "1",
		"state": binlogplayer.BlpStopped,
	}

	ct, err := newController(context.Background(), params, nil, nil, nil, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	select {
	case <-ct.done:
	default:
		t.Errorf("context should be closed, but is not: %v", ct)
	}
}

func TestControllerOverrides(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":           "1",
		"state":        binlogplayer.BlpRunning,
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
		"cell":         env.Cells[0],
		"tablet_types": "replica",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "rdonly", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop()
	}()

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}

func TestControllerCanceledContext(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ct, err := newController(ctx, params, nil, nil, env.TopoServ, env.Cells[0], "rdonly", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	select {
	case <-ct.done:
	case <-time.After(1 * time.Second):
		t.Errorf("context should be closed, but is not: %v", ct)
	}
}

func TestControllerRetry(t *testing.T) {
	savedDelay := *retryDelay
	defer func() { *retryDelay = savedDelay }()
	*retryDelay = 10 * time.Millisecond

	resetBinlogClient()
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

	params := map[string]string{
		"id":           "1",
		"state":        binlogplayer.BlpRunning,
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
		"cell":         env.Cells[0],
		"tablet_types": "replica",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", nil, errors.New("(expected error)"))
	dbClient.ExpectRequest("update _vt.vreplication set state='Error', message='error (expected error) in selecting vreplication settings select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "rdonly", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	dbClient.Wait()
}

func TestControllerStopPosition(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:<end:"\200" > `, env.KeyspaceName),
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	withStop := &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),    // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1235"),    // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
			},
		},
	}
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=1", withStop, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='Reached stopping position, done playing logs' where id=1", testDMLResponse, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "replica", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop()
	}()

	// Also confirm that replication stopped.
	select {
	case <-ct.done:
	case <-time.After(1 * time.Second):
		t.Errorf("context should be closed, but is not: %v", ct)
	}

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}
