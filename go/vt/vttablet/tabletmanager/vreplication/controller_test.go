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
	"reflect"
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
	testTPSResponse = &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),    // pos
				sqltypes.NULL,                                // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
			},
		},
	}
	testDMLResponse = &sqltypes.Result{RowsAffected: 1}
	testPos         = "MariaDB/0-1-1083"
)

func TestControllerKeyRange(t *testing.T) {
	ts := createTopo()
	fbc := newFakeBinlogClient()
	wantTablet := addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": `keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
	}

	dbClient := binlogplayer.NewVtClientMock()
	// update state
	dbClient.AddResult(testDMLResponse)
	// select tps
	dbClient.AddResult(testTPSResponse)
	// insert into t
	dbClient.AddResult(testDMLResponse)
	// update _vt.vreplication
	dbClient.AddResult(testDMLResponse)
	dbClient.CommitChannel = make(chan []string, 10)

	dbClientFactory := func() binlogplayer.VtClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, ts, testCell, "replica", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	expectCommit(t, dbClient, []string{
		"UPDATE _vt.vreplication SET state='Running', message='' WHERE id=1",
		"SELECT pos, stop_pos, max_tps, max_replication_lag FROM _vt.vreplication WHERE id=1",
		"BEGIN",
		"insert into t values(1)",
		"UPDATE _vt.vreplication SET pos='MariaDB/0-1-1235', time_updated=",
		"COMMIT",
	})
	expectFBCRequest(t, fbc, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}

func TestControllerTables(t *testing.T) {
	ts := createTopo()
	wantTablet := addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)
	fbc := newFakeBinlogClient()

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": `keyspace:"ks" shard:"0" tables:"table1" tables:"/funtables_/" `,
	}

	dbClient := binlogplayer.NewVtClientMock()
	// update state
	dbClient.AddResult(testDMLResponse)
	// select tps
	dbClient.AddResult(testTPSResponse)
	// insert into t
	dbClient.AddResult(testDMLResponse)
	// update _vt.vreplication
	dbClient.AddResult(testDMLResponse)
	dbClient.CommitChannel = make(chan []string, 10)

	dbClientFactory := func() binlogplayer.VtClient { return dbClient }
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

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, ts, testCell, "replica", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	expectCommit(t, dbClient, []string{
		"UPDATE _vt.vreplication SET state='Running', message='' WHERE id=1",
		"SELECT pos, stop_pos, max_tps, max_replication_lag FROM _vt.vreplication WHERE id=1",
		"BEGIN",
		"insert into t values(1)",
		"UPDATE _vt.vreplication SET pos='MariaDB/0-1-1235', time_updated=",
		"COMMIT",
	})
	expectFBCRequest(t, fbc, wantTablet, testPos, []string{"table1", "funtables_one"}, nil)
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
	ts := createTopo()
	fbc := newFakeBinlogClient()
	wantTablet := addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)

	params := map[string]string{
		"id":           "1",
		"state":        binlogplayer.BlpRunning,
		"source":       `keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
		"cell":         testCell,
		"tablet_types": "replica",
	}

	dbClient := binlogplayer.NewVtClientMock()
	// update state
	dbClient.AddResult(testDMLResponse)
	// select tps
	dbClient.AddResult(testTPSResponse)
	// insert into t
	dbClient.AddResult(testDMLResponse)
	// update _vt.vreplication
	dbClient.AddResult(&sqltypes.Result{RowsAffected: 1})
	dbClient.CommitChannel = make(chan []string, 10)

	dbClientFactory := func() binlogplayer.VtClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, ts, testCell, "rdonly", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	expectCommit(t, dbClient, []string{
		"UPDATE _vt.vreplication SET state='Running', message='' WHERE id=1",
		"SELECT pos, stop_pos, max_tps, max_replication_lag FROM _vt.vreplication WHERE id=1",
		"BEGIN",
		"insert into t values(1)",
		"UPDATE _vt.vreplication SET pos='MariaDB/0-1-1235', time_updated=",
		"COMMIT",
	})
	expectFBCRequest(t, fbc, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}

func TestControllerCanceledContext(t *testing.T) {
	ts := createTopo()
	_ = addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": `keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ct, err := newController(ctx, params, nil, nil, ts, testCell, "rdonly", nil)
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

var badConnFailed = make(chan bool, 10)

type badConn struct {
	binlogplayer.VtClient
}

func (bc *badConn) Connect() error {
	badConnFailed <- true
	return errors.New("err")
}

func TestControllerRetry(t *testing.T) {
	savedDelay := *retryDelay
	defer func() { *retryDelay = savedDelay }()
	*retryDelay = 10 * time.Millisecond

	ts := createTopo()
	_ = newFakeBinlogClient()
	_ = addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)

	params := map[string]string{
		"id":           "1",
		"state":        binlogplayer.BlpRunning,
		"source":       `keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
		"cell":         testCell,
		"tablet_types": "replica",
	}

	dbClient := &badConn{VtClient: binlogplayer.NewVtClientMock()}
	dbClientFactory := func() binlogplayer.VtClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, ts, testCell, "rdonly", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Do it twice to ensure retry happened.
	<-badConnFailed
	<-badConnFailed
	ct.Stop()
}

func TestControllerStopPosition(t *testing.T) {
	ts := createTopo()
	fbc := newFakeBinlogClient()
	wantTablet := addTablet(ts, 100, "0", topodatapb.TabletType_REPLICA, true, true)

	params := map[string]string{
		"id":     "1",
		"state":  binlogplayer.BlpRunning,
		"source": `keyspace:"ks" shard:"0" key_range:<end:"\200" > `,
	}

	dbClient := binlogplayer.NewVtClientMock()
	// update state
	dbClient.AddResult(testDMLResponse)
	// select tps
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
	dbClient.AddResult(withStop)
	// insert into t
	dbClient.AddResult(testDMLResponse)
	// update _vt.vreplication
	dbClient.AddResult(testDMLResponse)
	dbClient.CommitChannel = make(chan []string, 10)

	dbClientFactory := func() binlogplayer.VtClient { return dbClient }
	mysqld := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, ts, testCell, "replica", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	expectCommit(t, dbClient, []string{
		"UPDATE _vt.vreplication SET state='Running', message='' WHERE id=1",
		"SELECT pos, stop_pos, max_tps, max_replication_lag FROM _vt.vreplication WHERE id=1",
		"BEGIN",
		"insert into t values(1)",
		"UPDATE _vt.vreplication SET pos='MariaDB/0-1-1235', time_updated=",
		"COMMIT",
	})

	// Also confirm that replication stopped.
	select {
	case <-ct.done:
	case <-time.After(1 * time.Second):
		t.Errorf("context should be closed, but is not: %v", ct)
	}

	// And verify that the state was changed. This should be checked after the stop to prevent data race in dbClient.
	want := []string{"UPDATE _vt.vreplication SET state='Stopped', message='Reached stopping position, done playing logs' WHERE id=1"}
	if !reflect.DeepEqual(dbClient.Stdout, want) {
		t.Errorf("dbClient.Stdout: %v, want %v", dbClient.Stdout, want)
	}
	expectFBCRequest(t, fbc, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}
