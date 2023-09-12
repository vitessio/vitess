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
	"errors"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	testSettingsResponse = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pos", Type: sqltypes.VarBinary},
			{Name: "stop_pos", Type: sqltypes.VarBinary},
			{Name: "max_tps", Type: sqltypes.Int64},
			{Name: "max_replication_lag", Type: sqltypes.Int64},
			{Name: "state", Type: sqltypes.VarBinary},
			{Name: "workflow_type", Type: sqltypes.Int64},
			{Name: "workflow", Type: sqltypes.VarChar},
			{Name: "workflow_sub_type", Type: sqltypes.Int64},
			{Name: "defer_secondary_keys", Type: sqltypes.Int64},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"), // pos
				sqltypes.NULL,                          // stop_pos
				sqltypes.NewInt64(9223372036854775807), // max_tps
				sqltypes.NewInt64(9223372036854775807), // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),      // workflow_type
				sqltypes.NewVarChar("wf"), // workflow
				sqltypes.NewInt64(0),      // workflow_sub_type
				sqltypes.NewInt64(0),      // defer_secondary_keys
			},
		},
	}
	testSelectorResponse1 = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}}}
	testSelectorResponse2 = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}}
	testDMLResponse       = &sqltypes.Result{RowsAffected: 1}
	testPos               = "MariaDB/0-1-1083"
)

func TestControllerKeyRange(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)
	params := map[string]string{
		"id":     "1",
		"state":  binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "replica", nil, vre)
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
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)
	resetBinlogClient()

	params := map[string]string{
		"id":     "1",
		"state":  binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" tables:"table1" tables:"/funtables_/" `, env.KeyspaceName),
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{
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
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "replica", nil, vre)
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
	_, err := newController(context.Background(), params, nil, nil, nil, "", "", nil, nil)
	want := `strconv.ParseInt: parsing "bad": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("newController err: %v, want %v", err, want)
	}
}

func TestControllerStopped(t *testing.T) {
	params := map[string]string{
		"id":    "1",
		"state": binlogdatapb.VReplicationWorkflowState_Stopped.String(),
	}

	ct, err := newController(context.Background(), params, nil, nil, nil, "", "", nil, nil)
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
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":           "1",
		"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"cell":         env.Cells[0],
		"tablet_types": "replica",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "rdonly", nil, vre)
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
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":     "1",
		"state":  binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, nil, nil, nil, "", nil)

	ct, err := newController(ctx, params, nil, nil, env.TopoServ, env.Cells[0], "rdonly", nil, vre)
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
	savedDelay := retryDelay
	defer func() { retryDelay = savedDelay }()
	retryDelay = 10 * time.Millisecond

	resetBinlogClient()
	defer deleteTablet(addTablet(100))

	params := map[string]string{
		"id":           "1",
		"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"cell":         env.Cells[0],
		"tablet_types": "replica",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1", nil, errors.New("(expected error)"))
	dbClient.ExpectRequest("update _vt.vreplication set state='Error', message='error (expected error) in selecting vreplication settings select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "rdonly", nil, vre)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop()

	dbClient.Wait()
}

func TestControllerStopPosition(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":     "1",
		"state":  binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source": fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	withStop := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pos", Type: sqltypes.VarBinary},
			{Name: "stop_pos", Type: sqltypes.VarBinary},
			{Name: "max_tps", Type: sqltypes.Int64},
			{Name: "max_replication_lag", Type: sqltypes.Int64},
			{Name: "state", Type: sqltypes.VarBinary},
			{Name: "workflow_type", Type: sqltypes.Int64},
			{Name: "workflow", Type: sqltypes.VarChar},
			{Name: "workflow_sub_type", Type: sqltypes.Int64},
			{Name: "defer_secondary_keys", Type: sqltypes.Int64},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),                                      // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1235"),                                      // stop_pos
				sqltypes.NewInt64(9223372036854775807),                                         // max_tps
				sqltypes.NewInt64(9223372036854775807),                                         // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),                                                           // workflow_type
				sqltypes.NewVarChar("wf"),                                                      // workflow
				sqltypes.NewInt64(1),                                                           // workflow_sub_type
				sqltypes.NewInt64(1),                                                           // defer_secondary_keys
			},
		},
	}
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1", withStop, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='Reached stopping position, done playing logs' where id=1", testDMLResponse, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], "replica", nil, vre)
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
