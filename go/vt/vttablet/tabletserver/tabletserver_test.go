/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestTabletServerGetState(t *testing.T) {
	states := []int64{
		StateNotConnected,
		StateNotServing,
		StateServing,
		StateTransitioning,
		StateShuttingDown,
	}
	// Don't reuse stateName.
	names := []string{
		"NOT_SERVING",
		"NOT_SERVING",
		"SERVING",
		"NOT_SERVING",
		"SHUTTING_DOWN",
	}
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	for i, state := range states {
		tsv.setState(state)
		if stateName := tsv.GetState(); stateName != names[i] {
			t.Errorf("GetState: %s, want %s", stateName, names[i])
		}
	}
	tsv.EnterLameduck()
	if stateName := tsv.GetState(); stateName != "NOT_SERVING" {
		t.Errorf("GetState: %s, want NOT_SERVING", stateName)
	}
}

func TestTabletServerAllowQueriesFailBadConn(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	db.EnableConnFail()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	checkTabletServerState(t, tsv, StateNotConnected)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err == nil {
		t.Fatalf("TabletServer.StartService should fail")
	}
	checkTabletServerState(t, tsv, StateNotConnected)
}

func TestTabletServerAllowQueries(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	checkTabletServerState(t, tsv, StateNotConnected)
	dbcfgs := testUtils.newDBConfigs(db)
	tsv.setState(StateServing)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	tsv.StopService()
	want := "InitDBConfig failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("TabletServer.StartService: %v, must contain %s", err, want)
	}
	tsv.setState(StateShuttingDown)
	err = tsv.StartService(target, dbcfgs)
	if err == nil {
		t.Fatalf("TabletServer.StartService should fail")
	}
	tsv.StopService()
}

func TestTabletServerInitDBConfig(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	tsv.setState(StateServing)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	dbcfgs := testUtils.newDBConfigs(db)
	err := tsv.InitDBConfig(target, dbcfgs)
	want := "InitDBConfig failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("InitDBConfig: %v, must contain %s", err, want)
	}
	tsv.setState(StateNotConnected)
	err = tsv.InitDBConfig(target, dbcfgs)
	if err != nil {
		t.Error(err)
	}
}

func TestDecideAction(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	dbcfgs := testUtils.newDBConfigs(db)
	err := tsv.InitDBConfig(target, dbcfgs)
	if err != nil {
		t.Error(err)
	}

	tsv.setState(StateNotConnected)
	action, err := tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	if err != nil {
		t.Error(err)
	}
	if action != actionNone {
		t.Errorf("decideAction: %v, want %v", action, actionNone)
	}

	tsv.setState(StateNotConnected)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, true, nil)
	if err != nil {
		t.Error(err)
	}
	if action != actionFullStart {
		t.Errorf("decideAction: %v, want %v", action, actionFullStart)
	}
	if tsv.state != StateTransitioning {
		t.Errorf("tsv.state: %v, want %v", tsv.state, StateTransitioning)
	}

	tsv.setState(StateNotServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	if err != nil {
		t.Error(err)
	}
	if action != actionNone {
		t.Errorf("decideAction: %v, want %v", action, actionNone)
	}

	tsv.setState(StateNotServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, true, nil)
	if err != nil {
		t.Error(err)
	}
	if action != actionServeNewType {
		t.Errorf("decideAction: %v, want %v", action, actionServeNewType)
	}
	if tsv.state != StateTransitioning {
		t.Errorf("tsv.state: %v, want %v", tsv.state, StateTransitioning)
	}

	tsv.setState(StateServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	if err != nil {
		t.Error(err)
	}
	if action != actionGracefulStop {
		t.Errorf("decideAction: %v, want %v", action, actionGracefulStop)
	}
	if tsv.state != StateShuttingDown {
		t.Errorf("tsv.state: %v, want %v", tsv.state, StateShuttingDown)
	}

	tsv.setState(StateServing)
	action, err = tsv.decideAction(topodatapb.TabletType_REPLICA, true, nil)
	if err != nil {
		t.Error(err)
	}
	if action != actionServeNewType {
		t.Errorf("decideAction: %v, want %v", action, actionServeNewType)
	}
	if tsv.state != StateTransitioning {
		t.Errorf("tsv.state: %v, want %v", tsv.state, StateTransitioning)
	}
	tsv.target.TabletType = topodatapb.TabletType_MASTER

	tsv.setState(StateServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, true, nil)
	if err != nil {
		t.Error(err)
	}
	if action != actionNone {
		t.Errorf("decideAction: %v, want %v", action, actionNone)
	}
	if tsv.state != StateServing {
		t.Errorf("tsv.state: %v, want %v", tsv.state, StateServing)
	}

	tsv.setState(StateTransitioning)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	want := "cannot SetServingType"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("decideAction: %v, must contain %s", err, want)
	}

	tsv.setState(StateShuttingDown)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	want = "cannot SetServingType"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("decideAction: %v, must contain %s", err, want)
	}
}

func TestSetServingType(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.InitDBConfig(target, dbcfgs)
	if err != nil {
		t.Error(err)
	}

	stateChanged, err := tsv.SetServingType(topodatapb.TabletType_REPLICA, false, nil)
	if stateChanged != false {
		t.Errorf("SetServingType() should NOT have changed the QueryService state, but did")
	}
	if err != nil {
		t.Error(err)
	}
	checkTabletServerState(t, tsv, StateNotConnected)

	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	if stateChanged != true {
		t.Errorf("SetServingType() should have changed the QueryService state, but did not")
	}
	if err != nil {
		t.Error(err)
	}
	checkTabletServerState(t, tsv, StateServing)

	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_RDONLY, true, nil)
	if stateChanged != true {
		t.Errorf("SetServingType() should have changed the tablet type, but did not")
	}
	if err != nil {
		t.Error(err)
	}
	checkTabletServerState(t, tsv, StateServing)

	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_SPARE, false, nil)
	if stateChanged != true {
		t.Errorf("SetServingType() should have changed the QueryService state, but did not")
	}
	if err != nil {
		t.Error(err)
	}
	checkTabletServerState(t, tsv, StateNotServing)

	// Verify that we exit lameduck when SetServingType is called.
	tsv.EnterLameduck()
	if stateName := tsv.GetState(); stateName != "NOT_SERVING" {
		t.Errorf("GetState: %s, want NOT_SERVING", stateName)
	}
	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	if stateChanged != true {
		t.Errorf("SetServingType() should have changed the QueryService state, but did not")
	}
	if err != nil {
		t.Error(err)
	}
	checkTabletServerState(t, tsv, StateServing)
	if stateName := tsv.GetState(); stateName != "SERVING" {
		t.Errorf("GetState: %s, want SERVING", stateName)
	}

	tsv.StopService()
	checkTabletServerState(t, tsv, StateNotConnected)
}

func TestTabletServerSingleSchemaFailure(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()

	want := &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 2,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table", false, ""),
			// Return a table that tabletserver can't access (the mock will reject all queries to it).
			mysql.BaseShowTablesRow("rejected_table", false, ""),
		},
	}
	db.AddQuery(mysql.BaseShowTables, want)

	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	originalSchemaErrorCount := tabletenv.InternalErrors.Counts()["Schema"]
	err := tsv.StartService(target, dbcfgs)
	defer tsv.StopService()
	if err != nil {
		t.Fatalf("TabletServer should successfully start even if a table's schema is unloadable, but got error: %v", err)
	}
	newSchemaErrorCount := tabletenv.InternalErrors.Counts()["Schema"]
	schemaErrorDiff := newSchemaErrorCount - originalSchemaErrorCount
	if schemaErrorDiff != 1 {
		t.Errorf("InternalErrors.Schema counter should have increased by 1, instead got %v", schemaErrorDiff)
	}
}

func TestTabletServerAllSchemaFailure(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	// Return only tables that tabletserver can't access (the mock will reject all queries to them).
	want := &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 2,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("rejected_table_1", false, ""),
			mysql.BaseShowTablesRow("rejected_table_2", false, ""),
		},
	}
	db.AddQuery(mysql.BaseShowTables, want)

	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	defer tsv.StopService()
	// tabletsever shouldn't start if it can't access schema for any tables
	wantErr := "could not get schema for any tables"
	if err == nil || err.Error() != wantErr {
		t.Errorf("tsv.StartService: %v, want %s", err, wantErr)
	}
}

func TestTabletServerCheckMysql(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	defer tsv.StopService()
	if err != nil {
		t.Fatal(err)
	}
	if !tsv.isMySQLReachable() {
		t.Error("isMySQLReachable should return true")
	}
	stateChanged, err := tsv.SetServingType(topodatapb.TabletType_SPARE, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stateChanged != true {
		t.Errorf("SetServingType() should have changed the QueryService state, but did not")
	}
	if !tsv.isMySQLReachable() {
		t.Error("isMySQLReachable should return true")
	}
	checkTabletServerState(t, tsv, StateNotServing)
}

func TestTabletServerCheckMysqlFailInvalidConn(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	defer tsv.StopService()
	if err != nil {
		t.Fatalf("TabletServer.StartService should succeed, but got error: %v", err)
	}
	// make mysql conn fail
	db.Close()
	if tsv.isMySQLReachable() {
		t.Fatalf("isMySQLReachable should return false")
	}
}

func TestTabletServerReconnect(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	query := "select addr from test_table where pk = 1 limit 1000"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery("select addr from test_table where 1 != 1", &sqltypes.Result{})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	defer tsv.StopService()

	if tsv.GetState() != "SERVING" {
		t.Errorf("GetState: %s, must be SERVING", tsv.GetState())
	}
	if err != nil {
		t.Fatalf("TabletServer.StartService should success but get error: %v", err)
	}
	_, err = tsv.Execute(context.Background(), &target, query, nil, 0, nil)
	if err != nil {
		t.Error(err)
	}

	// make mysql conn fail
	db.Close()
	_, err = tsv.Execute(context.Background(), &target, query, nil, 0, nil)
	if err == nil {
		t.Error("Execute: want error, got nil")
	}
	time.Sleep(50 * time.Millisecond)
	if tsv.GetState() == "SERVING" {
		t.Error("GetState is still SERVING, must be NOT_SERVING")
	}

	// make mysql conn work
	db = setUpTabletServerTest(t)
	db.AddQuery(query, want)
	db.AddQuery("select addr from test_table where 1 != 1", &sqltypes.Result{})
	dbcfgs = testUtils.newDBConfigs(db)
	err = tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Error(err)
	}
	_, err = tsv.Execute(context.Background(), &target, query, nil, 0, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestTabletServerTarget(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target1 := querypb.Target{
		Keyspace:   "test_keyspace",
		Shard:      "test_shard",
		TabletType: topodatapb.TabletType_MASTER,
	}
	err := tsv.StartService(target1, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()

	// query that works
	db.AddQuery("select * from test_table limit 1000", &sqltypes.Result{})
	_, err = tsv.Execute(ctx, &target1, "select * from test_table limit 1000", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// wrong tablet type
	target2 := proto.Clone(&target1).(*querypb.Target)
	target2.TabletType = topodatapb.TabletType_REPLICA
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	want := "invalid tablet type"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// set expected target type to MASTER, but also accept REPLICA
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, []topodatapb.TabletType{topodatapb.TabletType_REPLICA})
	_, err = tsv.Execute(ctx, &target1, "select * from test_table limit 1000", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// wrong keyspace
	target2 = proto.Clone(&target1).(*querypb.Target)
	target2.Keyspace = "bad"
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	want = "invalid keyspace bad"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// wrong shard
	target2 = proto.Clone(&target1).(*querypb.Target)
	target2.Shard = "bad"
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	want = "invalid shard bad"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// no target
	_, err = tsv.Execute(ctx, nil, "select * from test_table limit 1000", nil, 0, nil)
	want = "No target"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// Disallow all if service is stopped.
	tsv.StopService()
	_, err = tsv.Execute(ctx, &target1, "select * from test_table limit 1000", nil, 0, nil)
	want = "operation not allowed in state NOT_SERVING"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
}

func TestBeginOnReplica(t *testing.T) {
	db := setUpTabletServerTest(t)
	db.AddQuery("set transaction isolation level REPEATABLE READ", &sqltypes.Result{})
	db.AddQuery("start transaction with consistent snapshot, read only", &sqltypes.Result{})
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target1 := querypb.Target{
		Keyspace:   "test_keyspace",
		Shard:      "test_shard",
		TabletType: topodatapb.TabletType_REPLICA,
	}
	err := tsv.StartService(target1, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	ctx := context.Background()
	options := querypb.ExecuteOptions{
		TransactionIsolation: querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY,
	}
	txID, err := tsv.Begin(ctx, &target1, &options)

	if err != nil {
		t.Errorf("err: %v, failed to create read only tx on replica", err)
	}

	err = tsv.Rollback(ctx, &target1, txID)
	if err != nil {
		t.Errorf("err: %v, failed to rollback read only tx", err)
	}

	// test that RW transactions are refused
	options = querypb.ExecuteOptions{
		TransactionIsolation: querypb.ExecuteOptions_DEFAULT,
	}
	_, err = tsv.Begin(ctx, &target1, &options)

	if err == nil {
		t.Error("expected write tx to be refused")
	}
}

func TestTabletServerMasterToReplica(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	txid1, err := tsv.Begin(ctx, &target, nil)
	if err != nil {
		t.Error(err)
	}
	if _, err := tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, txid1, nil); err != nil {
		t.Error(err)
	}
	if err = tsv.Prepare(ctx, &target, txid1, "aa"); err != nil {
		t.Error(err)
	}
	txid2, err := tsv.Begin(ctx, &target, nil)
	if err != nil {
		t.Error(err)
	}
	// This makes txid2 busy
	conn2, err := tsv.te.txPool.Get(txid2, "for query")
	if err != nil {
		t.Error(err)
	}
	ch := make(chan bool)
	go func() {
		tsv.SetServingType(topodatapb.TabletType_REPLICA, true, []topodatapb.TabletType{topodatapb.TabletType_MASTER})
		ch <- true
	}()

	// SetServingType must rollback the prepared transaction,
	// but it must wait for the unprepared (txid2) to become non-busy.
	select {
	case <-ch:
		t.Fatal("ch should not fire")
	case <-time.After(10 * time.Millisecond):
	}
	if tsv.te.txPool.activePool.Size() != 1 {
		t.Errorf("len(tsv.te.txPool.activePool.Size()): %d, want 1", len(tsv.te.preparedPool.conns))
	}

	// Concluding conn2 will allow the transition to go through.
	tsv.te.txPool.LocalConclude(ctx, conn2)
	<-ch
}

func TestTabletServerRedoLogIsKeptBetweenRestarts(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)

	turnOnTxEngine := func() {
		tsv.SetServingType(topodatapb.TabletType_MASTER, true, nil)
	}
	turnOffTxEngine := func() {
		tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	}

	tpc := tsv.te.twoPC

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{})
	turnOnTxEngine()
	if len(tsv.te.preparedPool.conns) != 0 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 0", len(tsv.te.preparedPool.conns))
	}
	turnOffTxEngine()

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("dtid0"),
			sqltypes.NewInt64(RedoStatePrepared),
			sqltypes.NewVarBinary(""),
			sqltypes.NewVarBinary("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"),
		}},
	})
	turnOnTxEngine()
	if len(tsv.te.preparedPool.conns) != 1 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 1", len(tsv.te.preparedPool.conns))
	}
	got := tsv.te.preparedPool.conns["dtid0"].Queries
	want := []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Prepared queries: %v, want %v", got, want)
	}
	turnOffTxEngine()
	if v := len(tsv.te.preparedPool.conns); v != 0 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 0", v)
	}

	tsv.te.txPool.lastID.Set(1)
	// Ensure we continue past errors.
	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("bogus"),
			sqltypes.NewInt64(RedoStatePrepared),
			sqltypes.NewVarBinary(""),
			sqltypes.NewVarBinary("bogus"),
		}, {
			sqltypes.NewVarBinary("a:b:10"),
			sqltypes.NewInt64(RedoStatePrepared),
			sqltypes.NewVarBinary(""),
			sqltypes.NewVarBinary("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"),
		}, {
			sqltypes.NewVarBinary("a:b:20"),
			sqltypes.NewInt64(RedoStateFailed),
			sqltypes.NewVarBinary(""),
			sqltypes.NewVarBinary("unused"),
		}},
	})
	turnOnTxEngine()
	if len(tsv.te.preparedPool.conns) != 1 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 1", len(tsv.te.preparedPool.conns))
	}
	got = tsv.te.preparedPool.conns["a:b:10"].Queries
	want = []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Prepared queries: %v, want %v", got, want)
	}
	wantFailed := map[string]error{"a:b:20": errPrepFailed}
	if !reflect.DeepEqual(tsv.te.preparedPool.reserved, wantFailed) {
		t.Errorf("Failed dtids: %v, want %v", tsv.te.preparedPool.reserved, wantFailed)
	}
	// Verify last id got adjusted.
	if v := tsv.te.txPool.lastID.Get(); v != 20 {
		t.Errorf("tsv.te.txPool.lastID.Get(): %d, want 20", v)
	}
	turnOffTxEngine()
	if v := len(tsv.te.preparedPool.conns); v != 0 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 0", v)
	}
}

func TestTabletServerCreateTransaction(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	db.AddQueryPattern(fmt.Sprintf("insert into `_vt`\\.dt_state\\(dtid, state, time_created\\) values \\('aa', %d,.*", int(querypb.TransactionState_PREPARE)), &sqltypes.Result{})
	db.AddQueryPattern("insert into `_vt`\\.dt_participant\\(dtid, id, keyspace, shard\\) values \\('aa', 1,.*", &sqltypes.Result{})
	err := tsv.CreateTransaction(ctx, &target, "aa", []*querypb.Target{{
		Keyspace: "t1",
		Shard:    "0",
	}})
	if err != nil {
		t.Error(err)
	}
}

func TestTabletServerStartCommit(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	commitTransition := fmt.Sprintf("update `_vt`.dt_state set state = %d where dtid = 'aa' and state = %d", int(querypb.TransactionState_COMMIT), int(querypb.TransactionState_PREPARE))
	db.AddQuery(commitTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := tsv.StartCommit(ctx, &target, txid, "aa")
	if err != nil {
		t.Error(err)
	}

	db.AddQuery(commitTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = tsv.StartCommit(ctx, &target, txid, "aa")
	want := "could not transition to COMMIT: aa"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTabletserverSetRollback(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	rollbackTransition := fmt.Sprintf("update `_vt`.dt_state set state = %d where dtid = 'aa' and state = %d", int(querypb.TransactionState_ROLLBACK), int(querypb.TransactionState_PREPARE))
	db.AddQuery(rollbackTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := tsv.SetRollback(ctx, &target, "aa", txid)
	if err != nil {
		t.Error(err)
	}

	db.AddQuery(rollbackTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = tsv.SetRollback(ctx, &target, "aa", txid)
	want := "could not transition to ROLLBACK: aa"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTabletServerReadTransaction(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", &sqltypes.Result{})
	got, err := tsv.ReadTransaction(ctx, &target, "aa")
	if err != nil {
		t.Error(err)
	}
	want := &querypb.TransactionMetadata{}
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("aa"),
			sqltypes.NewInt64(int64(querypb.TransactionState_PREPARE)),
			sqltypes.NewVarBinary("1"),
		}},
	}
	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", txResult)
	db.AddQuery("select keyspace, shard from `_vt`.dt_participant where dtid = 'aa'", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("test1"),
			sqltypes.NewVarBinary("0"),
		}, {
			sqltypes.NewVarBinary("test2"),
			sqltypes.NewVarBinary("1"),
		}},
	})
	got, err = tsv.ReadTransaction(ctx, &target, "aa")
	if err != nil {
		t.Error(err)
	}
	want = &querypb.TransactionMetadata{
		Dtid:        "aa",
		State:       querypb.TransactionState_PREPARE,
		TimeCreated: 1,
		Participants: []*querypb.Target{{
			Keyspace:   "test1",
			Shard:      "0",
			TabletType: topodatapb.TabletType_MASTER,
		}, {
			Keyspace:   "test2",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("aa"),
			sqltypes.NewInt64(int64(querypb.TransactionState_COMMIT)),
			sqltypes.NewVarBinary("1"),
		}},
	}
	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_COMMIT
	got, err = tsv.ReadTransaction(ctx, &target, "aa")
	if err != nil {
		t.Error(err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("aa"),
			sqltypes.NewInt64(int64(querypb.TransactionState_ROLLBACK)),
			sqltypes.NewVarBinary("1"),
		}},
	}
	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_ROLLBACK
	got, err = tsv.ReadTransaction(ctx, &target, "aa")
	if err != nil {
		t.Error(err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}
}

func TestTabletServerConcludeTransaction(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	db.AddQuery("delete from `_vt`.dt_state where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("delete from `_vt`.dt_participant where dtid = 'aa'", &sqltypes.Result{})
	err := tsv.ConcludeTransaction(ctx, &target, "aa")
	if err != nil {
		t.Error(err)
	}
}

func TestTabletServerBeginFail(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	tsv.Begin(ctx, &target, nil)
	_, err = tsv.Begin(ctx, &target, nil)
	want := "transaction pool connection limit exceeded"
	if err == nil || err.Error() != want {
		t.Fatalf("Begin err: %v, want %v", err, want)
	}
}

func TestTabletServerCommitTransaction(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	transactionID, err := tsv.Begin(ctx, &target, nil)
	if err != nil {
		t.Fatalf("call TabletServer.Begin failed: %v", err)
	}
	if _, err := tsv.Execute(ctx, &target, executeSQL, nil, transactionID, nil); err != nil {
		t.Fatalf("failed to execute query: %s: %s", executeSQL, err)
	}
	if err := tsv.Commit(ctx, &target, transactionID); err != nil {
		t.Fatalf("call TabletServer.Commit failed: %v", err)
	}
}

func TestTabletServerCommiRollbacktFail(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	err = tsv.Commit(ctx, &target, -1)
	want := "transaction -1: not found"
	if err == nil || err.Error() != want {
		t.Fatalf("Commit err: %v, want %v", err, want)
	}
	err = tsv.Rollback(ctx, &target, -1)
	if err == nil || err.Error() != want {
		t.Fatalf("Commit err: %v, want %v", err, want)
	}
}

func TestTabletServerRollback(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	transactionID, err := tsv.Begin(ctx, &target, nil)
	if err != nil {
		t.Fatalf("call TabletServer.Begin failed: %v", err)
	}
	if _, err := tsv.Execute(ctx, &target, executeSQL, nil, transactionID, nil); err != nil {
		t.Fatalf("failed to execute query: %s: %v", executeSQL, err)
	}
	if err := tsv.Rollback(ctx, &target, transactionID); err != nil {
		t.Fatalf("call TabletServer.Rollback failed: %v", err)
	}
}

func TestTabletServerPrepare(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, transactionID, nil); err != nil {
		t.Fatal(err)
	}
	defer tsv.RollbackPrepared(ctx, &target, "aa", 0)
	if err := tsv.Prepare(ctx, &target, transactionID, "aa"); err != nil {
		t.Fatal(err)
	}
}

func TestTabletServerCommitPrepared(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, transactionID, nil); err != nil {
		t.Fatal(err)
	}
	if err := tsv.Prepare(ctx, &target, transactionID, "aa"); err != nil {
		t.Fatal(err)
	}
	defer tsv.RollbackPrepared(ctx, &target, "aa", 0)
	if err := tsv.CommitPrepared(ctx, &target, "aa"); err != nil {
		t.Fatal(err)
	}
}

func TestTabletServerRollbackPrepared(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, transactionID, nil); err != nil {
		t.Fatal(err)
	}
	if err := tsv.Prepare(ctx, &target, transactionID, "aa"); err != nil {
		t.Fatal(err)
	}
	if err := tsv.RollbackPrepared(ctx, &target, "aa", transactionID); err != nil {
		t.Fatal(err)
	}
}

func TestTabletServerStreamExecute(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)

	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	callback := func(*sqltypes.Result) error { return nil }
	if err := tsv.StreamExecute(ctx, &target, executeSQL, nil, 0, nil, callback); err != nil {
		t.Fatalf("TabletServer.StreamExecute should success: %s, but get error: %v",
			executeSQL, err)
	}
}

func TestTabletServerExecuteBatch(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	sql := "insert into test_table values (1, 2, 'addr', 'name')"
	sqlResult := &sqltypes.Result{}
	expanedSQL := "insert into test_table(pk, name, addr, name_string) values (1, 2, 'addr', 'name') /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSQL, sqlResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, true, 0, nil); err != nil {
		t.Fatal(err)
	}
}

func TestTabletServerExecuteBatchFailEmptyQueryList(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{}, false, 0, nil)
	want := "Empty query list"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("ExecuteBatch: %v, must contain %s", err, want)
	}
}

func TestTabletServerExecuteBatchFailAsTransaction(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
	}, true, 1, nil)
	want := "cannot start a new transaction"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("ExecuteBatch: %v, must contain %s", err, want)
	}
}

func TestTabletServerExecuteBatchBeginFail(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	// make "begin" query fail
	db.AddRejectedQuery("begin", errRejected)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
	}, false, 0, nil); err == nil {
		t.Fatalf("TabletServer.ExecuteBatch should fail")
	}
}

func TestTabletServerExecuteBatchCommitFail(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	// make "commit" query fail
	db.AddRejectedQuery("commit", errRejected)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
		{
			Sql:           "commit",
			BindVariables: nil,
		},
	}, false, 0, nil); err == nil {
		t.Fatalf("TabletServer.ExecuteBatch should fail")
	}
}

func TestTabletServerExecuteBatchSqlExecFailInTransaction(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &sqltypes.Result{}
	expanedSQL := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSQL, sqlResult)

	// make this query fail
	db.AddRejectedQuery(sql, errRejected)
	db.AddRejectedQuery(expanedSQL, errRejected)

	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if db.GetQueryCalledNum("rollback") != 0 {
		t.Fatalf("rollback should not be executed.")
	}

	if _, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, true, 0, nil); err == nil {
		t.Fatalf("TabletServer.ExecuteBatch should fail")
	}

	if db.GetQueryCalledNum("rollback") != 1 {
		t.Fatalf("rollback should be executed only once.")
	}
}

func TestTabletServerExecuteBatchSqlSucceedInTransaction(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	sql := "insert into test_table values (1, 2, 'addr', 'name')"
	sqlResult := &sqltypes.Result{}
	expanedSQL := "insert into test_table(pk, name, addr, name_string) values (1, 2, 'addr', 'name') /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSQL, sqlResult)

	// cause execution error for this particular sql query
	db.AddRejectedQuery(sql, errRejected)

	config := testUtils.newQueryServiceConfig()
	config.EnableAutoCommit = true
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, false, 0, nil); err != nil {
		t.Fatal(err)
	}
}

func TestTabletServerExecuteBatchCallCommitWithoutABegin(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "commit",
			BindVariables: nil,
		},
	}, false, 0, nil); err == nil {
		t.Fatalf("TabletServer.ExecuteBatch should fail")
	}
}

func TestExecuteBatchNestedTransaction(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &sqltypes.Result{}
	expanedSQL := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSQL, sqlResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
		{
			Sql:           "begin",
			BindVariables: nil,
		},
		{
			Sql:           sql,
			BindVariables: nil,
		},
		{
			Sql:           "commit",
			BindVariables: nil,
		},
		{
			Sql:           "commit",
			BindVariables: nil,
		},
	}, false, 0, nil); err == nil {
		t.Fatalf("TabletServer.Execute should fail because of nested transaction")
	}
	tsv.te.txPool.SetTimeout(10)
}

func TestSerializeTransactionsSameRow(t *testing.T) {
	// This test runs three transaction in parallel:
	// tx1 | tx2 | tx3
	// However, tx1 and tx2 have the same WHERE clause (i.e. target the same row)
	// and therefore tx2 cannot start until the first query of tx1 has finished.
	// The actual execution looks like this:
	// tx1 | tx3
	// tx2
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnableHotRowProtection = true
	config.HotRowProtectionConcurrentTransactions = 1
	// Reduce the txpool to 2 because we should never consume more than two slots.
	config.TransactionCap = 2
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbcfgs); err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	countStart := tabletenv.WaitStats.Counts()["TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and name = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and name = :name"
	q3 := "update test_table set name_string = 'tx3' where pk = :pk and name = :name"
	// Every request needs their own bind variables to avoid data races.
	bvTx1 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx2 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx3 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(2),
		"name": sqltypes.Int64BindVariable(1),
	}

	// Make sure that tx2 and tx3 start only after tx1 is running its Execute().
	tx1Started := make(chan struct{})
	// Make sure that tx3 could finish while tx2 could not.
	tx3Finished := make(chan struct{})

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk in (1) /* _stream test_table (pk ) (1 ); */",
		func() {
			close(tx1Started)
			if err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 2); err != nil {
				t.Fatal(err)
			}
		})

	// Run all three transactions.
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, tx1, err := tsv.BeginExecute(ctx, &target, q1, bvTx1, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q1, err)
		}
		if err := tsv.Commit(ctx, &target, tx1); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		_, tx2, err := tsv.BeginExecute(ctx, &target, q2, bvTx2, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q2, err)
		}
		// TODO(mberlin): This should actually be in the BeforeFunc() of tx1 but
		// then the test is hanging. It looks like the MySQL C client library cannot
		// open a second connection while the request of the first connection is
		// still pending.
		<-tx3Finished
		if err := tsv.Commit(ctx, &target, tx2); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		_, tx3, err := tsv.BeginExecute(ctx, &target, q3, bvTx3, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q3, err)
		}
		if err := tsv.Commit(ctx, &target, tx3); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
		close(tx3Finished)
	}()

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	want := countStart + 1
	if !ok || got != want {
		t.Fatalf("only tx2 should have been serialized: ok? %v got: %v want: %v", ok, got, want)
	}
}

// TestSerializeTransactionsSameRow_ExecuteBatchAsTransaction tests the same as
// TestSerializeTransactionsSameRow but for the ExecuteBatch method with
// asTransaction=true (i.e. vttablet wraps the query in a BEGIN/Query/COMMIT
// sequence).
// One subtle difference is that we have no control over the commit of tx2 and
// therefore we cannot reproduce that tx2 is still pending after tx3 has
// finished. Nonetheless, we check the overall serialization count and verify
// that only tx1 and tx2, 2 transactions in total, are serialized.
func TestSerializeTransactionsSameRow_ExecuteBatchAsTransaction(t *testing.T) {
	// This test runs three transaction in parallel:
	// tx1 | tx2 | tx3
	// However, tx1 and tx2 have the same WHERE clause (i.e. target the same row)
	// and therefore tx2 cannot start until the first query of tx1 has finished.
	// The actual execution looks like this:
	// tx1 | tx3
	// tx2
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnableHotRowProtection = true
	config.HotRowProtectionConcurrentTransactions = 1
	// Reduce the txpool to 2 because we should never consume more than two slots.
	config.TransactionCap = 2
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbcfgs); err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	countStart := tabletenv.WaitStats.Counts()["TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and name = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and name = :name"
	q3 := "update test_table set name_string = 'tx3' where pk = :pk and name = :name"
	// Every request needs their own bind variables to avoid data races.
	bvTx1 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx2 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx3 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(2),
		"name": sqltypes.Int64BindVariable(1),
	}

	// Make sure that tx2 and tx3 start only after tx1 is running its Execute().
	tx1Started := make(chan struct{})

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk in (1) /* _stream test_table (pk ) (1 ); */",
		func() {
			close(tx1Started)
			if err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 2); err != nil {
				t.Fatal(err)
			}
		})

	// Run all three transactions.
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		results, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{{
			Sql:           q1,
			BindVariables: bvTx1,
		}}, true /*asTransaction*/, 0 /*transactionID*/, nil /*options*/)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q1, err)
		}
		if len(results) != 1 || results[0].RowsAffected != 1 {
			t.Fatalf("TabletServer.ExecuteBatch returned wrong results: %+v", results)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		results, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{{
			Sql:           q2,
			BindVariables: bvTx2,
		}}, true /*asTransaction*/, 0 /*transactionID*/, nil /*options*/)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q2, err)
		}
		if len(results) != 1 || results[0].RowsAffected != 1 {
			t.Fatalf("TabletServer.ExecuteBatch returned wrong results: %+v", results)
		}
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		results, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{{
			Sql:           q3,
			BindVariables: bvTx3,
		}}, true /*asTransaction*/, 0 /*transactionID*/, nil /*options*/)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q3, err)
		}
		if len(results) != 1 || results[0].RowsAffected != 1 {
			t.Fatalf("TabletServer.ExecuteBatch returned wrong results: %+v", results)
		}
	}()

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	want := countStart + 1
	if !ok || got != want {
		t.Fatalf("only tx2 should have been serialized: ok? %v got: %v want: %v", ok, got, want)
	}
}

func TestSerializeTransactionsSameRow_ConcurrentTransactions(t *testing.T) {
	// This test runs three transaction in parallel:
	// tx1 | tx2 | tx3
	// Out of these three, two can run in parallel because we increased the
	// ConcurrentTransactions limit to 2.
	// One out of the three transaction will always get serialized though.
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnableHotRowProtection = true
	config.HotRowProtectionConcurrentTransactions = 2
	// Reduce the txpool to 2 because we should never consume more than two slots.
	config.TransactionCap = 2
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbcfgs); err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	countStart := tabletenv.WaitStats.Counts()["TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and name = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and name = :name"
	q3 := "update test_table set name_string = 'tx3' where pk = :pk and name = :name"
	// Every request needs their own bind variables to avoid data races.
	bvTx1 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx2 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx3 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}

	tx1Started := make(chan struct{})
	allQueriesPending := make(chan struct{})
	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk in (1) /* _stream test_table (pk ) (1 ); */",
		func() {
			close(tx1Started)
			<-allQueriesPending
		})

	// Run all three transactions.
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, tx1, err := tsv.BeginExecute(ctx, &target, q1, bvTx1, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q1, err)
		}

		if err := tsv.Commit(ctx, &target, tx1); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for tx1 to avoid that this tx could pass tx1, without any contention.
		// In that case, we would see less than 3 pending transactions.
		<-tx1Started

		_, tx2, err := tsv.BeginExecute(ctx, &target, q2, bvTx2, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q2, err)
		}

		if err := tsv.Commit(ctx, &target, tx2); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for tx1 to avoid that this tx could pass tx1, without any contention.
		// In that case, we would see less than 3 pending transactions.
		<-tx1Started

		_, tx3, err := tsv.BeginExecute(ctx, &target, q3, bvTx3, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q3, err)
		}

		if err := tsv.Commit(ctx, &target, tx3); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// At this point, all three transactions should be blocked in BeginExecute()
	// and therefore count as pending transaction by the Hot Row Protection.
	//
	// NOTE: We are not doing more sophisticated synchronizations between the
	// transactions via db.SetBeforeFunc() for the same reason as mentioned
	// in TestSerializeTransactionsSameRow: The MySQL C client does not seem
	// to allow more than connection attempt at a time.
	if err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 3); err != nil {
		t.Fatal(err)
	}
	close(allQueriesPending)

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	want := countStart + 1
	if !ok || got != want {
		t.Fatalf("One out of the three transactions must have waited: ok? %v got: %v want: %v", ok, got, want)
	}
}

func waitForTxSerializationPendingQueries(tsv *TabletServer, key string, i int) error {
	start := time.Now()
	for {
		got, want := tsv.qe.txSerializer.Pending(key), i
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("wait for query count increase in TxSerializer timed out: got = %v, want = %v", got, want)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func TestSerializeTransactionsSameRow_TooManyPendingRequests(t *testing.T) {
	// This test is similar to TestSerializeTransactionsSameRow, but tests only
	// that there must not be too many pending BeginExecute() requests which are
	// serialized.
	// Since we start to queue before the transaction pool would queue, we need
	// to enforce an upper limit as well to protect vttablet.
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnableHotRowProtection = true
	config.HotRowProtectionMaxQueueSize = 1
	config.HotRowProtectionConcurrentTransactions = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbcfgs); err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	countStart := tabletenv.WaitStats.Counts()["TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and name = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and name = :name"
	// Every request needs their own bind variables to avoid data races.
	bvTx1 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx2 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}

	// Make sure that tx2 starts only after tx1 is running its Execute().
	tx1Started := make(chan struct{})
	// Signal when tx2 is done.
	tx2Failed := make(chan struct{})

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk in (1) /* _stream test_table (pk ) (1 ); */",
		func() {
			close(tx1Started)
			<-tx2Failed
		})

	// Run the two transactions.
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, tx1, err := tsv.BeginExecute(ctx, &target, q1, bvTx1, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q1, err)
		}
		if err := tsv.Commit(ctx, &target, tx1); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tx2Failed)

		<-tx1Started
		_, _, err := tsv.BeginExecute(ctx, &target, q2, bvTx2, nil)
		if err == nil || vterrors.Code(err) != vtrpcpb.Code_RESOURCE_EXHAUSTED || err.Error() != "hot row protection: too many queued transactions (1 >= 1) for the same row (table + WHERE clause: 'test_table where pk = 1 and name = 1')" {
			t.Fatalf("tx2 should have failed because there are too many pending requests: %v", err)
		}
		// No commit necessary because the Begin failed.
	}()

	wg.Wait()

	got, _ := tabletenv.WaitStats.Counts()["TxSerializer"]
	want := countStart + 0
	if got != want {
		t.Fatalf("tx2 should have failed early and not tracked as serialized: got: %v want: %v", got, want)
	}
}

// TestSerializeTransactionsSameRow_TooManyPendingRequests_ExecuteBatchAsTransaction
// tests the same thing as TestSerializeTransactionsSameRow_TooManyPendingRequests
// but for the ExecuteBatch method with asTransaction=true.
// We have this test to verify that the error handling in ExecuteBatch() works
// correctly for the hot row protection integration.
func TestSerializeTransactionsSameRow_TooManyPendingRequests_ExecuteBatchAsTransaction(t *testing.T) {
	// This test rejects queries if more than one transaction is currently in
	// progress for the hot row i.e. we check that tx2 actually fails.
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnableHotRowProtection = true
	config.HotRowProtectionMaxQueueSize = 1
	config.HotRowProtectionConcurrentTransactions = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbcfgs); err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	countStart := tabletenv.WaitStats.Counts()["TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and name = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and name = :name"
	// Every request needs their own bind variables to avoid data races.
	bvTx1 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx2 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}

	// Make sure that tx2 starts only after tx1 is running its Execute().
	tx1Started := make(chan struct{})
	// Signal when tx2 is done.
	tx2Failed := make(chan struct{})

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk in (1) /* _stream test_table (pk ) (1 ); */",
		func() {
			close(tx1Started)
			<-tx2Failed
		})

	// Run the two transactions.
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		results, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{{
			Sql:           q1,
			BindVariables: bvTx1,
		}}, true /*asTransaction*/, 0 /*transactionID*/, nil /*options*/)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q1, err)
		}
		if len(results) != 1 || results[0].RowsAffected != 1 {
			t.Fatalf("TabletServer.ExecuteBatch returned wrong results: %+v", results)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tx2Failed)

		<-tx1Started
		results, err := tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{{
			Sql:           q2,
			BindVariables: bvTx2,
		}}, true /*asTransaction*/, 0 /*transactionID*/, nil /*options*/)
		if err == nil || vterrors.Code(err) != vtrpcpb.Code_RESOURCE_EXHAUSTED || err.Error() != "hot row protection: too many queued transactions (1 >= 1) for the same row (table + WHERE clause: 'test_table where pk = 1 and name = 1')" {
			t.Fatalf("tx2 should have failed because there are too many pending requests: %v results: %+v", err, results)
		}
	}()

	wg.Wait()

	got, _ := tabletenv.WaitStats.Counts()["TxSerializer"]
	want := countStart + 0
	if got != want {
		t.Fatalf("tx2 should have failed early and not tracked as serialized: got: %v want: %v", got, want)
	}
}

func TestSerializeTransactionsSameRow_RequestCanceled(t *testing.T) {
	// This test is similar to TestSerializeTransactionsSameRow, but tests only
	// that a queued request unblocks itself when its context is done.
	//
	// tx1 and tx2 run against the same row.
	// tx2 is blocked on tx1. Eventually, tx2 is canceled and its request fails.
	// Only after that tx1 commits and finishes.
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnableHotRowProtection = true
	config.HotRowProtectionConcurrentTransactions = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbcfgs); err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	countStart := tabletenv.WaitStats.Counts()["TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and name = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and name = :name"
	q3 := "update test_table set name_string = 'tx3' where pk = :pk and name = :name"
	// Every request needs their own bind variables to avoid data races.
	bvTx1 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx2 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}
	bvTx3 := map[string]*querypb.BindVariable{
		"pk":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(1),
	}

	// Make sure that tx2 starts only after tx1 is running its Execute().
	tx1Started := make(chan struct{})
	// Signal when tx2 is done.
	tx2Done := make(chan struct{})

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk in (1) /* _stream test_table (pk ) (1 ); */",
		func() {
			close(tx1Started)
			// Keep blocking until tx2 was canceled.
			<-tx2Done
		})

	// Run the two transactions.
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, tx1, err := tsv.BeginExecute(ctx, &target, q1, bvTx1, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q1, err)
		}

		if err := tsv.Commit(ctx, &target, tx1); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx2.
	ctxTx2, cancelTx2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tx2Done)

		// Wait until tx1 has started to make the test deterministic.
		<-tx1Started

		_, _, err := tsv.BeginExecute(ctxTx2, &target, q2, bvTx2, nil)
		if err == nil || vterrors.Code(err) != vtrpcpb.Code_CANCELED || err.Error() != "context canceled" {
			t.Fatalf("tx2 should have failed because the context was canceled: %v", err)
		}
		// No commit necessary because the Begin failed.
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait until tx1 and tx2 are pending to make the test deterministic.
		if err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 2); err != nil {
			t.Fatal(err)
		}

		_, tx3, err := tsv.BeginExecute(ctx, &target, q3, bvTx3, nil)
		if err != nil {
			t.Fatalf("failed to execute query: %s: %s", q3, err)
		}

		if err := tsv.Commit(ctx, &target, tx3); err != nil {
			t.Fatalf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// Wait until tx1, 2 and 3 are pending.
	if err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 3); err != nil {
		t.Fatal(err)
	}
	// Now unblock tx2 and cancel it.
	cancelTx2()

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	want := countStart + 2
	if got != want {
		t.Fatalf("tx2 and tx3 should have been serialized: ok? %v got: %v want: %v", ok, got, want)
	}
}

func TestMessageStream(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	err := tsv.MessageStream(ctx, &target, "nomsg", func(qr *sqltypes.Result) error {
		return nil
	})
	wantErr := "table nomsg not found in schema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("tsv.MessageStream: %v, want %s", err, wantErr)
	}

	// Check that the streaming mechanism works.
	called := false
	err = tsv.MessageStream(ctx, &target, "msg", func(qr *sqltypes.Result) error {
		called = true
		return io.EOF
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("callback was not called for MessageStream")
	}
}

func TestMessageAck(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	ids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("2"),
	}}
	_, err := tsv.MessageAck(ctx, &target, "nonmsg", ids)
	want := "message table nonmsg not found in schema"
	if err == nil || strings.HasPrefix(err.Error(), want) {
		t.Errorf("tsv.MessageAck(invalid): %v, want %s", err, want)
	}

	_, err = tsv.MessageAck(ctx, &target, "msg", ids)
	want = "query: 'select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update' is not supported on fakesqldb"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("tsv.MessageAck(invalid): %v, want %s", err, want)
	}

	db.AddQuery(
		"select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
			},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("1"),
				sqltypes.NewVarBinary("1"),
			}},
		},
	)
	db.AddQueryPattern("update msg set time_acked = .*", &sqltypes.Result{RowsAffected: 1})
	count, err := tsv.MessageAck(ctx, &target, "msg", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
}

func TestRescheduleMessages(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	_, err := tsv.PostponeMessages(ctx, &target, "nonmsg", []string{"1", "2"})
	want := "message table nonmsg not found in schema"
	if err == nil || strings.HasPrefix(err.Error(), want) {
		t.Errorf("tsv.PostponeMessages(invalid): %v, want %s", err, want)
	}

	_, err = tsv.PostponeMessages(ctx, &target, "msg", []string{"1", "2"})
	want = "query: 'select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update' is not supported"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("tsv.PostponeMessages(invalid):\n%v, want\n%s", err, want)
	}

	db.AddQuery(
		"select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
			},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("1"),
				sqltypes.NewVarBinary("1"),
			}},
		},
	)
	db.AddQueryPattern("update msg set time_next = .*", &sqltypes.Result{RowsAffected: 1})
	count, err := tsv.PostponeMessages(ctx, &target, "msg", []string{"1", "2"})
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
}

func TestPurgeMessages(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	_, err := tsv.PurgeMessages(ctx, &target, "nonmsg", 0)
	want := "message table nonmsg not found in schema"
	if err == nil || strings.HasPrefix(err.Error(), want) {
		t.Errorf("tsv.PurgeMessages(invalid): %v, want %s", err, want)
	}

	_, err = tsv.PurgeMessages(ctx, &target, "msg", 0)
	want = "query: 'select time_scheduled, id from msg where time_scheduled < 0 and time_acked is not null limit 500 for update' is not supported"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("tsv.PurgeMessages(invalid):\n%v, want\n%s", err, want)
	}

	db.AddQuery(
		"select time_scheduled, id from msg where time_scheduled < 3 and time_acked is not null limit 500 for update",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
			},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("1"),
				sqltypes.NewVarBinary("1"),
			}},
		},
	)
	db.AddQuery("delete from msg where (time_scheduled = 1 and id = 1) /* _stream msg (time_scheduled id ) (1 1 ); */", &sqltypes.Result{RowsAffected: 1})
	count, err := tsv.PurgeMessages(ctx, &target, "msg", 3)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
}

func TestTabletServerSplitQuery(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	db.AddQuery("SELECT MIN(pk), MAX(pk) FROM test_table", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int32},
			{Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(100),
			},
		},
	})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table where count > :count"
	splits, err := tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		&querypb.BoundQuery{Sql: sql},
		[]string{}, /* splitColumns */
		10,         /* splitCount */
		0,          /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	if err != nil {
		t.Fatalf("TabletServer.SplitQuery should succeed: %v, but get error: %v", sql, err)
	}
	if len(splits) != 10 {
		t.Fatalf("got: %v, want: %v.\nsplits: %+v", len(splits), 10, splits)
	}
}

func TestTabletServerSplitQueryKeywords(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	db.AddQuery("SELECT MIN(`by`), MAX(`by`) FROM `order`", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int32},
			{Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(100),
			},
		},
	})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from `order` where `value` > :count"
	splits, err := tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		&querypb.BoundQuery{Sql: sql},
		[]string{}, /* splitColumns */
		10,         /* splitCount */
		0,          /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	if err != nil {
		t.Fatalf("TabletServer.SplitQuery should succeed: %v, but get error: %v", sql, err)
	}
	if len(splits) != 10 {
		t.Fatalf("got: %v, want: %v.\nsplits: %+v", len(splits), 10, splits)
	}
}

func TestTabletServerSplitQueryInvalidQuery(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	// SplitQuery should not support SQLs with a LIMIT clause:
	sql := "select * from test_table where count > :count limit 10"
	_, err = tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		&querypb.BoundQuery{Sql: sql},
		[]string{}, /* splitColumns */
		10,         /* splitCount */
		0,          /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	if err == nil {
		t.Fatalf("TabletServer.SplitQuery should fail")
	}
}

func TestTabletServerSplitQueryInvalidParams(t *testing.T) {
	// Tests that SplitQuery returns an error when both numRowsPerQueryPart and splitCount are given.
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table where count > :count"
	_, err = tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		&querypb.BoundQuery{Sql: sql},
		[]string{}, /* splitColumns */
		10,         /* splitCount */
		11,         /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	if err == nil {
		t.Fatalf("TabletServer.SplitQuery should fail")
	}
}

// Tests that using Equal Splits on a string column returns an error.
func TestTabletServerSplitQueryEqualSplitsOnStringColumn(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table"
	_, err = tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		&querypb.BoundQuery{Sql: sql},
		// EQUAL_SPLITS should not work on a string column.
		[]string{"name_string"}, /* splitColumns */
		10,                      /* splitCount */
		0,                       /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	want :=
		"using the EQUAL_SPLITS algorithm in SplitQuery" +
			" requires having a numeric (integral or float) split-column." +
			" Got type: {Name: 'name_string', Type: VARCHAR}"
	if err.Error() != want {
		t.Fatalf("got: %v, want: %v", err, want)
	}
}

func TestHandleExecUnknownError(t *testing.T) {
	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "TestHandleExecError")
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	defer tsv.handlePanicAndSendLogStats("select * from test_table", nil, logStats)
	panic("unknown exec error")
}

var testLogs []string

func recordInfof(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	testLogs = append(testLogs, msg)
	log.Infof(msg)
}

func recordErrorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	testLogs = append(testLogs, msg)
	log.Errorf(msg)
}

func setupTestLogger() {
	testLogs = make([]string, 0)
	tabletenv.Infof = recordInfof
	tabletenv.Errorf = recordErrorf
}

func clearTestLogger() {
	tabletenv.Infof = log.Infof
	tabletenv.Errorf = log.Errorf
}

func getTestLog(i int) string {
	if i < len(testLogs) {
		return testLogs[i]
	}
	return fmt.Sprintf("ERROR: log %d/%d does not exist", i, len(testLogs))
}

func TestHandleExecTabletError(t *testing.T) {
	ctx := context.Background()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	setupTestLogger()
	defer clearTestLogger()
	err := tsv.convertAndLogError(
		ctx,
		"select * from test_table",
		nil,
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tablet error"),
		nil,
	)
	fmt.Println(">>>>>"+err.Error())
	want := "tablet error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("got `%v`, want '%s'", err, want)
	}
	want = "Sql: \"select * from test_table\", BindVars: {}"
	if !strings.Contains(getTestLog(0), want) {
		t.Errorf("error log %s, want '%s'", getTestLog(0), want)
	}
}

func TestTerseErrorsNonSQLError(t *testing.T) {
	ctx := context.Background()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	setupTestLogger()
	defer clearTestLogger()
	err := tsv.convertAndLogError(
		ctx,
		"select * from test_table",
		nil,
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tablet error"),
		nil,
	)
	want := "tablet error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%v, want '%s'", err, want)
	}
	want = "Sql: \"select * from test_table\", BindVars: {}"
	if !strings.Contains(getTestLog(0), want) {
		t.Errorf("error log %s, want '%s'", getTestLog(0), want)
	}
}

func TestTerseErrorsBindVars(t *testing.T) {
	ctx := context.Background()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	setupTestLogger()
	defer clearTestLogger()

	sqlErr := mysql.NewSQLError(10, "HY000", "sensitive message")
	sqlErr.Query = "select * from test_table where a = 1"

	err := tsv.convertAndLogError(
		ctx,
		"select * from test_table where a = :a",
		map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(1)},
		sqlErr,
		nil,
	)
	want := "(errno 10) (sqlstate HY000): Sql: \"select * from test_table where a = :a\", BindVars: {}"
	if err == nil || err.Error() != want {
		t.Errorf("error got '%v', want '%s'", err, want)
	}

	wantLog := "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where a = :a\", BindVars: {a: \"type:INT64 value:\\\"1\\\" \"}"
	if wantLog != getTestLog(0) {
		t.Errorf("log got '%s', want '%s'", getTestLog(0), wantLog)
	}
}

func TestTerseErrorsNoBindVars(t *testing.T) {
	ctx := context.Background()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	setupTestLogger()
	defer clearTestLogger()
	err := tsv.convertAndLogError(ctx, "", nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "sensitive message"), nil)
	want := "sensitive message"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%v, want '%s'", err, want)
	}
	want = "Sql: \"\", BindVars: {}"
	if !strings.Contains(getTestLog(0), want) {
		t.Errorf("error log '%s', want '%s'", getTestLog(0), want)
	}
}

func TestTruncateErrors(t *testing.T) {
	ctx := context.Background()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	setupTestLogger()
	defer clearTestLogger()

	*sqlparser.TruncateErrLen = 52
	sql := "select * from test_table where xyz = :vtg1 order by abc desc"
	sqlErr := mysql.NewSQLError(10, "HY000", "sensitive message")
	sqlErr.Query = "select * from test_table where xyz = 'this is kinda long eh'"
	err := tsv.convertAndLogError(
		ctx,
		sql,
		map[string]*querypb.BindVariable{"vtg1": sqltypes.StringBindVariable("this is kinda long eh")},
		sqlErr,
		nil,
	)
	wantErr := "(errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {}"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error got '%v', want '%s'", err, wantErr)
	}

	wantLog := "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vt [TRUNCATED]\", BindVars: {vtg1: \"type:VARCHAR value:\\\"t [TRUNCATED]"
	if wantLog != getTestLog(0) {
		t.Errorf("log got '%s', want '%s'", getTestLog(0), wantLog)
	}

	*sqlparser.TruncateErrLen = 140
	err = tsv.convertAndLogError(
		ctx,
		sql,
		map[string]*querypb.BindVariable{"vtg1": sqltypes.StringBindVariable("this is kinda long eh")},
		sqlErr,
		nil,
	)

	wantErr = "(errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {}"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error got '%v', want '%s'", err, wantErr)
	}

	wantLog = "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {vtg1: \"type:VARCHAR value:\\\"this is kinda long eh\\\" \"}"
	if wantLog != getTestLog(1) {
		t.Errorf("log got '%s', want '%s'", getTestLog(1), wantLog)
	}
	*sqlparser.TruncateErrLen = 0
}

func TestTerseErrorsIgnoreFailoverInProgress(t *testing.T) {
	ctx := context.Background()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	setupTestLogger()
	defer clearTestLogger()
	err := tsv.convertAndLogError(ctx, "select * from test_table where id = :a",
		map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(1)},
		mysql.NewSQLError(1227, "42000", "failover in progress"),
		nil,
	)
	if got, want := err.Error(), "failover in progress (errno 1227) (sqlstate 42000)"; !strings.HasPrefix(got, want) {
		t.Fatalf("'failover in progress' text must never be stripped: got = %v, want = %v", got, want)
	}

	// errors during failover aren't logged at all
	if len(testLogs) != 0 {
		t.Errorf("unexpected error log during failover")
	}
}

var aclJSON1 = `{
  "table_groups": [
    {
      "name": "group01",
      "table_names_or_prefixes": ["test_table1"],
      "readers": ["vt1"],
      "writers": ["vt1"]
    }
  ]
}`
var aclJSON2 = `{
  "table_groups": [
    {
      "name": "group02",
      "table_names_or_prefixes": ["test_table2"],
      "readers": ["vt2"],
      "admins": ["vt2"]
    }
  ]
}`

func TestACLHUP(t *testing.T) {
	tableacl.Register("simpleacl", &simpleacl.Factory{})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)

	f, err := ioutil.TempFile("", "tableacl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	if _, err := io.WriteString(f, aclJSON1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	tsv.InitACL(f.Name(), true)

	groups1 := tableacl.GetCurrentConfig().TableGroups
	if name1 := groups1[0].GetName(); name1 != "group01" {
		t.Fatalf("Expected name 'group01', got '%s'", name1)
	}

	if f, err = os.Create(f.Name()); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(f, aclJSON2); err != nil {
		t.Fatal(err)
	}

	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(25 * time.Millisecond) // wait for signal handler

	groups2 := tableacl.GetCurrentConfig().TableGroups
	if len(groups2) != 1 {
		t.Fatalf("Expected only one table group")
	}
	group2 := groups2[0]
	if name2 := group2.GetName(); name2 != "group02" {
		t.Fatalf("Expected name 'group02', got '%s'", name2)
	}
	if group2.GetAdmins() == nil {
		t.Fatalf("Expected 'admins' to exist, but it didn't")
	}
	if group2.GetWriters() != nil {
		t.Fatalf("Expected 'writers' to not exist, got '%s'", group2.GetWriters())
	}
}

func TestConfigChanges(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	newSize := 10
	newDuration := time.Duration(10 * time.Millisecond)

	tsv.SetPoolSize(newSize)
	if val := tsv.PoolSize(); val != newSize {
		t.Errorf("PoolSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.conns.Capacity()); val != newSize {
		t.Errorf("tsv.qe.connPool.Capacity: %d, want %d", val, newSize)
	}

	tsv.SetStreamPoolSize(newSize)
	if val := tsv.StreamPoolSize(); val != newSize {
		t.Errorf("StreamPoolSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.streamConns.Capacity()); val != newSize {
		t.Errorf("tsv.qe.streamConnPool.Capacity: %d, want %d", val, newSize)
	}

	tsv.SetTxPoolSize(newSize)
	if val := tsv.TxPoolSize(); val != newSize {
		t.Errorf("TxPoolSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.te.txPool.conns.Capacity()); val != newSize {
		t.Errorf("tsv.te.txPool.pool.Capacity: %d, want %d", val, newSize)
	}

	tsv.SetTxTimeout(newDuration)
	if val := tsv.TxTimeout(); val != newDuration {
		t.Errorf("tsv.TxTimeout: %v, want %v", val, newDuration)
	}
	if val := tsv.te.txPool.Timeout(); val != newDuration {
		t.Errorf("tsv.te.txPool.Timeout: %v, want %v", val, newDuration)
	}

	tsv.SetQueryPlanCacheCap(newSize)
	if val := tsv.QueryPlanCacheCap(); val != newSize {
		t.Errorf("QueryPlanCacheCap: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.QueryPlanCacheCap()); val != newSize {
		t.Errorf("tsv.qe.QueryPlanCacheCap: %d, want %d", val, newSize)
	}

	tsv.SetAutoCommit(true)
	if val := tsv.qe.autoCommit.Get(); !val {
		t.Errorf("tsv.qe.autoCommit.Get: %v, want true", val)
	}

	tsv.SetMaxResultSize(newSize)
	if val := tsv.MaxResultSize(); val != newSize {
		t.Errorf("MaxResultSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.maxResultSize.Get()); val != newSize {
		t.Errorf("tsv.qe.maxResultSize.Get: %d, want %d", val, newSize)
	}

	tsv.SetWarnResultSize(newSize)
	if val := tsv.WarnResultSize(); val != newSize {
		t.Errorf("WarnResultSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.warnResultSize.Get()); val != newSize {
		t.Errorf("tsv.qe.warnResultSize.Get: %d, want %d", val, newSize)
	}

	tsv.SetMaxDMLRows(newSize)
	if val := tsv.MaxDMLRows(); val != newSize {
		t.Errorf("MaxDMLRows: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.maxDMLRows.Get()); val != newSize {
		t.Errorf("tsv.qe.maxDMLRows.Get: %d, want %d", val, newSize)
	}
}

func setUpTabletServerTest(t *testing.T) *fakesqldb.DB {
	db := fakesqldb.New(t)
	for query, result := range getSupportedQueries() {
		db.AddQuery(query, result)
	}
	return db
}

func checkTabletServerState(t *testing.T, tsv *TabletServer, expectState int64) {
	tsv.mu.Lock()
	state := tsv.state
	tsv.mu.Unlock()
	if state != expectState {
		t.Fatalf("TabletServer should in state: %d, but get state: %d", expectState, state)
	}
}

func getSupportedQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		// Queries for how row protection test (txserializer).
		"update test_table set name_string = 'tx1' where pk in (1) /* _stream test_table (pk ) (1 ); */": {
			RowsAffected: 1,
		},
		"update test_table set name_string = 'tx2' where pk in (1) /* _stream test_table (pk ) (1 ); */": {
			RowsAffected: 1,
		},
		"update test_table set name_string = 'tx3' where pk in (1) /* _stream test_table (pk ) (1 ); */": {
			RowsAffected: 1,
		},
		// tx3, but with different primary key.
		"update test_table set name_string = 'tx3' where pk in (2) /* _stream test_table (pk ) (2 ); */": {
			RowsAffected: 1,
		},
		// Complex WHERE clause requires SELECT of primary key first.
		"select pk from test_table where pk = 1 and name = 1 limit 10001 for update": {
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
			},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("1"),
			}},
		},
		// Complex WHERE clause requires SELECT of primary key first.
		"select pk from test_table where pk = 2 and name = 1 limit 10001 for update": {
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
			},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("2"),
			}},
		},
		// queries for twopc
		sqlTurnoffBinlog:                                  {},
		fmt.Sprintf(sqlCreateSidecarDB, "`_vt`"):          {},
		fmt.Sprintf(sqlDropLegacy1, "`_vt`"):              {},
		fmt.Sprintf(sqlDropLegacy2, "`_vt`"):              {},
		fmt.Sprintf(sqlDropLegacy3, "`_vt`"):              {},
		fmt.Sprintf(sqlDropLegacy4, "`_vt`"):              {},
		fmt.Sprintf(sqlCreateTableRedoState, "`_vt`"):     {},
		fmt.Sprintf(sqlCreateTableRedoStatement, "`_vt`"): {},
		fmt.Sprintf(sqlCreateTableDTState, "`_vt`"):       {},
		fmt.Sprintf(sqlCreateTableDTParticipant, "`_vt`"): {},
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1427325875")},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("STRICT_TRANS_TABLES")},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1")},
			},
		},
		"select @@sql_auto_is_null": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("0")},
			},
		},
		"show variables like 'binlog_format'": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}, {
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("binlog_format"),
				sqltypes.NewVarBinary("STATEMENT"),
			}},
		},
		mysql.BaseShowTables: {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table", false, ""),
				mysql.BaseShowTablesRow("order", false, ""),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		},
		"select * from test_table where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}, {
				Name: "name",
				Type: sqltypes.Int32,
			}, {
				Name: "addr",
				Type: sqltypes.Int32,
			}, {
				Name: "name_string",
				Type: sqltypes.VarChar,
			}},
		},
		"describe test_table": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("name", "int(11)", false, "", "0"),
				mysql.DescribeTableRow("addr", "int(11)", false, "", "0"),
				mysql.DescribeTableRow("name_string", "varchar(10)", false, "", "foo"),
			},
		},
		mysql.BaseShowTablesForTable("test_table"): {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table", false, ""),
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from test_table": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "pk", false),
				mysql.ShowIndexFromTableRow("test_table", false, "index", 1, "name", true),
				mysql.ShowIndexFromTableRow("test_table", false, "name_string_INDEX", 1, "name_string", true),
			},
		},
		// Define table that uses keywords to test SplitQuery escaping.
		"select * from `order` where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "by",
				Type: sqltypes.Int32,
			}, {
				Name: "value",
				Type: sqltypes.Int32,
			}},
		},
		"describe `order`": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("by", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("value", "int(11)", false, "", "0"),
			},
		},
		mysql.BaseShowTablesForTable("order"): {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("order", false, ""),
			},
		},
		"show index from `order`": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("order", true, "PRIMARY", 1, "by", false),
			},
		},
		"select * from msg where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "time_scheduled",
				Type: sqltypes.Int32,
			}, {
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "time_next",
				Type: sqltypes.Int64,
			}, {
				Name: "epoch",
				Type: sqltypes.Int64,
			}, {
				Name: "time_created",
				Type: sqltypes.Int64,
			}, {
				Name: "time_acked",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.Int64,
			}},
		},
		"describe msg": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("time_scheduled", "int(11)", false, "", "0"),
				mysql.DescribeTableRow("id", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_next", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("epoch", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_created", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_acked", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("message", "bigint(20)", false, "", "0"),
			},
		},
		"show index from msg": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("msg", true, "PRIMARY", 1, "time_scheduled", false),
				mysql.ShowIndexFromTableRow("msg", true, "PRIMARY", 2, "id", false),
			},
		},
		mysql.BaseShowTablesForTable("msg"): {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		},
		"begin":    {},
		"commit":   {},
		"rollback": {},
		fmt.Sprintf(sqlReadAllRedo, "`_vt`", "`_vt`"): {},
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
