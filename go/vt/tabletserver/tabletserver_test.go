// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"expvar"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"golang.org/x/net/context"
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
	setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
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
	db := setUpTabletServerTest()
	db.EnableConnFail()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	checkTabletServerState(t, tsv, StateNotConnected)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err == nil {
		t.Fatalf("TabletServer.StartService should fail")
	}
	checkTabletServerState(t, tsv, StateNotConnected)
}

func TestTabletServerAllowQueries(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	checkTabletServerState(t, tsv, StateNotConnected)
	dbconfigs := testUtils.newDBConfigs(db)
	tsv.setState(StateServing)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	tsv.StopService()
	want := "InitDBConfig failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("TabletServer.StartService: %v, must contain %s", err, want)
	}
	tsv.setState(StateShuttingDown)
	err = tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err == nil {
		t.Fatalf("TabletServer.StartService should fail")
	}
	tsv.StopService()
}

func TestTabletServerInitDBConfig(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	tsv.setState(StateServing)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	dbconfigs := testUtils.newDBConfigs(db)
	err := tsv.InitDBConfig(target, dbconfigs, nil)
	want := "InitDBConfig failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("InitDBConfig: %v, must contain %s", err, want)
	}
	tsv.setState(StateNotConnected)
	err = tsv.InitDBConfig(target, dbconfigs, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestDecideAction(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	dbconfigs := testUtils.newDBConfigs(db)
	err := tsv.InitDBConfig(target, dbconfigs, nil)
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
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.InitDBConfig(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
	db := setUpTabletServerTest()

	want := &sqltypes.Result{
		RowsAffected: 2,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte("test_table")),
				sqltypes.MakeString([]byte("USER TABLE")),
				sqltypes.MakeString([]byte("1427325875")),
				sqltypes.MakeString([]byte("")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("3")),
				sqltypes.MakeString([]byte("4")),
				sqltypes.MakeString([]byte("5")),
			},
			// Return a table that tabletserver can't access (the mock will reject all queries to it).
			{
				sqltypes.MakeString([]byte("rejected_table")),
				sqltypes.MakeString([]byte("USER TABLE")),
				sqltypes.MakeString([]byte("1427325876")),
				sqltypes.MakeString([]byte("")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("3")),
				sqltypes.MakeString([]byte("4")),
				sqltypes.MakeString([]byte("5")),
			},
		},
	}
	db.AddQuery(baseShowTables, want)

	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	originalSchemaErrorCount := tsv.qe.queryServiceStats.InternalErrors.Counts()["Schema"]
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	defer tsv.StopService()
	if err != nil {
		t.Fatalf("TabletServer should successfully start even if a table's schema is unloadable, but got error: %v", err)
	}
	newSchemaErrorCount := tsv.qe.queryServiceStats.InternalErrors.Counts()["Schema"]
	schemaErrorDiff := newSchemaErrorCount - originalSchemaErrorCount
	if schemaErrorDiff != 1 {
		t.Errorf("InternalErrors.Schema counter should have increased by 1, instead got %v", schemaErrorDiff)
	}
}

func TestTabletServerAllSchemaFailure(t *testing.T) {
	db := setUpTabletServerTest()
	// Return only tables that tabletserver can't access (the mock will reject all queries to them).
	want := &sqltypes.Result{
		RowsAffected: 2,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte("rejected_table_1")),
				sqltypes.MakeString([]byte("USER TABLE")),
				sqltypes.MakeString([]byte("1427325875")),
				sqltypes.MakeString([]byte("")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("3")),
				sqltypes.MakeString([]byte("4")),
				sqltypes.MakeString([]byte("5")),
			},
			{
				sqltypes.MakeString([]byte("rejected_table_2")),
				sqltypes.MakeString([]byte("USER TABLE")),
				sqltypes.MakeString([]byte("1427325876")),
				sqltypes.MakeString([]byte("")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("3")),
				sqltypes.MakeString([]byte("4")),
				sqltypes.MakeString([]byte("5")),
			},
		},
	}
	db.AddQuery(baseShowTables, want)

	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	defer tsv.StopService()
	// tabletsever shouldn't start if it can't access schema for any tables
	testUtils.checkTabletError(t, err, vtrpcpb.ErrorCode_INTERNAL_ERROR, "could not get schema for any tables")
}

func TestTabletServerCheckMysql(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	defer tsv.StopService()
	if err != nil {
		t.Fatalf("TabletServer.StartService should succeed, but got error: %v", err)
	}
	// make mysql conn fail
	db.EnableConnFail()
	if tsv.isMySQLReachable() {
		t.Fatalf("isMySQLReachable should return false")
	}
}

func TestTabletServerCheckMysqlInUnintialized(t *testing.T) {
	_ = setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnablePublishStats = true
	tsv := NewTabletServer(config)
	// TabletServer start request fail because we are in StateNotConnected;
	// however, isMySQLReachable should return true. Here, we always assume
	// MySQL is healthy unless we've verified it is not.
	if !tsv.isMySQLReachable() {
		t.Fatalf("isMySQLReachable should return true")
	}
	tabletState := expvar.Get(config.StatsPrefix + "TabletState")
	if tabletState == nil {
		t.Fatalf("%sTabletState should be exposed", config.StatsPrefix)
	}
	varzState, err := strconv.Atoi(tabletState.String())
	if err != nil {
		t.Fatalf("invalid state reported by expvar, should be a valid state code, but got: %s", tabletState.String())
	}
	if varzState != StateNotConnected {
		t.Fatalf("queryservice should be in %d state, but exposed varz reports: %d", StateNotConnected, varzState)
	}
}

func TestTabletServerReconnect(t *testing.T) {
	db := setUpTabletServerTest()
	query := "select addr from test_table where pk = 1 limit 1000"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery("select addr from test_table where 1 != 1", &sqltypes.Result{})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
	db.EnableConnFail()
	_, err = tsv.Execute(context.Background(), &target, query, nil, 0, nil)
	if err == nil {
		t.Error("Execute: want error, got nil")
	}
	time.Sleep(50 * time.Millisecond)
	if tsv.GetState() == "SERVING" {
		t.Error("GetState is still SERVING, must be NOT_SERVING")
	}

	// make mysql conn work
	db.DisableConnFail()
	err = tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Error(err)
	}
	_, err = tsv.Execute(context.Background(), &target, query, nil, 0, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestTabletServerTarget(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target1 := querypb.Target{
		Keyspace:   "test_keyspace",
		Shard:      "test_shard",
		TabletType: topodatapb.TabletType_MASTER,
	}
	err := tsv.StartService(target1, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
	want := "Invalid tablet type"
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
	want = "Invalid keyspace bad"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// wrong shard
	target2 = proto.Clone(&target1).(*querypb.Target)
	target2.Shard = "bad"
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	want = "Invalid shard bad"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// no target
	_, err = tsv.Execute(ctx, nil, "select * from test_table limit 1000", nil, 0, nil)
	want = "No target"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// Disallow tx statements if non-master.
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	_, err = tsv.Begin(ctx, &target1)
	want = "transactional statement disallowed on non-master tablet"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
	err = tsv.Commit(ctx, &target1, 1)
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

func TestTabletServerStopWithPrepare(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, _ := newTestTxExecutor()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target)
	if err != nil {
		t.Error(err)
	}
	if _, err := tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, transactionID, nil); err != nil {
		t.Error(err)
	}
	if err = tsv.Prepare(ctx, &target, transactionID, "aa"); err != nil {
		t.Error(err)
	}
	ch := make(chan bool)
	go func() {
		tsv.StopService()
		ch <- true
	}()

	// StopService must wait for the prepared transaction to resolve.
	select {
	case <-ch:
		t.Fatal("ch should not fire")
	case <-time.After(10 * time.Millisecond):
	}
	if len(tsv.qe.preparedPool.conns) != 1 {
		t.Errorf("len(tsv.qe.preparedPool.conns): %d, want 1", len(tsv.qe.preparedPool.conns))
	}

	// RollbackPrepared will allow StopService to complete.
	err = tsv.RollbackPrepared(ctx, &target, "aa", 0)
	if err != nil {
		t.Error(err)
	}
	<-ch
	if len(tsv.qe.preparedPool.conns) != 0 {
		t.Errorf("len(tsv.qe.preparedPool.conns): %d, want 0", len(tsv.qe.preparedPool.conns))
	}
}

func TestTabletServerMasterToReplica(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, _ := newTestTxExecutor()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	txid1, err := tsv.Begin(ctx, &target)
	if err != nil {
		t.Error(err)
	}
	if _, err := tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, txid1, nil); err != nil {
		t.Error(err)
	}
	if err = tsv.Prepare(ctx, &target, txid1, "aa"); err != nil {
		t.Error(err)
	}
	txid2, err := tsv.Begin(ctx, &target)
	if err != nil {
		t.Error(err)
	}
	// This makes txid2 busy
	conn2, err := tsv.qe.txPool.Get(txid2)
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
	if tsv.qe.txPool.activePool.Size() != 1 {
		t.Errorf("len(tsv.qe.txPool.activePool.Size()): %d, want 1", len(tsv.qe.preparedPool.conns))
	}

	// Concluding conn2 will allow the transition to go through.
	tsv.qe.txPool.LocalConclude(ctx, conn2)
	<-ch
}

func TestTabletServerReplicaToMaster(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	tpc := tsv.qe.twoPC

	db.AddQuery(tpc.readPrepared, &sqltypes.Result{})
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, nil)
	if len(tsv.qe.preparedPool.conns) != 0 {
		t.Errorf("len(tsv.qe.preparedPool.conns): %d, want 0", len(tsv.qe.preparedPool.conns))
	}
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)

	db.AddQuery(tpc.readPrepared, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */")),
		}},
	})
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, nil)
	if len(tsv.qe.preparedPool.conns) != 1 {
		t.Errorf("len(tsv.qe.preparedPool.conns): %d, want 1", len(tsv.qe.preparedPool.conns))
	}
	got := tsv.qe.preparedPool.conns["dtid0"].Queries
	want := []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Prepared queries: %v, want %v", got, want)
	}
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)

	// Ensure we continue past errors.
	db.AddQuery(tpc.readPrepared, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("bogus")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("bogus")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */")),
		}},
	})
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, nil)
	if len(tsv.qe.preparedPool.conns) != 1 {
		t.Errorf("len(tsv.qe.preparedPool.conns): %d, want 1", len(tsv.qe.preparedPool.conns))
	}
	got = tsv.qe.preparedPool.conns["dtid0"].Queries
	want = []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Prepared queries: %v, want %v", got, want)
	}
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
}

func TestTabletServerBeginFail(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	ctx, cancel := withTimeout(ctx, 1*time.Nanosecond)
	defer cancel()
	tsv.Begin(ctx, &target)
	_, err = tsv.Begin(ctx, &target)
	want := "tx_pool_full: Transaction pool connection limit exceeded"
	if err == nil || err.Error() != want {
		t.Fatalf("Begin err: %v, want %v", err, want)
	}
}

func TestTabletServerCommitTransaction(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	transactionID, err := tsv.Begin(ctx, &target)
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
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	err = tsv.Commit(ctx, &target, -1)
	want := "not_in_tx: Transaction -1: not found"
	if err == nil || err.Error() != want {
		t.Fatalf("Commit err: %v, want %v", err, want)
	}
	err = tsv.Rollback(ctx, &target, -1)
	if err == nil || err.Error() != want {
		t.Fatalf("Commit err: %v, want %v", err, want)
	}
}

func TestTabletServerRollback(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	transactionID, err := tsv.Begin(ctx, &target)
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
	_, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target)
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
	_, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target)
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
	_, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target)
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
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)

	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sendReply := func(*sqltypes.Result) error { return nil }
	if err := tsv.StreamExecute(ctx, &target, executeSQL, nil, nil, sendReply); err != nil {
		t.Fatalf("TabletServer.StreamExecute should success: %s, but get error: %v",
			executeSQL, err)
	}
}

func TestTabletServerExecuteBatch(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &sqltypes.Result{}
	expanedSQL := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSQL, sqlResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, &target, []querytypes.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, true, 0, nil); err != nil {
		t.Fatalf("TabletServer.ExecuteBatch should success: %v, but get error: %v",
			sql, err)
	}
}

func TestTabletServerExecuteBatchFailEmptyQueryList(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []querytypes.BoundQuery{}, false, 0, nil)
	verifyTabletError(t, err, vtrpcpb.ErrorCode_BAD_INPUT)
}

func TestTabletServerExecuteBatchFailAsTransaction(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []querytypes.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
	}, true, 1, nil)
	verifyTabletError(t, err, vtrpcpb.ErrorCode_BAD_INPUT)
}

func TestTabletServerExecuteBatchBeginFail(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	// make "begin" query fail
	db.AddRejectedQuery("begin", errRejected)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []querytypes.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
	}, false, 0, nil); err == nil {
		t.Fatalf("TabletServer.ExecuteBatch should fail")
	}
}

func TestTabletServerExecuteBatchCommitFail(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	// make "commit" query fail
	db.AddRejectedQuery("commit", errRejected)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []querytypes.BoundQuery{
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
	db := setUpTabletServerTest()
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
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if db.GetQueryCalledNum("rollback") != 0 {
		t.Fatalf("rollback should not be executed.")
	}

	if _, err := tsv.ExecuteBatch(ctx, &target, []querytypes.BoundQuery{
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
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &sqltypes.Result{}
	expanedSQL := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSQL, sqlResult)

	// cause execution error for this particular sql query
	db.AddRejectedQuery(sql, errRejected)

	config := testUtils.newQueryServiceConfig()
	config.EnableAutoCommit = true
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, &target, []querytypes.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, false, 0, nil); err != nil {
		t.Fatalf("TabletServer.ExecuteBatch should succeed")
	}
}

func TestTabletServerExecuteBatchCallCommitWithoutABegin(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []querytypes.BoundQuery{
		{
			Sql:           "commit",
			BindVariables: nil,
		},
	}, false, 0, nil); err == nil {
		t.Fatalf("TabletServer.ExecuteBatch should fail")
	}
}

func TestExecuteBatchNestedTransaction(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &sqltypes.Result{}
	expanedSQL := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSQL, sqlResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.ExecuteBatch(ctx, nil, []querytypes.BoundQuery{
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
	tsv.qe.txPool.SetTimeout(10)
}

func TestTabletServerSplitQuery(t *testing.T) {
	db := setUpTabletServerTest()
	db.AddQuery("SELECT MIN(pk), MAX(pk) FROM test_table", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pk", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("100")),
			},
		},
	})
	db.AddQuery("SELECT pk FROM test_table LIMIT 0", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pk", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			},
		},
	})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table where count > :count"
	if _, err := tsv.SplitQuery(ctx, &target, sql, nil, "", 10); err != nil {
		t.Fatalf("TabletServer.SplitQuery should success: %v, but get error: %v", sql, err)
	}
}

// TODO(erez): Rename to TestTabletServerSplitQuery once migration to SplitQuery is done.
func TestTabletServerSplitQueryV2(t *testing.T) {
	db := setUpTabletServerTest()
	db.AddQuery("SELECT MIN(pk), MAX(pk) FROM test_table", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pk", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("100")),
			},
		},
	})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table where count > :count"
	splits, err := tsv.SplitQueryV2(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		sql,
		nil,        /* bindVariables */
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
	db := setUpTabletServerTest()
	db.AddQuery("SELECT MIN(pk), MAX(pk) FROM test_table", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pk", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("100")),
			},
		},
	})
	db.AddQuery("SELECT pk FROM test_table LIMIT 0", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pk", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			},
		},
	})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	// add limit clause to make SplitQuery fail
	if _, err := tsv.SplitQuery(ctx, nil, "select * from test_table where count > :count limit 1000", nil, "", 10); err == nil {
		t.Fatalf("TabletServer.SplitQuery should fail")
	}
}

// TODO(erez): Rename to TestTabletServerSplitQueryInvalidQuery once migration to SplitQuery
// is done.
func TestTabletServerSplitQueryV2InvalidQuery(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	// SplitQuery should not support SQLs with a LIMIT clause:
	sql := "select * from test_table where count > :count limit 10"
	_, err = tsv.SplitQueryV2(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		sql,
		nil,        /* bindVariables */
		[]string{}, /* splitColumns */
		10,         /* splitCount */
		0,          /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	if err == nil {
		t.Fatalf("TabletServer.SplitQuery should fail")
	}
}

func TestTabletServerSplitQueryInvalidMinMax(t *testing.T) {
	// Tests that split query returns an error when the query is invalid.
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	pkMinMaxQuery := "SELECT MIN(pk), MAX(pk) FROM test_table"
	pkMinMaxQueryResp := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pk", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			// this make SplitQueryFail
			{
				sqltypes.MakeString([]byte("invalid")),
				sqltypes.MakeString([]byte("invalid")),
			},
		},
	}
	db.AddQuery("SELECT pk FROM test_table LIMIT 0", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pk", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			},
		},
	})
	db.AddQuery(pkMinMaxQuery, pkMinMaxQueryResp)

	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	if _, err := tsv.SplitQuery(ctx, nil, "select * from test_table where count > :count", nil, "", 10); err == nil {
		t.Fatalf("TabletServer.SplitQuery should fail")
	}
}

// TODO(erez): Rename to TestTabletServerSplitQueryInvalidParams once migration to SplitQuery
// is done.
func TestTabletServerSplitQueryV2InvalidParams(t *testing.T) {
	// Tests that SplitQuery returns an error when both numRowsPerQueryPart and splitCount are given.
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table where count > :count"
	_, err = tsv.SplitQueryV2(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		sql,
		nil,        /* bindVariables */
		[]string{}, /* splitColumns */
		10,         /* splitCount */
		11,         /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	if err == nil {
		t.Fatalf("TabletServer.SplitQuery should fail")
	}
}

func TestHandleExecUnknownError(t *testing.T) {
	ctx := context.Background()
	logStats := newLogStats("TestHandleExecError", ctx)
	var err error
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	defer tsv.handleError("select * from test_table", nil, &err, logStats)
	panic("unknown exec error")
}

func TestHandleExecTabletError(t *testing.T) {
	ctx := context.Background()
	logStats := newLogStats("TestHandleExecError", ctx)
	var err error
	defer func() {
		want := "fatal: tablet error"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want '%s'", err, want)
		}
	}()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	defer tsv.handleError("select * from test_table", nil, &err, logStats)
	panic(NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "tablet error"))
}

func TestTerseErrorsNonSQLError(t *testing.T) {
	ctx := context.Background()
	logStats := newLogStats("TestHandleExecError", ctx)
	var err error
	defer func() {
		want := "fatal: tablet error"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want '%s'", err, want)
		}
	}()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	tsv.config.TerseErrors = true
	defer tsv.handleError("select * from test_table", nil, &err, logStats)
	panic(NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "tablet error"))
}

func TestTerseErrorsBindVars(t *testing.T) {
	ctx := context.Background()
	logStats := newLogStats("TestHandleExecError", ctx)
	var err error
	defer func() {
		want := "error: (errno 10) (sqlstate HY000) during query: select * from test_table"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want '%s'", err, want)
		}
	}()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	tsv.config.TerseErrors = true
	defer tsv.handleError("select * from test_table", map[string]interface{}{"a": 1}, &err, logStats)
	panic(&TabletError{
		ErrorCode: vtrpcpb.ErrorCode_DEADLINE_EXCEEDED,
		Message:   "msg",
		SQLError:  10,
		SQLState:  "HY000",
	})
}

func TestTerseErrorsNoBindVars(t *testing.T) {
	ctx := context.Background()
	logStats := newLogStats("TestHandleExecError", ctx)
	var err error
	defer func() {
		want := "error: msg"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want '%s'", err, want)
		}
	}()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	tsv.config.TerseErrors = true
	defer tsv.handleError("select * from test_table", nil, &err, logStats)
	panic(&TabletError{
		ErrorCode: vtrpcpb.ErrorCode_DEADLINE_EXCEEDED,
		Message:   "msg",
		SQLError:  10,
	})
}

func TestConfigChanges(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
	if val := int(tsv.qe.connPool.Capacity()); val != newSize {
		t.Errorf("tsv.qe.connPool.Capacity: %d, want %d", val, newSize)
	}

	tsv.SetStreamPoolSize(newSize)
	if val := tsv.StreamPoolSize(); val != newSize {
		t.Errorf("StreamPoolSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.streamConnPool.Capacity()); val != newSize {
		t.Errorf("tsv.qe.streamConnPool.Capacity: %d, want %d", val, newSize)
	}

	tsv.SetTxPoolSize(newSize)
	if val := tsv.TxPoolSize(); val != newSize {
		t.Errorf("TxPoolSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.txPool.pool.Capacity()); val != newSize {
		t.Errorf("tsv.qe.txPool.pool.Capacity: %d, want %d", val, newSize)
	}

	tsv.SetTxTimeout(newDuration)
	if val := tsv.TxTimeout(); val != newDuration {
		t.Errorf("tsv.TxTimeout: %v, want %v", val, newDuration)
	}
	if val := tsv.qe.txPool.Timeout(); val != newDuration {
		t.Errorf("tsv.qe.txPool.Timeout: %v, want %v", val, newDuration)
	}

	tsv.SetQueryCacheCap(newSize)
	if val := tsv.QueryCacheCap(); val != newSize {
		t.Errorf("QueryCacheCap: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.schemaInfo.QueryCacheCap()); val != newSize {
		t.Errorf("tsv.qe.schemaInfo.QueryCacheCap: %d, want %d", val, newSize)
	}

	tsv.SetStrictMode(false)
	if val := tsv.qe.strictMode.Get(); val != 0 {
		t.Errorf("tsv.qe.strictMode.Get: %d, want 0", val)
	}

	tsv.SetAutoCommit(true)
	if val := tsv.qe.autoCommit.Get(); val == 0 {
		t.Errorf("tsv.qe.autoCommit.Get: %d, want non-0", val)
	}

	tsv.SetMaxResultSize(newSize)
	if val := tsv.MaxResultSize(); val != newSize {
		t.Errorf("MaxResultSize: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.maxResultSize.Get()); val != newSize {
		t.Errorf("tsv.qe.maxResultSize.Get: %d, want %d", val, newSize)
	}

	tsv.SetMaxDMLRows(newSize)
	if val := tsv.MaxDMLRows(); val != newSize {
		t.Errorf("MaxDMLRows: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.maxDMLRows.Get()); val != newSize {
		t.Errorf("tsv.qe.maxDMLRows.Get: %d, want %d", val, newSize)
	}
}

func setUpTabletServerTest() *fakesqldb.DB {
	db := fakesqldb.Register()
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
		// queries for twopc
		sqlTurnoffBinlog:                                     {},
		fmt.Sprintf(sqlCreateSidecarDB, "_vt"):               {},
		fmt.Sprintf(sqlCreateTableRedoLogTransaction, "_vt"): {},
		fmt.Sprintf(sqlCreateTableRedoLogStatement, "_vt"):   {},
		fmt.Sprintf(sqlCreateTableTransaction, "_vt"):        {},
		fmt.Sprintf(sqlCreateTableParticipant, "_vt"):        {},
		// queries for schema info
		"select unix_timestamp()": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		"select @@autocommit": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1"))},
			},
		},
		"select * from test_table where 1 != 1": {
			Fields: getTestTableFields(),
		},
		"select * from `test_table` where 1 != 1": {
			Fields: getTestTableFields(),
		},
		baseShowTables: {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("test_table")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeString([]byte("1427325875")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte("2")),
					sqltypes.MakeString([]byte("3")),
					sqltypes.MakeString([]byte("4")),
					sqltypes.MakeString([]byte("5")),
				},
			},
		},
		"describe `test_table`": {
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("addr")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from `test_table`": {
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("INDEX")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		"begin":    {},
		"commit":   {},
		"rollback": {},
		baseShowTables + " and table_name = 'test_table'": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("test_table")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeString([]byte("1427325875")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte("2")),
					sqltypes.MakeString([]byte("3")),
					sqltypes.MakeString([]byte("4")),
					sqltypes.MakeString([]byte("5")),
				},
			},
		},
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
