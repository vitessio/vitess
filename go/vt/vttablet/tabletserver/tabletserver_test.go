// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"expvar"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/messager"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
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
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	db := setUpTabletServerTest(t)
	defer db.Close()

	want := &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 2,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow("test_table", false, ""),
			// Return a table that tabletserver can't access (the mock will reject all queries to it).
			mysqlconn.BaseShowTablesRow("rejected_table", false, ""),
		},
	}
	db.AddQuery(mysqlconn.BaseShowTables, want)

	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	originalSchemaErrorCount := tabletenv.InternalErrors.Counts()["Schema"]
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 2,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow("rejected_table_1", false, ""),
			mysqlconn.BaseShowTablesRow("rejected_table_2", false, ""),
		},
	}
	db.AddQuery(mysqlconn.BaseShowTables, want)

	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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

func TestTabletServerCheckMysqlInUnintialized(t *testing.T) {
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	// TabletServer start request fail because we are in StateNotConnected;
	// however, isMySQLReachable should return true. Here, we always assume
	// MySQL is healthy unless we've verified it is not.
	if !tsv.isMySQLReachable() {
		t.Fatalf("isMySQLReachable should return true")
	}
	tabletState := expvar.Get("TabletState")
	if tabletState == nil {
		t.Fatal("TabletState should be exposed")
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	query := "select addr from test_table where pk = 1 limit 1000"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery("select addr from test_table where 1 != 1", &sqltypes.Result{})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	dbconfigs = testUtils.newDBConfigs(db)
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
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
	if len(tsv.te.preparedPool.conns) != 1 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 1", len(tsv.te.preparedPool.conns))
	}

	// RollbackPrepared will allow StopService to complete.
	err = tsv.RollbackPrepared(ctx, &target, "aa", 0)
	if err != nil {
		t.Error(err)
	}
	<-ch
	if len(tsv.te.preparedPool.conns) != 0 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 0", len(tsv.te.preparedPool.conns))
	}
}

func TestTabletServerMasterToReplica(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
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

func TestTabletServerReplicaToMaster(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	tpc := tsv.te.twoPC

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{})
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, nil)
	if len(tsv.te.preparedPool.conns) != 0 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 0", len(tsv.te.preparedPool.conns))
	}
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */")),
		}},
	})
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, nil)
	if len(tsv.te.preparedPool.conns) != 1 {
		t.Errorf("len(tsv.te.preparedPool.conns): %d, want 1", len(tsv.te.preparedPool.conns))
	}
	got := tsv.te.preparedPool.conns["dtid0"].Queries
	want := []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Prepared queries: %v, want %v", got, want)
	}
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)

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
			sqltypes.MakeString([]byte("bogus")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("bogus")),
		}, {
			sqltypes.MakeString([]byte("a:b:10")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */")),
		}, {
			sqltypes.MakeString([]byte("a:b:20")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStateFailed))),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("unused")),
		}},
	})
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, nil)
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
	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
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
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("aa")),
			sqltypes.MakeString([]byte(strconv.Itoa(int(querypb.TransactionState_PREPARE)))),
			sqltypes.MakeString([]byte("1")),
		}},
	}
	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", txResult)
	db.AddQuery("select keyspace, shard from `_vt`.dt_participant where dtid = 'aa'", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("test1")),
			sqltypes.MakeString([]byte("0")),
		}, {
			sqltypes.MakeString([]byte("test2")),
			sqltypes.MakeString([]byte("1")),
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
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("aa")),
			sqltypes.MakeString([]byte(strconv.Itoa(int(querypb.TransactionState_COMMIT)))),
			sqltypes.MakeString([]byte("1")),
		}},
	}
	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_COMMIT
	got, err = tsv.ReadTransaction(ctx, &target, "aa")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
			{Type: sqltypes.Uint64},
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("aa")),
			sqltypes.MakeString([]byte(strconv.Itoa(int(querypb.TransactionState_ROLLBACK)))),
			sqltypes.MakeString([]byte("1")),
		}},
	}
	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_ROLLBACK
	got, err = tsv.ReadTransaction(ctx, &target, "aa")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, want) {
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
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	tsv.Begin(ctx, &target)
	_, err = tsv.Begin(ctx, &target)
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
			{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
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
			{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
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
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
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
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
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
			{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)

	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	callback := func(*sqltypes.Result) error { return nil }
	if err := tsv.StreamExecute(ctx, &target, executeSQL, nil, nil, callback); err != nil {
		t.Fatalf("TabletServer.StreamExecute should success: %s, but get error: %v",
			executeSQL, err)
	}
}

func TestTabletServerExecuteBatch(t *testing.T) {
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []querytypes.BoundQuery{}, false, 0, nil)
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
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	// make "commit" query fail
	db.AddRejectedQuery("commit", errRejected)
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	db := setUpTabletServerTest(t)
	defer db.Close()
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
	tsv := NewTabletServerWithNilTopoServer(config)
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
		t.Fatal(err)
	}
}

func TestTabletServerExecuteBatchCallCommitWithoutABegin(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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
	tsv.te.txPool.SetTimeout(10)
}

func TestMessageStream(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	wanterr := "message table nomsg not found"
	if err := tsv.MessageStream(ctx, &target, "nomsg", func(qr *sqltypes.Result) error {
		return nil
	}); err == nil || err.Error() != wanterr {
		t.Errorf("tsv.MessageStream: %v, want %s", err, wanterr)
	}
	ch := make(chan *sqltypes.Result, 1)
	done := make(chan struct{})
	skippedField := false
	go func() {
		if err := tsv.MessageStream(ctx, &target, "msg", func(qr *sqltypes.Result) error {
			if !skippedField {
				skippedField = true
				return nil
			}
			ch <- qr
			return io.EOF
		}); err != nil {
			t.Error(err)
		}
		close(done)
	}()
	// Skip first result (field info).
	newMessages := map[string][]*messager.MessageRow{
		"msg": {
			&messager.MessageRow{ID: sqltypes.MakeString([]byte("1"))},
		},
	}
	// We may have to iterate a few times before the stream kicks in.
	for {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		unlock := tsv.messager.LockDB(newMessages, nil)
		tsv.messager.UpdateCaches(newMessages, nil)
		unlock()
		want := &sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeString([]byte("1")),
				sqltypes.NULL,
			}},
		}
		select {
		case got := <-ch:
			if !reflect.DeepEqual(want, got) {
				t.Errorf("Stream:\n%v, want\n%v", got, want)
			}
		default:
			continue
		}
		break
	}
	// This should not hang.
	<-done
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
	if err == nil || err.Error() != want {
		t.Errorf("tsv.MessageAck(invalid): %v, want %s", err, want)
	}

	_, err = tsv.MessageAck(ctx, &target, "msg", ids)
	want = "query: select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update is not supported on fakesqldb"
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
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("1")),
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
	if err == nil || err.Error() != want {
		t.Errorf("tsv.PostponeMessages(invalid): %v, want %s", err, want)
	}

	_, err = tsv.PostponeMessages(ctx, &target, "msg", []string{"1", "2"})
	want = "query: select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update is not supported"
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
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("1")),
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
	if err == nil || err.Error() != want {
		t.Errorf("tsv.PurgeMessages(invalid): %v, want %s", err, want)
	}

	_, err = tsv.PurgeMessages(ctx, &target, "msg", 0)
	want = "query: select time_scheduled, id from msg where time_scheduled < 0 and time_acked is not null limit 500 for update is not supported"
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
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("1")),
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
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("100")),
			},
		},
	})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table where count > :count"
	splits, err := tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		querytypes.BoundQuery{Sql: sql},
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
	_, err = tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		querytypes.BoundQuery{Sql: sql},
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
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table where count > :count"
	_, err = tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		querytypes.BoundQuery{Sql: sql},
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
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	ctx := context.Background()
	sql := "select * from test_table"
	_, err = tsv.SplitQuery(
		ctx,
		&querypb.Target{TabletType: topodatapb.TabletType_RDONLY},
		querytypes.BoundQuery{Sql: sql},
		// EQUAL_SPLITS should not work on a string column.
		[]string{"name_string"}, /* splitColumns */
		10, /* splitCount */
		0,  /* numRowsPerQueryPart */
		querypb.SplitQueryRequest_EQUAL_SPLITS)
	want :=
		"splitquery: using the EQUAL_SPLITS algorithm in SplitQuery" +
			" requires having a numeric (integral or float) split-column." +
			" Got type: {Name: 'name_string', Type: VARCHAR}"
	if err.Error() != want {
		t.Fatalf("got: %v, want: %v", err, want)
	}
}

func TestHandleExecUnknownError(t *testing.T) {
	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "TestHandleExecError")
	var err error
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	defer tsv.handlePanicAndSendLogStats("select * from test_table", nil, &err, logStats)
	panic("unknown exec error")
}

func TestHandleExecTabletError(t *testing.T) {
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	err := tsv.convertError(
		"select * from test_table",
		nil,
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tablet error"),
	)
	want := "tablet error"
	if err == nil || err.Error() != want {
		t.Errorf("%v, want '%s'", err, want)
	}
}

func TestTerseErrorsNonSQLError(t *testing.T) {
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	err := tsv.convertError(
		"select * from test_table",
		nil,
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tablet error"),
	)
	want := "tablet error"
	if err == nil || err.Error() != want {
		t.Errorf("%v, want '%s'", err, want)
	}
}

func TestTerseErrorsBindVars(t *testing.T) {
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	err := tsv.convertError(
		"select * from test_table",
		map[string]interface{}{"a": 1},
		sqldb.NewSQLError(10, "HY000", "msg"),
	)
	want := "(errno 10) (sqlstate HY000) during query: select * from test_table"
	if err == nil || err.Error() != want {
		t.Errorf("%v, want '%s'", err, want)
	}
}

func TestTerseErrorsNoBindVars(t *testing.T) {
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	err := tsv.convertError("", nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "msg"))
	want := "msg"
	if err == nil || err.Error() != want {
		t.Errorf("%v, want '%s'", err, want)
	}
}

func TestTerseErrorsIgnoreFailoverInProgress(t *testing.T) {
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)

	err := tsv.convertError("select * from test_table where id = :a",
		map[string]interface{}{"a": 1},
		sqldb.NewSQLError(1227, "42000", "failover in progress"),
	)
	if got, want := err.Error(), "failover in progress (errno 1227) (sqlstate 42000)"; got != want {
		t.Fatalf("'failover in progress' text must never be stripped: got = %v, want = %v", got, want)
	}
}

func TestConfigChanges(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
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

	tsv.SetQueryCacheCap(newSize)
	if val := tsv.QueryCacheCap(); val != newSize {
		t.Errorf("QueryCacheCap: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.QueryCacheCap()); val != newSize {
		t.Errorf("tsv.qe.QueryCacheCap: %d, want %d", val, newSize)
	}

	tsv.SetStrictMode(false)
	if val := tsv.qe.strictMode.Get(); val {
		t.Errorf("tsv.qe.strictMode.Get: %d, want false", val)
	}

	tsv.SetAutoCommit(true)
	if val := tsv.qe.autoCommit.Get(); !val {
		t.Errorf("tsv.qe.autoCommit.Get: %d, want true", val)
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
				{sqltypes.MakeString([]byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1"))},
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
		mysqlconn.BaseShowTables: {
			Fields:       mysqlconn.BaseShowTablesFields,
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				mysqlconn.BaseShowTablesRow("test_table", false, ""),
				mysqlconn.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		},
		"describe test_table": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
				mysqlconn.DescribeTableRow("name", "int(11)", false, "", "0"),
				mysqlconn.DescribeTableRow("addr", "int(11)", false, "", "0"),
				mysqlconn.DescribeTableRow("name_string", "varchar(10)", false, "", "foo"),
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from test_table": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "pk", false),
				mysqlconn.ShowIndexFromTableRow("test_table", false, "index", 1, "name", true),
				mysqlconn.ShowIndexFromTableRow("test_table", false, "name_string_INDEX", 1, "name_string", true),
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
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("time_scheduled", "int(11)", false, "", "0"),
				mysqlconn.DescribeTableRow("id", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("time_next", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("epoch", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("time_created", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("time_acked", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("message", "bigint(20)", false, "", "0"),
			},
		},
		"show index from msg": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("msg", true, "PRIMARY", 1, "time_scheduled", false),
				mysqlconn.ShowIndexFromTableRow("msg", true, "PRIMARY", 2, "id", false),
			},
		},
		mysqlconn.BaseShowTablesForTable("msg"): {
			Fields:       mysqlconn.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		},
		"begin":    {},
		"commit":   {},
		"rollback": {},
		mysqlconn.BaseShowTablesForTable("test_table"): {
			Fields:       mysqlconn.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.BaseShowTablesRow("test_table", false, ""),
			},
		},
		fmt.Sprintf(sqlReadAllRedo, "`_vt`", "`_vt`"): {},
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
