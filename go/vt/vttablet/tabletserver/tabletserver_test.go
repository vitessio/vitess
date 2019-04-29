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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestTabletServerGetState(t *testing.T) {
	assert := require.New(t)

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
		assert.EqualValues(names[i], tsv.GetState())
	}
	tsv.EnterLameduck()
	assert.EqualValues("NOT_SERVING", tsv.GetState())
}

func TestTabletServerAllowQueriesFailBadConn(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	db.EnableConnFail()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	checkTabletServerState(t, tsv, StateNotConnected)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	assert.Error(tsv.StartService(target, dbcfgs), "TabletServer.StartService should fail")
	checkTabletServerState(t, tsv, StateNotConnected)
}

func TestTabletServerAllowQueries(t *testing.T) {
	assert := require.New(t)
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
	assert.Contains(err.Error(), "InitDBConfig failed")
	tsv.setState(StateShuttingDown)
	assert.Error(tsv.StartService(target, dbcfgs), "tsv.StartService should fail")
	tsv.StopService()
}

func TestTabletServerInitDBConfig(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	tsv.setState(StateServing)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	dbcfgs := testUtils.newDBConfigs(db)
	err := tsv.InitDBConfig(target, dbcfgs)
	assert.Contains(err.Error(), "InitDBConfig failed")
	tsv.setState(StateNotConnected)
	assert.NoError(tsv.InitDBConfig(target, dbcfgs))
}

func TestDecideAction(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	dbcfgs := testUtils.newDBConfigs(db)
	assert.NoError(tsv.InitDBConfig(target, dbcfgs))

	tsv.setState(StateNotConnected)
	action, err := tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	assert.NoError(err)
	assert.EqualValues(action, actionNone, "decideAction")

	tsv.setState(StateNotConnected)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, true, nil)
	assert.NoError(err)
	assert.EqualValues(actionFullStart, action, "decideAction")
	assert.EqualValues(StateTransitioning, tsv.state, "tsv.state")

	tsv.setState(StateNotServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	assert.NoError(err)
	assert.EqualValues(actionNone, action)

	tsv.setState(StateNotServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, true, nil)
	assert.NoError(err)
	assert.EqualValues(actionServeNewType, action, "decideAction")
	assert.EqualValues(StateTransitioning, tsv.state, "tsv.state")

	tsv.setState(StateServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	assert.NoError(err)
	assert.EqualValues(actionGracefulStop, action, "decideAction")
	assert.EqualValues(StateShuttingDown, tsv.state, "tsv.state")

	tsv.setState(StateServing)
	action, err = tsv.decideAction(topodatapb.TabletType_REPLICA, true, nil)
	assert.NoError(err)
	assert.EqualValues(actionServeNewType, action, "decideAction")
	assert.EqualValues(StateTransitioning, tsv.state, "tsv.state")
	tsv.target.TabletType = topodatapb.TabletType_MASTER

	tsv.setState(StateServing)
	action, err = tsv.decideAction(topodatapb.TabletType_MASTER, true, nil)
	assert.NoError(err)
	assert.EqualValues(actionNone, action, "decideAction")
	assert.EqualValues(StateServing, tsv.state, "tsv.state")

	tsv.setState(StateTransitioning)
	_, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "cannot SetServingType", "decideAction")

	tsv.setState(StateShuttingDown)
	_, err = tsv.decideAction(topodatapb.TabletType_MASTER, false, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "cannot SetServingType", "decideAction")
}

func TestSetServingType(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.InitDBConfig(target, dbcfgs)
	assert.NoError(err)

	stateChanged, err := tsv.SetServingType(topodatapb.TabletType_REPLICA, false, nil)
	assert.False(stateChanged, "SetServingType() should NOT have changed the QueryService state, but did")
	assert.NoError(err)
	checkTabletServerState(t, tsv, StateNotConnected)

	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	assert.True(stateChanged, "SetServingType() should have changed the QueryService state, but did not")
	assert.NoError(err)
	checkTabletServerState(t, tsv, StateServing)

	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_RDONLY, true, nil)
	assert.True(stateChanged, "SetServingType() should have changed the tablet type, but did not")
	assert.NoError(err)
	checkTabletServerState(t, tsv, StateServing)

	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_SPARE, false, nil)
	assert.True(stateChanged, "SetServingType() should have changed the QueryService state, but did not")
	assert.NoError(err)
	checkTabletServerState(t, tsv, StateNotServing)

	// Verify that we exit lameduck when SetServingType is called.
	tsv.EnterLameduck()
	assert.EqualValues("NOT_SERVING", tsv.GetState())
	stateChanged, err = tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	assert.True(stateChanged, "SetServingType() should have changed the QueryService state, but did not")
	assert.NoError(err)
	checkTabletServerState(t, tsv, StateServing)
	assert.EqualValues("SERVING", tsv.GetState())

	tsv.StopService()
	checkTabletServerState(t, tsv, StateNotConnected)
}

func TestTabletServerSingleSchemaFailure(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "TabletServer should successfully start even if a table's schema is unloadable, but got error: %v", err)
	defer tsv.StopService()
	newSchemaErrorCount := tabletenv.InternalErrors.Counts()["Schema"]
	schemaErrorDiff := newSchemaErrorCount - originalSchemaErrorCount
	assert.EqualValues(1, schemaErrorDiff, "InternalErrors.Schema counter should have increased by 1")
}

func TestTabletServerAllSchemaFailure(t *testing.T) {
	assert := require.New(t)
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
	assert.Error(err)
	assert.EqualValues("could not get schema for any tables", err.Error())
}

func TestTabletServerCheckMysql(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	defer tsv.StopService()
	assert.NoError(err)
	assert.True(tsv.isMySQLReachable(), "isMySQLReachable should return true")

	stateChanged, err := tsv.SetServingType(topodatapb.TabletType_SPARE, false, nil)
	assert.NoError(err)
	assert.True(stateChanged, "SetServingType() should have changed the QueryService state, but did not")
	assert.True(tsv.isMySQLReachable(), "isMySQLReachable should return true")
	checkTabletServerState(t, tsv, StateNotServing)
}

func TestTabletServerCheckMysqlFailInvalidConn(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	defer tsv.StopService()
	assert.NoError(err, "TabletServer.StartService should succeed, but got error")
	// make mysql conn fail
	db.Close()
	assert.False(tsv.isMySQLReachable())
}

func TestTabletServerReconnect(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err)

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
	assert.NoError(err)
	_, err = tsv.Execute(context.Background(), &target, query, nil, 0, nil)
	assert.NoError(err)
}

func TestTabletServerTarget(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err)
	defer tsv.StopService()
	ctx := context.Background()

	// query that works
	db.AddQuery("select * from test_table limit 1000", &sqltypes.Result{})
	_, err = tsv.Execute(ctx, &target1, "select * from test_table limit 1000", nil, 0, nil)
	assert.NoError(err)

	// wrong tablet type
	target2 := proto.Clone(&target1).(*querypb.Target)
	target2.TabletType = topodatapb.TabletType_REPLICA
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "invalid tablet type")

	// set expected target type to MASTER, but also accept REPLICA
	tsv.SetServingType(topodatapb.TabletType_MASTER, true, []topodatapb.TabletType{topodatapb.TabletType_REPLICA})
	_, err = tsv.Execute(ctx, &target1, "select * from test_table limit 1000", nil, 0, nil)
	assert.NoError(err)
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	assert.NoError(err)

	// wrong keyspace
	target2 = proto.Clone(&target1).(*querypb.Target)
	target2.Keyspace = "bad"
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "invalid keyspace bad")

	// wrong shard
	target2 = proto.Clone(&target1).(*querypb.Target)
	target2.Shard = "bad"
	_, err = tsv.Execute(ctx, target2, "select * from test_table limit 1000", nil, 0, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "invalid shard bad")

	// no target
	_, err = tsv.Execute(ctx, nil, "select * from test_table limit 1000", nil, 0, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "No target")

	// Disallow all if service is stopped.
	tsv.StopService()
	_, err = tsv.Execute(ctx, &target1, "select * from test_table limit 1000", nil, 0, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "operation not allowed in state NOT_SERVING")
}

func TestBeginOnReplica(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err)
	defer tsv.StopService()

	tsv.SetServingType(topodatapb.TabletType_REPLICA, true, nil)
	ctx := context.Background()
	options := querypb.ExecuteOptions{
		TransactionIsolation: querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY,
	}
	txID, err := tsv.Begin(ctx, &target1, &options)
	assert.NoError(err, "failed to create read only tx on replica", err)

	err = tsv.Rollback(ctx, &target1, txID)
	assert.NoError(err, "failed to rollback read only tx")

	// test that RW transactions are refused
	options = querypb.ExecuteOptions{
		TransactionIsolation: querypb.ExecuteOptions_DEFAULT,
	}
	_, err = tsv.Begin(ctx, &target1, &options)
	assert.Error(err, "expected write tx to be refused")
}

func TestTabletServerMasterToReplica(t *testing.T) {
	assert := require.New(t)

	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	txid1, err := tsv.Begin(ctx, &target, nil)
	assert.NoError(err)
	_, err = tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, txid1, nil)
	assert.NoError(err)

	err = tsv.Prepare(ctx, &target, txid1, "aa")
	assert.NoError(err)
	txid2, err := tsv.Begin(ctx, &target, nil)
	assert.NoError(err)
	// This makes txid2 busy
	conn2, err := tsv.te.txPool.Get(txid2, "for query")
	assert.NoError(err)
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
	assert.EqualValues(1, tsv.te.txPool.activePool.Size(), "tsv.te.txPool.activePool.Size()")

	// Concluding conn2 will allow the transition to go through.
	tsv.te.txPool.LocalConclude(ctx, conn2)
	<-ch
}

func TestTabletServerRedoLogIsKeptBetweenRestarts(t *testing.T) {
	assert := require.New(t)
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
	assert.EqualValues(0, len(tsv.te.preparedPool.conns), "len(tsv.te.preparedPool.conns)")
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
	assert.EqualValues(1, len(tsv.te.preparedPool.conns), "len(tsv.te.preparedPool.conns)")
	got := tsv.te.preparedPool.conns["dtid0"].Queries
	want := []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	assert.True(reflect.DeepEqual(got, want), fmt.Sprintf("Prepared queries: %v, want %v", got, want))
	turnOffTxEngine()

	assert.EqualValues(0, len(tsv.te.preparedPool.conns), "len(tsv.te.preparedPool.conns)")

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
	assert.EqualValues(1, len(tsv.te.preparedPool.conns), "len(tsv.te.preparedPool.conns)")
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
	assert.EqualValues(20, tsv.te.txPool.lastID.Get(), "tsv.te.txPool.lastID.Get()")
	turnOffTxEngine()

	assert.EqualValues(0, len(tsv.te.preparedPool.conns), "len(tsv.te.preparedPool.conns)")
}

func TestTabletServerCreateTransaction(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err)
}

func TestTabletServerStartCommit(t *testing.T) {
	assert := require.New(t)

	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	commitTransition := fmt.Sprintf("update `_vt`.dt_state set state = %d where dtid = 'aa' and state = %d", int(querypb.TransactionState_COMMIT), int(querypb.TransactionState_PREPARE))
	db.AddQuery(commitTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := tsv.StartCommit(ctx, &target, txid, "aa")
	assert.NoError(err)

	db.AddQuery(commitTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = tsv.StartCommit(ctx, &target, txid, "aa")
	assert.Error(err)
	assert.EqualValues("could not transition to COMMIT: aa", err.Error())
}

func TestTabletserverSetRollback(t *testing.T) {
	assert := require.New(t)
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	rollbackTransition := fmt.Sprintf("update `_vt`.dt_state set state = %d where dtid = 'aa' and state = %d", int(querypb.TransactionState_ROLLBACK), int(querypb.TransactionState_PREPARE))
	db.AddQuery(rollbackTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := tsv.SetRollback(ctx, &target, "aa", txid)
	assert.NoError(err)

	db.AddQuery(rollbackTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = tsv.SetRollback(ctx, &target, "aa", txid)
	want := "could not transition to ROLLBACK: aa"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTabletServerReadTransaction(t *testing.T) {
	assert := require.New(t)
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	db.AddQuery("select dtid, state, time_created from `_vt`.dt_state where dtid = 'aa'", &sqltypes.Result{})
	got, err := tsv.ReadTransaction(ctx, &target, "aa")
	assert.NoError(err)
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
	assert.NoError(err)
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
	assert.NoError(err)
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
	assert.NoError(err)
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}
}

func TestTabletServerConcludeTransaction(t *testing.T) {
	assert := require.New(t)
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	db.AddQuery("delete from `_vt`.dt_state where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("delete from `_vt`.dt_participant where dtid = 'aa'", &sqltypes.Result{})
	err := tsv.ConcludeTransaction(ctx, &target, "aa")
	assert.NoError(err)
}

func TestTabletServerBeginFail(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err)
	defer tsv.StopService()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	tsv.Begin(ctx, &target, nil)
	_, err = tsv.Begin(ctx, &target, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "transaction pool connection limit exceeded")
}

func TestTabletServerCommitTransaction(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	transactionID, err := tsv.Begin(ctx, &target, nil)
	assert.NoError(err, "call TabletServer.Begin failed")
	_, err = tsv.Execute(ctx, &target, executeSQL, nil, transactionID, nil)
	assert.NoError(err, "failed to execute query: %s", executeSQL)
	err = tsv.Commit(ctx, &target, transactionID)
	assert.NoError(err, "call TabletServer.Commit failed")
}

func TestTabletServerCommiRollbacktFail(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err)
	defer tsv.StopService()
	ctx := context.Background()

	err = tsv.Commit(ctx, &target, -1)
	assert.Error(err)
	assert.EqualValues("transaction -1: not found", err.Error())

	err = tsv.Rollback(ctx, &target, -1)
	assert.Error(err)
	assert.EqualValues("transaction -1: not found", err.Error())
}

func TestTabletServerRollback(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err)
	defer tsv.StopService()
	ctx := context.Background()
	transactionID, err := tsv.Begin(ctx, &target, nil)
	assert.NoError(err, "call TabletServer.Begin failed")
	_, err = tsv.Execute(ctx, &target, executeSQL, nil, transactionID, nil)
	assert.NoError(err, "failed to execute query: %s", executeSQL)
	err = tsv.Rollback(ctx, &target, transactionID)
	assert.NoError(err, "call TabletServer.Rollback failed")
}

func TestTabletServerPrepare(t *testing.T) {
	assert := require.New(t)
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target, nil)
	assert.NoError(err)
	_, err = tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, transactionID, nil)
	assert.NoError(err)
	err = tsv.Prepare(ctx, &target, transactionID, "aa")
	tsv.RollbackPrepared(ctx, &target, "aa", 0)
	assert.NoError(err)
}

func TestTabletServerCommitPrepared(t *testing.T) {
	assert := require.New(t)
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target, nil)
	assert.NoError(err)

	_, err = tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, transactionID, nil)
	assert.NoError(err)

	err = tsv.Prepare(ctx, &target, transactionID, "aa")
	assert.NoError(err)

	defer tsv.RollbackPrepared(ctx, &target, "aa", 0)
	err = tsv.CommitPrepared(ctx, &target, "aa")
	assert.NoError(err)
}

func TestTabletServerRollbackPrepared(t *testing.T) {
	assert := require.New(t)
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	transactionID, err := tsv.Begin(ctx, &target, nil)
	assert.NoError(err)

	_, err = tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, transactionID, nil)
	assert.NoError(err)

	err = tsv.Prepare(ctx, &target, transactionID, "aa")
	assert.NoError(err)

	err = tsv.RollbackPrepared(ctx, &target, "aa", transactionID)
	assert.NoError(err)
}

func TestTabletServerStreamExecute(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err)
	defer tsv.StopService()
	ctx := context.Background()
	callback := func(*sqltypes.Result) error { return nil }
	err = tsv.StreamExecute(ctx, &target, executeSQL, nil, 0, nil, callback)
	assert.NoError(err, "TabletServer.StreamExecute should success: %s, but get error", executeSQL)
}

func TestTabletServerExecuteBatch(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()

	_, err = tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, true, 0, nil)

	assert.NoError(err)
}

func TestTabletServerExecuteBatchFailEmptyQueryList(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{}, false, 0, nil)
	assert.Error(err)
	assert.Contains(err.Error(), "Empty query list")
}

func TestTabletServerExecuteBatchFailAsTransaction(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
	}, true, 1, nil)
	assert.Error(err)
	want := "cannot start a new transaction"
	assert.Contains(err.Error(), want)
}

func TestTabletServerExecuteBatchBeginFail(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
	}, false, 0, nil)
	assert.Error(err)
}

func TestTabletServerExecuteBatchCommitFail(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "begin",
			BindVariables: nil,
		},
		{
			Sql:           "commit",
			BindVariables: nil,
		},
	}, false, 0, nil)
	assert.Error(err)
}

func TestTabletServerExecuteBatchSqlExecFailInTransaction(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	assert.EqualValues(0, db.GetQueryCalledNum("rollback"), "rollback should not be executed.")

	_, err = tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, true, 0, nil)

	assert.Error(err, "TabletServer.ExecuteBatch should fail")
	assert.EqualValues(1, db.GetQueryCalledNum("rollback"), "rollback should be executed only once.")
}

func TestTabletServerExecuteBatchSqlSucceedInTransaction(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, &target, []*querypb.BoundQuery{
		{
			Sql:           sql,
			BindVariables: nil,
		},
	}, false, 0, nil)
	assert.NoError(err)
}

func TestTabletServerExecuteBatchCallCommitWithoutABegin(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
		{
			Sql:           "commit",
			BindVariables: nil,
		},
	}, false, 0, nil)

	assert.Error(err, "TabletServer.ExecuteBatch should fail")
}

func TestExecuteBatchNestedTransaction(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()
	ctx := context.Background()
	_, err = tsv.ExecuteBatch(ctx, nil, []*querypb.BoundQuery{
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
	}, false, 0, nil)
	assert.Error(err, "TabletServer.Execute should fail because of nested transaction")
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
	assert := require.New(t)
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
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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
		assert.NoError(err, "failed to execute %s", q1)
		err = tsv.Commit(ctx, &target, tx1)
		assert.NoError(err, "call TabletServer.Commit failed: %v")
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		_, tx2, err := tsv.BeginExecute(ctx, &target, q2, bvTx2, nil)
		assert.NoError(err, "failed to execute %s", q2)
		// TODO(mberlin): This should actually be in the BeforeFunc() of tx1 but
		// then the test is hanging. It looks like the MySQL C client library cannot
		// open a second connection while the request of the first connection is
		// still pending.
		<-tx3Finished
		err = tsv.Commit(ctx, &target, tx2)
		assert.NoError(err, "call TabletServer.Commit failed")
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		_, tx3, err := tsv.BeginExecute(ctx, &target, q3, bvTx3, nil)
		assert.NoError(err, "failed to execute query: %s", q3)
		err = tsv.Commit(ctx, &target, tx3)
		assert.NoError(err, "call TabletServer.Commit failed: %v", err)
		close(tx3Finished)
	}()

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	assert.True(ok, "failed to get counts")
	assert.EqualValues(countStart+1, got, "only tx2 should have been serialized")
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
	assert := require.New(t)
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
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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
			assert.NoError(waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 2))
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
		assert.NoError(err, "failed to execute query: %s", q1)
		assert.EqualValues(1, len(results))
		assert.EqualValues(1, results[0].RowsAffected)
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

		assert.NoError(err, "failed to execute query: %s", q2)
		assert.EqualValues(1, len(results), results)
		assert.EqualValues(1, results[0].RowsAffected, results)
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
		assert.NoError(err, "failed to execute query: %s", q3)
		assert.EqualValues(1, len(results), results)
		assert.EqualValues(1, results[0].RowsAffected, results)
	}()

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	assert.True(ok)
	assert.EqualValues(countStart+1, got, "only tx2 should have been serialized")
}

func TestSerializeTransactionsSameRow_ConcurrentTransactions(t *testing.T) {
	// This test runs three transaction in parallel:
	// tx1 | tx2 | tx3
	// Out of these three, two can run in parallel because we increased the
	// ConcurrentTransactions limit to 2.
	// One out of the three transaction will always get serialized though.
	assert := require.New(t)
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

	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")

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
		assert.NoError(err, "failed to execute query: %s", q1)
		assert.NoError(tsv.Commit(ctx, &target, tx1), "call TabletServer.Commit failed")
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for tx1 to avoid that this tx could pass tx1, without any contention.
		// In that case, we would see less than 3 pending transactions.
		<-tx1Started

		_, tx2, err := tsv.BeginExecute(ctx, &target, q2, bvTx2, nil)
		assert.NoError(err, "failed to execute query: %s", q2)
		assert.NoError(tsv.Commit(ctx, &target, tx2), "call TabletServer.Commit failed")
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for tx1 to avoid that this tx could pass tx1, without any contention.
		// In that case, we would see less than 3 pending transactions.
		<-tx1Started

		_, tx3, err := tsv.BeginExecute(ctx, &target, q3, bvTx3, nil)
		assert.NoError(err, "failed to execute query: %s", q3)
		assert.NoError(tsv.Commit(ctx, &target, tx3), "call TabletServer.Commit failed")
	}()

	// At this point, all three transactions should be blocked in BeginExecute()
	// and therefore count as pending transaction by the Hot Row Protection.
	//
	// NOTE: We are not doing more sophisticated synchronizations between the
	// transactions via db.SetBeforeFunc() for the same reason as mentioned
	// in TestSerializeTransactionsSameRow: The MySQL C client does not seem
	// to allow more than connection attempt at a time.
	err = waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 3)
	assert.NoError(err)
	close(allQueriesPending)

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	assert.True(ok)
	assert.EqualValues(countStart+1, got, "One out of the three transactions must have waited")
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
	assert := require.New(t)
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
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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
		assert.NoError(err, "failed to execute %s", q1)
		assert.NoError(tsv.Commit(ctx, &target, tx1))
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tx2Failed)

		<-tx1Started
		_, _, err := tsv.BeginExecute(ctx, &target, q2, bvTx2, nil)

		if err == nil || vterrors.Code(err) != vtrpcpb.Code_RESOURCE_EXHAUSTED || err.Error() != "hot row protection: too many queued transactions (1 >= 1) for the same row (table + WHERE clause: 'test_table where pk = 1 and name = 1')" {
			t.Errorf("tx2 should have failed because there are too many pending requests: %v", err)
		}
		// No commit necessary because the Begin failed.
	}()

	wg.Wait()

	got := tabletenv.WaitStats.Counts()["TxSerializer"]
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
	assert := require.New(t)
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
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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
		assert.NoError(err, "failed to execute query: %s", q1)
		assert.EqualValues(1, len(results), "TabletServer.ExecuteBatch returned wrong results: %+v", results)
		assert.EqualValues(1, results[0].RowsAffected)
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
		assert.Error(err)
		assert.EqualValues(vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
		assert.Equal("hot row protection: too many queued transactions (1 >= 1) for the same row (table + WHERE clause: 'test_table where pk = 1 and name = 1')", err.Error(), results)
	}()

	wg.Wait()

	got := tabletenv.WaitStats.Counts()["TxSerializer"]
	assert.EqualValues(countStart+0, got)
}

func TestSerializeTransactionsSameRow_RequestCanceled(t *testing.T) {
	// This test is similar to TestSerializeTransactionsSameRow, but tests only
	// that a queued request unblocks itself when its context is done.
	//
	// tx1 and tx2 run against the same row.
	// tx2 is blocked on tx1. Eventually, tx2 is canceled and its request fails.
	// Only after that tx1 commits and finishes.
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.EnableHotRowProtection = true
	config.HotRowProtectionConcurrentTransactions = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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
		assert.NoError(err, "failed to execute query: %s", q1)
		assert.NoError(tsv.Commit(ctx, &target, tx1), "call TabletServer.Commit failed")
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
		assert.Error(err, "tx2 should have failed because the context was canceled")
		assert.EqualValues(vtrpcpb.Code_CANCELED, vterrors.Code(err))
		assert.Equal("context canceled", err.Error(), "tx2 should have failed because the context was canceled")
		// No commit necessary because the Begin failed.
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait until tx1 and tx2 are pending to make the test deterministic.
		err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 2)
		assert.NoError(err)

		_, tx3, err := tsv.BeginExecute(ctx, &target, q3, bvTx3, nil)
		assert.NoError(err, "failed to execute query: %s", q3)
		assert.NoError(tsv.Commit(ctx, &target, tx3), "call TabletServer.Commit failed")
	}()

	// Wait until tx1, 2 and 3 are pending.
	err = waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and name = 1", 3)
	assert.NoError(err)

	// Now unblock tx2 and cancel it.
	cancelTx2()

	wg.Wait()

	got, ok := tabletenv.WaitStats.Counts()["TxSerializer"]
	assert.True(ok)
	assert.EqualValues(countStart+2, got, "tx2 and tx3 should have been serialized")
}

func TestMessageStream(t *testing.T) {
	assert := require.New(t)
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	err := tsv.MessageStream(ctx, &target, "nomsg", func(qr *sqltypes.Result) error {
		return nil
	})
	wantErr := "table nomsg not found in schema"
	assert.Error(err)
	assert.Equal(err.Error(), wantErr, "tsv.MessageStream")

	// Check that the streaming mechanism works.
	called := false
	err = tsv.MessageStream(ctx, &target, "msg", func(qr *sqltypes.Result) error {
		called = true
		return io.EOF
	})
	assert.NoError(err)
	assert.True(called, "callback was not called for MessageStream")
}

func TestMessageAck(t *testing.T) {
	assert := require.New(t)
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
	assert.Error(err)
	assert.Contains(err.Error(), "message table nonmsg not found in schema", "tsv.MessageAck(invalid)")

	_, err = tsv.MessageAck(ctx, &target, "msg", ids)
	assert.Error(err)
	assert.Contains(err.Error(), "query: 'select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update' is not supported on fakesqldb", "tsv.MessageAck(invalid): %v, want %s")

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
	assert.NoError(err)
	assert.EqualValues(1, count, "count")
}

func TestRescheduleMessages(t *testing.T) {
	assert := require.New(t)
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	_, err := tsv.PostponeMessages(ctx, &target, "nonmsg", []string{"1", "2"})
	assert.Error(err)
	assert.Contains(err.Error(), "message table nonmsg not found in schema", "tsv.PostponeMessages(invalid)")

	_, err = tsv.PostponeMessages(ctx, &target, "msg", []string{"1", "2"})
	assert.Error(err)
	assert.Contains(err.Error(), "query: 'select time_scheduled, id from msg where id in ('1', '2') and time_acked is null limit 10001 for update' is not supported", "tsv.PostponeMessages(invalid)")

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
	assert.NoError(err)
	assert.EqualValues(1, count, "count")
}

func TestPurgeMessages(t *testing.T) {
	assert := require.New(t)
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	ctx := context.Background()
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}

	_, err := tsv.PurgeMessages(ctx, &target, "nonmsg", 0)
	assert.Error(err)
	assert.Contains(err.Error(), "message table nonmsg not found in schema", "tsc.PurgeMessages(invalid)")

	_, err = tsv.PurgeMessages(ctx, &target, "msg", 0)
	assert.Error(err)
	assert.Contains(err.Error(), "query: 'select time_scheduled, id from msg where time_scheduled < 0 and time_acked is not null limit 500 for update' is not supported", "tsv.PurgeMessages()")

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
	assert.NoError(err)
	assert.EqualValues(1, count, "count")
}

func TestTabletServerSplitQuery(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
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
	assert.NoError(err, "TabletServer.SplitQuery should succeed")
	assert.EqualValues(10, len(splits), "splits")
}

func TestTabletServerSplitQueryKeywords(t *testing.T) {
	assert := require.New(t)
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
	assert.NoError(err, "StartService failed")
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
	assert.NoError(err, "TabletServer.SplitQuery should succeed")
	assert.EqualValues(10, len(splits), "splits")
}

func TestTabletServerSplitQueryInvalidQuery(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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
	assert.Error(err, "TabletServer.SplitQuery should fail")
}

func TestTabletServerSplitQueryInvalidParams(t *testing.T) {
	// Tests that SplitQuery returns an error when both numRowsPerQueryPart and splitCount are given.
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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
	assert.Error(err, "TabletServer.SplitQuery should fail")
}

// Tests that using Equal Splits on a string column returns an error.
func TestTabletServerSplitQueryEqualSplitsOnStringColumn(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_RDONLY}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
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

	assert.Error(err)
	assert.Equal(want, err.Error())
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
	assert := require.New(t)
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
	assert.Error(err)
	assert.Contains(err.Error(), "tablet error")
	assert.Contains(getTestLog(0), "Sql: \"select * from test_table\", BindVars: {}")
}

func TestTerseErrorsNonSQLError(t *testing.T) {
	assert := require.New(t)
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
	assert.Error(err)
	assert.Contains(err.Error(), "tablet error")
	assert.Contains(getTestLog(0), "Sql: \"select * from test_table\", BindVars: {}")
}

func TestTerseErrorsBindVars(t *testing.T) {
	assert := require.New(t)
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
	assert.Error(err)
	assert.Equal("(errno 10) (sqlstate HY000): Sql: \"select * from test_table where a = :a\", BindVars: {}", err.Error())
	assert.Equal("sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where a = :a\", BindVars: {a: \"type:INT64 value:\\\"1\\\" \"}", getTestLog(0))
}

func TestTerseErrorsNoBindVars(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TerseErrors = true
	tsv := NewTabletServerWithNilTopoServer(config)
	setupTestLogger()
	defer clearTestLogger()
	err := tsv.convertAndLogError(ctx, "", nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "sensitive message"), nil)
	assert.Error(err)
	assert.Contains(err.Error(), "sensitive message")
	assert.Contains(getTestLog(0), "Sql: \"\", BindVars: {}", "log message")
}

func TestTruncateErrors(t *testing.T) {
	assert := require.New(t)
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
	assert.Error(err)
	assert.Equal("(errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {}", err.Error())

	assert.Equal("sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vt [TRUNCATED]\", BindVars: {vtg1: \"type:VARCHAR value:\\\"t [TRUNCATED]", getTestLog(0), "log message")

	*sqlparser.TruncateErrLen = 140
	err = tsv.convertAndLogError(
		ctx,
		sql,
		map[string]*querypb.BindVariable{"vtg1": sqltypes.StringBindVariable("this is kinda long eh")},
		sqlErr,
		nil,
	)

	assert.Error(err)
	assert.EqualValues("(errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {}", err.Error())

	assert.Equal(
		"sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {vtg1: \"type:VARCHAR value:\\\"this is kinda long eh\\\" \"}",
		getTestLog(1), "log message")

	*sqlparser.TruncateErrLen = 0
}

func TestTerseErrorsIgnoreFailoverInProgress(t *testing.T) {
	assert := require.New(t)
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

	assert.Error(err)
	assert.Regexp("^failover in progress \\(errno 1227\\) \\(sqlstate 42000\\)", err.Error(), "'failover in progress' text must never be stripped")

	// errors during failover aren't logged at all
	assert.Empty(testLogs, "unexpected error log during failover")
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
	assert := require.New(t)
	tableacl.Register("simpleacl", &simpleacl.Factory{})
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)

	f, err := ioutil.TempFile("", "tableacl")
	assert.NoError(err)
	defer os.Remove(f.Name())

	_, err = io.WriteString(f, aclJSON1)
	assert.NoError(err)
	assert.NoError(f.Close())

	tsv.InitACL(f.Name(), true)

	groups1 := tableacl.GetCurrentConfig().TableGroups
	assert.EqualValues("group01", groups1[0].GetName())

	f, err = os.Create(f.Name())
	assert.NoError(err)

	_, err = io.WriteString(f, aclJSON2)
	assert.NoError(err)

	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(25 * time.Millisecond) // wait for signal handler

	groups2 := tableacl.GetCurrentConfig().TableGroups
	assert.EqualValues(1, len(groups2), "Expected only one table group")

	group2 := groups2[0]
	assert.EqualValues("group02", group2.GetName())
	assert.NotNil(group2.GetAdmins(), "Expected 'admins' to exist, but it didn't")
	assert.Nil(group2.GetWriters(), "Expected 'writers' to not exist")
}

func TestConfigChanges(t *testing.T) {
	assert := require.New(t)
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbcfgs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbcfgs)
	assert.NoError(err, "StartService failed")
	defer tsv.StopService()

	newSize := 10

	tsv.SetPoolSize(newSize)
	assert.EqualValues(newSize, tsv.PoolSize(), "PoolSize")
	assert.EqualValues(newSize, tsv.qe.conns.Capacity(), "tsv.qe.conns.Capacity")

	tsv.SetStreamPoolSize(newSize)
	assert.EqualValues(newSize, tsv.StreamPoolSize(), "StreamPoolSize")
	assert.EqualValues(newSize, tsv.qe.streamConns.Capacity(), "tsv.qe.streamConns.Capacity")

	tsv.SetTxPoolSize(newSize)
	assert.EqualValues(newSize, tsv.TxPoolSize(), "TxPoolSize")
	assert.EqualValues(newSize, tsv.te.txPool.conns.Capacity(), "tsv.te.txPool.conns.Capacity")

	newDuration := time.Duration(10 * time.Millisecond)
	tsv.SetTxTimeout(newDuration)
	assert.EqualValues(newDuration, tsv.TxTimeout(), "tsv.TxTimeout")
	assert.EqualValues(newDuration, tsv.te.txPool.Timeout(), "tsv.te.txPool.Timeout")

	tsv.SetQueryPlanCacheCap(newSize)
	assert.EqualValues(newSize, tsv.QueryPlanCacheCap(), "tsv.QueryPlanCacheCap")
	assert.EqualValues(newSize, tsv.qe.QueryPlanCacheCap(), "tsv.qe.QueryPlanCacheCap")

	tsv.SetAutoCommit(true)
	assert.True(tsv.qe.autoCommit.Get(), "tsv.qe.autoCommit.Get")

	tsv.SetMaxResultSize(newSize)
	assert.EqualValues(newSize, tsv.MaxResultSize(), "tsv.MaxResultSize")
	assert.EqualValues(newSize, tsv.qe.maxResultSize.Get(), "tsv.qe.maxResultSize.Get")

	tsv.SetWarnResultSize(newSize)
	assert.EqualValues(newSize, tsv.WarnResultSize(), "tsv.WarnResultSize")
	assert.EqualValues(newSize, tsv.qe.warnResultSize.Get(), "tsv.qe.warnResultSize.Get")

	tsv.SetMaxDMLRows(newSize)
	assert.EqualValues(newSize, tsv.MaxDMLRows(), "tsv.MaxDMLRows")
	assert.EqualValues(newSize, tsv.qe.maxDMLRows.Get(), "tsv.qe.maxDMLRows.Get")
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
