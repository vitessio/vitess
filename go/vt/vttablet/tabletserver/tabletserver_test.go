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

package tabletserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/callerid"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestTabletServerHealthz(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	req, err := http.NewRequest("GET", "/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(tsv.healthzHandler)
	handler.ServeHTTP(rr, req)

	expectedCode := http.StatusOK
	if status := rr.Code; status != expectedCode {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, expectedCode)
	}

	expected := "ok\n"
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestTabletServerHealthzNotConnected(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	tsv.sm.SetServingType(topodatapb.TabletType_PRIMARY, time.Time{}, StateNotConnected, "test disconnected")

	req, err := http.NewRequest("GET", "/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(tsv.healthzHandler)
	handler.ServeHTTP(rr, req)

	expectedCode := http.StatusInternalServerError
	if status := rr.Code; status != expectedCode {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, expectedCode)
	}

	expected := "500 internal server error: vttablet is not serving\n"
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestBeginOnReplica(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	db.AddQueryPattern(".*", &sqltypes.Result{})
	target := querypb.Target{TabletType: topodatapb.TabletType_REPLICA}
	err := tsv.SetServingType(topodatapb.TabletType_REPLICA, time.Time{}, true, "")
	require.NoError(t, err)

	options := querypb.ExecuteOptions{
		TransactionIsolation: querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY,
	}
	state, err := tsv.Begin(ctx, &target, &options)
	require.NoError(t, err, "failed to create read only tx on replica")
	assert.Equal(t, tsv.alias, state.TabletAlias, "Wrong tablet alias from Begin")
	_, err = tsv.Rollback(ctx, &target, state.TransactionID)
	require.NoError(t, err, "failed to rollback read only tx")

	// test that we can still create transactions even in read-only mode
	options = querypb.ExecuteOptions{}
	state, err = tsv.Begin(ctx, &target, &options)
	require.NoError(t, err, "expected write tx to be allowed")
	_, err = tsv.Rollback(ctx, &target, state.TransactionID)
	require.NoError(t, err)
}

func TestTabletServerPrimaryToReplica(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	state1, err := tsv.Begin(ctx, &target, nil)
	require.NoError(t, err)

	_, err = tsv.Execute(ctx, &target, "update test_table set `name` = 2 where pk = 1", nil, state1.TransactionID, 0, nil)
	require.NoError(t, err)
	err = tsv.Prepare(ctx, &target, state1.TransactionID, "aa")
	require.NoError(t, err)
	state2, err := tsv.Begin(ctx, &target, nil)
	require.NoError(t, err)

	// This makes txid2 busy
	conn2, err := tsv.te.txPool.GetAndLock(state2.TransactionID, "for query")
	require.NoError(t, err)
	ch := make(chan bool)
	go func() {
		tsv.SetServingType(topodatapb.TabletType_REPLICA, time.Time{}, true, "")
		ch <- true
	}()

	// SetServingType must rollback the prepared transaction,
	// but it must wait for the unprepared (txid2) to become non-busy.
	select {
	case <-ch:
		t.Fatal("ch should not fire")
	case <-time.After(10 * time.Millisecond):
	}
	require.EqualValues(t, 1, tsv.te.txPool.scp.active.Size(), "tsv.te.txPool.scp.active.Size()")

	// Concluding conn2 will allow the transition to go through.
	tsv.te.txPool.RollbackAndRelease(ctx, conn2)
	<-ch
}

func TestTabletServerRedoLogIsKeptBetweenRestarts(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	tsv.SetServingType(topodatapb.TabletType_REPLICA, time.Time{}, true, "")

	turnOnTxEngine := func() {
		tsv.SetServingType(topodatapb.TabletType_PRIMARY, time.Time{}, true, "")
		tsv.TwoPCEngineWait()
	}
	turnOffTxEngine := func() {
		tsv.SetServingType(topodatapb.TabletType_REPLICA, time.Time{}, true, "")
	}

	tpc := tsv.te.twoPC

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{})
	turnOnTxEngine()
	assert.Empty(t, tsv.te.preparedPool.conns, "tsv.te.preparedPool.conns")
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
			sqltypes.NewVarBinary("update test_table set `name` = 2 where pk = 1 limit 10001"),
		}},
	})
	turnOnTxEngine()
	assert.EqualValues(t, 1, len(tsv.te.preparedPool.conns), "len(tsv.te.preparedPool.conns)")
	got := tsv.te.preparedPool.conns["dtid0"].TxProperties().Queries
	want := []string{"update test_table set `name` = 2 where pk = 1 limit 10001"}
	utils.MustMatch(t, want, got, "Prepared queries")
	turnOffTxEngine()
	assert.Empty(t, tsv.te.preparedPool.conns, "tsv.te.preparedPool.conns")

	tsv.te.txPool.scp.lastID.Set(1)
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
			sqltypes.NewVarBinary("update test_table set `name` = 2 where pk = 1 limit 10001"),
		}, {
			sqltypes.NewVarBinary("a:b:20"),
			sqltypes.NewInt64(RedoStateFailed),
			sqltypes.NewVarBinary(""),
			sqltypes.NewVarBinary("unused"),
		}},
	})
	turnOnTxEngine()
	assert.EqualValues(t, 1, len(tsv.te.preparedPool.conns), "len(tsv.te.preparedPool.conns)")
	got = tsv.te.preparedPool.conns["a:b:10"].TxProperties().Queries
	want = []string{"update test_table set `name` = 2 where pk = 1 limit 10001"}
	utils.MustMatch(t, want, got, "Prepared queries")
	wantFailed := map[string]error{"a:b:20": errPrepFailed}
	if !reflect.DeepEqual(tsv.te.preparedPool.reserved, wantFailed) {
		t.Errorf("Failed dtids: %v, want %v", tsv.te.preparedPool.reserved, wantFailed)
	}
	// Verify last id got adjusted.
	assert.EqualValues(t, 20, tsv.te.txPool.scp.lastID.Get(), "tsv.te.txPool.lastID.Get()")
	turnOffTxEngine()
	assert.Empty(t, tsv.te.preparedPool.conns, "tsv.te.preparedPool.conns")
}

func TestTabletServerCreateTransaction(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	db.AddQueryPattern(fmt.Sprintf("insert into _vt\\.dt_state\\(dtid, state, time_created\\) values \\('aa', %d,.*", int(querypb.TransactionState_PREPARE)), &sqltypes.Result{})
	db.AddQueryPattern("insert into _vt\\.dt_participant\\(dtid, id, keyspace, shard\\) values \\('aa', 1,.*", &sqltypes.Result{})
	err := tsv.CreateTransaction(ctx, &target, "aa", []*querypb.Target{{
		Keyspace: "t1",
		Shard:    "0",
	}})
	require.NoError(t, err)
}

func TestTabletServerStartCommit(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	commitTransition := fmt.Sprintf("update _vt.dt_state set state = %d where dtid = 'aa' and state = %d", int(querypb.TransactionState_COMMIT), int(querypb.TransactionState_PREPARE))
	db.AddQuery(commitTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := tsv.StartCommit(ctx, &target, txid, "aa")
	require.NoError(t, err)

	db.AddQuery(commitTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = tsv.StartCommit(ctx, &target, txid, "aa")
	assert.EqualError(t, err, "could not transition to COMMIT: aa", "Prepare err")
}

func TestTabletserverSetRollback(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	rollbackTransition := fmt.Sprintf("update _vt.dt_state set state = %d where dtid = 'aa' and state = %d", int(querypb.TransactionState_ROLLBACK), int(querypb.TransactionState_PREPARE))
	db.AddQuery(rollbackTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := tsv.SetRollback(ctx, &target, "aa", txid)
	require.NoError(t, err)

	db.AddQuery(rollbackTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = tsv.SetRollback(ctx, &target, "aa", txid)
	assert.EqualError(t, err, "could not transition to ROLLBACK: aa", "Prepare err")
}

func TestTabletServerReadTransaction(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = 'aa'", &sqltypes.Result{})
	got, err := tsv.ReadTransaction(ctx, &target, "aa")
	require.NoError(t, err)
	want := &querypb.TransactionMetadata{}
	utils.MustMatch(t, want, got, "ReadTransaction")

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
	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = 'aa'", txResult)
	db.AddQuery("select keyspace, shard from _vt.dt_participant where dtid = 'aa'", &sqltypes.Result{
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
	require.NoError(t, err)
	want = &querypb.TransactionMetadata{
		Dtid:        "aa",
		State:       querypb.TransactionState_PREPARE,
		TimeCreated: 1,
		Participants: []*querypb.Target{{
			Keyspace:   "test1",
			Shard:      "0",
			TabletType: topodatapb.TabletType_PRIMARY,
		}, {
			Keyspace:   "test2",
			Shard:      "1",
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}
	utils.MustMatch(t, want, got, "ReadTransaction")

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
	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_COMMIT
	got, err = tsv.ReadTransaction(ctx, &target, "aa")
	require.NoError(t, err)
	utils.MustMatch(t, want, got, "ReadTransaction")

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
	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_ROLLBACK
	got, err = tsv.ReadTransaction(ctx, &target, "aa")
	require.NoError(t, err)
	utils.MustMatch(t, want, got, "ReadTransaction")
}

func TestTabletServerConcludeTransaction(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	db.AddQuery("delete from _vt.dt_state where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("delete from _vt.dt_participant where dtid = 'aa'", &sqltypes.Result{})
	err := tsv.ConcludeTransaction(ctx, &target, "aa")
	require.NoError(t, err)
}

func TestTabletServerBeginFail(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TxPool.Size = 1
	db, tsv := setupTabletServerTestCustom(t, config, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	tsv.Begin(ctx, &target, nil)
	_, err := tsv.Begin(ctx, &target, nil)
	require.EqualError(t, err, "transaction pool aborting request due to already expired context", "Begin err")
}

func TestTabletServerCommitTransaction(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	state, err := tsv.Begin(ctx, &target, nil)
	require.NoError(t, err)
	_, err = tsv.Execute(ctx, &target, executeSQL, nil, state.TransactionID, 0, nil)
	require.NoError(t, err)
	_, err = tsv.Commit(ctx, &target, state.TransactionID)
	require.NoError(t, err)
}

func TestTabletServerCommiRollbacktFail(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	_, err := tsv.Commit(ctx, &target, -1)
	want := "transaction -1: not found"
	require.Equal(t, want, err.Error())
	_, err = tsv.Rollback(ctx, &target, -1)
	require.Equal(t, want, err.Error())
}

func TestTabletServerRollback(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	state, err := tsv.Begin(ctx, &target, nil)
	require.NoError(t, err)
	if err != nil {
		t.Fatalf("call TabletServer.Begin failed: %v", err)
	}
	_, err = tsv.Execute(ctx, &target, executeSQL, nil, state.TransactionID, 0, nil)
	require.NoError(t, err)
	_, err = tsv.Rollback(ctx, &target, state.TransactionID)
	require.NoError(t, err)
}

func TestTabletServerPrepare(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	state, err := tsv.Begin(ctx, &target, nil)
	require.NoError(t, err)
	_, err = tsv.Execute(ctx, &target, "update test_table set `name` = 2 where pk = 1", nil, state.TransactionID, 0, nil)
	require.NoError(t, err)
	defer tsv.RollbackPrepared(ctx, &target, "aa", 0)
	err = tsv.Prepare(ctx, &target, state.TransactionID, "aa")
	require.NoError(t, err)
}

func TestTabletServerCommitPrepared(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	state, err := tsv.Begin(ctx, &target, nil)
	require.NoError(t, err)
	_, err = tsv.Execute(ctx, &target, "update test_table set `name` = 2 where pk = 1", nil, state.TransactionID, 0, nil)
	require.NoError(t, err)
	err = tsv.Prepare(ctx, &target, state.TransactionID, "aa")
	require.NoError(t, err)
	defer tsv.RollbackPrepared(ctx, &target, "aa", 0)
	err = tsv.CommitPrepared(ctx, &target, "aa")
	require.NoError(t, err)
}

func TestSmallerTimeout(t *testing.T) {
	testcases := []struct {
		t1, t2, want time.Duration
	}{{
		t1:   0,
		t2:   0,
		want: 0,
	}, {
		t1:   0,
		t2:   1 * time.Millisecond,
		want: 1 * time.Millisecond,
	}, {
		t1:   1 * time.Millisecond,
		t2:   0,
		want: 1 * time.Millisecond,
	}, {
		t1:   1 * time.Millisecond,
		t2:   2 * time.Millisecond,
		want: 1 * time.Millisecond,
	}, {
		t1:   2 * time.Millisecond,
		t2:   1 * time.Millisecond,
		want: 1 * time.Millisecond,
	}}
	for _, tcase := range testcases {
		got := smallerTimeout(tcase.t1, tcase.t2)
		assert.Equal(t, tcase.want, got, tcase.t1, tcase.t2)
	}
}

func TestTabletServerReserveConnection(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	db.AddQueryPattern(".*", &sqltypes.Result{})
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	options := &querypb.ExecuteOptions{}

	// reserve a connection
	state, _, err := tsv.ReserveExecute(ctx, &target, nil, "select 42", nil, 0, options)
	require.NoError(t, err)

	// run a query in it
	_, err = tsv.Execute(ctx, &target, "select 42", nil, 0, state.ReservedID, options)
	require.NoError(t, err)

	// release the connection
	err = tsv.Release(ctx, &target, 0, state.ReservedID)
	require.NoError(t, err)
}

func TestTabletServerExecNonExistentConnection(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	db.AddQueryPattern(".*", &sqltypes.Result{})
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	options := &querypb.ExecuteOptions{}

	// run a query with a non-existent reserved id
	_, err := tsv.Execute(ctx, &target, "select 42", nil, 0, 123456, options)
	require.Error(t, err)
}

func TestTabletServerReleaseNonExistentConnection(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	db.AddQueryPattern(".*", &sqltypes.Result{})
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	// run a query with a non-existent reserved id
	err := tsv.Release(ctx, &target, 0, 123456)
	require.Error(t, err)
}

func TestMakeSureToCloseDbConnWhenBeginQueryFails(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	db.AddRejectedQuery("begin", errors.New("it broke"))
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	options := &querypb.ExecuteOptions{}

	// run a query with a non-existent reserved id
	_, _, err := tsv.ReserveBeginExecute(ctx, &target, []string{}, nil, "select 42", nil, options)
	require.Error(t, err)
}

func TestTabletServerReserveAndBeginCommit(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	db.AddQueryPattern(".*", &sqltypes.Result{})
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	options := &querypb.ExecuteOptions{}

	// reserve a connection and a transaction
	state, _, err := tsv.ReserveBeginExecute(ctx, &target, nil, nil, "select 42", nil, options)
	require.NoError(t, err)
	defer func() {
		// fallback so the test finishes quickly
		tsv.Release(ctx, &target, state.TransactionID, state.ReservedID)
	}()

	// run a query in it
	_, err = tsv.Execute(ctx, &target, "select 42", nil, state.TransactionID, state.ReservedID, options)
	require.NoError(t, err)

	// run a query in a non-existent connection
	_, err = tsv.Execute(ctx, &target, "select 42", nil, state.TransactionID, state.ReservedID+100, options)
	require.Error(t, err)
	_, err = tsv.Execute(ctx, &target, "select 42", nil, state.TransactionID+100, state.ReservedID, options)
	require.Error(t, err)

	// commit
	newRID, err := tsv.Commit(ctx, &target, state.TransactionID)
	require.NoError(t, err)
	assert.NotEqual(t, state.ReservedID, newRID)
	rID := newRID

	// begin and rollback
	beginState, _, err := tsv.BeginExecute(ctx, &target, nil, "select 42", nil, rID, options)
	require.NoError(t, err)
	assert.Equal(t, newRID, beginState.TransactionID)
	rID = newRID

	newRID, err = tsv.Rollback(ctx, &target, beginState.TransactionID)
	require.NoError(t, err)
	assert.NotEqual(t, rID, newRID)
	rID = newRID

	// release the connection
	err = tsv.Release(ctx, &target, 0, rID)
	require.NoError(t, err)

	// release the connection again and fail
	err = tsv.Release(ctx, &target, 0, rID)
	require.Error(t, err)
}

func TestTabletServerRollbackPrepared(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	state, err := tsv.Begin(ctx, &target, nil)
	require.NoError(t, err)
	_, err = tsv.Execute(ctx, &target, "update test_table set `name` = 2 where pk = 1", nil, state.TransactionID, 0, nil)
	require.NoError(t, err)
	err = tsv.Prepare(ctx, &target, state.TransactionID, "aa")
	require.NoError(t, err)
	err = tsv.RollbackPrepared(ctx, &target, "aa", state.TransactionID)
	require.NoError(t, err)
}

func TestTabletServerStreamExecute(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	callback := func(*sqltypes.Result) error { return nil }
	if err := tsv.StreamExecute(ctx, &target, executeSQL, nil, 0, 0, nil, callback); err != nil {
		t.Fatalf("TabletServer.StreamExecute should success: %s, but get error: %v",
			executeSQL, err)
	}
}

func TestTabletServerStreamExecuteComments(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "/* leading */ select * from test_table limit 1000 /* trailing */"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	callback := func(*sqltypes.Result) error { return nil }

	ch := tabletenv.StatsLogger.Subscribe("test stats logging")
	defer tabletenv.StatsLogger.Unsubscribe(ch)

	if err := tsv.StreamExecute(ctx, &target, executeSQL, nil, 0, 0, nil, callback); err != nil {
		t.Fatalf("TabletServer.StreamExecute should success: %s, but get error: %v",
			executeSQL, err)
	}

	wantSQL := executeSQL
	select {
	case out := <-ch:
		stats, ok := out.(*tabletenv.LogStats)
		if !ok {
			t.Errorf("Unexpected value in query logs: %#v (expecting value of type %T)", out, &tabletenv.LogStats{})
		}

		if wantSQL != stats.OriginalSQL {
			t.Errorf("logstats: SQL want %s got %s", wantSQL, stats.OriginalSQL)
		}
	default:
		t.Fatal("stats are empty")
	}
}

func TestSerializeTransactionsSameRow(t *testing.T) {
	// This test runs three transaction in parallel:
	// tx1 | tx2 | tx3
	// However, tx1 and tx2 have the same WHERE clause (i.e. target the same row)
	// and therefore tx2 cannot start until the first query of tx1 has finished.
	// The actual execution looks like this:
	// tx1 | tx3
	// tx2
	config := tabletenv.NewDefaultConfig()
	config.HotRowProtection.Mode = tabletenv.Enable
	config.HotRowProtection.MaxConcurrency = 1
	// Reduce the txpool to 2 because we should never consume more than two slots.
	config.TxPool.Size = 2
	db, tsv := setupTabletServerTestCustom(t, config, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	countStart := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and `name` = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and `name` = :name"
	q3 := "update test_table set name_string = 'tx3' where pk = :pk and `name` = :name"
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

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk = 1 and `name` = 1 limit 10001",
		func() {
			close(tx1Started)
			if err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and `name` = 1", 2); err != nil {
				t.Fatal(err)
			}
		})

	// Run all three transactions.
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		state1, _, err := tsv.BeginExecute(ctx, &target, nil, q1, bvTx1, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q1, err)
		}
		if _, err := tsv.Commit(ctx, &target, state1.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		state2, _, err := tsv.BeginExecute(ctx, &target, nil, q2, bvTx2, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q2, err)
		}
		// TODO(mberlin): This should actually be in the BeforeFunc() of tx1 but
		// then the test is hanging. It looks like the MySQL C client library cannot
		// open a second connection while the request of the first connection is
		// still pending.
		<-tx3Finished
		if _, err := tsv.Commit(ctx, &target, state2.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-tx1Started
		state3, _, err := tsv.BeginExecute(ctx, &target, nil, q3, bvTx3, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q3, err)
		}
		if _, err := tsv.Commit(ctx, &target, state3.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
		close(tx3Finished)
	}()

	wg.Wait()

	got, ok := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]
	want := countStart + 1
	if !ok || got != want {
		t.Fatalf("only tx2 should have been serialized: ok? %v got: %v want: %v", ok, got, want)
	}
}

func TestDMLQueryWithoutWhereClause(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.HotRowProtection.Mode = tabletenv.Enable
	config.HotRowProtection.MaxConcurrency = 1
	config.TxPool.Size = 2
	db, tsv := setupTabletServerTestCustom(t, config, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	q := "delete from test_table"

	db.AddQuery(q+" limit 10001", &sqltypes.Result{})

	state, _, err := tsv.BeginExecute(ctx, &target, nil, q, nil, 0, nil)
	require.NoError(t, err)
	_, err = tsv.Commit(ctx, &target, state.TransactionID)
	require.NoError(t, err)
}

func TestSerializeTransactionsSameRow_ConcurrentTransactions(t *testing.T) {
	// This test runs three transaction in parallel:
	// tx1 | tx2 | tx3
	// Out of these three, two can run in parallel because we increased the
	// ConcurrentTransactions limit to 2.
	// One out of the three transaction will always get serialized though.
	config := tabletenv.NewDefaultConfig()
	config.HotRowProtection.Mode = tabletenv.Enable
	config.HotRowProtection.MaxConcurrency = 2
	// Reduce the txpool to 2 because we should never consume more than two slots.
	config.TxPool.Size = 2
	db, tsv := setupTabletServerTestCustom(t, config, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	countStart := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and `name` = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and `name` = :name"
	q3 := "update test_table set name_string = 'tx3' where pk = :pk and `name` = :name"
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
	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk = 1 and `name` = 1 limit 10001",
		func() {
			close(tx1Started)
			<-allQueriesPending
		})

	// Run all three transactions.
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		state1, _, err := tsv.BeginExecute(ctx, &target, nil, q1, bvTx1, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q1, err)
		}

		if _, err := tsv.Commit(ctx, &target, state1.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for tx1 to avoid that this tx could pass tx1, without any contention.
		// In that case, we would see less than 3 pending transactions.
		<-tx1Started

		state2, _, err := tsv.BeginExecute(ctx, &target, nil, q2, bvTx2, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q2, err)
		}

		if _, err := tsv.Commit(ctx, &target, state2.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for tx1 to avoid that this tx could pass tx1, without any contention.
		// In that case, we would see less than 3 pending transactions.
		<-tx1Started

		state3, _, err := tsv.BeginExecute(ctx, &target, nil, q3, bvTx3, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q3, err)
		}

		if _, err := tsv.Commit(ctx, &target, state3.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// At this point, all three transactions should be blocked in BeginExecute()
	// and therefore count as pending transaction by the Hot Row Protection.
	//
	// NOTE: We are not doing more sophisticated synchronizations between the
	// transactions via db.SetBeforeFunc() for the same reason as mentioned
	// in TestSerializeTransactionsSameRow: The MySQL C client does not seem
	// to allow more than connection attempt at a time.
	err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and `name` = 1", 3)
	require.NoError(t, err)
	close(allQueriesPending)

	wg.Wait()

	got, ok := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]
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
	config := tabletenv.NewDefaultConfig()
	config.HotRowProtection.Mode = tabletenv.Enable
	config.HotRowProtection.MaxQueueSize = 1
	config.HotRowProtection.MaxConcurrency = 1
	db, tsv := setupTabletServerTestCustom(t, config, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	countStart := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and `name` = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and `name` = :name"
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

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk = 1 and `name` = 1 limit 10001",
		func() {
			close(tx1Started)
			<-tx2Failed
		})

	// Run the two transactions.
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		state1, _, err := tsv.BeginExecute(ctx, &target, nil, q1, bvTx1, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q1, err)
		}
		if _, err := tsv.Commit(ctx, &target, state1.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// tx2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tx2Failed)

		<-tx1Started
		_, _, err := tsv.BeginExecute(ctx, &target, nil, q2, bvTx2, 0, nil)
		if err == nil || vterrors.Code(err) != vtrpcpb.Code_RESOURCE_EXHAUSTED || err.Error() != "hot row protection: too many queued transactions (1 >= 1) for the same row (table + WHERE clause: 'test_table where pk = 1 and `name` = 1')" {
			t.Errorf("tx2 should have failed because there are too many pending requests: %v", err)
		}
		// No commit necessary because the Begin failed.
	}()

	wg.Wait()

	got := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]
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
	config := tabletenv.NewDefaultConfig()
	config.HotRowProtection.Mode = tabletenv.Enable
	config.HotRowProtection.MaxConcurrency = 1
	db, tsv := setupTabletServerTestCustom(t, config, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	countStart := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]

	// Fake data.
	q1 := "update test_table set name_string = 'tx1' where pk = :pk and `name` = :name"
	q2 := "update test_table set name_string = 'tx2' where pk = :pk and `name` = :name"
	q3 := "update test_table set name_string = 'tx3' where pk = :pk and `name` = :name"
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

	db.SetBeforeFunc("update test_table set name_string = 'tx1' where pk = 1 and `name` = 1 limit 10001",
		func() {
			close(tx1Started)
			// Keep blocking until tx2 was canceled.
			<-tx2Done
		})

	// Run the two transactions.
	wg := sync.WaitGroup{}

	// tx1.
	wg.Add(1)
	go func() {
		defer wg.Done()

		state1, _, err := tsv.BeginExecute(ctx, &target, nil, q1, bvTx1, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q1, err)
		}

		if _, err := tsv.Commit(ctx, &target, state1.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
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

		_, _, err := tsv.BeginExecute(ctxTx2, &target, nil, q2, bvTx2, 0, nil)
		if err == nil || vterrors.Code(err) != vtrpcpb.Code_CANCELED || err.Error() != "context canceled" {
			t.Errorf("tx2 should have failed because the context was canceled: %v", err)
		}
		// No commit necessary because the Begin failed.
	}()

	// tx3.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait until tx1 and tx2 are pending to make the test deterministic.
		if err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and `name` = 1", 2); err != nil {
			t.Error(err)
		}

		state3, _, err := tsv.BeginExecute(ctx, &target, nil, q3, bvTx3, 0, nil)
		if err != nil {
			t.Errorf("failed to execute query: %s: %s", q3, err)
		}

		if _, err := tsv.Commit(ctx, &target, state3.TransactionID); err != nil {
			t.Errorf("call TabletServer.Commit failed: %v", err)
		}
	}()

	// Wait until tx1, 2 and 3 are pending.
	err := waitForTxSerializationPendingQueries(tsv, "test_table where pk = 1 and `name` = 1", 3)
	require.NoError(t, err)
	// Now unblock tx2 and cancel it.
	cancelTx2()

	wg.Wait()

	got, ok := tsv.stats.WaitTimings.Counts()["TabletServerTest.TxSerializer"]
	want := countStart + 2
	if got != want {
		t.Fatalf("tx2 and tx3 should have been serialized: ok? %v got: %v want: %v", ok, got, want)
	}
}

func TestMessageStream(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

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
	require.NoError(t, err)
	if !called {
		t.Fatal("callback was not called for MessageStream")
	}
}

func TestCheckMySQLGauge(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()

	// Check that initially checkMySQLGauge has 0 value
	assert.EqualValues(t, 0, tsv.checkMysqlGaugeFunc.Get())
	tsv.CheckMySQL()
	// After the checkMySQL call checkMySQLGauge should have 1 value
	assert.EqualValues(t, 1, tsv.checkMysqlGaugeFunc.Get())

	// Wait for CheckMySQL to finish.
	// This wait is required because CheckMySQL waits for 1 second after it finishes execution
	// before letting go of the acquired locks.
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf("Timedout waiting for CheckMySQL to finish")
		default:
			if tsv.checkMysqlGaugeFunc.Get() == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestMessageAck(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	ids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("2"),
	}}
	_, err := tsv.MessageAck(ctx, &target, "nonmsg", ids)
	want := "message table nonmsg not found in schema"
	require.Error(t, err)
	require.Contains(t, err.Error(), want)

	_, err = tsv.MessageAck(ctx, &target, "msg", ids)
	want = "query: 'update msg set time_acked"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want)

	db.AddQueryPattern("update msg set time_acked = .*", &sqltypes.Result{RowsAffected: 1})
	count, err := tsv.MessageAck(ctx, &target, "msg", ids)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestRescheduleMessages(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	_, err := tsv.messager.GetGenerator("nonmsg")
	want := "message table nonmsg not found in schema"
	require.Error(t, err)
	require.Contains(t, err.Error(), want)

	gen, err := tsv.messager.GetGenerator("msg")
	require.NoError(t, err)

	_, err = tsv.PostponeMessages(ctx, &target, gen, []string{"1", "2"})
	want = "query: 'update msg set time_next"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want)
	db.AddQueryPattern("update msg set time_next = .*", &sqltypes.Result{RowsAffected: 1})
	count, err := tsv.PostponeMessages(ctx, &target, gen, []string{"1", "2"})
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestPurgeMessages(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	_, err := tsv.messager.GetGenerator("nonmsg")
	want := "message table nonmsg not found in schema"
	require.Error(t, err)
	require.Contains(t, err.Error(), want)

	gen, err := tsv.messager.GetGenerator("msg")
	require.NoError(t, err)

	_, err = tsv.PurgeMessages(ctx, &target, gen, 0)
	want = "query: 'delete from msg where time_acked"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want)

	db.AddQuery("delete from msg where time_acked < 3 limit 500", &sqltypes.Result{RowsAffected: 1})
	count, err := tsv.PurgeMessages(ctx, &target, gen, 3)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestHandleExecUnknownError(t *testing.T) {
	logStats := tabletenv.NewLogStats(ctx, "TestHandleExecError")
	config := tabletenv.NewDefaultConfig()
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	defer tsv.handlePanicAndSendLogStats("select * from test_table", nil, logStats)
	panic("unknown exec error")
}

type testLogger struct {
	logs        []string
	savedInfof  func(format string, args ...any)
	savedErrorf func(format string, args ...any)
}

func newTestLogger() *testLogger {
	tl := &testLogger{
		savedInfof:  log.Infof,
		savedErrorf: log.Errorf,
	}
	log.Infof = tl.recordInfof
	log.Errorf = tl.recordErrorf
	return tl
}

func (tl *testLogger) Close() {
	log.Infof = tl.savedInfof
	log.Errorf = tl.savedErrorf
}

func (tl *testLogger) recordInfof(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	tl.logs = append(tl.logs, msg)
	tl.savedInfof(msg)
}

func (tl *testLogger) recordErrorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	tl.logs = append(tl.logs, msg)
	tl.savedErrorf(msg)
}

func (tl *testLogger) getLog(i int) string {
	if i < len(tl.logs) {
		return tl.logs[i]
	}
	return fmt.Sprintf("ERROR: log %d/%d does not exist", i, len(tl.logs))
}

func TestHandleExecTabletError(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()
	err := tsv.convertAndLogError(
		ctx,
		"select * from test_table",
		nil,
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tablet error"),
		nil,
	)
	want := "tablet error"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want)
	want = "Sql: \"select * from test_table\", BindVars: {}"
	if !strings.Contains(tl.getLog(0), want) {
		t.Errorf("error log %s, want '%s'", tl.getLog(0), want)
	}
}

func TestTerseErrors(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = true
	config.SanitizeLogMessages = false
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()

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

	// The client error message should be redacted (made terse)
	wantErr := "(errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {[REDACTED]}"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error got '%v', want '%s'", err, wantErr)
	}

	// But the log message should NOT be
	wantLog := "(errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {vtg1: \"type:VARCHAR value:\\\"this is kinda long eh\\\"\"}"
	if wantLog != tl.getLog(0) {
		t.Errorf("log got '%s', want '%s'", tl.getLog(0), wantLog)
	}
}

func TestSanitizeLogMessages(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = false
	config.SanitizeLogMessages = true
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()

	sql := "select * from test_table where xyz = :vtg1 order by abc desc"
	sqlErr := mysql.NewSQLError(10, "HY000", "sensitive message")
	sqlErr.Query = "select * from test_table where xyz = 'this is pretty rad my doo, getting swole'"
	err := tsv.convertAndLogError(
		ctx,
		sql,
		map[string]*querypb.BindVariable{"vtg1": sqltypes.StringBindVariable("this is pretty rad my doo, getting swole")},
		sqlErr,
		nil,
	)

	// Error is not sanitized, nor truncated
	wantErr := "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {vtg1: \"type:VARCHAR value:\\\"this is pretty rad my doo, getting swole\\\"\"}"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error got '%v', want '%s'", err, wantErr)
	}

	// But the log message is sanitized
	wantLog := "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {[REDACTED]}"
	if wantLog != tl.getLog(0) {
		t.Errorf("log got '%s', want '%s'", tl.getLog(0), wantLog)
	}
}

func TestTerseErrorsNonSQLError(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = true
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()
	err := tsv.convertAndLogError(
		ctx,
		"select * from test_table",
		nil,
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tablet error"),
		nil,
	)
	want := "tablet error"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want)
	want = "Sql: \"select * from test_table\", BindVars: {}"
	if !strings.Contains(tl.getLog(0), want) {
		t.Errorf("error log %s, want '%s'", tl.getLog(0), want)
	}
}

func TestSanitizeLogMessagesNonSQLError(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = false
	config.SanitizeLogMessages = true
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()
	err := tsv.convertAndLogError(
		ctx,
		"select * from test_table where a = :a",
		map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(5)},
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tablet error"),
		nil,
	)
	want := "tablet error"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want)
	want = "Sql: \"select * from test_table where a = :a\", BindVars: {[REDACTED]}"
	if !strings.Contains(tl.getLog(0), want) {
		t.Errorf("error log %s, want '%s'", tl.getLog(0), want)
	}
}

func TestSanitizeMessagesBindVars(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = true
	config.SanitizeLogMessages = true
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()

	sqlErr := mysql.NewSQLError(10, "HY000", "sensitive message")
	sqlErr.Query = "select * from test_table where a = 1"

	err := tsv.convertAndLogError(
		ctx,
		"select * from test_table where a = :a",
		map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(1)},
		sqlErr,
		nil,
	)
	wantErr := "(errno 10) (sqlstate HY000): Sql: \"select * from test_table where a = :a\", BindVars: {[REDACTED]}"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error got '%v', want '%s'", err, wantErr)
	}

	wantLog := wantErr
	if wantLog != tl.getLog(0) {
		t.Errorf("log got '%s', want '%s'", tl.getLog(0), wantLog)
	}
}

func TestSanitizeMessagesNoBindVars(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = true
	config.SanitizeLogMessages = true
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()
	err := tsv.convertAndLogError(ctx, "", nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "sensitive message"), nil)
	want := "sensitive message"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want)
	want = "Sql: \"\", BindVars: {}"
	if !strings.Contains(tl.getLog(0), want) {
		t.Errorf("error log '%s', want '%s'", tl.getLog(0), want)
	}
}

func TestTruncateMessages(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = false
	// Sanitize the log messages, which means that the bind vars are omitted
	config.SanitizeLogMessages = true
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()

	sqlparser.SetTruncateErrLen(52)
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

	// Error not truncated
	wantErr := "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {vtg1: \"type:VARCHAR value:\\\"this is kinda long eh\\\"\"}"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error got '%v', want '%s'", err, wantErr)
	}

	// but log *is* truncated, and sanitized
	wantLog := "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vt [TRUNCATED]\", BindVars: {[REDACTED]}"
	if wantLog != tl.getLog(0) {
		t.Errorf("log got '%s', want '%s'", tl.getLog(0), wantLog)
	}

	sqlparser.SetTruncateErrLen(140)
	err = tsv.convertAndLogError(
		ctx,
		sql,
		map[string]*querypb.BindVariable{"vtg1": sqltypes.StringBindVariable("this is kinda long eh")},
		sqlErr,
		nil,
	)

	// Error not truncated
	wantErr = "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {vtg1: \"type:VARCHAR value:\\\"this is kinda long eh\\\"\"}"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error got '%v', want '%s'", err, wantErr)
	}

	// Log not truncated, since our limit is large enough now, but it is still sanitized
	wantLog = "sensitive message (errno 10) (sqlstate HY000): Sql: \"select * from test_table where xyz = :vtg1 order by abc desc\", BindVars: {[REDACTED]}"
	if wantLog != tl.getLog(1) {
		t.Errorf("log got '%s', want '%s'", tl.getLog(1), wantLog)
	}
	sqlparser.SetTruncateErrLen(0)
}

func TestTerseErrorsIgnoreFailoverInProgress(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.TerseErrors = true
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	tl := newTestLogger()
	defer tl.Close()
	err := tsv.convertAndLogError(ctx, "select * from test_table where id = :a",
		map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(1)},
		mysql.NewSQLError(1227, mysql.SSClientError, "failover in progress"),
		nil,
	)
	if got, want := err.Error(), "failover in progress (errno 1227) (sqlstate 42000)"; !strings.HasPrefix(got, want) {
		t.Fatalf("'failover in progress' text must never be stripped: got = %v, want = %v", got, want)
	}

	// errors during failover aren't logged at all
	require.Empty(t, tl.logs, "unexpected error log during failover")
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
	config := tabletenv.NewDefaultConfig()
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})

	f, err := os.CreateTemp("", "tableacl")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	_, err = io.WriteString(f, aclJSON1)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	tsv.InitACL(f.Name(), true, 0)

	groups1 := tableacl.GetCurrentConfig().TableGroups
	if name1 := groups1[0].GetName(); name1 != "group01" {
		t.Fatalf("Expected name 'group01', got '%s'", name1)
	}

	f, err = os.Create(f.Name())
	require.NoError(t, err)
	_, err = io.WriteString(f, aclJSON2)
	require.NoError(t, err)

	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond) // wait for signal handler

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
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

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
	if val := int(tsv.te.txPool.scp.Capacity()); val != newSize {
		t.Errorf("tsv.te.txPool.pool.Capacity: %d, want %d", val, newSize)
	}

	tsv.Config().SetTxTimeoutForWorkload(newDuration, querypb.ExecuteOptions_OLTP)
	if val := tsv.Config().TxTimeoutForWorkload(querypb.ExecuteOptions_OLTP); val != newDuration {
		t.Errorf("tsv.TxTimeout: %v, want %v", val, newDuration)
	}
	if val := tsv.te.txPool.env.Config().TxTimeoutForWorkload(querypb.ExecuteOptions_OLTP); val != newDuration {
		t.Errorf("tsv.te.Pool().Timeout: %v, want %v", val, newDuration)
	}

	tsv.SetQueryPlanCacheCap(newSize)
	if val := tsv.QueryPlanCacheCap(); val != newSize {
		t.Errorf("QueryPlanCacheCap: %d, want %d", val, newSize)
	}
	if val := int(tsv.qe.QueryPlanCacheCap()); val != newSize {
		t.Errorf("tsv.qe.QueryPlanCacheCap: %d, want %d", val, newSize)
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
}

func TestReserveBeginExecute(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	state, _, err := tsv.ReserveBeginExecute(ctx, &target, []string{"select 43"}, nil, "select 42", nil, &querypb.ExecuteOptions{})
	require.NoError(t, err)

	assert.Greater(t, state.TransactionID, int64(0), "transactionID")
	assert.Equal(t, state.ReservedID, state.TransactionID, "reservedID should equal transactionID")
	expected := []string{
		"select 43",
		"begin",
		"select 42 from dual limit 10001",
	}
	splitOutput := strings.Split(db.QueryLog(), ";")
	for _, exp := range expected {
		assert.Contains(t, splitOutput, exp, "expected queries to run")
	}
	err = tsv.Release(ctx, &target, state.TransactionID, state.ReservedID)
	require.NoError(t, err)
}

func TestReserveExecute_WithoutTx(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	state, _, err := tsv.ReserveExecute(ctx, &target, []string{"select 43"}, "select 42", nil, 0, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.NotEqual(t, int64(0), state.ReservedID, "reservedID should not be zero")
	expected := []string{
		"select 43",
		"select 42 from dual limit 10001",
	}
	splitOutput := strings.Split(db.QueryLog(), ";")
	for _, exp := range expected {
		assert.Contains(t, splitOutput, exp, "expected queries to run")
	}
	err = tsv.Release(ctx, &target, 0, state.ReservedID)
	require.NoError(t, err)
}

func TestReserveExecute_WithTx(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	beginState, err := tsv.Begin(ctx, &target, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	require.NotEqual(t, int64(0), beginState.TransactionID)
	db.ResetQueryLog()

	reserveState, _, err := tsv.ReserveExecute(ctx, &target, []string{"select 43"}, "select 42", nil, beginState.TransactionID, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	defer tsv.Release(ctx, &target, beginState.TransactionID, reserveState.ReservedID)
	assert.Equal(t, beginState.TransactionID, reserveState.ReservedID, "reservedID should be equal to transactionID")
	expected := []string{
		"select 43",
		"select 42 from dual limit 10001",
	}
	splitOutput := strings.Split(db.QueryLog(), ";")
	for _, exp := range expected {
		assert.Contains(t, splitOutput, exp, "expected queries to run")
	}
	err = tsv.Release(ctx, &target, beginState.TransactionID, reserveState.ReservedID)
	require.NoError(t, err)
}

func TestRelease(t *testing.T) {
	type testcase struct {
		begin, reserve  bool
		expectedQueries []string
		err             bool
	}

	tests := []testcase{{
		begin:           true,
		reserve:         false,
		expectedQueries: []string{"rollback"},
	}, {
		begin:   true,
		reserve: true,
	}, {
		begin:   false,
		reserve: true,
	}, {
		begin:   false,
		reserve: false,
		err:     true,
	}}

	for i, test := range tests {

		name := fmt.Sprintf("%d", i)
		if test.begin {
			name += " begin"
		}
		if test.reserve {
			name += " reserve"
		}
		t.Run(name, func(t *testing.T) {
			db, tsv := setupTabletServerTest(t, "")
			defer tsv.StopService()
			defer db.Close()
			db.AddQueryPattern(".*", &sqltypes.Result{})
			target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

			var transactionID, reservedID int64

			switch {
			case test.begin && test.reserve:
				state, _, err := tsv.ReserveBeginExecute(ctx, &target, []string{"select 1212"}, nil, "select 42", nil, &querypb.ExecuteOptions{})
				require.NoError(t, err)
				transactionID = state.TransactionID
				reservedID = state.ReservedID
				require.NotEqual(t, int64(0), transactionID)
				require.NotEqual(t, int64(0), reservedID)
			case test.begin:
				state, _, err := tsv.BeginExecute(ctx, &target, nil, "select 42", nil, 0, &querypb.ExecuteOptions{})
				require.NoError(t, err)
				transactionID = state.TransactionID
				require.NotEqual(t, int64(0), transactionID)
			case test.reserve:
				state, _, err := tsv.ReserveExecute(ctx, &target, nil, "select 42", nil, 0, &querypb.ExecuteOptions{})
				require.NoError(t, err)
				reservedID = state.ReservedID
				require.NotEqual(t, int64(0), reservedID)
			}

			db.ResetQueryLog()

			err := tsv.Release(ctx, &target, transactionID, reservedID)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Contains(t, db.QueryLog(), strings.Join(test.expectedQueries, ";"), "expected queries to run")
		})
	}
}

func TestReserveStats(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "")
	defer tsv.StopService()
	defer db.Close()

	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	callerID := &querypb.VTGateCallerID{
		Username: "test",
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)

	// Starts reserved connection and transaction
	rbeState, _, err := tsv.ReserveBeginExecute(ctx, &target, nil, nil, "select 42", nil, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])

	// Starts reserved connection
	reState, _, err := tsv.ReserveExecute(ctx, &target, nil, "select 42", nil, 0, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])

	// Use previous reserved connection to start transaction
	reBeState, _, err := tsv.BeginExecute(ctx, &target, nil, "select 42", nil, reState.ReservedID, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])

	// Starts transaction.
	beState, _, err := tsv.BeginExecute(ctx, &target, nil, "select 42", nil, 0, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])

	// Reserved the connection on previous transaction
	beReState, _, err := tsv.ReserveExecute(ctx, &target, nil, "select 42", nil, beState.TransactionID, &querypb.ExecuteOptions{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])

	err = tsv.Release(ctx, &target, rbeState.TransactionID, rbeState.ReservedID)
	require.NoError(t, err)
	assert.EqualValues(t, 2, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])
	assert.EqualValues(t, 1, tsv.te.txPool.env.Stats().UserReservedCount.Counts()["test"])

	err = tsv.Release(ctx, &target, reBeState.TransactionID, reState.ReservedID)
	require.NoError(t, err)
	assert.EqualValues(t, 1, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])
	assert.EqualValues(t, 2, tsv.te.txPool.env.Stats().UserReservedCount.Counts()["test"])

	err = tsv.Release(ctx, &target, beState.TransactionID, beReState.ReservedID)
	require.NoError(t, err)
	assert.Zero(t, tsv.te.txPool.env.Stats().UserActiveReservedCount.Counts()["test"])
	assert.EqualValues(t, 3, tsv.te.txPool.env.Stats().UserReservedCount.Counts()["test"])
	assert.NotEmpty(t, tsv.te.txPool.env.Stats().UserReservedTimesNs.Counts()["test"])
}

func TestDatabaseNameReplaceByKeyspaceNameExecuteMethod(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "keyspaceName")
	setDBName(db, tsv, "databaseInMysql")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type:     sqltypes.VarBinary,
				Database: "databaseInMysql",
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	target := tsv.sm.target

	// Testing Execute Method
	state, err := tsv.Begin(ctx, target, nil)
	require.NoError(t, err)
	res, err := tsv.Execute(ctx, target, executeSQL, nil, state.TransactionID, 0, &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
	})
	require.NoError(t, err)
	for _, field := range res.Fields {
		require.Equal(t, "keyspaceName", field.Database)
	}
	_, err = tsv.Commit(ctx, target, state.TransactionID)
	require.NoError(t, err)
}

func TestDatabaseNameReplaceByKeyspaceNameStreamExecuteMethod(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "keyspaceName")
	setDBName(db, tsv, "databaseInMysql")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type:     sqltypes.VarBinary,
				Database: "databaseInMysql",
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	target := tsv.sm.target

	// Testing StreamExecute Method
	callback := func(res *sqltypes.Result) error {
		for _, field := range res.Fields {
			if field.Database != "" {
				require.Equal(t, "keyspaceName", field.Database)
			}
		}
		return nil
	}
	err := tsv.StreamExecute(ctx, target, executeSQL, nil, 0, 0, &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
	}, callback)
	require.NoError(t, err)
}

func TestDatabaseNameReplaceByKeyspaceNameBeginExecuteMethod(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "keyspaceName")
	setDBName(db, tsv, "databaseInMysql")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type:     sqltypes.VarBinary,
				Database: "databaseInMysql",
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	target := tsv.sm.target

	// Test BeginExecute Method
	state, res, err := tsv.BeginExecute(ctx, target, nil, executeSQL, nil, 0, &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
	})
	require.NoError(t, err)
	for _, field := range res.Fields {
		require.Equal(t, "keyspaceName", field.Database)
	}
	_, err = tsv.Commit(ctx, target, state.TransactionID)
	require.NoError(t, err)
}

func setDBName(db *fakesqldb.DB, tsv *TabletServer, s string) {
	tsv.config.DB.DBName = "databaseInMysql"
	db.SetName("databaseInMysql")
}

func TestDatabaseNameReplaceByKeyspaceNameReserveExecuteMethod(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "keyspaceName")
	setDBName(db, tsv, "databaseInMysql")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type:     sqltypes.VarBinary,
				Database: "databaseInMysql",
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	target := tsv.sm.target

	// Test ReserveExecute
	state, res, err := tsv.ReserveExecute(ctx, target, nil, executeSQL, nil, 0, &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
	})
	require.NoError(t, err)
	for _, field := range res.Fields {
		require.Equal(t, "keyspaceName", field.Database)
	}
	err = tsv.Release(ctx, target, 0, state.ReservedID)
	require.NoError(t, err)
}

func TestDatabaseNameReplaceByKeyspaceNameReserveBeginExecuteMethod(t *testing.T) {
	db, tsv := setupTabletServerTest(t, "keyspaceName")
	setDBName(db, tsv, "databaseInMysql")
	defer tsv.StopService()
	defer db.Close()

	executeSQL := "select * from test_table limit 1000"
	executeSQLResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type:     sqltypes.VarBinary,
				Database: "databaseInMysql",
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("row01")},
		},
	}
	db.AddQuery(executeSQL, executeSQLResult)
	target := tsv.sm.target

	// Test for ReserveBeginExecute
	state, res, err := tsv.ReserveBeginExecute(ctx, target, nil, nil, executeSQL, nil, &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
	})
	require.NoError(t, err)
	for _, field := range res.Fields {
		require.Equal(t, "keyspaceName", field.Database)
	}
	err = tsv.Release(ctx, target, state.TransactionID, state.ReservedID)
	require.NoError(t, err)
}

func setupTabletServerTest(t *testing.T, keyspaceName string) (*fakesqldb.DB, *TabletServer) {
	config := tabletenv.NewDefaultConfig()
	return setupTabletServerTestCustom(t, config, keyspaceName)
}

func setupTabletServerTestCustom(t *testing.T, config *tabletenv.TabletConfig, keyspaceName string) (*fakesqldb.DB, *TabletServer) {
	db := setupFakeDB(t)
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), &topodatapb.TabletAlias{})
	require.Equal(t, StateNotConnected, tsv.sm.State())
	dbcfgs := newDBConfigs(db)
	target := &querypb.Target{
		Keyspace:   keyspaceName,
		TabletType: topodatapb.TabletType_PRIMARY,
	}
	err := tsv.StartService(target, dbcfgs, nil /* mysqld */)
	require.NoError(t, err)
	return db, tsv
}

func setupFakeDB(t *testing.T) *fakesqldb.DB {
	db := fakesqldb.New(t)
	addTabletServerSupportedQueries(db)
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table", false, ""),
			mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
		},
	})
	db.AddQuery("show status like 'Innodb_rows_read'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Innodb_rows_read|0",
	))

	return db
}

func addTabletServerSupportedQueries(db *fakesqldb.DB) {
	queryResultMap := map[string]*sqltypes.Result{
		// Queries for how row protection test (txserializer).
		"update test_table set name_string = 'tx1' where pk = 1 and `name` = 1 limit 10001": {
			RowsAffected: 1,
		},
		"update test_table set name_string = 'tx2' where pk = 1 and `name` = 1 limit 10001": {
			RowsAffected: 1,
		},
		"update test_table set name_string = 'tx3' where pk = 1 and `name` = 1 limit 10001": {
			RowsAffected: 1,
		},
		// tx3, but with different primary key.
		"update test_table set name_string = 'tx3' where pk = 2 and `name` = 1 limit 10001": {
			RowsAffected: 1,
		},
		// queries for twopc
		fmt.Sprintf(sqlCreateSidecarDB, "_vt"):          {},
		fmt.Sprintf(sqlDropLegacy1, "_vt"):              {},
		fmt.Sprintf(sqlDropLegacy2, "_vt"):              {},
		fmt.Sprintf(sqlDropLegacy3, "_vt"):              {},
		fmt.Sprintf(sqlDropLegacy4, "_vt"):              {},
		fmt.Sprintf(sqlCreateTableRedoState, "_vt"):     {},
		fmt.Sprintf(sqlCreateTableRedoStatement, "_vt"): {},
		fmt.Sprintf(sqlCreateTableDTState, "_vt"):       {},
		fmt.Sprintf(sqlCreateTableDTParticipant, "_vt"): {},
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1427325875")},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("STRICT_TRANS_TABLES")},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1")},
			},
		},
		"select @@sql_auto_is_null": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("0")},
			},
		},
		mysql.BaseShowPrimary: {
			Fields: mysql.ShowPrimaryFields,
			Rows: [][]sqltypes.Value{
				mysql.ShowPrimaryRow("test_table", "pk"),
				mysql.ShowPrimaryRow("msg", "id"),
			},
		},
		// queries for TestReserve*
		"select 42 from dual where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "42",
				Type: sqltypes.Int32,
			}},
		},
		"select 42 from dual limit 10001": {
			Fields: []*querypb.Field{{
				Name: "42",
				Type: sqltypes.Int32,
			}},
		},
		"select 43": {
			Fields: []*querypb.Field{{
				Name: "43",
				Type: sqltypes.Int32,
			}},
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
		"select * from msg where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "priority",
				Type: sqltypes.Int64,
			}, {
				Name: "time_next",
				Type: sqltypes.Int64,
			}, {
				Name: "epoch",
				Type: sqltypes.Int64,
			}, {
				Name: "time_acked",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.Int64,
			}},
		},
		"begin":    {},
		"commit":   {},
		"rollback": {},
		fmt.Sprintf(sqlReadAllRedo, "_vt", "_vt"): {},
	}
	for query, result := range queryResultMap {
		db.AddQuery(query, result)
	}
	db.MockQueriesForTable("test_table", &sqltypes.Result{
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
	})
	db.MockQueriesForTable("msg", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "priority",
			Type: sqltypes.Int64,
		}, {
			Name: "time_next",
			Type: sqltypes.Int64,
		}, {
			Name: "epoch",
			Type: sqltypes.Int64,
		}, {
			Name: "time_acked",
			Type: sqltypes.Int64,
		}, {
			Name: "message",
			Type: sqltypes.Int64,
		}},
	})
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
