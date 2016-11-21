// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/vtgate/fakerpcvtgateconn"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
)

func TestTxExecutorEmptyPrepare(t *testing.T) {
	txe, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTransaction(tsv)
	err := txe.Prepare(txid, "aa")
	if err != nil {
		t.Error(err)
	}
	// Nothing should be prepared.
	if len(txe.te.preparedPool.conns) != 0 {
		t.Errorf("len(txe.te.preparedPool.conns): %d, want 0", len(txe.te.preparedPool.conns))
	}
}

func TestTxExecutorPrepare(t *testing.T) {
	txe, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	err := txe.Prepare(txid, "aa")
	if err != nil {
		t.Error(err)
	}
	err = txe.RollbackPrepared("aa", 1)
	if err != nil {
		t.Error(err)
	}
	// A retry should still succeed.
	err = txe.RollbackPrepared("aa", 1)
	if err != nil {
		t.Error(err)
	}
	// A retry  with no original id should also succeed.
	err = txe.RollbackPrepared("aa", 0)
	if err != nil {
		t.Error(err)
	}
}

func TestTxExecutorPrepareNotInTx(t *testing.T) {
	txe, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	err := txe.Prepare(0, "aa")
	want := "not_in_tx: Transaction 0: not found"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTxExecutorPreparePoolFail(t *testing.T) {
	txe, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	txid1 := newTxForPrep(tsv)
	txid2 := newTxForPrep(tsv)
	err := txe.Prepare(txid1, "aa")
	if err != nil {
		t.Error(err)
	}
	defer txe.RollbackPrepared("aa", 0)
	err = txe.Prepare(txid2, "bb")
	want := "prepared transactions exceeded limit"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Prepare err: %v, must contain %s", err, want)
	}
}

func TestTxExecutorPrepareRedoBeginFail(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	db.AddRejectedQuery("begin", errors.New("begin fail"))
	err := txe.Prepare(txid, "aa")
	defer txe.RollbackPrepared("aa", 0)
	want := "error: error: begin fail"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTxExecutorPrepareRedoFail(t *testing.T) {
	txe, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	err := txe.Prepare(txid, "bb")
	defer txe.RollbackPrepared("bb", 0)
	want := "is not supported"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Prepare err: %v, must contain %s", err, want)
	}
}

func TestTxExecutorPrepareRedoCommitFail(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	db.AddRejectedQuery("commit", errors.New("commit fail"))
	err := txe.Prepare(txid, "aa")
	defer txe.RollbackPrepared("aa", 0)
	want := "error: error: commit fail"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTxExecutorCommit(t *testing.T) {
	txe, tsv, _ := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	err := txe.Prepare(txid, "aa")
	if err != nil {
		t.Error(err)
	}
	err = txe.CommitPrepared("aa")
	if err != nil {
		t.Error(err)
	}
	// Commiting an absent transaction should succeed.
	err = txe.CommitPrepared("bb")
	if err != nil {
		t.Error(err)
	}
}

func TestTxExecutorCommitRedoFail(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	// Allow all additions to redo logs to succeed
	db.AddQueryPattern("insert into `_vt`\\.redo_log_transaction.*", &sqltypes.Result{})
	err := txe.Prepare(txid, "bb")
	if err != nil {
		t.Error(err)
	}
	defer txe.RollbackPrepared("bb", 0)
	db.AddQuery("update `_vt`.redo_log_transaction set state = 'Failed' where dtid = 'bb'", &sqltypes.Result{})
	err = txe.CommitPrepared("bb")
	want := "is not supported"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("txe.CommitPrepared err: %v, must contain %s", err, want)
	}
	// A retry should fail differently.
	err = txe.CommitPrepared("bb")
	want = "cannot commit dtid bb, state: failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("txe.CommitPrepared err: %v, must contain %s", err, want)
	}
}

func TestTxExecutorCommitRedoCommitFail(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	err := txe.Prepare(txid, "aa")
	if err != nil {
		t.Error(err)
	}
	defer txe.RollbackPrepared("aa", 0)
	db.AddRejectedQuery("commit", errors.New("commit fail"))
	err = txe.CommitPrepared("aa")
	want := "error: error: commit fail"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTxExecutorRollbackBeginFail(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	err := txe.Prepare(txid, "aa")
	if err != nil {
		t.Error(err)
	}
	db.AddRejectedQuery("begin", errors.New("begin fail"))
	err = txe.RollbackPrepared("aa", txid)
	want := "error: error: begin fail"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestTxExecutorRollbackRedoFail(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	txid := newTxForPrep(tsv)
	// Allow all additions to redo logs to succeed
	db.AddQueryPattern("insert into `_vt`\\.redo_log_transaction.*", &sqltypes.Result{})
	err := txe.Prepare(txid, "bb")
	if err != nil {
		t.Error(err)
	}
	err = txe.RollbackPrepared("bb", txid)
	want := "is not supported"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Prepare err: %v, must contain %s", err, want)
	}
}

func TestExecutorCreateTransaction(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()

	db.AddQueryPattern("insert into `_vt`\\.transaction\\(dtid, state, time_created, time_updated\\) values \\('aa', 'Prepare',.*", &sqltypes.Result{})
	db.AddQueryPattern("insert into `_vt`\\.participant\\(dtid, id, keyspace, shard\\) values \\('aa', 1,.*", &sqltypes.Result{})
	err := txe.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "t1",
		Shard:    "0",
	}})
	if err != nil {
		t.Error(err)
	}
}

func TestExecutorStartCommit(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()

	commitTransition := "update `_vt`.transaction set state = 'Commit' where dtid = 'aa' and state = 'Prepare'"
	db.AddQuery(commitTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := txe.StartCommit(txid, "aa")
	if err != nil {
		t.Error(err)
	}

	db.AddQuery(commitTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = txe.StartCommit(txid, "aa")
	want := "error: could not transition to Commit: aa"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestExecutorSetRollback(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()

	rollbackTransition := "update `_vt`.transaction set state = 'Rollback' where dtid = 'aa' and state = 'Prepare'"
	db.AddQuery(rollbackTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(tsv)
	err := txe.SetRollback("aa", txid)
	if err != nil {
		t.Error(err)
	}

	db.AddQuery(rollbackTransition, &sqltypes.Result{})
	txid = newTxForPrep(tsv)
	err = txe.SetRollback("aa", txid)
	want := "error: could not transition to Rollback: aa"
	if err == nil || err.Error() != want {
		t.Errorf("Prepare err: %v, want %s", err, want)
	}
}

func TestExecutorConcludeTransaction(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()

	db.AddQuery("delete from `_vt`.transaction where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("delete from `_vt`.participant where dtid = 'aa'", &sqltypes.Result{})
	err := txe.ConcludeTransaction("aa")
	if err != nil {
		t.Error(err)
	}
}

func TestExecutorReadTransaction(t *testing.T) {
	txe, tsv, db := newTestTxExecutor()
	defer tsv.StopService()

	db.AddQuery("select dtid, state, time_created, time_updated from `_vt`.transaction where dtid = 'aa'", &sqltypes.Result{})
	got, err := txe.ReadTransaction("aa")
	if err != nil {
		t.Error(err)
	}
	want := &querypb.TransactionMetadata{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("aa")),
			sqltypes.MakeString([]byte("Prepare")),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("2")),
		}},
	}
	db.AddQuery("select dtid, state, time_created, time_updated from `_vt`.transaction where dtid = 'aa'", txResult)
	db.AddQuery("select keyspace, shard from `_vt`.participant where dtid = 'aa'", &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("test1")),
			sqltypes.MakeString([]byte("0")),
		}, {
			sqltypes.MakeString([]byte("test2")),
			sqltypes.MakeString([]byte("1")),
		}},
	})
	got, err = txe.ReadTransaction("aa")
	if err != nil {
		t.Error(err)
	}
	want = &querypb.TransactionMetadata{
		Dtid:        "aa",
		State:       querypb.TransactionState_PREPARE,
		TimeCreated: 1,
		TimeUpdated: 2,
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

	txResult.Rows[0][1] = sqltypes.MakeString([]byte("Commit"))
	db.AddQuery("select dtid, state, time_created, time_updated from `_vt`.transaction where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_COMMIT
	got, err = txe.ReadTransaction("aa")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult.Rows[0][1] = sqltypes.MakeString([]byte("Rollback"))
	db.AddQuery("select dtid, state, time_created, time_updated from `_vt`.transaction where dtid = 'aa'", txResult)
	want.State = querypb.TransactionState_ROLLBACK
	got, err = txe.ReadTransaction("aa")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}
}

// These vars and types are used only for TestExecutorResolveTransaction
var dtidCh = make(chan string)

type FakeVTGateConn struct {
	fakerpcvtgateconn.FakeVTGateConn
}

func (conn *FakeVTGateConn) ResolveTransaction(ctx context.Context, dtid string) error {
	dtidCh <- dtid
	return nil
}

func TestExecutorResolveTransaction(t *testing.T) {
	protocol := "resolveTest"
	var save string
	save, *vtgateconn.VtgateProtocol = *vtgateconn.VtgateProtocol, protocol
	defer func() { *vtgateconn.VtgateProtocol = save }()

	vtgateconn.RegisterDialer(protocol, func(context.Context, string, time.Duration) (vtgateconn.Impl, error) {
		return &FakeVTGateConn{
			FakeVTGateConn: fakerpcvtgateconn.FakeVTGateConn{},
		}, nil
	})
	_, tsv, db := newShortAgeExecutor()
	defer tsv.StopService()
	want := "aa"
	db.AddQueryPattern(
		"select dtid, time_created from `_vt`\\.transaction where time_created.*",
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeString([]byte(want)),
				sqltypes.MakeString([]byte("1")),
			}},
		})
	got := <-dtidCh
	if got != want {
		t.Errorf("ResolveTransaction: %s, want %s", got, want)
	}
}

func TestNoTwopc(t *testing.T) {
	txe, tsv, _ := newNoTwopcExecutor()
	defer tsv.StopService()

	testcases := []struct {
		desc string
		fun  func() error
	}{{
		desc: "Prepare",
		fun:  func() error { return txe.Prepare(1, "aa") },
	}, {
		desc: "CommitPrepared",
		fun:  func() error { return txe.CommitPrepared("aa") },
	}, {
		desc: "RollbackPrepared",
		fun:  func() error { return txe.RollbackPrepared("aa", 1) },
	}, {
		desc: "CreateTransaction",
		fun:  func() error { return txe.CreateTransaction("aa", nil) },
	}, {
		desc: "StartCommit",
		fun:  func() error { return txe.StartCommit(1, "aa") },
	}, {
		desc: "SetRollback",
		fun:  func() error { return txe.SetRollback("aa", 1) },
	}, {
		desc: "ConcludeTransaction",
		fun:  func() error { return txe.ConcludeTransaction("aa") },
	}, {
		desc: "ReadTransaction",
		fun: func() error {
			_, err := txe.ReadTransaction("aa")
			return err
		},
	}}

	want := "error: 2pc is not enabled"
	for _, tc := range testcases {
		err := tc.fun()
		if err == nil || err.Error() != want {
			t.Errorf("%s: %v, want %s", tc.desc, err, want)
		}
	}
}

func newTestTxExecutor() (txe *TxExecutor, tsv *TabletServer, db *fakesqldb.DB) {
	db = setUpQueryExecutorTest()
	ctx := context.Background()
	logStats := NewLogStats("TestTxExecutor", ctx)
	tsv = newTestTabletServer(ctx, smallTxPool, db)
	db.AddQueryPattern("insert into `_vt`\\.redo_log_transaction\\(dtid, state, time_created\\) values \\('aa', 'Prepared',.*", &sqltypes.Result{})
	db.AddQueryPattern("insert into `_vt`\\.redo_log_statement.*", &sqltypes.Result{})
	db.AddQuery("delete from `_vt`.redo_log_transaction where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("delete from `_vt`.redo_log_statement where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	return &TxExecutor{
		ctx:      ctx,
		logStats: logStats,
		te:       tsv.te,
	}, tsv, db
}

// newShortAgeExecutor is same as newTestTxExecutor, but shorter transaction abandon age.
func newShortAgeExecutor() (txe *TxExecutor, tsv *TabletServer, db *fakesqldb.DB) {
	db = setUpQueryExecutorTest()
	ctx := context.Background()
	logStats := NewLogStats("TestTxExecutor", ctx)
	tsv = newTestTabletServer(ctx, smallTxPool|shortTwopcAge, db)
	db.AddQueryPattern("insert into `_vt`\\.redo_log_transaction\\(dtid, state, time_created\\) values \\('aa', 'Prepared',.*", &sqltypes.Result{})
	db.AddQueryPattern("insert into `_vt`\\.redo_log_statement.*", &sqltypes.Result{})
	db.AddQuery("delete from `_vt`.redo_log_transaction where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("delete from `_vt`.redo_log_statement where dtid = 'aa'", &sqltypes.Result{})
	db.AddQuery("update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	return &TxExecutor{
		ctx:      ctx,
		logStats: logStats,
		te:       tsv.te,
	}, tsv, db
}

// newNoTwopcExecutor is same as newTestTxExecutor, but 2pc disabled.
func newNoTwopcExecutor() (txe *TxExecutor, tsv *TabletServer, db *fakesqldb.DB) {
	db = setUpQueryExecutorTest()
	ctx := context.Background()
	logStats := NewLogStats("TestTxExecutor", ctx)
	tsv = newTestTabletServer(ctx, noTwopc, db)
	return &TxExecutor{
		ctx:      ctx,
		logStats: logStats,
		te:       tsv.te,
	}, tsv, db
}

// newTxForPrep creates a non-empty transaction.
func newTxForPrep(tsv *TabletServer) int64 {
	txid := newTransaction(tsv)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	_, err := tsv.Execute(context.Background(), &target, "update test_table set name = 2 where pk = 1", nil, txid, nil)
	if err != nil {
		panic(err)
	}
	return txid
}
