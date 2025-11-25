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
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/event/syslogger"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
)

func TestTxExecutorEmptyPrepare(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()

	// start a transaction.
	txid := newTransaction(tsv, nil)

	// taint the connection.
	sc, err := tsv.te.txPool.GetAndLock(txid, "taint")
	require.NoError(t, err)
	sc.Taint(ctx, tsv.te.reservedConnStats)
	sc.Unlock()

	err = txe.Prepare(txid, "aa")
	require.NoError(t, err)
	// Nothing should be prepared.
	require.Empty(t, txe.te.preparedPool.conns, "txe.te.preparedPool.conns")
	require.False(t, sc.IsInTransaction(), "transaction should be roll back before returning the connection to the pool")
}

func TestExecutorPrepareFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()

	// start a transaction
	txid := newTxForPrep(ctx, tsv)

	// taint the connection.
	sc, err := tsv.te.txPool.GetAndLock(txid, "taint")
	require.NoError(t, err)
	sc.Taint(ctx, tsv.te.reservedConnStats)
	sc.Unlock()

	// try 2pc commit of Metadata Manager.
	err = txe.Prepare(txid, "aa")
	require.EqualError(t, err, "VT10002: atomic distributed transaction not allowed: cannot prepare the transaction on a reserved connection")
}

func TestTxExecutorPrepare(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	err := txe.Prepare(txid, "aa")
	require.NoError(t, err)
	err = txe.RollbackPrepared("aa", 1)
	require.NoError(t, err)
	// A retry should still succeed.
	err = txe.RollbackPrepared("aa", 1)
	require.NoError(t, err)
	// A retry  with no original id should also succeed.
	err = txe.RollbackPrepared("aa", 0)
	require.NoError(t, err)
}

// TestTxExecutorPrepareResevedConn tests the case where a reserved connection is used for prepare.
func TestDTExecutorPrepareResevedConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)

	// Reserve a connection
	txe.te.Reserve(ctx, nil, txid, nil)

	err := txe.Prepare(txid, "aa")
	require.ErrorContains(t, err, "VT10002: atomic distributed transaction not allowed: cannot prepare the transaction on a reserved connection")
}

func TestTxExecutorPrepareNotInTx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, _, _, closer := newTestTxExecutor(t, ctx)
	defer closer()
	err := txe.Prepare(0, "aa")
	require.EqualError(t, err, "transaction 0: not found (potential transaction timeout)")
}

func TestTxExecutorPreparePoolFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid1 := newTxForPrep(ctx, tsv)
	txid2 := newTxForPrep(ctx, tsv)
	err := txe.Prepare(txid1, "aa")
	require.NoError(t, err)
	defer txe.RollbackPrepared("aa", 0)
	err = txe.Prepare(txid2, "bb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "prepared transactions exceeded limit")
}

func TestTxExecutorPrepareRedoBeginFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	db.AddRejectedQuery("begin", errors.New("begin fail"))
	err := txe.Prepare(txid, "aa")
	defer txe.RollbackPrepared("aa", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "begin fail")
}

func TestTxExecutorPrepareRedoFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	err := txe.Prepare(txid, "bb")
	defer txe.RollbackPrepared("bb", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported")
}

func TestTxExecutorPrepareRedoCommitFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	db.AddRejectedQuery("commit", errors.New("commit fail"))
	err := txe.Prepare(txid, "aa")
	defer txe.RollbackPrepared("aa", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit fail")
}

func TestExecutorPrepareRuleFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()

	alterRule := rules.NewQueryRule("disable update", "disable update", rules.QRBuffer)
	alterRule.AddTableCond("test_table")

	r := rules.New()
	r.Add(alterRule)
	txe.qe.queryRuleSources.RegisterSource("bufferQuery")
	err := txe.qe.queryRuleSources.SetRules("bufferQuery", r)
	require.NoError(t, err)

	// start a transaction
	txid := newTxForPrep(ctx, tsv)

	// taint the connection.
	sc, err := tsv.te.txPool.GetAndLock(txid, "adding query property")
	require.NoError(t, err)
	sc.txProps.Queries = append(sc.txProps.Queries, tx.Query{
		Sql:    "update test_table set col = 5",
		Tables: []string{"test_table"},
	})
	sc.Unlock()

	// try 2pc commit of Metadata Manager.
	err = txe.Prepare(txid, "aa")
	require.EqualError(t, err, "VT10002: atomic distributed transaction not allowed: cannot prepare the transaction due to query rule")
}

func TestExecutorPrepareConnFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()

	// start a transaction
	txid := newTxForPrep(ctx, tsv)

	// taint the connection.
	sc, err := tsv.te.txPool.GetAndLock(txid, "adding query property")
	require.NoError(t, err)
	sc.Unlock()
	sc.dbConn.Close()

	// try 2pc commit of Metadata Manager.
	err = txe.Prepare(txid, "aa")
	require.EqualError(t, err, "VT10002: atomic distributed transaction not allowed: cannot prepare the transaction on a closed connection")
}

func TestTxExecutorCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, _, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	err := txe.Prepare(txid, "aa")
	require.NoError(t, err)
	err = txe.CommitPrepared("aa")
	require.NoError(t, err)
	// Committing an absent transaction should succeed.
	err = txe.CommitPrepared("bb")
	require.NoError(t, err)
}

func TestTxExecutorCommitRedoFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	tl := syslogger.NewTestLogger()
	defer tl.Close()

	// start a transaction.
	txid := newTxForPrep(ctx, tsv)

	// prepare the transaction
	db.AddQueryPattern("insert into _vt\\.redo_state.*", &sqltypes.Result{})
	err := txe.Prepare(txid, "bb")
	require.NoError(t, err)

	// fail commit prepare as the delete redo query is in rejected query.
	db.AddRejectedQuery("delete from _vt.redo_state where dtid = _binary'bb'", errors.New("delete redo log fail"))
	db.AddQuery("update _vt.redo_state set state = 0 where dtid = _binary'bb'", sqltypes.MakeTestResult(nil))
	err = txe.CommitPrepared("bb")
	require.ErrorContains(t, err, "delete redo log fail")

	// A retry should fail differently as the prepared transaction is marked as failed.
	err = txe.CommitPrepared("bb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot commit dtid bb, err: VT09025: atomic transaction error: failed to commit")

	require.Contains(t, strings.Join(tl.GetAllLogs(), "|"),
		"failed to commit the prepared transaction 'bb' with error: unknown error: delete redo log fail")
}

func TestTxExecutorCommitRedoCommitFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	err := txe.Prepare(txid, "aa")
	require.NoError(t, err)
	defer txe.RollbackPrepared("aa", 0)
	db.AddRejectedQuery("commit", errors.New("commit fail"))
	err = txe.CommitPrepared("aa")
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit fail")
}

func TestTxExecutorRollbackBeginFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	err := txe.Prepare(txid, "aa")
	require.NoError(t, err)
	db.AddRejectedQuery("begin", errors.New("begin fail"))
	err = txe.RollbackPrepared("aa", txid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "begin fail")
}

func TestTxExecutorRollbackRedoFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()
	txid := newTxForPrep(ctx, tsv)
	// Allow all additions to redo logs to succeed
	db.AddQueryPattern("insert into _vt\\.redo_state.*", &sqltypes.Result{})
	err := txe.Prepare(txid, "bb")
	require.NoError(t, err)
	err = txe.RollbackPrepared("bb", txid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported")
}

func TestExecutorCreateTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, _, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	db.AddQueryPattern(fmt.Sprintf("insert into _vt\\.dt_state\\(dtid, state, time_created\\) values \\(_binary'aa', %d,.*", int(querypb.TransactionState_PREPARE)), &sqltypes.Result{})
	db.AddQueryPattern("insert into _vt\\.dt_participant\\(dtid, id, keyspace, shard\\) values \\(_binary'aa', 1,.*", &sqltypes.Result{})
	err := txe.CreateTransaction("aa", []*querypb.Target{{
		Keyspace: "t1",
		Shard:    "0",
	}})
	require.NoError(t, err)
}

func TestExecutorStartCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	commitTransition := fmt.Sprintf("update _vt.dt_state set state = %d where dtid = _binary'aa' and state = %d", int(querypb.TransactionState_COMMIT), int(querypb.TransactionState_PREPARE))
	db.AddQuery(commitTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(ctx, tsv)
	state, err := txe.StartCommit(txid, "aa")
	require.NoError(t, err)
	assert.Equal(t, querypb.StartCommitState_Success, state)

	db.AddQuery(commitTransition, &sqltypes.Result{})
	txid = newTxForPrep(ctx, tsv)
	state, err = txe.StartCommit(txid, "aa")
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not transition to COMMIT: aa")
	assert.Equal(t, querypb.StartCommitState_Fail, state)
}

func TestExecutorStartCommitFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	// start a transaction
	txid := newTxForPrep(ctx, tsv)

	// taint the connection.
	sc, err := tsv.te.txPool.GetAndLock(txid, "taint")
	require.NoError(t, err)
	sc.Taint(ctx, tsv.te.reservedConnStats)
	sc.Unlock()

	// add rollback state update expectation
	rollbackTransition := fmt.Sprintf("update _vt.dt_state set state = %d where dtid = _binary'aa' and state = %d", int(querypb.TransactionState_ROLLBACK), int(querypb.TransactionState_PREPARE))
	db.AddQuery(rollbackTransition, sqltypes.MakeTestResult(nil))

	// try 2pc commit of Metadata Manager.
	state, err := txe.StartCommit(txid, "aa")
	require.EqualError(t, err, "VT10002: atomic distributed transaction not allowed: cannot commit the transaction on a reserved connection")
	assert.Equal(t, querypb.StartCommitState_Fail, state)
}

func TestExecutorSetRollback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	rollbackTransition := fmt.Sprintf("update _vt.dt_state set state = %d where dtid = _binary'aa' and state = %d", int(querypb.TransactionState_ROLLBACK), int(querypb.TransactionState_PREPARE))
	db.AddQuery(rollbackTransition, &sqltypes.Result{RowsAffected: 1})
	txid := newTxForPrep(ctx, tsv)
	err := txe.SetRollback("aa", txid)
	require.NoError(t, err)

	db.AddQuery(rollbackTransition, &sqltypes.Result{})
	txid = newTxForPrep(ctx, tsv)
	err = txe.SetRollback("aa", txid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not transition to ROLLBACK: aa")
}

// TestExecutorUnresolvedTransactions tests with what timestamp value the query is executed to fetch unresolved transactions.
func TestExecutorUnresolvedTransactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, _, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	pattern := `(?i)select\s+t\.dtid,\s+t\.state,\s+t\.time_created,\s+p\.keyspace,\s+p\.shard\s+from\s+_vt\.dt_state\s+t\s+join\s+_vt\.dt_participant\s+p\s+on\s+t\.dtid\s+=\s+p\.dtid\s+where\s+time_created\s+<\s+(\d+)\s+order\s+by\s+t\.state\s+desc,\s+t\.dtid`
	re := regexp.MustCompile(pattern)

	var executedQuery string
	db.AddQueryPatternWithCallback(pattern, &sqltypes.Result{}, func(query string) {
		executedQuery = query
	})

	tcases := []struct {
		abandonAge time.Duration
		expected   time.Time
	}{
		{abandonAge: 0, expected: time.Now().Add(-txe.te.abandonAge)},
		{abandonAge: 100 * time.Second, expected: time.Now().Add(-100 * time.Second)},
	}

	for _, tcase := range tcases {
		t.Run(fmt.Sprintf("abandonAge=%v", tcase.abandonAge), func(t *testing.T) {
			_, err := txe.UnresolvedTransactions(tcase.abandonAge)
			require.NoError(t, err)
			require.NotEmpty(t, executedQuery)

			// extract the time value
			matches := re.FindStringSubmatch(executedQuery)
			require.Len(t, matches, 2)
			timeCreated := convertNanoStringToTime(t, matches[1])

			// diff should be in microseconds, so we allow 10ms difference
			require.WithinDuration(t, timeCreated, tcase.expected, 10*time.Millisecond)
		})
	}
}

func convertNanoStringToTime(t *testing.T, unixNanoStr string) time.Time {
	t.Helper()

	// Convert the string to an integer (int64)
	unixNano, err := strconv.ParseInt(unixNanoStr, 10, 64)
	require.NoError(t, err)

	// Convert nanoseconds to seconds and nanoseconds
	seconds := unixNano / int64(time.Second)
	nanos := unixNano % int64(time.Second)

	// Create a time.Time object
	return time.Unix(seconds, nanos)
}

func TestExecutorConcludeTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, _, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	db.AddQuery("delete from _vt.dt_state where dtid = _binary'aa'", &sqltypes.Result{})
	db.AddQuery("delete from _vt.dt_participant where dtid = _binary'aa'", &sqltypes.Result{})
	err := txe.ConcludeTransaction("aa")
	require.NoError(t, err)
}

func TestExecutorReadTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, _, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = _binary'aa'", &sqltypes.Result{})
	got, err := txe.ReadTransaction("aa")
	require.NoError(t, err)
	want := &querypb.TransactionMetadata{}
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("aa"),
			sqltypes.NewInt64(int64(querypb.TransactionState_PREPARE)),
			sqltypes.NewVarBinary("1"),
		}},
	}
	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = _binary'aa'", txResult)
	db.AddQuery("select keyspace, shard from _vt.dt_participant where dtid = _binary'aa'", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("test1"),
			sqltypes.NewVarBinary("0"),
		}, {
			sqltypes.NewVarBinary("test2"),
			sqltypes.NewVarBinary("1"),
		}},
	})
	got, err = txe.ReadTransaction("aa")
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
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("aa"),
			sqltypes.NewInt64(int64(querypb.TransactionState_COMMIT)),
			sqltypes.NewVarBinary("1"),
		}},
	}
	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = _binary'aa'", txResult)
	want.State = querypb.TransactionState_COMMIT
	got, err = txe.ReadTransaction("aa")
	require.NoError(t, err)
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("aa"),
			sqltypes.NewInt64(int64(querypb.TransactionState_ROLLBACK)),
			sqltypes.NewVarBinary("1"),
		}},
	}
	db.AddQuery("select dtid, state, time_created from _vt.dt_state where dtid = _binary'aa'", txResult)
	want.State = querypb.TransactionState_ROLLBACK
	got, err = txe.ReadTransaction("aa")
	require.NoError(t, err)
	if !proto.Equal(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}
}

func TestExecutorReadAllTransactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, _, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	db.AddQuery(txe.te.twoPC.readAllTransactions, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("dtid0"),
			sqltypes.NewInt64(int64(querypb.TransactionState_PREPARE)),
			sqltypes.NewVarBinary("1"),
			sqltypes.NewVarBinary("ks01"),
			sqltypes.NewVarBinary("shard01"),
		}},
	})
	got, _, _, err := txe.ReadTwopcInflight()
	require.NoError(t, err)
	want := []*tx.DistributedTx{{
		Dtid:    "dtid0",
		State:   "PREPARE",
		Created: time.Unix(0, 1),
		Participants: []querypb.Target{{
			Keyspace: "ks01",
			Shard:    "shard01",
		}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadAllTransactions:\n%s, want\n%s", jsonStr(got), jsonStr(want))
	}
}

// TestTransactionNotifier tests that the transaction notifier is called
// when a transaction watcher receives unresolved transaction count more than zero.
func TestTransactionNotifier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, tsv, db := newShortAgeExecutor(t, ctx)
	defer db.Close()
	defer tsv.StopService()
	db.AddQueryPattern(
		"select count\\(\\*\\) from _vt\\.redo_state where time_created.*",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("count(*)", "int64"), "0"))

	// zero unresolved transactions
	db.AddQueryPattern(
		"select count\\(\\*\\) from _vt\\.dt_state where time_created.*",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("count(*)", "int64"), "0"))
	notifyCh := make(chan any)
	tsv.te.dxNotify = func() {
		notifyCh <- nil
	}
	select {
	case <-notifyCh:
		t.Error("unresolved transaction notifier call unexpected")
	case <-time.After(1 * time.Second):
	}

	// non zero unresolved transactions
	db.AddQueryPattern(
		"select count\\(\\*\\) from _vt\\.dt_state where time_created.*",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("count(*)", "int64"), "1"))
	select {
	case <-notifyCh:
	case <-time.After(1 * time.Second):
		t.Error("unresolved transaction notifier expected but not received")
	}
}

func TestNoTwopc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txe, tsv, db := newNoTwopcExecutor(t, ctx)
	defer db.Close()
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
		fun: func() error {
			_, err := txe.StartCommit(1, "aa")
			return err
		},
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
	}, {
		desc: "ReadAllTransactions",
		fun: func() error {
			_, _, _, err := txe.ReadTwopcInflight()
			return err
		},
	}, {
		desc: "UnresolvedTransactions",
		fun: func() error {
			_, err := txe.UnresolvedTransactions(0 /* requestedAge */)
			return err
		},
	}}

	want := "2pc is not enabled"
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.fun()
			require.EqualError(t, err, want)
		})
	}
}

func newTestTxExecutor(t *testing.T, ctx context.Context) (txe *DTExecutor, tsv *TabletServer, db *fakesqldb.DB, closer func()) {
	db = setUpQueryExecutorTest(t)
	logStats := tabletenv.NewLogStats(ctx, "TestTxExecutor", streamlog.NewQueryLogConfigForTest())
	tsv = newTestTabletServer(ctx, smallTxPool, db)
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = newDBConfigs(db)
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)
	db.AddQueryPattern("insert into _vt\\.redo_state\\(dtid, state, time_created\\) values \\(_binary'aa', 1,.*", &sqltypes.Result{})
	db.AddQueryPattern("insert into _vt\\.redo_statement.*", &sqltypes.Result{})
	db.AddQuery("delete from _vt.redo_state where dtid = _binary'aa'", &sqltypes.Result{})
	db.AddQuery("delete from _vt.redo_statement where dtid = _binary'aa'", &sqltypes.Result{})
	db.AddQuery("update test_table set `name` = 2 where pk = 1 limit 10001", &sqltypes.Result{})
	db.AddRejectedQuery("bogus", sqlerror.NewSQLError(sqlerror.ERUnknownError, sqlerror.SSUnknownSQLState, "bogus query"))
	return &DTExecutor{
			ctx:      ctx,
			logStats: logStats,
			te:       tsv.te,
			qe:       qe,
		}, tsv, db, func() {
			db.Close()
			tsv.StopService()
		}
}

// newShortAgeExecutor is same as newTestTxExecutor, but shorter transaction abandon age.
func newShortAgeExecutor(t *testing.T, ctx context.Context) (txe *DTExecutor, tsv *TabletServer, db *fakesqldb.DB) {
	db = setUpQueryExecutorTest(t)
	logStats := tabletenv.NewLogStats(ctx, "TestTxExecutor", streamlog.NewQueryLogConfigForTest())
	tsv = newTestTabletServer(ctx, smallTxPool|shortTwopcAge, db)
	db.AddQueryPattern("insert into _vt\\.redo_state\\(dtid, state, time_created\\) values \\(_binary'aa', 1,.*", &sqltypes.Result{})
	db.AddQueryPattern("insert into _vt\\.redo_statement.*", &sqltypes.Result{})
	db.AddQuery("delete from _vt.redo_state where dtid = _binary'aa'", &sqltypes.Result{})
	db.AddQuery("delete from _vt.redo_statement where dtid = _binary'aa'", &sqltypes.Result{})
	db.AddQuery("update test_table set `name` = 2 where pk = 1 limit 10001", &sqltypes.Result{})
	return &DTExecutor{
		ctx:      ctx,
		logStats: logStats,
		te:       tsv.te,
	}, tsv, db
}

// newNoTwopcExecutor is same as newTestTxExecutor, but 2pc disabled.
func newNoTwopcExecutor(t *testing.T, ctx context.Context) (txe *DTExecutor, tsv *TabletServer, db *fakesqldb.DB) {
	db = setUpQueryExecutorTest(t)
	logStats := tabletenv.NewLogStats(ctx, "TestTxExecutor", streamlog.NewQueryLogConfigForTest())
	tsv = newTestTabletServer(ctx, noTwopc, db)
	return &DTExecutor{
		ctx:      ctx,
		logStats: logStats,
		te:       tsv.te,
	}, tsv, db
}

// newTxForPrep creates a non-empty transaction.
func newTxForPrep(ctx context.Context, tsv *TabletServer) int64 {
	txid := newTransaction(tsv, nil)
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	_, err := tsv.Execute(ctx, &target, "update test_table set name = 2 where pk = 1", nil, txid, 0, nil)
	if err != nil {
		panic(err)
	}
	return txid
}
