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
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/streamlog"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestQueryExecutorPlans(t *testing.T) {
	type dbResponse struct {
		query  string
		result *sqltypes.Result
	}

	dmlResult := &sqltypes.Result{
		RowsAffected: 1,
	}
	fields := sqltypes.MakeTestFields("a|b", "int64|varchar")
	selectResult := sqltypes.MakeTestResult(fields, "1|aaa")
	emptyResult := &sqltypes.Result{}

	// The queries are run both in and outside a transaction.
	testcases := []struct {
		// input is the input query.
		input string
		// passThrough specifies if planbuilder.PassthroughDML must be set.
		passThrough bool
		inDMLExec   bool
		// dbResponses specifies the list of queries and responses to add to the fake db.
		dbResponses []dbResponse
		// resultWant is the result we want.
		resultWant *sqltypes.Result
		// planWant is the PlanType we want to see built.
		planWant string
		// logWant is the log of queries we expect to be executed.
		logWant string
		// inTxWant is the query log we expect if we're in a transation.
		// If empty, then we should expect the same as logWant.
		inTxWant string
		// errorWant is the error we expect to get, if any, and should be nil if no error should be returned
		errorWant    string
		onlyInTxErr  bool
		outsideTxErr bool
		// TxThrottler allows the test case to override the transaction throttler
		txThrottler txthrottler.TxThrottler
	}{
		{
			input: "select * from t",
			dbResponses: []dbResponse{{
				query:  "select * from t limit 10001",
				result: selectResult,
			}},
			resultWant: selectResult,
			planWant:   "Select",
			logWant:    "select * from t limit 10001",
			inTxWant:   "select * from t limit 10001",
		}, {
			input: "select * from t limit 1",
			dbResponses: []dbResponse{{
				query:  "select * from t limit 1",
				result: selectResult,
			}},
			resultWant: selectResult,
			planWant:   "Select",
			logWant:    "select * from t limit 1",
			inTxWant:   "select * from t limit 1",
		}, {
			input: "show engines",
			dbResponses: []dbResponse{{
				query:  "show engines",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "Show",
			logWant:    "show engines",
		}, {
			input: "repair t",
			dbResponses: []dbResponse{{
				query:  "repair t",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "OtherAdmin",
			logWant:    "repair t",
		}, {
			input: "insert into test_table(a) values(1)",
			dbResponses: []dbResponse{{
				query:  "insert into test_table(a) values (1)",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "Insert",
			logWant:    "insert into test_table(a) values (1)",
		}, {
			input: "replace into test_table(a) values(1)",
			dbResponses: []dbResponse{{
				query:  "replace into test_table(a) values (1)",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "Insert",
			logWant:    "replace into test_table(a) values (1)",
		}, {
			input: "update test_table set a=1",
			dbResponses: []dbResponse{{
				query:  "update test_table set a = 1 limit 10001",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "UpdateLimit",
			// The UpdateLimit query will not use autocommit because
			// it needs to roll back on failure.
			logWant:  "begin; update test_table set a = 1 limit 10001; commit",
			inTxWant: "update test_table set a = 1 limit 10001",
		}, {
			input:       "select a, b from test_table",
			passThrough: true,
			inDMLExec:   true,
			dbResponses: []dbResponse{{
				query:  "select a, b from test_table",
				result: selectResult,
			}},
			resultWant:   selectResult,
			planWant:     "SelectNoLimit",
			logWant:      "select a, b from test_table",
			outsideTxErr: true,
			errorWant:    "[BUG] SelectNoLimit unexpected plan type",
		}, {
			input:       "update test_table set a=1",
			passThrough: true,
			dbResponses: []dbResponse{{
				query:  "update test_table set a = 1",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "Update",
			logWant:    "update test_table set a = 1",
		}, {
			input: "delete from test_table",
			dbResponses: []dbResponse{{
				query:  "delete from test_table limit 10001",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "DeleteLimit",
			// The DeleteLimit query will not use autocommit because
			// it needs to roll back on failure.
			logWant:  "begin; delete from test_table limit 10001; commit",
			inTxWant: "delete from test_table limit 10001",
		}, {
			input:       "delete from test_table",
			passThrough: true,
			dbResponses: []dbResponse{{
				query:  "delete from test_table",
				result: dmlResult,
			}},
			resultWant: dmlResult,
			planWant:   "Delete",
			logWant:    "delete from test_table",
		}, {
			input: "alter table test_table add zipcode int",
			dbResponses: []dbResponse{{
				query:  "alter table test_table add column zipcode int",
				result: dmlResult,
			}},
			resultWant:  dmlResult,
			planWant:    "DDL",
			logWant:     "alter table test_table add column zipcode int",
			onlyInTxErr: true,
			errorWant:   "DDL statement executed inside a transaction",
		}, {
			input: "savepoint a",
			dbResponses: []dbResponse{{
				query:  "savepoint a",
				result: emptyResult,
			}},
			resultWant: emptyResult,
			planWant:   "Savepoint",
			logWant:    "savepoint a",
			inTxWant:   "savepoint a",
		}, {
			input: "create index a on user(id)",
			dbResponses: []dbResponse{{
				query:  "alter table `user` add key a (id)",
				result: emptyResult,
			}},
			resultWant:  emptyResult,
			planWant:    "DDL",
			logWant:     "alter table `user` add key a (id)",
			inTxWant:    "alter table `user` add key a (id)",
			onlyInTxErr: true,
			errorWant:   "DDL statement executed inside a transaction",
		}, {
			input: "create index a on user(id1 + id2)",
			dbResponses: []dbResponse{{
				query:  "create index a on user(id1 + id2)",
				result: emptyResult,
			}},
			resultWant:  emptyResult,
			planWant:    "DDL",
			logWant:     "create index a on user(id1 + id2)",
			inTxWant:    "create index a on user(id1 + id2)",
			onlyInTxErr: true,
			errorWant:   "DDL statement executed inside a transaction",
		}, {
			input: "ROLLBACK work to SAVEPOINT a",
			dbResponses: []dbResponse{{
				query:  "ROLLBACK work to SAVEPOINT a",
				result: emptyResult,
			}},
			resultWant: emptyResult,
			planWant:   "RollbackSavepoint",
			logWant:    "ROLLBACK work to SAVEPOINT a",
			inTxWant:   "ROLLBACK work to SAVEPOINT a",
		}, {
			input: "RELEASE savepoint a",
			dbResponses: []dbResponse{{
				query:  "RELEASE savepoint a",
				result: emptyResult,
			}},
			resultWant: emptyResult,
			planWant:   "Release",
			logWant:    "RELEASE savepoint a",
			inTxWant:   "RELEASE savepoint a",
		}, {
			input: "show create database db_name",
			dbResponses: []dbResponse{{
				query:  "show create database ks",
				result: emptyResult,
			}},
			resultWant: emptyResult,
			planWant:   "Show",
			logWant:    "show create database ks",
		}, {
			input: "show create database mysql",
			dbResponses: []dbResponse{{
				query:  "show create database mysql",
				result: emptyResult,
			}},
			resultWant: emptyResult,
			planWant:   "Show",
			logWant:    "show create database mysql",
		}, {
			input: "show create table mysql.user",
			dbResponses: []dbResponse{{
				query:  "show create table mysql.`user`",
				result: emptyResult,
			}},
			resultWant: emptyResult,
			planWant:   "Show",
			logWant:    "show create table mysql.`user`",
		}, {
			input: "update test_table set a=1",
			dbResponses: []dbResponse{{
				query:  "update test_table set a = 1 limit 10001",
				result: dmlResult,
			}},
			errorWant:   "Transaction throttled",
			txThrottler: &mockTxThrottler{true},
		}, {
			input:       "update test_table set a=1",
			passThrough: true,
			dbResponses: []dbResponse{{
				query:  "update test_table set a = 1 limit 10001",
				result: dmlResult,
			}},
			errorWant:   "Transaction throttled",
			txThrottler: &mockTxThrottler{true},
		},
	}
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			db := setUpQueryExecutorTest(t)
			defer db.Close()
			for _, dbr := range tcase.dbResponses {
				db.AddQuery(dbr.query, dbr.result)
			}
			ctx := context.Background()
			tsv := newTestTabletServer(ctx, noFlags, db)
			if tcase.txThrottler != nil {
				tsv.txThrottler = tcase.txThrottler
			}
			tsv.config.DB.DBName = "ks"
			defer tsv.StopService()

			tsv.SetPassthroughDMLs(tcase.passThrough)

			// Test outside a transaction.
			qre := newTestQueryExecutorWithRowsLimit(ctx, tsv, tcase.input, 0, tcase.passThrough && tcase.inDMLExec)
			got, err := qre.Execute()
			if tcase.outsideTxErr || (tcase.errorWant != "" && !tcase.onlyInTxErr) {
				assert.EqualError(t, err, tcase.errorWant)
			} else {
				require.NoError(t, err, tcase.input)
				assert.Equal(t, tcase.resultWant, got, tcase.input)
				assert.Equal(t, tcase.planWant, qre.logStats.PlanType, tcase.input)
				assert.Equal(t, tcase.logWant, qre.logStats.RewrittenSQL(), tcase.input)
			}
			// Wait for the existing query to be processed by the cache
			time.Sleep(100 * time.Millisecond)

			// Test inside a transaction.
			target := tsv.sm.Target()
			state, err := tsv.Begin(ctx, nil, target, nil)
			if !tcase.outsideTxErr && tcase.errorWant != "" && !tcase.onlyInTxErr {
				require.EqualError(t, err, tcase.errorWant)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, state.TabletAlias, "alias should not be nil")
			assert.Equal(t, tsv.alias, state.TabletAlias, "Wrong alias returned by Begin")
			defer tsv.Commit(ctx, target, state.TransactionID)

			qre = newTestQueryExecutorWithRowsLimit(ctx, tsv, tcase.input, state.TransactionID, tcase.passThrough && tcase.inDMLExec)
			got, err = qre.Execute()
			if tcase.onlyInTxErr {
				require.EqualError(t, err, tcase.errorWant)
				return
			}
			require.NoError(t, err, tcase.input)
			assert.Equal(t, tcase.resultWant, got, "in tx: %v", tcase.input)
			assert.Equal(t, tcase.planWant, qre.logStats.PlanType, "in tx: %v", tcase.input)
			want := tcase.logWant
			if tcase.inTxWant != "" {
				want = tcase.inTxWant
			}
			assert.Equal(t, want, qre.logStats.RewrittenSQL(), "in tx: %v", tcase.input)
		})
	}
}

func TestQueryExecutorQueryAnnotation(t *testing.T) {
	type dbResponse struct {
		query  string
		result *sqltypes.Result
	}

	fields := sqltypes.MakeTestFields("a|b", "int64|varchar")
	selectResult := sqltypes.MakeTestResult(fields, "1|aaa")

	testcases := []struct {
		// input is the input query.
		input string
		// passThrough specifies if planbuilder.PassthroughDML must be set.
		passThrough bool
		// dbResponses specifes the list of queries and responses to add to the fake db.
		dbResponses []dbResponse
		// resultWant is the result we want.
		resultWant *sqltypes.Result
		// planWant is the PlanType we want to see built.
		planWant string
		// logWant is the log of queries we expect to be executed.
		logWant string
		// If empty, then we should expect the same as logWant.
		inTxWant string
	}{{
		input: "select * from t",
		dbResponses: []dbResponse{{
			query:  "select * from t limit 10001",
			result: selectResult,
		}, {
			query:  "/* u1@PRIMARY */ select * from t limit 10001",
			result: selectResult,
		}},
		resultWant: selectResult,
		planWant:   "Select",
		logWant:    "/* u1@PRIMARY */ select * from t limit 10001",
		inTxWant:   "/* u1@PRIMARY */ select * from t limit 10001",
	}}
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			db := setUpQueryExecutorTest(t)
			defer db.Close()
			for _, dbr := range tcase.dbResponses {
				db.AddQuery(dbr.query, dbr.result)
			}
			callerID := &querypb.VTGateCallerID{
				Username: "u1",
			}
			ctx := callerid.NewContext(context.Background(), nil, callerID)
			tsv := newTestTabletServer(ctx, noFlags, db)
			tsv.config.DB.DBName = "ks"
			tsv.config.AnnotateQueries = true
			defer tsv.StopService()

			tsv.SetPassthroughDMLs(tcase.passThrough)

			// Test outside a transaction.
			qre := newTestQueryExecutor(ctx, tsv, tcase.input, 0)
			got, err := qre.Execute()
			require.NoError(t, err, tcase.input)
			assert.Equal(t, tcase.resultWant, got, tcase.input)
			assert.Equal(t, tcase.planWant, qre.logStats.PlanType, tcase.input)
			assert.Equal(t, tcase.logWant, qre.logStats.RewrittenSQL(), tcase.input)

			// Wait for the existing query to be processed by the cache
			time.Sleep(100 * time.Millisecond)

			// Test inside a transaction.
			target := tsv.sm.Target()
			state, err := tsv.Begin(ctx, nil, target, nil)
			require.NoError(t, err)
			require.NotNil(t, state.TabletAlias, "alias should not be nil")
			assert.Equal(t, tsv.alias, state.TabletAlias, "Wrong alias returned by Begin")
			defer tsv.Commit(ctx, target, state.TransactionID)

			qre = newTestQueryExecutor(ctx, tsv, tcase.input, state.TransactionID)
			got, err = qre.Execute()
			require.NoError(t, err, tcase.input)
			assert.Equal(t, tcase.resultWant, got, "in tx: %v", tcase.input)
			assert.Equal(t, tcase.planWant, qre.logStats.PlanType, "in tx: %v", tcase.input)
			want := tcase.logWant
			if tcase.inTxWant != "" {
				want = tcase.inTxWant
			}
			assert.Equal(t, want, qre.logStats.RewrittenSQL(), "in tx: %v", tcase.input)
		})
	}
}

// TestQueryExecutorSelectImpossible is separate because it's a special case
// because the "in transaction" case is a no-op.
func TestQueryExecutorSelectImpossible(t *testing.T) {
	type dbResponse struct {
		query  string
		result *sqltypes.Result
	}

	fields := sqltypes.MakeTestFields("a|b", "int64|varchar")
	fieldResult := sqltypes.MakeTestResult(fields)

	testcases := []struct {
		input       string
		dbResponses []dbResponse
		resultWant  *sqltypes.Result
		planWant    string
		logWant     string
		inTxWant    string
	}{{
		input: "select * from t where 1 != 1",
		dbResponses: []dbResponse{{
			query:  "select * from t where 1 != 1 limit 10001",
			result: fieldResult,
		}},
		resultWant: fieldResult,
		planWant:   "SelectImpossible",
		logWant:    "select * from t where 1 != 1 limit 10001",
		inTxWant:   "select * from t where 1 != 1 limit 10001",
	}}
	for _, tcase := range testcases {
		func() {
			db := setUpQueryExecutorTest(t)
			defer db.Close()
			for _, dbr := range tcase.dbResponses {
				db.AddQuery(dbr.query, dbr.result)
			}
			ctx := context.Background()
			tsv := newTestTabletServer(ctx, noFlags, db)
			defer tsv.StopService()

			qre := newTestQueryExecutor(ctx, tsv, tcase.input, 0)
			got, err := qre.Execute()
			require.NoError(t, err, tcase.input)
			assert.Equal(t, tcase.resultWant, got, tcase.input)
			assert.Equal(t, tcase.planWant, qre.logStats.PlanType, tcase.input)
			assert.Equal(t, tcase.logWant, qre.logStats.RewrittenSQL(), tcase.input)
			target := tsv.sm.Target()
			state, err := tsv.Begin(ctx, nil, target, nil)
			require.NoError(t, err)
			require.NotNil(t, state.TabletAlias, "alias should not be nil")
			assert.Equal(t, tsv.alias, state.TabletAlias, "Wrong tablet alias from Begin")
			defer tsv.Commit(ctx, target, state.TransactionID)

			qre = newTestQueryExecutor(ctx, tsv, tcase.input, state.TransactionID)
			got, err = qre.Execute()
			require.NoError(t, err, tcase.input)
			assert.Equal(t, tcase.resultWant, got, "in tx: %v", tcase.input)
			assert.Equal(t, tcase.planWant, qre.logStats.PlanType, "in tx: %v", tcase.input)
			assert.Equal(t, tcase.inTxWant, qre.logStats.RewrittenSQL(), "in tx: %v", tcase.input)
		}()
	}
}

// TestDisableOnlineDDL checks whether disabling online DDLs throws the correct error or not
func TestDisableOnlineDDL(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "ALTER VITESS_MIGRATION CANCEL ALL"

	db.SetNeverFail(true)
	defer db.SetNeverFail(false)

	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)

	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	_, err := qre.Execute()
	require.NoError(t, err)
	tsv.StopService()

	tsv = newTestTabletServer(ctx, disableOnlineDDL, db)
	defer tsv.StopService()

	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	_, err = qre.Execute()
	require.EqualError(t, err, "online DDL is disabled")
}

func TestQueryExecutorLimitFailure(t *testing.T) {
	type dbResponse struct {
		query  string
		result *sqltypes.Result
	}

	dmlResult := &sqltypes.Result{
		RowsAffected: 3,
	}
	fields := sqltypes.MakeTestFields("a|b", "int64|varchar")
	fieldResult := sqltypes.MakeTestResult(fields)
	selectResult := sqltypes.MakeTestResult(fields, "1|aaa", "2|bbb", "3|ccc")

	// The queries are run both in and outside a transaction.
	testcases := []struct {
		input        string
		dbResponses  []dbResponse
		err          string
		logWant      string
		inTxWant     string
		testRollback bool
	}{{
		input: "select * from t",
		dbResponses: []dbResponse{{
			query:  "select * from t where 1 != 1",
			result: fieldResult,
		}, {
			query:  "select * from t limit 3",
			result: selectResult,
		}},
		err:      "count exceeded",
		logWant:  "select * from t limit 3",
		inTxWant: "select * from t limit 3",
	}, {
		input: "update test_table set a=1",
		dbResponses: []dbResponse{{
			query:  "update test_table set a = 1 limit 3",
			result: dmlResult,
		}},
		err:          "count exceeded",
		logWant:      "begin; update test_table set a = 1 limit 3; rollback",
		inTxWant:     "update test_table set a = 1 limit 3; rollback",
		testRollback: true,
	}, {
		input: "delete from test_table",
		dbResponses: []dbResponse{{
			query:  "delete from test_table limit 3",
			result: dmlResult,
		}},
		err:          "count exceeded",
		logWant:      "begin; delete from test_table limit 3; rollback",
		inTxWant:     "delete from test_table limit 3; rollback",
		testRollback: true,
	}, {
		// There should be no rollback on normal failures.
		input:       "update test_table set a=1",
		dbResponses: nil,
		err:         "not supported",
		logWant:     "begin; update test_table set a = 1 limit 3; rollback",
		inTxWant:    "update test_table set a = 1 limit 3",
	}, {
		// There should be no rollback on normal failures.
		input:       "delete from test_table",
		dbResponses: nil,
		err:         "not supported",
		logWant:     "begin; delete from test_table limit 3; rollback",
		inTxWant:    "delete from test_table limit 3",
	}}
	for i, tcase := range testcases {
		t.Run(fmt.Sprintf("%d - %s", i, tcase.input), func(t *testing.T) {
			db := setUpQueryExecutorTest(t)
			defer db.Close()
			for _, dbr := range tcase.dbResponses {
				db.AddQuery(dbr.query, dbr.result)
			}
			ctx := callerid.NewContext(context.Background(), callerid.NewEffectiveCallerID("a", "b", "c"), callerid.NewImmediateCallerID("d"))
			tsv := newTestTabletServer(ctx, smallResultSize, db)
			defer tsv.StopService()

			tsv.SetPassthroughDMLs(false)

			// Test outside a transaction.
			qre := newTestQueryExecutor(ctx, tsv, tcase.input, 0)
			_, err := qre.Execute()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tcase.err)
			assert.Equal(t, tcase.logWant, qre.logStats.RewrittenSQL(), tcase.input)

			// Test inside a transaction.
			target := tsv.sm.Target()
			state, err := tsv.Begin(ctx, nil, target, nil)
			require.NoError(t, err)
			require.NotNil(t, state.TabletAlias, "alias should not be nil")
			assert.Equal(t, tsv.alias, state.TabletAlias, "Wrong tablet alias from Begin")
			defer tsv.Commit(ctx, target, state.TransactionID)

			qre = newTestQueryExecutor(ctx, tsv, tcase.input, state.TransactionID)
			_, err = qre.Execute()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tcase.err)

			want := tcase.logWant
			if tcase.inTxWant != "" {
				want = tcase.inTxWant
			}
			assert.Equal(t, want, qre.logStats.RewrittenSQL(), "in tx: %v", tcase.input)

			if !tcase.testRollback {
				return
			}
			// Ensure transaction was rolled back.
			conn, err := tsv.te.txPool.GetAndLock(state.TransactionID, "")
			require.NoError(t, err)
			defer conn.Release(tx.TxClose)

			require.False(t, conn.IsInTransaction(), "connection is still in a transaction")
		})
	}
}

func TestQueryExecutorPlanPassSelectWithLockOutsideATransaction(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table for update"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table limit 10001 for update", &sqltypes.Result{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	_, err := qre.Execute()
	assert.NoError(t, err)
}

func TestQueryExecutorPlanNextval(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	selQuery := "select next_id, cache from seq where id = 0 for update"
	db.AddQuery(selQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(3),
		}},
	})
	updateQuery := "update seq set next_id = 4 where id = 0"
	db.AddQuery(updateQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	qre := newTestQueryExecutor(ctx, tsv, "select next value from seq", 0)
	assert.Equal(t, planbuilder.PlanNextval, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
	}
	assert.Equal(t, want, got)

	// At this point, NextVal==2, LastVal==4.
	// So, a single value gen should not cause a db access.
	db.DeleteQuery(selQuery)
	qre = newTestQueryExecutor(ctx, tsv, "select next 1 values from seq", 0)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(2),
		}},
	}
	if !got.Equal(want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}

	// NextVal==3, LastVal==4
	// Let's try the next 2 values.
	db.AddQuery(selQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(4),
			sqltypes.NewInt64(3),
		}},
	})
	updateQuery = "update seq set next_id = 7 where id = 0"
	db.AddQuery(updateQuery, &sqltypes.Result{})
	qre = newTestQueryExecutor(ctx, tsv, "select next 2 values from seq", 0)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(3),
		}},
	}
	if !got.Equal(want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}

	// NextVal==5, LastVal==7
	// Let's try jumping a full cache range.
	db.AddQuery(selQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(7),
			sqltypes.NewInt64(3),
		}},
	})
	updateQuery = "update seq set next_id = 13 where id = 0"
	db.AddQuery(updateQuery, &sqltypes.Result{})
	qre = newTestQueryExecutor(ctx, tsv, "select next 6 values from seq", 0)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(5),
		}},
	}
	if !got.Equal(want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}
}

func TestQueryExecutorMessageStreamACL(t *testing.T) {
	ctx := t.Context()
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"msg"},
			Readers:              []string{"u2"},
			Writers:              []string{"u1"},
		}},
	}
	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	db := setUpQueryExecutorTest(t)
	defer db.Close()

	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	defer tsv.StopService()

	plan, err := tsv.qe.GetMessageStreamPlan("msg")
	if err != nil {
		t.Fatal(err)
	}

	callerID := &querypb.VTGateCallerID{
		Username: "u1",
	}
	ctx = callerid.NewContext(ctx, nil, callerID)
	qre := &QueryExecutor{
		ctx:      ctx,
		query:    "stream from msg",
		plan:     plan,
		logStats: tabletenv.NewLogStats(ctx, "TestQueryExecutor", streamlog.NewQueryLogConfigForTest()),
		tsv:      tsv,
	}

	// Should not fail because u1 has permission.
	err = qre.MessageStream(func(qr *sqltypes.Result) error {
		return io.EOF
	})
	if err != nil {
		t.Fatal(err)
	}

	callerID = &querypb.VTGateCallerID{
		Username: "u2",
		Groups:   []string{"non-admin"},
	}
	qre.ctx = callerid.NewContext(context.Background(), nil, callerID)
	// Should fail because u2 does not have permission.
	err = qre.MessageStream(func(qr *sqltypes.Result) error {
		return io.EOF
	})

	assert.EqualError(t, err, `MessageStream command denied to user 'u2', in groups [non-admin], for table 'msg' (ACL check error)`)
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}
}

func TestQueryExecutorTableAcl(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{username},
		}},
	}
	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !got.Equal(want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
}

func TestQueryExecutorTableAclNoPermission(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"superuser"},
		}},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}
	// without enabling Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !got.Equal(want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	tsv.StopService()

	// enable Config.StrictTableAcl
	tsv = newTestTabletServer(ctx, enableStrictTableACL, db)
	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	// query should fail because current user do not have read permissions
	_, err = qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}
}

// TestQueryExecutorTableAclPassthroughDenied covers statements whose table set
// the planner cannot determine (DO, CALL, REPAIR, OPTIMIZE, LOAD DATA). They are
// forwarded to MySQL as opaque text and can still read or modify tables — a
// table-reading subquery inside DO, a stored procedure body, LOAD DATA's target
// table — but BuildPermissions derives no permissions for them, so before this
// fix the ACL loop had nothing to check and let any authenticated caller run
// them under strict table ACL, bypassing the table ACL entirely
// (GHSA-w6mx-2f8x-pqf4). Under strict ACL these must now be denied for a
// non-exempt caller; an exempt caller, a dry run, and strict ACL off must
// still run them.
func TestQueryExecutorTableAclPassthroughDenied(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	t.Cleanup(db.Close)

	// The opaque statements never reach the backend under strict ACL (they are
	// denied first), but the exempt, dry-run and non-strict cases execute them,
	// so the fake backend must answer all three passthrough shapes.
	db.AddQueryPattern("(?is)do .*", &sqltypes.Result{})
	db.AddQueryPattern("(?is)call .*", &sqltypes.Result{})
	db.AddQueryPattern("(?is)load data .*", &sqltypes.Result{})

	// A subquery-reading DO, a stored-procedure CALL, and a LOAD DATA: one per
	// tablet plan type whose statement the parser leaves opaque.
	cases := []struct {
		name   string
		query  string
		planID planbuilder.PlanType
	}{
		{"do with table subquery", "do (select email from test_table where pk = 3 limit 1)", planbuilder.PlanOtherAdmin},
		{"call stored procedure", "call test_proc()", planbuilder.PlanCallProc},
		// LOAD DATA is the same gap on the write side: the parser discards
		// everything after LOAD DATA, and the stock init_db.sql grants vt_app
		// the FILE privilege, so a server-side INFILE into a denied table runs.
		{"load data into table", "load data infile '/var/lib/mysql-files/x.csv' into table test_table", planbuilder.PlanLoad},
	}

	// test_table is readable only by "superuser"; the caller "u2" is in no group.
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"superuser"},
		}},
	}
	require.NoError(t, tableacl.InitFromProto(config))
	callerID := &querypb.VTGateCallerID{Username: "u2", Groups: []string{"eng", "beta"}}
	ctx := callerid.NewContext(t.Context(), nil, callerID)

	// newServer starts a tablet server for one sub-case and stops it when that
	// sub-case ends, whether or not its assertions pass, so a failure cannot
	// leak the server into the next one.
	newServer := func(t *testing.T, flags executorFlags) *TabletServer {
		t.Helper()
		tsv := newTestTabletServer(ctx, flags, db)
		t.Cleanup(tsv.StopService)
		return tsv
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The denial has no table to name, so it is recorded under a table
			// label no unquoted table name can share (and no group), rather
			// than a blank.
			statsKey := strings.Join([]string{"undetermined-table-set", "", tc.planID.String(), "u2"}, ".")

			t.Run("strict table ACL denies", func(t *testing.T) {
				// The caller has no grants and the statement's table set is
				// unknown, so it must be denied.
				tsv := newServer(t, enableStrictTableACL)
				qre := newTestQueryExecutor(ctx, tsv, tc.query, 0)
				require.Equal(t, tc.planID, qre.plan.PlanID)
				require.True(t, qre.plan.TablesUndetermined, "the planner must flag the statement's table set as undetermined")
				deniedBefore := tsv.stats.TableaclDenied.Counts()[statsKey]
				calledBefore := db.GetQueryCalledNum(tc.query)
				_, err := qre.Execute()
				require.Error(t, err, "an authenticated caller with no grants must not run an opaque statement under strict table ACL")
				// Denied means denied before execution: the backend must not have
				// seen the statement at all.
				assert.Equal(t, calledBefore, db.GetQueryCalledNum(tc.query), "the backend must not see a statement the ACL denied")
				assert.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err))
				// The denial names the caller's groups like a per-table one does.
				require.EqualError(t, err, tc.planID.String()+" command denied to user 'u2', in groups [eng, beta], for a table set that cannot be determined (ACL check error)")
				assert.Equal(t, deniedBefore+1, tsv.stats.TableaclDenied.Counts()[statsKey], "the denial must be counted under the undetermined-table key")
			})

			t.Run("exempt caller runs", func(t *testing.T) {
				// The escape hatch the gate relies on is applied before it.
				tsv := newServer(t, enableStrictTableACL)
				f, err := tableacl.GetCurrentACLFactory()
				require.NoError(t, err)
				tsv.qe.exemptACL, err = f.New([]string{"exempt-acl"})
				require.NoError(t, err)
				exemptCtx := callerid.NewContext(t.Context(), nil, &querypb.VTGateCallerID{Username: "exempt-acl"})
				qre := newTestQueryExecutor(exemptCtx, tsv, tc.query, 0)
				calledBefore := db.GetQueryCalledNum(tc.query)
				_, err = qre.Execute()
				require.NoError(t, err, "an exempt caller must still be able to run the statement under strict table ACL")
				assert.Equal(t, calledBefore+1, db.GetQueryCalledNum(tc.query), "the statement must reach the backend")
			})

			t.Run("dry run only records", func(t *testing.T) {
				tsv := newServer(t, enableStrictTableACL)
				tsv.qe.enableTableACLDryRun = true
				qre := newTestQueryExecutor(ctx, tsv, tc.query, 0)
				pseudoBefore := tsv.stats.TableaclPseudoDenied.Counts()[statsKey]
				calledBefore := db.GetQueryCalledNum(tc.query)
				_, err := qre.Execute()
				require.NoError(t, err, "a dry run must not enforce the ACL")
				assert.Equal(t, calledBefore+1, db.GetQueryCalledNum(tc.query), "the statement must reach the backend")
				assert.Equal(t, pseudoBefore+1, tsv.stats.TableaclPseudoDenied.Counts()[statsKey], "a dry run must count the denial under the undetermined-table key")
			})

			t.Run("strict table ACL off runs", func(t *testing.T) {
				tsv := newServer(t, noFlags)
				qre := newTestQueryExecutor(ctx, tsv, tc.query, 0)
				calledBefore := db.GetQueryCalledNum(tc.query)
				_, err := qre.Execute()
				require.NoError(t, err, "with strict table ACL off the statement must still run")
				assert.Equal(t, calledBefore+1, db.GetQueryCalledNum(tc.query), "the statement must reach the backend")
			})
		})
	}
}

// TestQueryExecutorTableAclCTEBypass guards against GHSA-mv22-c3rp-c6m4: a
// non-recursive CTE that shares its name with a real table used to suppress the
// table's READER permission, letting a user denied READER read the table by
// wrapping it in a same-named CTE. The wrapped read must be denied just like a
// plain read.
func TestQueryExecutorTableAclCTEBypass(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	t.Cleanup(db.Close)

	username := "u2"
	callerID := &querypb.VTGateCallerID{Username: username}
	ctx := callerid.NewContext(t.Context(), nil, callerID)
	// u2 is not a reader of test_table; only superuser is.
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"superuser"},
		}},
	}
	require.NoError(t, tableacl.InitFromProto(config))

	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	t.Cleanup(tsv.StopService)

	query := "with test_table as (select * from test_table) select * from test_table"
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	require.NotEmpty(t, qre.plan.Permissions, "a permission must be derived for the real table read inside the CTE body")
	_, err := qre.Execute()
	require.Error(t, err, "CTE-wrapped read of test_table must not bypass the ACL")
	require.Equalf(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err), "qre.Execute: %v, want %v", vterrors.Code(err), vtrpcpb.Code_PERMISSION_DENIED)
}

func TestQueryExecutorTableAclDualTableExempt(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	callerID := &querypb.VTGateCallerID{
		Username: "basic_username",
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	// enable Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	query := "select * from test_table where 1 != 1"
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	assert.Equal(t, planbuilder.PlanSelectImpossible, qre.plan.PlanID)
	// query should fail because nobody has read access to test_table
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}

	assert.EqualError(t, err, `SelectImpossible command denied to user 'basic_username' for table 'test_table' (ACL check error)`)

	// table acl should be ignored when querying against dual table
	query = "select @@version_comment from dual limit 1"
	ctx = callerid.NewContext(context.Background(), nil, callerID)
	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	_, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute: %v, want: nil", err)
	}

	query = "(select 0 as x from dual where 1 != 1) union (select 1 as y from dual where 1 != 1)"
	ctx = callerid.NewContext(context.Background(), nil, callerID)
	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	_, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute: %v, want: nil", err)
	}
}

func TestQueryExecutorTableAclExemptACL(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
		Groups:   []string{"eng", "beta"},
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"u1"},
		}},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	// enable Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}
	assert.EqualError(t, err, `Select command denied to user 'u2', in groups [eng, beta], for table 'test_table' (ACL check error)`)

	// table acl should be ignored since this is an exempt user.
	username = "exempt-acl"
	f, _ := tableacl.GetCurrentACLFactory()
	if tsv.qe.exemptACL, err = f.New([]string{username}); err != nil {
		t.Fatalf("Cannot load exempt ACL for Table ACL: %v", err)
	}
	callerID = &querypb.VTGateCallerID{
		Username: username,
	}
	ctx = callerid.NewContext(context.Background(), nil, callerID)

	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	_, err = qre.Execute()
	if err != nil {
		t.Fatal("qre.Execute: nil, want: error")
	}
}

func TestQueryExecutorTableAclDryRun(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"u1"},
		}},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	tableACLStatsKey := strings.Join([]string{
		"test_table",
		"group02",
		planbuilder.PlanSelect.String(),
		username,
	}, ".")
	// enable Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	tsv.qe.enableTableACLDryRun = true
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	beforeCount := tsv.stats.TableaclPseudoDenied.Counts()[tableACLStatsKey]
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want: nil", err)
	}
	afterCount := tsv.stats.TableaclPseudoDenied.Counts()[tableACLStatsKey]
	if afterCount-beforeCount != 1 {
		t.Fatalf("table acl pseudo denied count should increase by one. got: %d, want: %d", afterCount, beforeCount+1)
	}
}

func TestQueryExecutorDenyListQRFail(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (`index`) where name = 1 limit 1000"
	expected := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	bannedAddr := "127.0.0.1"
	bannedUser := "u2"

	alterRule := rules.NewQueryRule("disable update", "disable update", rules.QRFail)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PlanSelect)
	alterRule.AddTableCond("test_table")

	rulesName := "denyListRulesQRFail"
	rules := rules.New()
	rules.Add(alterRule)

	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: bannedAddr,
		User:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	tsv := newTestTabletServer(ctx, noFlags, db)
	tsv.qe.queryRuleSources.UnRegisterSource(rulesName)
	tsv.qe.queryRuleSources.RegisterSource(rulesName)
	defer tsv.qe.queryRuleSources.UnRegisterSource(rulesName)

	if err := tsv.qe.queryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()

	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	// execute should fail because query has a table which is part of the denylist
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
}

func TestQueryExecutorDenyListQRRetry(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (`index`) where name = 1 limit 1000"
	expected := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	bannedAddr := "127.0.0.1"
	bannedUser := "x"

	alterRule := rules.NewQueryRule("disable update", "disable update", rules.QRFailRetry)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PlanSelect)
	alterRule.AddTableCond("test_table")

	rulesName := "denyListRulesQRRetry"
	rules := rules.New()
	rules.Add(alterRule)

	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: bannedAddr,
		User:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	tsv := newTestTabletServer(ctx, noFlags, db)
	tsv.qe.queryRuleSources.UnRegisterSource(rulesName)
	tsv.qe.queryRuleSources.RegisterSource(rulesName)
	defer tsv.qe.queryRuleSources.UnRegisterSource(rulesName)

	if err := tsv.qe.queryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()

	assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_FAILED_PRECONDITION {
		t.Fatalf("tsv.qe.queryRuleSources.SetRules: %v, want %v", code, vtrpcpb.Code_FAILED_PRECONDITION)
	}
}

func TestReplaceSchemaName(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	queryFmt := "select * from information_schema.`schema_name` where `schema_name` = %s"
	inQuery := fmt.Sprintf(queryFmt, ":"+sqltypes.BvSchemaName)
	wantQuery := fmt.Sprintf(queryFmt, fmt.Sprintf(
		"'%s' limit %d",
		db.Name(),
		10001,
	))
	wantQueryStream := fmt.Sprintf(queryFmt, fmt.Sprintf(
		"'%s'",
		db.Name(),
	))

	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	db.AddQuery(wantQuery, &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	db.AddQuery(wantQueryStream, &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	// Test non streaming execute.
	{
		qre := newTestQueryExecutor(ctx, tsv, inQuery, 0)
		assert.Equal(t, planbuilder.PlanSelect, qre.plan.PlanID)
		// Any value other than nil should cause QueryExecutor to replace the
		// schema name.
		qre.bindVars[sqltypes.BvReplaceSchemaName] = sqltypes.NullBindVariable
		_, err := qre.Execute()
		require.NoError(t, err)
		_, ok := qre.bindVars[sqltypes.BvSchemaName]
		require.True(t, ok)
	}

	// Test streaming execute.
	{
		qre := newTestQueryExecutorStreaming(ctx, tsv, inQuery, 0)
		// Stream only replaces schema name when plan is PlanSelectStream.
		assert.Equal(t, planbuilder.PlanSelectStream, qre.plan.PlanID)
		// Any value other than nil should cause QueryExecutor to replace the
		// schema name.
		qre.bindVars[sqltypes.BvReplaceSchemaName] = sqltypes.NullBindVariable
		err := qre.Stream(func(_ *sqltypes.Result) error {
			_, ok := qre.bindVars[sqltypes.BvSchemaName]
			require.True(t, ok)
			return nil
		})
		require.NoError(t, err)
	}
}

func TestQueryExecutorShouldConsolidate(t *testing.T) {
	testCases := []struct {
		// whether or not the consolidator is enabled by default on the tablet
		consolidatorEnabledByDefault bool
		// query-specific consolidator override, unspecified by default
		consolidatorExecuteOption querypb.ExecuteOptions_Consolidator
		// whether or not the consolidator is waiting on the results of an
		// identical running query
		consolidatorHasIdenticalQuery bool
		// whether or not the query should be consolidated
		expectConsolidate bool
		// whether or not the query should be exec'd (= sent to db)
		expectExec bool
		// query to run
		input string
	}{
		{
			consolidatorEnabledByDefault:  true,
			consolidatorExecuteOption:     querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED,
			consolidatorHasIdenticalQuery: false,
			expectConsolidate:             true,
			expectExec:                    true,
			input:                         "select * from t limit 10001",
		},
		{
			consolidatorEnabledByDefault:  true,
			consolidatorExecuteOption:     querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED,
			consolidatorHasIdenticalQuery: true,
			expectConsolidate:             true,
			expectExec:                    false,
			input:                         "select * from t limit 10001",
		},
		{
			consolidatorEnabledByDefault:  true,
			consolidatorExecuteOption:     querypb.ExecuteOptions_CONSOLIDATOR_DISABLED,
			consolidatorHasIdenticalQuery: true,
			expectConsolidate:             false,
			expectExec:                    true,
			input:                         "select * from t limit 10001",
		},
		{
			consolidatorEnabledByDefault:  false,
			consolidatorExecuteOption:     querypb.ExecuteOptions_CONSOLIDATOR_DISABLED,
			consolidatorHasIdenticalQuery: true,
			expectConsolidate:             false,
			expectExec:                    true,
			input:                         "select * from t limit 10001",
		},
		{
			consolidatorEnabledByDefault:  false,
			consolidatorExecuteOption:     querypb.ExecuteOptions_CONSOLIDATOR_ENABLED,
			consolidatorHasIdenticalQuery: false,
			expectConsolidate:             true,
			expectExec:                    true,
			input:                         "select * from t limit 10001",
		},
		{
			consolidatorEnabledByDefault:  false,
			consolidatorExecuteOption:     querypb.ExecuteOptions_CONSOLIDATOR_ENABLED,
			consolidatorHasIdenticalQuery: true,
			expectConsolidate:             true,
			expectExec:                    false,
			input:                         "select * from t limit 10001",
		},
	}
	for _, tcase := range testCases {
		name := fmt.Sprintf("table-consolidator:%t;query-consolidator:%v;identical-query:%t",
			tcase.consolidatorEnabledByDefault, tcase.consolidatorExecuteOption, tcase.consolidatorHasIdenticalQuery)
		t.Run(name, func(t *testing.T) {
			// Set up fake db, tablet server (with fake consolidator), and executor.

			db := setUpQueryExecutorTest(t)
			defer db.Close()

			ctx := context.Background()
			flags := noFlags
			if tcase.consolidatorEnabledByDefault {
				flags = enableConsolidator
			}

			tsv := newTestTabletServer(ctx, flags, db)
			defer tsv.StopService()

			fakeConsolidator := sync2.NewFakeConsolidator()
			tsv.qe.consolidator = fakeConsolidator

			qre := newTestQueryExecutor(context.Background(), tsv, tcase.input, 0)
			qre.options = &querypb.ExecuteOptions{Consolidator: tcase.consolidatorExecuteOption}

			result := &sqltypes.Result{
				Fields: getTestTableFields(),
			}

			// Set up consolidator pre-conditions.

			fakePendingResult := &sync2.FakePendingResult{}
			fakePendingResult.SetResult(result)
			fakeConsolidator.CreateReturn = &sync2.FakeConsolidatorCreateReturn{
				Created:       !tcase.consolidatorHasIdenticalQuery,
				PendingResult: fakePendingResult,
			}

			// Set up database query/response.

			db.AddQuery(tcase.input, result)

			// Execute query.

			_, err := qre.Execute()
			require.Nil(t, err)

			// Verify expectations.

			if tcase.expectConsolidate {
				require.Len(t, fakeConsolidator.CreateCalls, 1)
				require.Len(t, fakeConsolidator.CreateReturns, 1)
				if tcase.consolidatorHasIdenticalQuery {
					require.Equal(t, 0, fakePendingResult.BroadcastCalls)
					require.Equal(t, 1, fakePendingResult.WaitCalls)
				} else {
					require.Equal(t, 1, fakePendingResult.BroadcastCalls)
					require.Equal(t, 0, fakePendingResult.WaitCalls)
				}
			} else {
				require.Len(t, fakeConsolidator.CreateCalls, 0)
			}

			if tcase.expectExec {
				require.Equal(t, 1, db.GetQueryCalledNum(tcase.input))
			} else {
				require.Equal(t, 0, db.GetQueryCalledNum(tcase.input))
			}

			db.VerifyAllExecutedOrFail()
		})
	}
}

func TestQueryExecutorConsolidatorWaiterCapFallback(t *testing.T) {
	// Test that when the consolidator waiter cap is reached, queries fall back
	// to independent execution instead of returning empty results.

	db := setUpQueryExecutorTest(t)
	defer db.Close()

	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableConsolidator, db)
	defer tsv.StopService()

	// Set a waiter cap of 1
	tsv.config.ConsolidatorQueryWaiterCap = 1

	fakeConsolidator := sync2.NewFakeConsolidator()
	tsv.qe.consolidator = fakeConsolidator

	input := "select * from t limit 10001"
	result := &sqltypes.Result{
		Fields: getTestTableFields(),
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),   // pk
			sqltypes.NewInt32(100), // name
			sqltypes.NewInt32(200), // addr
		}},
	}

	// Set up consolidator to simulate an identical query already running (Created=false)
	fakePendingResult := &sync2.FakePendingResult{}
	fakePendingResult.SetResult(result)
	// Start with waiter count above the cap (2 > 1), so the condition fails
	fakePendingResult.WaiterCount = 2

	fakeConsolidator.CreateReturn = &sync2.FakeConsolidatorCreateReturn{
		Created:       false, // Simulate identical query already running
		PendingResult: fakePendingResult,
	}

	// Set up database query/response for fallback execution
	db.AddQuery(input, result)

	qre := newTestQueryExecutor(context.Background(), tsv, input, 0)
	qre.options = &querypb.ExecuteOptions{Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED}

	// Execute query
	actualResult, err := qre.Execute()
	require.NoError(t, err)
	require.NotNil(t, actualResult)

	// Verify we got the correct result (not empty)
	require.Equal(t, result.Fields, actualResult.Fields)
	require.Equal(t, result.Rows, actualResult.Rows)

	// Verify consolidator was attempted
	require.Len(t, fakeConsolidator.CreateCalls, 1)

	// Verify we did NOT wait (because waiter cap was exceeded)
	require.Equal(t, 0, fakePendingResult.WaitCalls)

	// Verify we did NOT broadcast (because we're not the original)
	require.Equal(t, 0, fakePendingResult.BroadcastCalls)

	// Verify AddWaiterCounter was called: once with 0 (to check count), once with -1 (cleanup)
	require.Len(t, fakePendingResult.AddWaiterCounterCalls, 2)
	require.Equal(t, int64(0), fakePendingResult.AddWaiterCounterCalls[0])  // Check current count
	require.Equal(t, int64(-1), fakePendingResult.AddWaiterCounterCalls[1]) // Decrement

	// Verify fallback executed the query independently
	require.Equal(t, 1, db.GetQueryCalledNum(input))

	db.VerifyAllExecutedOrFail()
}

func TestGetConnectionLogStats(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()

	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	input := "select * from test_table limit 1"

	// getConn() happy path
	qre := newTestQueryExecutor(ctx, tsv, input, 0)
	conn, err := qre.getConn()
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.True(t, qre.logStats.WaitingForConnection > 0)

	// getStreamConn() happy path
	qre = newTestQueryExecutor(ctx, tsv, input, 0)
	conn, err = qre.getStreamConn()
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.True(t, qre.logStats.WaitingForConnection > 0)

	// Close the db connection to induce connection errors
	db.Close()

	// getConn() error path
	qre = newTestQueryExecutor(ctx, tsv, input, 0)
	_, err = qre.getConn()
	assert.Error(t, err)
	assert.True(t, qre.logStats.WaitingForConnection > 0)

	// getStreamConn() error path
	qre = newTestQueryExecutor(ctx, tsv, input, 0)
	_, err = qre.getStreamConn()
	assert.Error(t, err)
	assert.True(t, qre.logStats.WaitingForConnection > 0)
}

type executorFlags int64

const (
	noFlags              executorFlags = 0
	enableStrictTableACL               = 1 << iota
	smallTxPool
	noTwopc
	shortTwopcAge
	smallResultSize
	disableOnlineDDL
	enableConsolidator
)

// newTestQueryExecutor uses a package level variable testTabletServer defined in tabletserver_test.go
func newTestTabletServer(ctx context.Context, flags executorFlags, db *fakesqldb.DB) *TabletServer {
	cfg := tabletenv.NewDefaultConfig()
	cfg.OltpReadPool.Size = 100
	if flags&smallTxPool > 0 {
		cfg.TxPool.Size = 3
	} else {
		cfg.TxPool.Size = 100
	}
	if flags&enableStrictTableACL > 0 {
		cfg.StrictTableACL = true
	} else {
		cfg.StrictTableACL = false
	}
	if flags&disableOnlineDDL > 0 {
		cfg.EnableOnlineDDL = false
	} else {
		cfg.EnableOnlineDDL = true
	}
	if flags&noTwopc > 0 {
		cfg.TwoPCAbandonAge = 0
	} else if flags&shortTwopcAge > 0 {
		cfg.TwoPCAbandonAge = 500 * time.Millisecond
	} else {
		cfg.TwoPCAbandonAge = 10 * time.Second
	}
	if flags&smallResultSize > 0 {
		cfg.Oltp.MaxRows = 2
	}
	if flags&enableConsolidator > 0 {
		cfg.Consolidator = tabletenv.Enable
	} else {
		cfg.Consolidator = tabletenv.Disable
	}
	dbconfigs := newDBConfigs(db)
	cfg.DB = dbconfigs
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	tsv := NewTabletServer(ctx, vtenv.NewTestEnv(), "TabletServerTest", cfg, memorytopo.NewServer(ctx, ""), &topodatapb.TabletAlias{}, srvTopoCounts)
	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	err := tsv.StartService(target, dbconfigs, nil /* mysqld */)
	if err != nil {
		panic(err)
	}
	return tsv
}

func newTransaction(tsv *TabletServer, options *querypb.ExecuteOptions) int64 {
	target := tsv.sm.Target()
	state, err := tsv.Begin(context.Background(), nil, target, options)
	if err != nil {
		panic(vterrors.Wrap(err, "failed to start a transaction"))
	}
	return state.TransactionID
}

func newTestQueryExecutor(ctx context.Context, tsv *TabletServer, sql string, txID int64) *QueryExecutor {
	return newTestQueryExecutorWithRowsLimit(ctx, tsv, sql, txID, false)
}

func newTestQueryExecutorWithRowsLimit(ctx context.Context, tsv *TabletServer, sql string, txID int64, noRowsLimit bool) *QueryExecutor {
	logStats := tabletenv.NewLogStats(ctx, "TestQueryExecutor", streamlog.NewQueryLogConfigForTest())
	plan, err := tsv.qe.GetPlan(ctx, logStats, sql, false, noRowsLimit)
	if err != nil {
		panic(err)
	}
	return newQueryExec(ctx, tsv, sql, txID, plan, logStats)
}

func newTestQueryExecutorStreaming(ctx context.Context, tsv *TabletServer, sql string, txID int64) *QueryExecutor {
	logStats := tabletenv.NewLogStats(ctx, "TestQueryExecutorStreaming", streamlog.NewQueryLogConfigForTest())
	plan, err := tsv.qe.GetStreamPlan(ctx, logStats, sql, false)
	if err != nil {
		panic(err)
	}
	return newQueryExec(ctx, tsv, sql, txID, plan, logStats)
}

func newQueryExec(ctx context.Context, tsv *TabletServer, sql string, txID int64, plan *TabletPlan, logStats *tabletenv.LogStats) *QueryExecutor {
	return &QueryExecutor{
		ctx:      ctx,
		query:    sql,
		bindVars: make(map[string]*querypb.BindVariable),
		connID:   txID,
		plan:     plan,
		logStats: logStats,
		tsv:      tsv,
	}
}

func setUpQueryExecutorTest(t *testing.T) *fakesqldb.DB {
	db := fakesqldb.New(t)
	initQueryExecutorTestDB(db)
	return db
}

const baseShowTablesWithSizesPattern = `SELECT t\.table_name.*SUM\(i\.file_size\).*`

func initQueryExecutorTestDB(db *fakesqldb.DB) {
	addQueryExecutorSupportedQueries(db)
	db.AddQueryPattern(baseShowTablesWithSizesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesWithSizesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesWithSizesRow("test_table", false, ""),
			mysql.BaseShowTablesWithSizesRow("seq", false, "vitess_sequence"),
			mysql.BaseShowTablesWithSizesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
		},
	})
	db.AddQuery(mysql.BaseShowTables,
		&sqltypes.Result{
			Fields: mysql.BaseShowTablesFields,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table", false, ""),
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		})
	db.AddQuery("show status like 'Innodb_rows_read'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Innodb_rows_read|0",
	))
	sidecardb.AddSchemaInitQueries(db, true, sqlparser.NewTestParser())
}

func getTestTableFields() []*querypb.Field {
	return []*querypb.Field{
		{Name: "pk", Type: sqltypes.Int32},
		{Name: "name", Type: sqltypes.Int32},
		{Name: "addr", Type: sqltypes.Int32},
	}
}

func addQueryExecutorSupportedQueries(db *fakesqldb.DB) {
	queryResultMap := map[string]*sqltypes.Result{
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1427325875)},
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
		"select @@version_comment from dual where 1 != 1": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
		},
		"select @@version_comment from dual limit 1": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("fakedb server")},
			},
		},
		"select 0 as x from dual where 1 != 1 union select 1 as y from dual where 1 != 1": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"select 0 as x from dual where 1 != 1 union select 1 as y from dual where 1 != 1 limit 10001": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"select * from t where 1 != 1 limit 10001": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}, {
				Type: sqltypes.VarChar,
			}},
			Rows: [][]sqltypes.Value{},
		},
		mysql.BaseShowPrimary: {
			Fields: mysql.ShowPrimaryFields,
			Rows: [][]sqltypes.Value{
				mysql.ShowPrimaryRow("test_table", "pk"),
				mysql.ShowPrimaryRow("seq", "id"),
				mysql.ShowPrimaryRow("msg", "id"),
			},
		},
		"begin":                                {},
		"commit":                               {},
		"rollback":                             {},
		fmt.Sprintf(readAllRedo, "_vt", "_vt"): {},
	}

	sidecardb.AddSchemaInitQueries(db, true, sqlparser.NewTestParser())
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
		}},
	})
	db.MockQueriesForTable("seq", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int32,
		}, {
			Name: "next_id",
			Type: sqltypes.Int64,
		}, {
			Name: "cache",
			Type: sqltypes.Int64,
		}, {
			Name: "increment",
			Type: sqltypes.Int64,
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

func TestQueryExecSchemaReloadCount(t *testing.T) {
	type dbResponse struct {
		query  string
		result *sqltypes.Result
	}

	dmlResult := &sqltypes.Result{
		RowsAffected: 1,
	}
	fields := sqltypes.MakeTestFields("a|b", "int64|varchar")
	selectResult := sqltypes.MakeTestResult(fields, "1|aaa")
	emptyResult := &sqltypes.Result{}

	// The queries are run both in and outside a transaction.
	testcases := []struct {
		// input is the input query.
		input string
		// dbResponses specifies the list of queries and responses to add to the fake db.
		dbResponses       []dbResponse
		schemaReloadCount int
	}{{
		input: "select * from t",
		dbResponses: []dbResponse{{
			query:  `select \* from t.*`,
			result: selectResult,
		}},
	}, {
		input: "insert into t values(1, 'aaa')",
		dbResponses: []dbResponse{{
			query:  "insert.*",
			result: dmlResult,
		}},
	}, {
		input: "create table t(a int, b varchar(64))",
		dbResponses: []dbResponse{{
			query:  "create.*",
			result: emptyResult,
		}},
		schemaReloadCount: 1,
	}, {
		input: "drop table t",
		dbResponses: []dbResponse{{
			query:  "drop.*",
			result: dmlResult,
		}},
		schemaReloadCount: 1,
	}, {
		input: "create table t(a int, b varchar(64))",
		dbResponses: []dbResponse{{
			query:  "create.*",
			result: emptyResult,
		}},
		schemaReloadCount: 1,
	}, {
		input: "drop table t",
		dbResponses: []dbResponse{{
			query:  "drop.*",
			result: dmlResult,
		}},
		schemaReloadCount: 1,
	}}
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(context.Background(), noFlags, db)
	tsv.config.DB.DBName = "ks"
	defer tsv.StopService()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			// reset the schema reload metric before running the test.
			tsv.se.SchemaReloadTimings.Reset()
			for _, dbr := range tcase.dbResponses {
				db.AddQueryPattern(dbr.query, dbr.result)
			}

			qre := newTestQueryExecutor(context.Background(), tsv, tcase.input, 0)
			_, err := qre.Execute()
			require.NoError(t, err)
			assert.EqualValues(t, tcase.schemaReloadCount, qre.tsv.se.SchemaReloadTimings.Counts()["TabletServerTest.SchemaReload"], "got: %v", qre.tsv.se.SchemaReloadTimings.Counts())
		})
	}
}

type mockTxThrottler struct {
	throttle bool
}

func (m mockTxThrottler) InitDBConfig(target *querypb.Target) {
	panic("implement me")
}

func (m mockTxThrottler) Open() (err error) {
	return nil
}

func (m mockTxThrottler) Close() {
}

func (m mockTxThrottler) Throttle(priority int, workload string) (result bool) {
	return m.throttle
}
<<<<<<< HEAD
||||||| parent of ea4a61357a (VTTablet: Discard the pooled connection after CALL so procedure session state cannot leak (#21062))

// TestExecProcClosesConnOnError verifies that a failed CALL on a reserved
// connection closes that connection. A stored procedure can start a
// transaction that Vitess does not track; since a reserved-connection timeout
// now kills only the query (KILL QUERY) and leaves the connection open, the
// connection must be closed on any CALL error so no untracked transaction (and
// its locks) survives to be kept alive by the heartbeat.
func TestExecProcClosesConnOnError(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	// Reserve a connection. The real caller unlocks it after the CALL returns;
	// once closed, Unlock releases it from the pool so shutdown can drain.
	conn, err := tsv.te.txPool.scp.NewConn(ctx, &querypb.ExecuteOptions{}, nil)
	require.NoError(t, err)
	defer conn.Unlock()
	require.False(t, conn.IsClosed())

	// A CALL that the server rejects.
	query := "call test_proc()"
	db.AddRejectedQuery(query, errors.New("procedure failed"))

	qre := newTestQueryExecutor(ctx, tsv, query, conn.ReservedID())
	require.Equal(t, planbuilder.PlanCallProc, qre.plan.PlanID)

	_, err = qre.execProc(conn)
	require.Error(t, err)
	require.True(t, conn.IsClosed(), "a reserved connection must be closed when its CALL fails, so no untracked transaction survives")

	// Inside a Vitess-tracked transaction the hazard does not exist: a query
	// timeout still kills the whole connection (ExecOnce insideTxn), so no
	// untracked in-proc transaction can outlive it. A benign CALL error — a
	// SIGNAL from the procedure, a typo'd name — must therefore leave the
	// transaction usable, exactly as it does on a direct MySQL connection.
	txConn, _, _, err := tsv.te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	defer txConn.Release(tx.TxRollback)
	require.True(t, txConn.IsInTransaction())

	qreTx := newTestQueryExecutor(ctx, tsv, query, txConn.ReservedID())
	_, err = qreTx.execProc(txConn)
	require.Error(t, err)
	require.False(t, txConn.IsClosed(), "a benign CALL error inside a tracked transaction must not destroy the transaction")
}

// TestExecStreamSQLTimeoutConnFateByPlan verifies the streaming stateful path
// shares the buffered path's timeout decision: a timed-out safe statement
// (reads) keeps the reserved connection (KILL QUERY only), while a statement
// whose interruption could leave unrecorded session state (SET) loses the
// whole connection — otherwise a half-applied streaming SET would survive on a
// connection the temp-table keepalive then pins alive.
func TestExecStreamSQLTimeoutConnFateByPlan(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(t.Context(), noFlags, db)
	defer tsv.StopService()

	db.AddQueryPattern(`kill query \d+`, &sqltypes.Result{})
	db.AddQueryPattern(`kill \d+`, &sqltypes.Result{})
	db.AddQuery(resetLastIDQuery, &sqltypes.Result{})

	cases := []struct {
		sql               string
		keepsConn         bool
		fetchLastInsertID bool
	}{
		{"select 1", true, false},
		{"set @@sql_mode = ''", false, false},
		// A safe plan carrying fetch_last_insert_id still loses the
		// connection: MySQL retains a last_insert_id(expr) assignment after
		// KILL QUERY, and the error return skips the post-exec fetch that
		// keeps the vtgate session in sync with the connection.
		{"select last_insert_id(42)", false, true},
		// DML normally keeps the connection (rows roll back atomically), but
		// a mutating lock function reached through DML can be granted or
		// released just as the kill lands — lock state the session never
		// recorded — so the connection is lost.
		{"update test_table set name = 1 where get_lock('foo', 10) = 1", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			db.AddQuery(tc.sql, &sqltypes.Result{})
			db.SetBeforeFunc(tc.sql, func() {
				// Outlasts the context deadline so the statement is interrupted.
				time.Sleep(1 * time.Second)
			})

			conn, err := tsv.te.txPool.scp.NewConn(t.Context(), &querypb.ExecuteOptions{}, nil)
			require.NoError(t, err)
			defer conn.Unlock()

			execCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			qre := newTestQueryExecutorStreaming(execCtx, tsv, tc.sql, conn.ReservedID())
			if tc.fetchLastInsertID {
				if qre.options == nil {
					qre.options = &querypb.ExecuteOptions{}
				}
				qre.options.FetchLastInsertId = true
			}
			err = qre.execStreamSQL(conn.UnderlyingDBConn(), true /* isStateful */, false /* insideTxn */, tc.sql, func(*sqltypes.Result) error { return nil })
			require.Error(t, err)
			if tc.keepsConn {
				require.False(t, conn.IsClosed(), "a timed-out safe streaming statement must keep its connection")
			} else {
				require.True(t, conn.IsClosed(), "a timed-out unsafe streaming statement must lose its connection")
			}
		})
	}
}

// TestPlanKeepsConnOnTimeout pins which plan types may keep their stateful
// connection when a timeout kills only the query: reads and DML (whose kill
// leaves no session state behind), and nothing else — a killed SET or lock
// function can leave session state the session never recorded, which the
// temp-table keepalive would then preserve indefinitely.
func TestPlanKeepsConnOnTimeout(t *testing.T) {
	safe := []planbuilder.PlanType{
		planbuilder.PlanSelect, planbuilder.PlanSelectImpossible, planbuilder.PlanSelectNoLimit, planbuilder.PlanShow,
		planbuilder.PlanInsert, planbuilder.PlanUpdate, planbuilder.PlanDelete, planbuilder.PlanInsertMessage,
		planbuilder.PlanUpdateLimit, planbuilder.PlanDeleteLimit,
	}
	for _, id := range safe {
		assert.True(t, planKeepsConnOnTimeout(id), "%s must keep its connection on a query timeout", id)
	}
	unsafe := []planbuilder.PlanType{
		planbuilder.PlanSet, planbuilder.PlanSelectLockFunc, planbuilder.PlanCallProc,
		planbuilder.PlanDDL, planbuilder.PlanLoad, planbuilder.PlanFlush,
		planbuilder.PlanOtherRead, planbuilder.PlanOtherAdmin, planbuilder.PlanUnlockTables,
	}
	for _, id := range unsafe {
		assert.False(t, planKeepsConnOnTimeout(id), "%s must lose its connection on a query timeout", id)
	}

	// fetch_last_insert_id overrides a safe plan: the killed statement can
	// retain a last_insert_id on the connection that the skipped post-exec
	// fetch never reported to the vtgate session.
	qre := &QueryExecutor{
		plan:    &TabletPlan{Plan: &planbuilder.Plan{PlanID: planbuilder.PlanSelect}},
		options: &querypb.ExecuteOptions{FetchLastInsertId: true},
	}
	assert.False(t, qre.keepsConnOnTimeout(), "fetch_last_insert_id must lose the connection on a query timeout")
	qre.options.FetchLastInsertId = false
	assert.True(t, qre.keepsConnOnTimeout(), "a safe plan without fetch_last_insert_id keeps the connection")

	// A mutating lock function in DML overrides the plan type the same way:
	// the rows roll back under KILL QUERY but the lock grant or release can
	// race the kill, leaving lock state the session never recorded.
	qre = &QueryExecutor{
		plan:    &TabletPlan{Plan: &planbuilder.Plan{PlanID: planbuilder.PlanUpdate, KillsConnOnTimeout: true}},
		options: &querypb.ExecuteOptions{},
	}
	assert.False(t, qre.keepsConnOnTimeout(), "DML with a mutating lock function must lose the connection on a query timeout")
}
=======

// TestExecCallProcDiscardsConn pins the fix for vitessio/vitess#21046: a CALL on
// a pooled connection may leave session state behind (a SET SESSION or a
// temporary table inside the procedure body is invisible to the tablet's
// statement classification), so the connection is closed after the CALL rather
// than returned to the pool — on success, after a statement error, and after
// draining a multi-resultset — and the CALL's own outcome is what the caller
// sees.
func TestExecCallProcDiscardsConn(t *testing.T) {
	ctx := t.Context()
	query := "call test_proc()"
	newExecutor := func(t *testing.T) (*fakesqldb.DB, *TabletServer) {
		db := setUpQueryExecutorTest(t)
		t.Cleanup(db.Close)
		tsv := newTestTabletServer(ctx, noFlags, db)
		t.Cleanup(tsv.StopService)
		return db, tsv
	}
	// The MySQL connection the CALL ran on must be gone from the server's side
	// afterwards, so no later borrower can ever be handed it.
	callConnDiscarded := func(t *testing.T, db *fakesqldb.DB, tsv *TabletServer) {
		t.Helper()
		callConns := db.QueryConnIDs(query)
		require.Len(t, callConns, 1, "the CALL must have run once")
		// Counted first: the discard is recorded before the CALL returns, so a
		// regression fails here immediately rather than after the wait below.
		require.EqualValues(t, 1, tsv.qe.conns.Metrics.DiscardedByCallerCount(), "the discarded connection must be counted")
		require.Eventually(t, func() bool { return !db.IsConnectionOpen(callConns[0]) },
			30*time.Second, 10*time.Millisecond, "the connection a CALL ran on must be closed, not returned to the pool")
	}

	t.Run("success", func(t *testing.T) {
		db, tsv := newExecutor(t)
		db.AddQuery(query, &sqltypes.Result{})
		qre := newTestQueryExecutor(ctx, tsv, query, 0)
		require.Equal(t, planbuilder.PlanCallProc, qre.plan.PlanID)

		_, err := qre.Execute()
		require.NoError(t, err)
		callConnDiscarded(t, db, tsv)
	})
	t.Run("statement error", func(t *testing.T) {
		// A procedure that dirtied the session and then failed (a SIGNAL, a
		// typo'd name) leaves the same residue as one that succeeded, and the
		// caller sees the procedure's error.
		db, tsv := newExecutor(t)
		db.AddRejectedQuery(query, errors.New("procedure failed"))
		qre := newTestQueryExecutor(ctx, tsv, query, 0)

		_, err := qre.Execute()
		require.ErrorContains(t, err, "procedure failed")
		callConnDiscarded(t, db, tsv)
	})
	t.Run("a CALL that never reached MySQL keeps its connection", func(t *testing.T) {
		// A missing bind variable fails the CALL before any statement is sent,
		// so the connection's session is untouched and must not be discarded:
		// malformed requests must not churn connections.
		db, tsv := newExecutor(t)
		// Warm exactly one pool connection; an idle pool hands the most recently
		// returned connection out first, so the same id afterwards proves the
		// CALL did not cost it.
		const next = "select 1 from dual limit 10001"
		db.AddQuery(next, &sqltypes.Result{})
		_, err := newTestQueryExecutor(ctx, tsv, "select 1 from dual", 0).Execute()
		require.NoError(t, err)
		warmConns := db.QueryConnIDs(next)
		require.Len(t, warmConns, 1)

		qre := newTestQueryExecutor(ctx, tsv, "call test_proc(:missing)", 0)
		require.Equal(t, planbuilder.PlanCallProc, qre.plan.PlanID)
		_, err = qre.Execute()
		require.ErrorContains(t, err, "missing bind var")
		assert.NotContains(t, db.QueryLog(), "call test_proc(", "nothing must have reached MySQL")
		assert.Zero(t, tsv.qe.conns.Metrics.DiscardedByCallerCount(), "a CALL that was never sent must not cost the connection")

		_, err = newTestQueryExecutor(ctx, tsv, "select 1 from dual", 0).Execute()
		require.NoError(t, err)
		afterConns := db.QueryConnIDs(next)
		require.Len(t, afterConns, 2)
		assert.Equal(t, warmConns[0], afterConns[1], "the connection must still be the one the pool had")
	})
	t.Run("a timed-out CALL is discarded and counted", func(t *testing.T) {
		// A query timeout kills only the query on a pooled connection, leaving
		// it open — so the CALL policy is what discards it, and that is counted.
		db, tsv := newExecutor(t)
		db.AddQuery(query, &sqltypes.Result{})
		execCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		// Cancelled from inside the CALL rather than on a deadline that could
		// expire while the plan is built or the connection borrowed: by the
		// time this runs the fake has recorded the connection the CALL is on,
		// and the pause holds it there while the kill lands.
		db.SetBeforeFunc(query, func() {
			cancel()
			time.Sleep(100 * time.Millisecond)
		})
		db.AddQueryPattern(`kill query \d+`, &sqltypes.Result{})
		qre := newTestQueryExecutor(execCtx, tsv, query, 0)

		_, err := qre.Execute()
		require.Error(t, err)
		callConnDiscarded(t, db, tsv)
	})
	t.Run("an appdebug connection is not counted as a discard", func(t *testing.T) {
		// The appdebug caller gets a standalone connection that Recycle closes
		// after every query regardless of the CALL policy: no pool member is
		// lost, so nothing must be counted against the pool.
		db, tsv := newExecutor(t)
		db.AddQuery(query, &sqltypes.Result{})
		debugParams, err := tsv.config.DB.AppDebugWithDB().MysqlParams()
		require.NoError(t, err)
		debugUser := debugParams.Uname
		require.NotEmpty(t, debugUser)
		debugCtx := callerid.NewContext(ctx, callerid.NewEffectiveCallerID("p", "c", "sc"), callerid.NewImmediateCallerID(debugUser))
		qre := newTestQueryExecutor(debugCtx, tsv, query, 0)

		_, err = qre.Execute()
		require.NoError(t, err)
		require.Len(t, db.QueryConnIDs(query), 1)
		assert.Zero(t, tsv.qe.conns.Metrics.DiscardedByCallerCount(), "an appdebug connection is never a pool member, so it must not count as a discard")
	})
	t.Run("streaming discard is attributed to the streaming pool", func(t *testing.T) {
		db, tsv := newExecutor(t)
		db.AddQuery(query, &sqltypes.Result{})
		qre := newTestQueryExecutorStreaming(ctx, tsv, query, 0)
		require.Equal(t, planbuilder.PlanCallProc, qre.plan.PlanID)

		err := qre.Stream(func(*sqltypes.Result) error { return nil })
		require.NoError(t, err)
		callConns := db.QueryConnIDs(query)
		require.Len(t, callConns, 1)
		require.EqualValues(t, 1, tsv.qe.streamConns.Metrics.DiscardedByCallerCount(), "the discard must be counted on the streaming pool")
		require.Zero(t, tsv.qe.conns.Metrics.DiscardedByCallerCount(), "and not on the OLTP pool")
		require.Eventually(t, func() bool { return !db.IsConnectionOpen(callConns[0]) },
			30*time.Second, 10*time.Millisecond, "the streaming connection a CALL ran on must be closed")

		// A streaming CALL that fails with the procedure's own error also
		// costs its connection, and that discard must be counted like the
		// buffered path counts it.
		db.AddRejectedQuery(query, errors.New("procedure failed"))
		err = newTestQueryExecutorStreaming(ctx, tsv, query, 0).Stream(func(*sqltypes.Result) error { return nil })
		require.ErrorContains(t, err, "procedure failed")
		assert.EqualValues(t, 2, tsv.qe.streamConns.Metrics.DiscardedByCallerCount(), "a failed streaming CALL's discard must be counted too")
	})
}

// TestExecProcKeepsReservedConn pins the scope of the post-CALL discard: on a
// reserved or transaction connection the session belongs to the caller, and
// closing it would destroy that caller's own SETs and temporary tables.
func TestExecProcKeepsReservedConn(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	query := "call test_proc()"
	db.AddQuery(query, &sqltypes.Result{})

	conn, err := tsv.te.txPool.scp.NewConn(ctx, &querypb.ExecuteOptions{}, nil)
	require.NoError(t, err)
	defer conn.Unlock()
	qre := newTestQueryExecutor(ctx, tsv, query, conn.ReservedID())
	_, err = qre.execProc(conn)
	require.NoError(t, err)
	assert.False(t, conn.IsClosed(), "a CALL on a reserved connection must keep the caller's own session")
	assert.Zero(t, tsv.qe.conns.Metrics.DiscardedByCallerCount())
}

// TestExecProcClosesConnOnError verifies that a failed CALL on a reserved
// connection closes that connection. A stored procedure can start a
// transaction that Vitess does not track; since a reserved-connection timeout
// now kills only the query (KILL QUERY) and leaves the connection open, the
// connection must be closed on any CALL error so no untracked transaction (and
// its locks) survives to be kept alive by the heartbeat.
func TestExecProcClosesConnOnError(t *testing.T) {
	ctx := t.Context()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()

	// Reserve a connection. The real caller unlocks it after the CALL returns;
	// once closed, Unlock releases it from the pool so shutdown can drain.
	conn, err := tsv.te.txPool.scp.NewConn(ctx, &querypb.ExecuteOptions{}, nil)
	require.NoError(t, err)
	defer conn.Unlock()
	require.False(t, conn.IsClosed())

	// A CALL that the server rejects.
	query := "call test_proc()"
	db.AddRejectedQuery(query, errors.New("procedure failed"))

	qre := newTestQueryExecutor(ctx, tsv, query, conn.ReservedID())
	require.Equal(t, planbuilder.PlanCallProc, qre.plan.PlanID)

	_, err = qre.execProc(conn)
	require.Error(t, err)
	require.True(t, conn.IsClosed(), "a reserved connection must be closed when its CALL fails, so no untracked transaction survives")

	// Inside a Vitess-tracked transaction the hazard does not exist: a query
	// timeout still kills the whole connection (ExecOnce insideTxn), so no
	// untracked in-proc transaction can outlive it. A benign CALL error — a
	// SIGNAL from the procedure, a typo'd name — must therefore leave the
	// transaction usable, exactly as it does on a direct MySQL connection.
	txConn, _, _, err := tsv.te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	defer txConn.Release(tx.TxRollback)
	require.True(t, txConn.IsInTransaction())

	qreTx := newTestQueryExecutor(ctx, tsv, query, txConn.ReservedID())
	_, err = qreTx.execProc(txConn)
	require.Error(t, err)
	require.False(t, txConn.IsClosed(), "a benign CALL error inside a tracked transaction must not destroy the transaction")
}

// TestExecStreamSQLTimeoutConnFateByPlan verifies the streaming stateful path
// shares the buffered path's timeout decision: a timed-out safe statement
// (reads) keeps the reserved connection (KILL QUERY only), while a statement
// whose interruption could leave unrecorded session state (SET) loses the
// whole connection — otherwise a half-applied streaming SET would survive on a
// connection the temp-table keepalive then pins alive.
func TestExecStreamSQLTimeoutConnFateByPlan(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(t.Context(), noFlags, db)
	defer tsv.StopService()

	db.AddQueryPattern(`kill query \d+`, &sqltypes.Result{})
	db.AddQueryPattern(`kill \d+`, &sqltypes.Result{})
	db.AddQuery(resetLastIDQuery, &sqltypes.Result{})

	cases := []struct {
		sql               string
		keepsConn         bool
		fetchLastInsertID bool
	}{
		{"select 1", true, false},
		{"set @@sql_mode = ''", false, false},
		// A safe plan carrying fetch_last_insert_id still loses the
		// connection: MySQL retains a last_insert_id(expr) assignment after
		// KILL QUERY, and the error return skips the post-exec fetch that
		// keeps the vtgate session in sync with the connection.
		{"select last_insert_id(42)", false, true},
		// DML normally keeps the connection (rows roll back atomically), but
		// a mutating lock function reached through DML can be granted or
		// released just as the kill lands — lock state the session never
		// recorded — so the connection is lost.
		{"update test_table set name = 1 where get_lock('foo', 10) = 1", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			db.AddQuery(tc.sql, &sqltypes.Result{})
			db.SetBeforeFunc(tc.sql, func() {
				// Outlasts the context deadline so the statement is interrupted.
				time.Sleep(1 * time.Second)
			})

			conn, err := tsv.te.txPool.scp.NewConn(t.Context(), &querypb.ExecuteOptions{}, nil)
			require.NoError(t, err)
			defer conn.Unlock()

			execCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			qre := newTestQueryExecutorStreaming(execCtx, tsv, tc.sql, conn.ReservedID())
			if tc.fetchLastInsertID {
				if qre.options == nil {
					qre.options = &querypb.ExecuteOptions{}
				}
				qre.options.FetchLastInsertId = true
			}
			err = qre.execStreamSQL(conn.UnderlyingDBConn(), true /* isStateful */, false /* insideTxn */, tc.sql, func(*sqltypes.Result) error { return nil })
			require.Error(t, err)
			if tc.keepsConn {
				require.False(t, conn.IsClosed(), "a timed-out safe streaming statement must keep its connection")
			} else {
				require.True(t, conn.IsClosed(), "a timed-out unsafe streaming statement must lose its connection")
			}
		})
	}
}

// TestPlanKeepsConnOnTimeout pins which plan types may keep their stateful
// connection when a timeout kills only the query: reads and DML (whose kill
// leaves no session state behind), and nothing else — a killed SET or lock
// function can leave session state the session never recorded, which the
// temp-table keepalive would then preserve indefinitely.
func TestPlanKeepsConnOnTimeout(t *testing.T) {
	safe := []planbuilder.PlanType{
		planbuilder.PlanSelect, planbuilder.PlanSelectImpossible, planbuilder.PlanSelectNoLimit, planbuilder.PlanShow,
		planbuilder.PlanInsert, planbuilder.PlanUpdate, planbuilder.PlanDelete, planbuilder.PlanInsertMessage,
		planbuilder.PlanUpdateLimit, planbuilder.PlanDeleteLimit,
	}
	for _, id := range safe {
		assert.True(t, planKeepsConnOnTimeout(id), "%s must keep its connection on a query timeout", id)
	}
	unsafe := []planbuilder.PlanType{
		planbuilder.PlanSet, planbuilder.PlanSelectLockFunc, planbuilder.PlanCallProc,
		planbuilder.PlanDDL, planbuilder.PlanLoad, planbuilder.PlanFlush,
		planbuilder.PlanOtherRead, planbuilder.PlanOtherAdmin, planbuilder.PlanUnlockTables,
	}
	for _, id := range unsafe {
		assert.False(t, planKeepsConnOnTimeout(id), "%s must lose its connection on a query timeout", id)
	}

	// fetch_last_insert_id overrides a safe plan: the killed statement can
	// retain a last_insert_id on the connection that the skipped post-exec
	// fetch never reported to the vtgate session.
	qre := &QueryExecutor{
		plan:    &TabletPlan{Plan: &planbuilder.Plan{PlanID: planbuilder.PlanSelect}},
		options: &querypb.ExecuteOptions{FetchLastInsertId: true},
	}
	assert.False(t, qre.keepsConnOnTimeout(), "fetch_last_insert_id must lose the connection on a query timeout")
	qre.options.FetchLastInsertId = false
	assert.True(t, qre.keepsConnOnTimeout(), "a safe plan without fetch_last_insert_id keeps the connection")

	// A mutating lock function in DML overrides the plan type the same way:
	// the rows roll back under KILL QUERY but the lock grant or release can
	// race the kill, leaving lock state the session never recorded.
	qre = &QueryExecutor{
		plan:    &TabletPlan{Plan: &planbuilder.Plan{PlanID: planbuilder.PlanUpdate, KillsConnOnTimeout: true}},
		options: &querypb.ExecuteOptions{},
	}
	assert.False(t, qre.keepsConnOnTimeout(), "DML with a mutating lock function must lose the connection on a query timeout")
}
>>>>>>> ea4a61357a (VTTablet: Discard the pooled connection after CALL so procedure session state cannot leak (#21062))
