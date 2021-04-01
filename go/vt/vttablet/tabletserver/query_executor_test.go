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
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

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
	fieldResult := sqltypes.MakeTestResult(fields)
	selectResult := sqltypes.MakeTestResult(fields, "1|aaa")
	emptyResult := &sqltypes.Result{}

	// The queries are run both in and outside a transaction.
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
		// inTxWant is the query log we expect if we're in a transation.
		// If empty, then we should expect the same as logWant.
		inTxWant string
	}{{
		input: "select * from t",
		dbResponses: []dbResponse{{
			query:  "select * from t where 1 != 1",
			result: fieldResult,
		}, {
			query:  "select * from t limit 10001",
			result: selectResult,
		}},
		resultWant: selectResult,
		planWant:   "Select",
		logWant:    "select * from t where 1 != 1; select * from t limit 10001",
		// Because the fields would have been cached before, the field query will
		// not get re-executed.
		inTxWant: "select * from t limit 10001",
	}, {
		input: "select * from t limit 1",
		dbResponses: []dbResponse{{
			query:  "select * from t where 1 != 1",
			result: fieldResult,
		}, {
			query:  "select * from t limit 1",
			result: selectResult,
		}},
		resultWant: selectResult,
		planWant:   "Select",
		logWant:    "select * from t where 1 != 1; select * from t limit 1",
		// Because the fields would have been cached before, the field query will
		// not get re-executed.
		inTxWant: "select * from t limit 1",
	}, {
		input: "show engines",
		dbResponses: []dbResponse{{
			query:  "show engines",
			result: dmlResult,
		}},
		resultWant: dmlResult,
		planWant:   "OtherRead",
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
		resultWant: dmlResult,
		planWant:   "DDL",
		logWant:    "alter table test_table add column zipcode int",
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
			query:  "alter table `user` add index a (id)",
			result: emptyResult,
		}},
		resultWant: emptyResult,
		planWant:   "DDL",
		logWant:    "alter table `user` add index a (id)",
		inTxWant:   "alter table `user` add index a (id)",
	}, {
		input: "create index a on user(id1 + id2)",
		dbResponses: []dbResponse{{
			query:  "create index a on user(id1 + id2)",
			result: emptyResult,
		}},
		resultWant: emptyResult,
		planWant:   "DDL",
		logWant:    "create index a on user(id1 + id2)",
		inTxWant:   "create index a on user(id1 + id2)",
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
	}}
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			db := setUpQueryExecutorTest(t)
			defer db.Close()
			for _, dbr := range tcase.dbResponses {
				db.AddQuery(dbr.query, dbr.result)
			}
			ctx := context.Background()
			tsv := newTestTabletServer(ctx, noFlags, db)
			tsv.config.DB.DBName = "ks"
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
			tsv.QueryPlanCacheWait()

			// Test inside a transaction.
			target := tsv.sm.Target()
			txid, alias, err := tsv.Begin(ctx, &target, nil)
			require.NoError(t, err)
			require.NotNil(t, alias, "alias should not be nil")
			assert.Equal(t, tsv.alias, *alias, "Wrong alias returned by Begin")
			defer tsv.Commit(ctx, &target, txid)

			qre = newTestQueryExecutor(ctx, tsv, tcase.input, txid)
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
			query:  "select * from t where 1 != 1",
			result: fieldResult,
		}},
		resultWant: fieldResult,
		planWant:   "SelectImpossible",
		logWant:    "select * from t where 1 != 1",
		inTxWant:   "",
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
			txid, alias, err := tsv.Begin(ctx, &target, nil)
			require.NoError(t, err)
			require.NotNil(t, alias, "alias should not be nil")
			assert.Equal(t, tsv.alias, *alias, "Wrong tablet alias from Begin")
			defer tsv.Commit(ctx, &target, txid)

			qre = newTestQueryExecutor(ctx, tsv, tcase.input, txid)
			got, err = qre.Execute()
			require.NoError(t, err, tcase.input)
			assert.Equal(t, tcase.resultWant, got, "in tx: %v", tcase.input)
			assert.Equal(t, tcase.planWant, qre.logStats.PlanType, "in tx: %v", tcase.input)
			assert.Equal(t, tcase.inTxWant, qre.logStats.RewrittenSQL(), "in tx: %v", tcase.input)
		}()
	}
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
		err:     "count exceeded",
		logWant: "select * from t where 1 != 1; select * from t limit 3",
		// Because the fields would have been cached before, the field query will
		// not get re-executed.
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
			txid, alias, err := tsv.Begin(ctx, &target, nil)
			require.NoError(t, err)
			require.NotNil(t, alias, "alias should not be nil")
			assert.Equal(t, tsv.alias, *alias, "Wrong tablet alias from Begin")
			defer tsv.Commit(ctx, &target, txid)

			qre = newTestQueryExecutor(ctx, tsv, tcase.input, txid)
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
			conn, err := tsv.te.txPool.GetAndLock(txid, "")
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
	if !reflect.DeepEqual(got, want) {
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
	if !reflect.DeepEqual(got, want) {
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
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}
}

func TestQueryExecutorMessageStreamACL(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
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
	ctx := callerid.NewContext(context.Background(), nil, callerID)
	qre := &QueryExecutor{
		ctx:      ctx,
		query:    "stream from msg",
		plan:     plan,
		logStats: tabletenv.NewLogStats(ctx, "TestQueryExecutor"),
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
	}
	qre.ctx = callerid.NewContext(context.Background(), nil, callerID)
	// Should fail because u2 does not have permission.
	err = qre.MessageStream(func(qr *sqltypes.Result) error {
		return io.EOF
	})

	want := `table acl error: "u2" [] cannot run MessageStream on table "msg"`
	if err == nil || err.Error() != want {
		t.Errorf("qre.MessageStream(msg) error: %v, want %s", err, want)
	}
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}
}

func TestQueryExecutorTableAcl(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
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
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
}

func TestQueryExecutorTableAclNoPermission(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
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
	if !reflect.DeepEqual(got, want) {
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

func TestQueryExecutorTableAclDualTableExempt(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
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
	wanterr := "table acl error"
	if !strings.Contains(err.Error(), wanterr) {
		t.Fatalf("qre.Execute: %v, want %s", err, wanterr)
	}

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
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
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
	wanterr := "table acl error"
	if !strings.Contains(err.Error(), wanterr) {
		t.Fatalf("qre.Execute: %v, want %s", err, wanterr)
	}

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
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
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

func TestQueryExecutorBlacklistQRFail(t *testing.T) {
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

	rulesName := "blacklistedRulesQRFail"
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
	// execute should fail because query has been blacklisted
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
}

func TestQueryExecutorBlacklistQRRetry(t *testing.T) {
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

	rulesName := "blacklistedRulesQRRetry"
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

type executorFlags int64

const (
	noFlags              executorFlags = 0
	enableStrictTableACL               = 1 << iota
	smallTxPool
	noTwopc
	shortTwopcAge
	smallResultSize
)

// newTestQueryExecutor uses a package level variable testTabletServer defined in tabletserver_test.go
func newTestTabletServer(ctx context.Context, flags executorFlags, db *fakesqldb.DB) *TabletServer {
	config := tabletenv.NewDefaultConfig()
	config.OltpReadPool.Size = 100
	if flags&smallTxPool > 0 {
		config.TxPool.Size = 3
	} else {
		config.TxPool.Size = 100
	}
	if flags&enableStrictTableACL > 0 {
		config.StrictTableACL = true
	} else {
		config.StrictTableACL = false
	}
	if flags&noTwopc > 0 {
		config.TwoPCEnable = false
	} else {
		config.TwoPCEnable = true
	}
	config.TwoPCCoordinatorAddress = "fake"
	if flags&shortTwopcAge > 0 {
		config.TwoPCAbandonAge = 0.5
	} else {
		config.TwoPCAbandonAge = 10
	}
	if flags&smallResultSize > 0 {
		config.Oltp.MaxRows = 2
	}
	tsv := NewTabletServer("TabletServerTest", config, memorytopo.NewServer(""), topodatapb.TabletAlias{})
	dbconfigs := newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, nil /* mysqld */)
	if err != nil {
		panic(err)
	}
	return tsv
}

func newTransaction(tsv *TabletServer, options *querypb.ExecuteOptions) int64 {
	target := tsv.sm.Target()
	transactionID, _, err := tsv.Begin(context.Background(), &target, options)
	if err != nil {
		panic(vterrors.Wrap(err, "failed to start a transaction"))
	}
	return transactionID
}

func newTestQueryExecutor(ctx context.Context, tsv *TabletServer, sql string, txID int64) *QueryExecutor {
	logStats := tabletenv.NewLogStats(ctx, "TestQueryExecutor")
	plan, err := tsv.qe.GetPlan(ctx, logStats, sql, false, false /* inReservedConn */)
	if err != nil {
		panic(err)
	}
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

const baseShowTablesPattern = `SELECT t\.table_name.*`

func initQueryExecutorTestDB(db *fakesqldb.DB) {
	for query, result := range getQueryExecutorSupportedQueries() {
		db.AddQuery(query, result)
	}
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
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
}

func getTestTableFields() []*querypb.Field {
	return []*querypb.Field{
		{Name: "pk", Type: sqltypes.Int32},
		{Name: "name", Type: sqltypes.Int32},
		{Name: "addr", Type: sqltypes.Int32},
	}
}

func getQueryExecutorSupportedQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
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
		"(select 0 as x from dual where 1 != 1) union (select 1 as y from dual where 1 != 1)": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"(select 0 as x from dual where 1 != 1) union (select 1 as y from dual where 1 != 1) limit 10001": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
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
			}},
		},
		"select * from seq where 1 != 1": {
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
}
