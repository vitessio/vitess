// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callinfo"
	tableaclpb "github.com/youtube/vitess/go/vt/proto/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
	"github.com/youtube/vitess/go/vt/tabletserver/fakecacheservice"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"golang.org/x/net/context"
)

func TestQueryExecutorPlanDDL(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "alter table test_table add zipcode int"
	want := &mproto.QueryResult{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_DDL, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanPassDmlStrictMode(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set pk = foo()"
	want := &mproto.QueryResult{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	// non strict mode
	sqlQuery := newTestSQLQuery(ctx, noFlags)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, newTransaction(sqlQuery))
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	testCommitHelper(t, sqlQuery, qre)
	sqlQuery.disallowQueries()

	// strict mode
	sqlQuery = newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre = newTestQueryExecutor(ctx, sqlQuery, query, newTransaction(sqlQuery))
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	got, err = qre.Execute()
	if err == nil {
		t.Fatal("qre.Execute() = nil, want error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: a TabletError", tabletError)
	}
	if tabletError.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(ErrFail))
	}
}

func TestQueryExecutorPlanPassDmlStrictModeAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set pk = foo()"
	want := &mproto.QueryResult{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	// non strict mode
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, noFlags)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	sqlQuery.disallowQueries()

	// strict mode
	// update should fail because strict mode is not enabled
	sqlQuery = newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre = newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	_, err = qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", tabletError)
	}
	if tabletError.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(ErrFail))
	}
}

func TestQueryExecutorPlanInsertPk(t *testing.T) {
	db := setUpQueryExecutorTest()
	db.AddQuery("insert into test_table values (1) /* _stream test_table (pk ) (1 ); */", &mproto.QueryResult{})
	want := &mproto.QueryResult{
		Rows: make([][]sqltypes.Value, 0),
	}
	query := "insert into test_table values(1)"
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_INSERT_PK, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanInsertSubQueryAutoCommmit(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "insert into test_table(pk) select pk from test_table where pk = 1 limit 1000"
	want := &mproto.QueryResult{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	selectQuery := "select pk from test_table where pk = 1 limit 1000"
	db.AddQuery(selectQuery, &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeNumeric([]byte("2"))},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &mproto.QueryResult{})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_INSERT_SUBQUERY, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanInsertSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "insert into test_table(pk) select pk from test_table where pk = 1 limit 1000"
	want := &mproto.QueryResult{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	selectQuery := "select pk from test_table where pk = 1 limit 1000"
	db.AddQuery(selectQuery, &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeNumeric([]byte("2"))},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &mproto.QueryResult{})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, newTransaction(sqlQuery))

	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_INSERT_SUBQUERY, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanDmlPk(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &mproto.QueryResult{}
	db.AddQuery(query, want)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, newTransaction(sqlQuery))
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_DML_PK, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanDmlAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &mproto.QueryResult{}
	db.AddQuery(query, want)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_DML_PK, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanDmlSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set addr = 3 where name = 1 limit 1000"
	expandedQuery := "select pk from test_table where name = 1 limit 1000 for update"
	want := &mproto.QueryResult{}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, want)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, newTransaction(sqlQuery))
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_DML_SUBQUERY, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanDmlSubQueryAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set addr = 3 where name = 1 limit 1000"
	expandedQuery := "select pk from test_table where name = 1 limit 1000 for update"
	want := &mproto.QueryResult{}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, want)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_DML_SUBQUERY, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanOtherWithinATransaction(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "show test_table"
	want := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, newTransaction(sqlQuery))
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_OTHER, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanPassSelectWithInATransaction(t *testing.T) {
	db := setUpQueryExecutorTest()
	fields := []mproto.Field{
		mproto.Field{Name: "addr", Type: mproto.VT_LONG},
	}
	query := "select addr from test_table where pk = 1 limit 1000"
	want := &mproto.QueryResult{
		Fields:       fields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("123"))},
		},
	}
	db.AddQuery(query, want)
	db.AddQuery("select addr from test_table where 1 != 1", &mproto.QueryResult{
		Fields: fields,
	})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, newTransaction(sqlQuery))
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanPassSelectWithLockOutsideATransaction(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table for update"
	want := &mproto.QueryResult{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(got.ErrorType))
	}
}

func TestQueryExecutorPlanPassSelect(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table limit 1000"
	want := &mproto.QueryResult{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanPKIn(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table where pk in (1, 2, 3) limit 1000"
	expandedQuery := "select pk, name, addr from test_table where pk in (1, 2, 3)"
	want := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeNumeric([]byte("1")),
				sqltypes.MakeNumeric([]byte("20")),
				sqltypes.MakeNumeric([]byte("30")),
			},
		},
	}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, want)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PK_IN, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}

	cachedQuery := "select pk, name, addr from test_table where pk in (1)"
	db.AddQuery(cachedQuery, &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeNumeric([]byte("1")),
				sqltypes.MakeNumeric([]byte("20")),
				sqltypes.MakeNumeric([]byte("30")),
			},
		},
	})

	nonCachedQuery := "select pk, name, addr from test_table where pk in (2, 3)"
	db.AddQuery(nonCachedQuery, &mproto.QueryResult{})
	db.AddQuery(cachedQuery, want)
	// run again, this time pk=1 should hit the rowcache
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanSelectSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (`index`) where name = 1 limit 1000"
	want := &mproto.QueryResult{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, want)

	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SELECT_SUBQUERY, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanSet(t *testing.T) {
	db := setUpQueryExecutorTest()
	setQuery := "set unknown_key = 1"
	db.AddQuery(setQuery, &mproto.QueryResult{})
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	qre := newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	// unrecognized set field will be delegated to MySQL and both Fields and Rows should be
	// empty arrays in this case.
	want := &mproto.QueryResult{
		Rows: make([][]sqltypes.Value, 0),
	}
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}

	// set vt_pool_size
	vtPoolSize := int64(37)
	setQuery = fmt.Sprintf("set vt_pool_size = %d", vtPoolSize)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if qre.qe.connPool.Capacity() != vtPoolSize {
		t.Fatalf("set query failed, expected to have vt_pool_size: %d, but got: %d",
			vtPoolSize, qre.qe.connPool.Capacity())
	}
	// set vt_stream_pool_size
	vtStreamPoolSize := int64(41)
	setQuery = fmt.Sprintf("set vt_stream_pool_size = %d", vtStreamPoolSize)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if qre.qe.streamConnPool.Capacity() != vtStreamPoolSize {
		t.Fatalf("set query failed, expected to have vt_stream_pool_size: %d, but got: %d", vtStreamPoolSize, qre.qe.streamConnPool.Capacity())
	}
	// set vt_transaction_cap
	vtTransactionCap := int64(43)
	setQuery = fmt.Sprintf("set vt_transaction_cap = %d", vtTransactionCap)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if qre.qe.txPool.pool.Capacity() != vtTransactionCap {
		t.Fatalf("set query failed, expected to have vt_transaction_cap: %d, but got: %d", vtTransactionCap, qre.qe.txPool.pool.Capacity())
	}
	// set vt_transaction_timeout
	vtTransactionTimeout := 47
	setQuery = fmt.Sprintf("set vt_transaction_timeout = %d", vtTransactionTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	vtTransactionTimeoutInMillis := time.Duration(vtTransactionTimeout) * time.Second
	if qre.qe.txPool.Timeout() != vtTransactionTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_transaction_timeout: %d, but got: %d", vtTransactionTimeoutInMillis, qre.qe.txPool.Timeout())
	}
	// set vt_schema_reload_time
	vtSchemaReloadTime := 53
	setQuery = fmt.Sprintf("set vt_schema_reload_time = %d", vtSchemaReloadTime)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	vtSchemaReloadTimeInMills := time.Duration(vtSchemaReloadTime) * time.Second
	if qre.qe.schemaInfo.ReloadTime() != vtSchemaReloadTimeInMills {
		t.Fatalf("set query failed, expected to have vt_schema_reload_time: %d, but got: %d", vtSchemaReloadTimeInMills, qre.qe.schemaInfo.ReloadTime())
	}
	// set vt_query_cache_size
	vtQueryCacheSize := int64(59)
	setQuery = fmt.Sprintf("set vt_query_cache_size = %d", vtQueryCacheSize)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if int64(qre.qe.schemaInfo.queries.Capacity()) != vtQueryCacheSize {
		t.Fatalf("set query failed, expected to have vt_query_cache_size: %d, but got: %d", vtQueryCacheSize, qre.qe.schemaInfo.queries.Capacity())
	}
	// set vt_query_timeout
	vtQueryTimeout := int64(61)
	setQuery = fmt.Sprintf("set vt_query_timeout = %d", vtQueryTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	vtQueryTimeoutInMillis := time.Duration(vtQueryTimeout) * time.Second
	if qre.qe.queryTimeout.Get() != vtQueryTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_query_timeout: %d, but got: %d", vtQueryTimeoutInMillis, qre.qe.queryTimeout.Get())
	}
	// set vt_idle_timeout
	vtIdleTimeout := int64(67)
	setQuery = fmt.Sprintf("set vt_idle_timeout = %d", vtIdleTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	vtIdleTimeoutInMillis := time.Duration(vtIdleTimeout) * time.Second
	if qre.qe.connPool.IdleTimeout() != vtIdleTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_idle_timeout: %d, but got: %d in conn pool", vtIdleTimeoutInMillis, qre.qe.connPool.IdleTimeout())
	}
	if qre.qe.streamConnPool.IdleTimeout() != vtIdleTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_idle_timeout: %d, but got: %d in stream conn pool", vtIdleTimeoutInMillis, qre.qe.streamConnPool.IdleTimeout())
	}
	if qre.qe.txPool.pool.IdleTimeout() != vtIdleTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_idle_timeout: %d, but got: %d in tx pool", vtIdleTimeoutInMillis, qre.qe.txPool.pool.IdleTimeout())
	}
	// set vt_query_timeout
	vtSpotCheckRatio := 0.771
	setQuery = fmt.Sprintf("set vt_spot_check_ratio = %f", vtSpotCheckRatio)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	vtSpotCheckFreq := int64(vtSpotCheckRatio * spotCheckMultiplier)
	if qre.qe.spotCheckFreq.Get() != vtSpotCheckFreq {
		t.Fatalf("set query failed, expected to have vt_spot_check_freq: %d, but got: %d", vtSpotCheckFreq, qre.qe.spotCheckFreq.Get())
	}
	// set vt_strict_mode, any non zero value enables strict mode
	vtStrictMode := int64(2)
	setQuery = fmt.Sprintf("set vt_strict_mode = %d", vtStrictMode)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if qre.qe.strictMode.Get() != vtStrictMode {
		t.Fatalf("set query failed, expected to have vt_strict_mode: %d, but got: %d", vtStrictMode, qre.qe.strictMode.Get())
	}
	// set vt_txpool_timeout
	vtTxPoolTimeout := int64(71)
	setQuery = fmt.Sprintf("set vt_txpool_timeout = %d", vtTxPoolTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre = newTestQueryExecutor(ctx, sqlQuery, setQuery, 0)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	want = &mproto.QueryResult{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	vtTxPoolTimeoutInMillis := time.Duration(vtTxPoolTimeout) * time.Second
	if qre.qe.txPool.PoolTimeout() != vtTxPoolTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_txpool_timeout: %d, but got: %d", vtTxPoolTimeoutInMillis, qre.qe.txPool.PoolTimeout())
	}
}

func TestQueryExecutorPlanSetMaxResultSize(t *testing.T) {
	setUpQueryExecutorTest()
	want := &mproto.QueryResult{}
	vtMaxResultSize := int64(128)
	query := fmt.Sprintf("set vt_max_result_size = %d", vtMaxResultSize)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if qre.qe.maxResultSize.Get() != vtMaxResultSize {
		t.Fatalf("set query failed, expected to have vt_max_result_size: %d, but got: %d", vtMaxResultSize, qre.qe.maxResultSize.Get())
	}
}

func TestQueryExecutorPlanSetMaxResultSizeFail(t *testing.T) {
	setUpQueryExecutorTest()
	query := "set vt_max_result_size = 0"
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	// vt_max_result_size out of range, should always larger than 0
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(got.ErrorType))
	}
}

func TestQueryExecutorPlanSetMaxDmlRows(t *testing.T) {
	setUpQueryExecutorTest()
	want := &mproto.QueryResult{}
	vtMaxDmlRows := int64(256)
	query := fmt.Sprintf("set vt_max_dml_rows = %d", vtMaxDmlRows)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if qre.qe.maxDMLRows.Get() != vtMaxDmlRows {
		t.Fatalf("set query failed, expected to have vt_max_dml_rows: %d, but got: %d", vtMaxDmlRows, qre.qe.maxDMLRows.Get())
	}
}

func TestQueryExecutorPlanSetMaxDmlRowsFail(t *testing.T) {
	setUpQueryExecutorTest()
	query := "set vt_max_dml_rows = 0"
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(got.ErrorType))
	}
}

func TestQueryExecutorPlanSetStreamBufferSize(t *testing.T) {
	setUpQueryExecutorTest()
	want := &mproto.QueryResult{}
	vtStreamBufferSize := int64(2048)
	query := fmt.Sprintf("set vt_stream_buffer_size = %d", vtStreamBufferSize)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	if qre.qe.streamBufferSize.Get() != vtStreamBufferSize {
		t.Fatalf("set query failed, expected to have vt_stream_buffer_size: %d, but got: %d", vtStreamBufferSize, qre.qe.streamBufferSize.Get())
	}
}

func TestQueryExecutorPlanSetStreamBufferSizeFail(t *testing.T) {
	setUpQueryExecutorTest()
	query := "set vt_stream_buffer_size = 128"
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(got.ErrorType))
	}
}

func TestQueryExecutorPlanOther(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "show test_table"
	want := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_OTHER, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
}

func TestQueryExecutorTableAcl(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest()
	query := "select * from test_table limit 1000"
	want := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callInfo := &fakeCallInfo{
		remoteAddr: "1.2.3.4",
		username:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
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

	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
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
	db := setUpQueryExecutorTest()
	query := "select * from test_table limit 1000"
	want := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callInfo := &fakeCallInfo{
		remoteAddr: "1.2.3.4",
		username:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)

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
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	sqlQuery.disallowQueries()

	// enable Config.StrictTableAcl
	sqlQuery = newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict|enableStrictTableAcl)
	qre = newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	// query should fail because current user do not have read permissions
	_, err = qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if tabletError.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(tabletError.ErrorType))
	}
}

func TestQueryExecutorTableAclExemptACL(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest()
	query := "select * from test_table limit 1000"
	want := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callInfo := &fakeCallInfo{
		remoteAddr: "1.2.3.4",
		username:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)

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
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict|enableStrictTableAcl)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if tabletError.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(tabletError.ErrorType))
	}
	if !strings.Contains(tabletError.Error(), "table acl error") {
		t.Fatalf("got %s, want tablet errorL table acl error", tabletError.Error())
	}

	// table acl should be ignored since this is an exempt user.
	username = "exempt-acl"
	sqlQuery.qe.exemptACL = username
	callInfo = &fakeCallInfo{
		remoteAddr: "1.2.3.4",
		username:   username,
	}
	ctx = callinfo.NewContext(context.Background(), callInfo)
	qre = newTestQueryExecutor(ctx, sqlQuery, query, 0)
	_, err = qre.Execute()
	if err != nil {
		t.Fatal("qre.Execute: nil, want: error")
	}
}

func TestQueryExecutorTableAclDryRun(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest()
	query := "select * from test_table limit 1000"
	want := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callInfo := &fakeCallInfo{
		remoteAddr: "1.2.3.4",
		username:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)

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
		username,
		planbuilder.PLAN_PASS_SELECT.String(),
		username,
	}, ".")
	// enable Config.StrictTableAcl
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableSchemaOverrides|enableStrict|enableStrictTableAcl)
	sqlQuery.qe.enableTableAclDryRun = true
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	beforeCount := sqlQuery.qe.tableaclPseudoDenied.Counters.Counts()[tableACLStatsKey]
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want: nil", err)
	}
	afterCount := sqlQuery.qe.tableaclPseudoDenied.Counters.Counts()[tableACLStatsKey]
	if afterCount-beforeCount != 1 {
		t.Fatalf("table acl pseudo denied count should increase by one. got: %d, want: %d", afterCount, beforeCount+1)
	}
}

func TestQueryExecutorBlacklistQRFail(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (`index`) where name = 1 limit 1000"
	expected := &mproto.QueryResult{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	bannedAddr := "127.0.0.1"
	bannedUser := "u2"

	alterRule := NewQueryRule("disable update", "disable update", QR_FAIL)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PLAN_SELECT_SUBQUERY)
	alterRule.AddTableCond("test_table")

	rulesName := "blacklistedRulesQRFail"
	rules := NewQueryRules()
	rules.Add(alterRule)

	QueryRuleSources.UnRegisterQueryRuleSource(rulesName)
	QueryRuleSources.RegisterQueryRuleSource(rulesName)
	defer QueryRuleSources.UnRegisterQueryRuleSource(rulesName)

	if err := QueryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	callInfo := &fakeCallInfo{
		remoteAddr: bannedAddr,
		username:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SELECT_SUBQUERY, qre.plan.PlanId)
	// execute should fail because query has been blacklisted
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorType != ErrFail {
		t.Fatalf("got: %s, want: ErrFail", getTabletErrorString(got.ErrorType))
	}
}

func TestQueryExecutorBlacklistQRRetry(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (`index`) where name = 1 limit 1000"
	expected := &mproto.QueryResult{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	bannedAddr := "127.0.0.1"
	bannedUser := "x"

	alterRule := NewQueryRule("disable update", "disable update", QR_FAIL_RETRY)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PLAN_SELECT_SUBQUERY)
	alterRule.AddTableCond("test_table")

	rulesName := "blacklistedRulesQRRetry"
	rules := NewQueryRules()
	rules.Add(alterRule)

	QueryRuleSources.UnRegisterQueryRuleSource(rulesName)
	QueryRuleSources.RegisterQueryRuleSource(rulesName)
	defer QueryRuleSources.UnRegisterQueryRuleSource(rulesName)

	if err := QueryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	callInfo := &fakeCallInfo{
		remoteAddr: bannedAddr,
		username:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	sqlQuery := newTestSQLQuery(ctx, enableRowCache|enableStrict)
	qre := newTestQueryExecutor(ctx, sqlQuery, query, 0)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SELECT_SUBQUERY, qre.plan.PlanId)
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorType != ErrRetry {
		t.Fatalf("got: %s, want: ErrRetry", getTabletErrorString(got.ErrorType))
	}
}

type executorFlags int64

const (
	noFlags        executorFlags = iota
	enableRowCache               = 1 << iota
	enableSchemaOverrides
	enableStrict
	enableStrictTableAcl
)

// newTestQueryExecutor uses a package level variable testSqlQuery defined in sqlquery_test.go
func newTestSQLQuery(ctx context.Context, flags executorFlags) *SqlQuery {
	randID := rand.Int63()
	config := DefaultQsConfig
	config.StatsPrefix = fmt.Sprintf("Stats-%d-", randID)
	config.DebugURLPrefix = fmt.Sprintf("/debug-%d-", randID)
	config.RowCache.StatsPrefix = fmt.Sprintf("Stats-%d-", randID)
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	config.PoolSize = 100
	config.TransactionCap = 100
	config.SpotCheckRatio = 1.0
	config.EnablePublishStats = false
	config.EnableAutoCommit = true

	if flags&enableStrict > 0 {
		config.StrictMode = true
	} else {
		config.StrictMode = false
	}
	if flags&enableRowCache > 0 {
		config.RowCache.Binary = "ls"
		config.RowCache.Connections = 100
	}
	if flags&enableStrictTableAcl > 0 {
		config.StrictTableAcl = true
	} else {
		config.StrictTableAcl = false
	}
	sqlQuery := NewSqlQuery(config)
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs()
	if flags&enableRowCache > 0 {
		dbconfigs.App.EnableRowcache = true
	} else {
		dbconfigs.App.EnableRowcache = false
	}
	schemaOverrides := []SchemaOverride{}
	if flags&enableSchemaOverrides > 0 {
		schemaOverrides = getTestTableSchemaOverrides()
	}
	sqlQuery.allowQueries(nil, &dbconfigs, schemaOverrides, testUtils.newMysqld(&dbconfigs))
	return sqlQuery
}

func newTransaction(sqlQuery *SqlQuery) int64 {
	session := proto.Session{
		SessionId:     sqlQuery.sessionID,
		TransactionId: 0,
	}
	txInfo := proto.TransactionInfo{TransactionId: 0}
	err := sqlQuery.Begin(context.Background(), sqlQuery.target, &session, &txInfo)
	if err != nil {
		panic(fmt.Errorf("failed to start a transaction: %v", err))
	}
	return txInfo.TransactionId
}

func newTestQueryExecutor(ctx context.Context, sqlQuery *SqlQuery, sql string, txID int64) *QueryExecutor {
	logStats := newSqlQueryStats("TestQueryExecutor", ctx)
	return &QueryExecutor{
		ctx:           ctx,
		query:         sql,
		bindVars:      make(map[string]interface{}),
		transactionID: txID,
		plan:          sqlQuery.qe.schemaInfo.GetPlan(ctx, logStats, sql),
		logStats:      logStats,
		qe:            sqlQuery.qe,
	}
}

func testCommitHelper(t *testing.T, sqlQuery *SqlQuery, queryExecutor *QueryExecutor) {
	session := proto.Session{
		SessionId:     sqlQuery.sessionID,
		TransactionId: queryExecutor.transactionID,
	}
	if err := sqlQuery.Commit(queryExecutor.ctx, sqlQuery.target, &session); err != nil {
		t.Fatalf("failed to commit transaction: %d, err: %v", queryExecutor.transactionID, err)
	}
}

func setUpQueryExecutorTest() *fakesqldb.DB {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	initQueryExecutorTestDB(db)
	return db
}

func initQueryExecutorTestDB(db *fakesqldb.DB) {
	for query, result := range getQueryExecutorSupportedQueries() {
		db.AddQuery(query, result)
	}
}

func getTestTableFields() []mproto.Field {
	return []mproto.Field{
		mproto.Field{Name: "pk", Type: mproto.VT_LONG},
		mproto.Field{Name: "name", Type: mproto.VT_LONG},
		mproto.Field{Name: "addr", Type: mproto.VT_LONG},
	}
}

func checkPlanID(
	t *testing.T,
	expectedPlanID planbuilder.PlanType,
	actualPlanID planbuilder.PlanType) {
	if expectedPlanID != actualPlanID {
		t.Fatalf("expect to get PlanId: %s, but got %s",
			expectedPlanID.String(), actualPlanID.String())
	}
}

func getTestTableSchemaOverrides() []SchemaOverride {
	return []SchemaOverride{
		SchemaOverride{
			Name:      "test_table",
			PKColumns: []string{"pk"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type:  "RW",
				Table: "test_table",
			},
		},
	}
}

func getQueryExecutorSupportedQueries() map[string]*mproto.QueryResult {
	return map[string]*mproto.QueryResult{
		// queries for schema info
		"select unix_timestamp()": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{sqltypes.MakeString([]byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		baseShowTables: &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_table")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeString([]byte("1427325875")),
					sqltypes.MakeString([]byte("")),
				},
			},
		},
		"describe `test_table`": &mproto.QueryResult{
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				[]sqltypes.Value{
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
		"show index from `test_table`": &mproto.QueryResult{
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("index")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		"begin":  &mproto.QueryResult{},
		"commit": &mproto.QueryResult{},
		baseShowTables + " and table_name = 'test_table'": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_table")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeString([]byte("1427325875")),
					sqltypes.MakeString([]byte("")),
				},
			},
		},
		"rollback": &mproto.QueryResult{},
	}
}
