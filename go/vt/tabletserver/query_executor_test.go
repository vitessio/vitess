// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callinfo"
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
	testUtils := &testUtils{}
	query := "alter table test_table add zipcode int"
	expected := &mproto.QueryResult{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_DDL, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanPassDmlStrictMode(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "update test_table set pk = foo()"
	expected := &mproto.QueryResult{
		Fields: []mproto.Field{},
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	// non strict mode
	qre, sqlQuery := newTestQueryExecutor(query, context.Background(), enableTx)
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	testCommitHelper(t, sqlQuery, qre)
	sqlQuery.disallowQueries()

	// strict mode
	qre, sqlQuery = newTestQueryExecutor(
		"update test_table set pk = foo()",
		context.Background(),
		enableTx|enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	defer handleAndVerifyTabletError(
		t,
		"update should fail because strict mode is enabled",
		ErrFail)
	qre.Execute()
}

func TestQueryExecutorPlanPassDmlStrictModeAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "update test_table set pk = foo()"
	expected := &mproto.QueryResult{
		Fields: []mproto.Field{},
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	// non strict mode
	qre, sqlQuery := newTestQueryExecutor(query, context.Background(), noFlags)
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	sqlQuery.disallowQueries()

	// strict mode
	qre, sqlQuery = newTestQueryExecutor(
		"update test_table set pk = foo()",
		context.Background(),
		enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_DML, qre.plan.PlanId)
	defer handleAndVerifyTabletError(
		t,
		"update should fail because strict mode is not enabled",
		ErrFail)
	qre.Execute()
}

func TestQueryExecutorPlanInsertPk(t *testing.T) {
	db := setUpQueryExecutorTest()
	db.AddQuery("insert into test_table values (1) /* _stream test_table (pk ) (1 ); */", &mproto.QueryResult{})
	want := &mproto.QueryResult{
		Fields: make([]mproto.Field, 0),
		Rows:   make([][]sqltypes.Value, 0),
	}
	sql := "insert into test_table values(1)"
	qre, sqlQuery := newTestQueryExecutor(
		sql,
		context.Background(),
		enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_INSERT_PK, qre.plan.PlanId)
	got := qre.Execute()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("query: %s, QueryExecutor.Execute() = %v, want: %v", sql, got, want)
	}
}

func TestQueryExecutorPlanInsertSubQueryAutoCommmit(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	fields := []mproto.Field{
		mproto.Field{Name: "pk", Type: mproto.VT_LONG},
	}
	query := "insert into test_table(pk) select pk from test_table where pk = 1 limit 1000"
	expected := &mproto.QueryResult{
		Fields: fields,
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	selectQuery := "select pk from test_table where pk = 1 limit 1000"
	db.AddQuery(selectQuery, &mproto.QueryResult{
		Fields:       fields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeNumeric([]byte("2"))},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &mproto.QueryResult{
		Fields: fields,
	})

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_INSERT_SUBQUERY, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanInsertSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	fields := []mproto.Field{
		mproto.Field{Name: "pk", Type: mproto.VT_LONG},
	}
	query := "insert into test_table(pk) select pk from test_table where pk = 1 limit 1000"
	expected := &mproto.QueryResult{
		Fields: fields,
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	selectQuery := "select pk from test_table where pk = 1 limit 1000"
	db.AddQuery(selectQuery, &mproto.QueryResult{
		Fields:       fields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeNumeric([]byte("2"))},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &mproto.QueryResult{
		Fields: fields,
	})

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableTx|enableStrict)
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_INSERT_SUBQUERY, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanDmlPk(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	// expected.Rows is always nil, intended
	expected := &mproto.QueryResult{}
	db.AddQuery(query, expected)

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableTx|enableStrict)
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_DML_PK, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanDmlAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	// expected.Rows is always nil, intended
	expected := &mproto.QueryResult{}
	db.AddQuery(query, expected)

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_DML_PK, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanDmlSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "update test_table set addr = 3 where name = 1 limit 1000"
	expandedQuery := "select pk from test_table where name = 1 limit 1000 for update"
	// expected.Rows is always nil, intended
	expected := &mproto.QueryResult{}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)
	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableTx|enableStrict)
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_DML_SUBQUERY, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanDmlSubQueryAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "update test_table set addr = 3 where name = 1 limit 1000"
	expandedQuery := "select pk from test_table where name = 1 limit 1000 for update"
	// expected.Rows is always nil, intended
	expected := &mproto.QueryResult{}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)
	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_DML_SUBQUERY, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanOtherWithinATransaction(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "show test_table"
	expected := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableTx|enableRowCache|enableSchemaOverrides|enableStrict)
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_OTHER, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanPassSelectWithInATransaction(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	fields := []mproto.Field{
		mproto.Field{Name: "addr", Type: mproto.VT_LONG},
	}

	query := "select addr from test_table where pk = 1 limit 1000"
	expected := &mproto.QueryResult{
		Fields:       fields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("123"))},
		},
	}
	db.AddQuery(query, expected)
	db.AddQuery("select addr from test_table where 1 != 1", &mproto.QueryResult{
		Fields: fields,
	})

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableTx|enableStrict)
	defer sqlQuery.disallowQueries()
	defer testCommitHelper(t, sqlQuery, qre)
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanPassSelectWithLockOutsideATransaction(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table for update"
	expected := &mproto.QueryResult{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableSchemaOverrides|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	defer handleAndVerifyTabletError(t, "query should fail because the select holds a lock but outside a transaction", ErrFail)
	qre.Execute()
}

func TestQueryExecutorPlanPassSelect(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "select * from test_table limit 1000"
	expected := &mproto.QueryResult{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableSchemaOverrides|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanPKIn(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "select * from test_table where pk in (1, 2, 3) limit 1000"
	expandedQuery := "select pk, name, addr from test_table where pk in (1, 2, 3)"

	expected := &mproto.QueryResult{
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
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableStrict|enableSchemaOverrides)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PK_IN, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())

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

	db.AddQuery(cachedQuery, expected)

	// run again, this time pk=1 should hit the rowcache
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanSelectSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (INDEX) where name = 1 limit 1000"

	expected := &mproto.QueryResult{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableSchemaOverrides|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SELECT_SUBQUERY, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorPlanSet(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	expected := &mproto.QueryResult{}

	setQuery := "set unknown_key = 1"
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery := newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	// unrecognized set field will be delegated to MySQL and both Fields and Rows should be
	// empty arrays in this case.
	want := &mproto.QueryResult{
		Fields: make([]mproto.Field, 0),
		Rows:   make([][]sqltypes.Value, 0),
	}
	got := qre.Execute()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("query: %s failed, got: %+v, want: %+v", setQuery, got, want)
	}
	sqlQuery.disallowQueries()

	// set vt_pool_size
	vtPoolSize := int64(37)
	setQuery = fmt.Sprintf("set vt_pool_size = %d", vtPoolSize)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	if qre.qe.connPool.Capacity() != vtPoolSize {
		t.Fatalf("set query failed, expected to have vt_pool_size: %d, but got: %d",
			vtPoolSize, qre.qe.connPool.Capacity())
	}
	sqlQuery.disallowQueries()

	// set vt_stream_pool_size
	vtStreamPoolSize := int64(41)
	setQuery = fmt.Sprintf("set vt_stream_pool_size = %d", vtStreamPoolSize)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	if qre.qe.streamConnPool.Capacity() != vtStreamPoolSize {
		t.Fatalf("set query failed, expected to have vt_stream_pool_size: %d, but got: %d", vtStreamPoolSize, qre.qe.streamConnPool.Capacity())
	}
	sqlQuery.disallowQueries()

	// set vt_transaction_cap
	vtTransactionCap := int64(43)
	setQuery = fmt.Sprintf("set vt_transaction_cap = %d", vtTransactionCap)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	if qre.qe.txPool.pool.Capacity() != vtTransactionCap {
		t.Fatalf("set query failed, expected to have vt_transaction_cap: %d, but got: %d", vtTransactionCap, qre.qe.txPool.pool.Capacity())
	}
	sqlQuery.disallowQueries()

	// set vt_transaction_timeout
	vtTransactionTimeout := 47
	setQuery = fmt.Sprintf("set vt_transaction_timeout = %d", vtTransactionTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	vtTransactionTimeoutInMillis := time.Duration(vtTransactionTimeout) * time.Second
	if qre.qe.txPool.Timeout() != vtTransactionTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_transaction_timeout: %d, but got: %d", vtTransactionTimeoutInMillis, qre.qe.txPool.Timeout())
	}
	sqlQuery.disallowQueries()

	// set vt_schema_reload_time
	vtSchemaReloadTime := 53
	setQuery = fmt.Sprintf("set vt_schema_reload_time = %d", vtSchemaReloadTime)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	vtSchemaReloadTimeInMills := time.Duration(vtSchemaReloadTime) * time.Second
	if qre.qe.schemaInfo.ReloadTime() != vtSchemaReloadTimeInMills {
		t.Fatalf("set query failed, expected to have vt_schema_reload_time: %d, but got: %d", vtSchemaReloadTimeInMills, qre.qe.schemaInfo.ReloadTime())
	}
	sqlQuery.disallowQueries()

	// set vt_query_cache_size
	vtQueryCacheSize := int64(59)
	setQuery = fmt.Sprintf("set vt_query_cache_size = %d", vtQueryCacheSize)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	if int64(qre.qe.schemaInfo.queries.Capacity()) != vtQueryCacheSize {
		t.Fatalf("set query failed, expected to have vt_query_cache_size: %d, but got: %d", vtQueryCacheSize, qre.qe.schemaInfo.queries.Capacity())
	}
	sqlQuery.disallowQueries()

	// set vt_query_timeout
	vtQueryTimeout := int64(61)
	setQuery = fmt.Sprintf("set vt_query_timeout = %d", vtQueryTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	vtQueryTimeoutInMillis := time.Duration(vtQueryTimeout) * time.Second
	if qre.qe.queryTimeout.Get() != vtQueryTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_query_timeout: %d, but got: %d", vtQueryTimeoutInMillis, qre.qe.queryTimeout.Get())
	}
	sqlQuery.disallowQueries()

	// set vt_idle_timeout
	vtIdleTimeout := int64(67)
	setQuery = fmt.Sprintf("set vt_idle_timeout = %d", vtIdleTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
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
	sqlQuery.disallowQueries()

	// set vt_query_timeout
	vtSpotCheckRatio := 0.771
	setQuery = fmt.Sprintf("set vt_spot_check_ratio = %f", vtSpotCheckRatio)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	vtSpotCheckFreq := int64(vtSpotCheckRatio * spotCheckMultiplier)
	if qre.qe.spotCheckFreq.Get() != vtSpotCheckFreq {
		t.Fatalf("set query failed, expected to have vt_spot_check_freq: %d, but got: %d", vtSpotCheckFreq, qre.qe.spotCheckFreq.Get())
	}
	sqlQuery.disallowQueries()

	// set vt_strict_mode, any non zero value enables strict mode
	vtStrictMode := int64(2)
	setQuery = fmt.Sprintf("set vt_strict_mode = %d", vtStrictMode)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	if qre.qe.strictMode.Get() != vtStrictMode {
		t.Fatalf("set query failed, expected to have vt_strict_mode: %d, but got: %d", vtStrictMode, qre.qe.strictMode.Get())
	}
	sqlQuery.disallowQueries()

	// set vt_txpool_timeout
	vtTxPoolTimeout := int64(71)
	setQuery = fmt.Sprintf("set vt_txpool_timeout = %d", vtTxPoolTimeout)
	db.AddQuery(setQuery, &mproto.QueryResult{})
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	vtTxPoolTimeoutInMillis := time.Duration(vtTxPoolTimeout) * time.Second
	if qre.qe.txPool.PoolTimeout() != vtTxPoolTimeoutInMillis {
		t.Fatalf("set query failed, expected to have vt_txpool_timeout: %d, but got: %d", vtTxPoolTimeoutInMillis, qre.qe.txPool.PoolTimeout())
	}
	sqlQuery.disallowQueries()
}

func TestQueryExecutorPlanSetMaxResultSize(t *testing.T) {
	setUpQueryExecutorTest()
	testUtils := &testUtils{}
	expected := &mproto.QueryResult{}
	vtMaxResultSize := int64(128)
	setQuery := fmt.Sprintf("set vt_max_result_size = %d", vtMaxResultSize)
	qre, sqlQuery := newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	if qre.qe.maxResultSize.Get() != vtMaxResultSize {
		t.Fatalf("set query failed, expected to have vt_max_result_size: %d, but got: %d", vtMaxResultSize, qre.qe.maxResultSize.Get())
	}
	// set vt_max_result_size fail
	setQuery = "set vt_max_result_size = 0"
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	defer handleAndVerifyTabletError(t, "vt_max_result_size out of range, should always larger than 0", ErrFail)
	qre.Execute()
}

func TestQueryExecutorPlanSetMaxDmlRows(t *testing.T) {
	setUpQueryExecutorTest()
	want := &mproto.QueryResult{}
	vtMaxDmlRows := int64(256)
	setQuery := fmt.Sprintf("set vt_max_dml_rows = %d", vtMaxDmlRows)
	qre, sqlQuery := newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	got := qre.Execute()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("query executor Execute() = %v, want: %v", got, want)
	}
	if qre.qe.maxDMLRows.Get() != vtMaxDmlRows {
		t.Fatalf("set query failed, expected to have vt_max_dml_rows: %d, but got: %d", vtMaxDmlRows, qre.qe.maxDMLRows.Get())
	}
	// set vt_max_result_size fail
	setQuery = "set vt_max_dml_rows = 0"
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	defer handleAndVerifyTabletError(t, "vt_max_dml_rows out of range, should always larger than 0", ErrFail)
	qre.Execute()
}

func TestQueryExecutorPlanSetStreamBufferSize(t *testing.T) {
	setUpQueryExecutorTest()
	testUtils := &testUtils{}
	expected := &mproto.QueryResult{}
	vtStreamBufferSize := int64(2048)
	setQuery := fmt.Sprintf("set vt_stream_buffer_size = %d", vtStreamBufferSize)
	qre, sqlQuery := newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	if qre.qe.streamBufferSize.Get() != vtStreamBufferSize {
		t.Fatalf("set query failed, expected to have vt_stream_buffer_size: %d, but got: %d", vtStreamBufferSize, qre.qe.streamBufferSize.Get())
	}
	// set vt_max_result_size fail
	setQuery = "set vt_stream_buffer_size = 128"
	qre, sqlQuery = newTestQueryExecutor(
		setQuery, context.Background(), enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SET, qre.plan.PlanId)
	defer handleAndVerifyTabletError(t, "vt_stream_buffer_size out of range, should always larger than or equal to 1024", ErrFail)
	qre.Execute()
}

func TestQueryExecutorPlanOther(t *testing.T) {
	db := setUpQueryExecutorTest()
	testUtils := &testUtils{}
	query := "show test_table"
	expected := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	qre, sqlQuery := newTestQueryExecutor(
		query, context.Background(), enableRowCache|enableSchemaOverrides|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_OTHER, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
}

func TestQueryExecutorTableAcl(t *testing.T) {
	testUtils := &testUtils{}
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)

	db := setUpQueryExecutorTest()
	query := "select * from test_table limit 1000"
	expected := &mproto.QueryResult{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, expected)
	db.AddQuery("select * from test_table where 1 != 1", &mproto.QueryResult{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callInfo := &fakeCallInfo{
		remoteAddr: "1.2.3.4",
		username:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	if err := tableacl.InitFromBytes(
		[]byte(fmt.Sprintf(`{"test_table":{"READER":"%s"}}`, username))); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	qre, sqlQuery := newTestQueryExecutor(
		query, ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	testUtils.checkEqual(t, expected, qre.Execute())
	sqlQuery.disallowQueries()

	if err := tableacl.InitFromBytes([]byte(`{"test_table":{"READER":"superuser"}}`)); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}
	// without enabling Config.StrictTableAcl
	qre, sqlQuery = newTestQueryExecutor(
		query, ctx, enableRowCache|enableSchemaOverrides|enableStrict)
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	qre.Execute()
	sqlQuery.disallowQueries()
	// enable Config.StrictTableAcl
	qre, sqlQuery = newTestQueryExecutor(
		query, ctx, enableRowCache|enableSchemaOverrides|enableStrict|enableStrictTableAcl)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_PASS_SELECT, qre.plan.PlanId)
	defer handleAndVerifyTabletError(t, "query should fail because current user do not have read permissions", ErrFail)
	qre.Execute()
}

func TestQueryExecutorBlacklistQRFail(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (INDEX) where name = 1 limit 1000"
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
	qre, sqlQuery := newTestQueryExecutor(query, ctx, enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SELECT_SUBQUERY, qre.plan.PlanId)
	defer handleAndVerifyTabletError(t, "execute should fail because query has been blacklisted", ErrFail)
	qre.Execute()
}

func TestQueryExecutorBlacklistQRRetry(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (INDEX) where name = 1 limit 1000"
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
	qre, sqlQuery := newTestQueryExecutor(query, ctx, enableRowCache|enableStrict)
	defer sqlQuery.disallowQueries()
	checkPlanID(t, planbuilder.PLAN_SELECT_SUBQUERY, qre.plan.PlanId)
	defer handleAndVerifyTabletError(t, "execute should fail because query has been blacklisted", ErrRetry)
	qre.Execute()
}

type executorFlags int64

const (
	noFlags  executorFlags = iota
	enableTx               = 1 << iota
	enableRowCache
	enableSchemaOverrides
	enableStrict
	enableStrictTableAcl
)

// newTestQueryExecutor uses a package level variable testSqlQuery defined in sqlquery_test.go
func newTestQueryExecutor(sql string, ctx context.Context, flags executorFlags) (*QueryExecutor, *SqlQuery) {
	logStats := newSqlQueryStats("TestQueryExecutor", ctx)
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

	txID := int64(0)
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
	sqlQuery.allowQueries(&dbconfigs, schemaOverrides, testUtils.newMysqld(&dbconfigs))
	if flags&enableTx > 0 {
		session := proto.Session{
			SessionId:     sqlQuery.sessionID,
			TransactionId: 0,
		}
		txInfo := proto.TransactionInfo{TransactionId: 0}
		err := sqlQuery.Begin(ctx, &session, &txInfo)
		if err != nil {
			panic(fmt.Errorf("failed to start a transaction: %v", err))
		}
		txID = txInfo.TransactionId
	}
	qre := &QueryExecutor{
		query:         sql,
		bindVars:      make(map[string]interface{}),
		transactionID: txID,
		plan:          sqlQuery.qe.schemaInfo.GetPlan(ctx, logStats, sql),
		ctx:           ctx,
		logStats:      logStats,
		qe:            sqlQuery.qe,
	}
	return qre, sqlQuery
}

func testCommitHelper(t *testing.T, sqlQuery *SqlQuery, queryExecutor *QueryExecutor) {
	session := proto.Session{
		SessionId:     sqlQuery.sessionID,
		TransactionId: queryExecutor.transactionID,
	}
	if err := sqlQuery.Commit(queryExecutor.ctx, &session); err != nil {
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
					sqltypes.MakeString([]byte("INDEX")),
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
