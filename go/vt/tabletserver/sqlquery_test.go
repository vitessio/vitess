// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"expvar"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/fakesqldb"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"
)

func TestSqlQueryAllowQueriesFailBadConn(t *testing.T) {
	db := setUpSqlQueryTest()
	db.EnableConnFail()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	checkSqlQueryState(t, sqlQuery, "NOT_SERVING")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err == nil {
		t.Fatalf("SqlQuery.allowQueries should fail")
	}
	checkSqlQueryState(t, sqlQuery, "NOT_SERVING")
}

func TestSqlQueryAllowQueriesFailStrictModeConflictWithRowCache(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	// disable strict mode
	config.StrictMode = false
	sqlQuery := NewSqlQuery(config, "")
	checkSqlQueryState(t, sqlQuery, "NOT_SERVING")
	dbconfigs := newTestDBConfigs()
	// enable rowcache
	dbconfigs.App.EnableRowcache = true
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err == nil {
		t.Fatalf("SqlQuery.allowQueries should fail because strict mode is disabled while rowcache is enabled.")
	}
	checkSqlQueryState(t, sqlQuery, "NOT_SERVING")
}

func TestSqlQueryAllowQueries(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	checkSqlQueryState(t, sqlQuery, "NOT_SERVING")
	dbconfigs := newTestDBConfigs()
	sqlQuery.setState(StateServing)
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	sqlQuery.disallowQueries()
	if err != nil {
		t.Fatalf("SqlQuery.allowQueries should success, but get error: %v", err)
	}
	sqlQuery.setState(StateShuttingTx)
	err = sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err == nil {
		t.Fatalf("SqlQuery.allowQueries should fail")
	}
	sqlQuery.disallowQueries()
}

func TestSqlQueryCheckMysql(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	defer sqlQuery.disallowQueries()
	if err != nil {
		t.Fatalf("SqlQuery.allowQueries should success but get error: %v", err)
	}
	if !sqlQuery.checkMySQL() {
		t.Fatalf("checkMySQL should return true")
	}
}

func TestSqlQueryCheckMysqlFailInvalidConn(t *testing.T) {
	db := setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	defer sqlQuery.disallowQueries()
	if err != nil {
		t.Fatalf("SqlQuery.allowQueries should success but get error: %v", err)
	}
	// make mysql conn fail
	db.EnableConnFail()
	if sqlQuery.checkMySQL() {
		t.Fatalf("checkMySQL should return false")
	}
}

func TestSqlQueryCheckMysqlFailUninitializedQueryEngine(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	// this causes QueryEngine not being initialized properly
	sqlQuery.setState(StateServing)
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	defer sqlQuery.disallowQueries()
	if err != nil {
		t.Fatalf("SqlQuery.allowQueries should success but get error: %v", err)
	}
	// QueryEngine.CheckMySQL shoudl panic and checkMySQL should return false
	if sqlQuery.checkMySQL() {
		t.Fatalf("checkMySQL should return false")
	}
}

func TestSqlQueryCheckMysqlInNotServingState(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "SqlQuery")
	// sqlquery start request fail because we are in StateNotServing;
	// however, checkMySQL should return true. Here, we always assume
	// MySQL is healthy unless we've verified it is not.
	if !sqlQuery.checkMySQL() {
		t.Fatalf("checkMySQL should return true")
	}

	tabletState := expvar.Get(config.StatsPrefix + "TabletState")
	if tabletState == nil {
		t.Fatalf("%sTabletState should be exported", config.StatsPrefix)
	}
	varzState, err := strconv.Atoi(tabletState.String())
	if err != nil {
		t.Fatalf("invalid state reported by expvar, should be a valid state code, but got: %s", tabletState.String())
	}
	if varzState != StateNotServing {
		t.Fatalf("queryservice should be in NOT_SERVING state, but exported varz reports: %s", stateName[varzState])
	}
}

func TestSqlQueryGetSessionId(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	if err := sqlQuery.GetSessionId(nil, nil); err == nil {
		t.Fatalf("call GetSessionId should get an error")
	}
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	sessionInfo := proto.SessionInfo{}
	err = sqlQuery.GetSessionId(
		&proto.SessionParams{Keyspace: keyspace, Shard: shard},
		&sessionInfo,
	)
	if err != nil {
		t.Fatalf("got GetSessionId error: %v", err)
	}
	if sessionInfo.SessionId != sqlQuery.sessionID {
		t.Fatalf("call GetSessionId returns an unexpected session id, "+
			"expect seesion id: %d but got %d", sqlQuery.sessionID,
			sessionInfo.SessionId)
	}
	err = sqlQuery.GetSessionId(
		&proto.SessionParams{Keyspace: keyspace},
		&sessionInfo,
	)
	if err == nil {
		t.Fatalf("call GetSessionId should fail because of missing shard in request")
	}
	err = sqlQuery.GetSessionId(
		&proto.SessionParams{Shard: shard},
		&sessionInfo,
	)
	if err == nil {
		t.Fatalf("call GetSessionId should fail because of missing keyspace in request")
	}
}

func TestSqlQueryCommandFailUnMatchedSessionId(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	session := proto.Session{
		SessionId:     0,
		TransactionId: 0,
	}
	txInfo := proto.TransactionInfo{TransactionId: 0}
	if err = sqlQuery.Begin(ctx, &session, &txInfo); err == nil {
		t.Fatalf("call SqlQuery.Begin should fail because of an invalid session id: 0")
	}

	if err = sqlQuery.Commit(ctx, &session); err == nil {
		t.Fatalf("call SqlQuery.Commit should fail because of an invalid session id: 0")
	}

	if err = sqlQuery.Rollback(ctx, &session); err == nil {
		t.Fatalf("call SqlQuery.Rollback should fail because of an invalid session id: 0")
	}

	query := proto.Query{
		Sql:           "select * from test_table limit 1000",
		BindVariables: nil,
		SessionId:     session.SessionId,
		TransactionId: session.TransactionId,
	}
	reply := mproto.QueryResult{}
	if err := sqlQuery.Execute(ctx, &query, &reply); err == nil {
		t.Fatalf("call SqlQuery.Execute should fail because of an invalid session id: 0")
	}

	streamSendReply := func(*mproto.QueryResult) error { return nil }
	if err = sqlQuery.StreamExecute(ctx, &query, streamSendReply); err == nil {
		t.Fatalf("call SqlQuery.StreamExecute should fail because of an invalid session id: 0")
	}

	batchQuery := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           "commit",
				BindVariables: nil,
			},
		},
		SessionId: session.SessionId,
	}

	batchReply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
			mproto.QueryResult{},
		},
	}
	if err = sqlQuery.ExecuteBatch(ctx, &batchQuery, &batchReply); err == nil {
		t.Fatalf("call SqlQuery.ExecuteBatch should fail because of an invalid session id: 0")
	}

	splitQuery := proto.SplitQueryRequest{
		Query: proto.BoundQuery{
			Sql:           "select * from test_table where count > :count",
			BindVariables: nil,
		},
		SplitCount: 10,
		SessionID:  session.SessionId,
	}

	splitQueryReply := proto.SplitQueryResult{
		Queries: []proto.QuerySplit{
			proto.QuerySplit{
				Query: proto.BoundQuery{
					Sql:           "",
					BindVariables: nil,
				},
				RowCount: 10,
			},
		},
	}
	if err = sqlQuery.SplitQuery(ctx, &splitQuery, &splitQueryReply); err == nil {
		t.Fatalf("call SqlQuery.SplitQuery should fail because of an invalid session id: 0")
	}
}

func TestSqlQueryCommitTransaciton(t *testing.T) {
	db := setUpSqlQueryTest()
	// sql that will be executed in this test
	executeSql := "select * from test_table limit 1000"
	executeSqlResult := &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSql, executeSqlResult)
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	session := proto.Session{
		SessionId:     sqlQuery.sessionID,
		TransactionId: 0,
	}
	txInfo := proto.TransactionInfo{TransactionId: 0}
	if err = sqlQuery.Begin(ctx, &session, &txInfo); err != nil {
		t.Fatalf("call SqlQuery.Begin failed")
	}
	session.TransactionId = txInfo.TransactionId
	query := proto.Query{
		Sql:           executeSql,
		BindVariables: nil,
		SessionId:     session.SessionId,
		TransactionId: session.TransactionId,
	}
	reply := mproto.QueryResult{}
	if err := sqlQuery.Execute(ctx, &query, &reply); err != nil {
		t.Fatalf("failed to execute query: %s", query.Sql)
	}
	if err := sqlQuery.Commit(ctx, &session); err != nil {
		t.Fatalf("call SqlQuery.Commit failed")
	}
}

func TestSqlQueryRollback(t *testing.T) {
	db := setUpSqlQueryTest()
	// sql that will be executed in this test
	executeSql := "select * from test_table limit 1000"
	executeSqlResult := &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSql, executeSqlResult)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	session := proto.Session{
		SessionId:     sqlQuery.sessionID,
		TransactionId: 0,
	}
	txInfo := proto.TransactionInfo{TransactionId: 0}
	if err = sqlQuery.Begin(ctx, &session, &txInfo); err != nil {
		t.Fatalf("call SqlQuery.Begin failed")
	}
	session.TransactionId = txInfo.TransactionId
	query := proto.Query{
		Sql:           executeSql,
		BindVariables: nil,
		SessionId:     session.SessionId,
		TransactionId: session.TransactionId,
	}
	reply := mproto.QueryResult{}
	if err := sqlQuery.Execute(ctx, &query, &reply); err != nil {
		t.Fatalf("failed to execute query: %s", query.Sql)
	}
	if err := sqlQuery.Rollback(ctx, &session); err != nil {
		t.Fatalf("call SqlQuery.Rollback failed")
	}
}

func TestSqlQueryStreamExecute(t *testing.T) {
	db := setUpSqlQueryTest()
	// sql that will be executed in this test
	executeSql := "select * from test_table limit 1000"
	executeSqlResult := &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("row01"))},
		},
	}
	db.AddQuery(executeSql, executeSqlResult)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	session := proto.Session{
		SessionId:     sqlQuery.sessionID,
		TransactionId: 0,
	}
	txInfo := proto.TransactionInfo{TransactionId: 0}
	if err = sqlQuery.Begin(ctx, &session, &txInfo); err != nil {
		t.Fatalf("call SqlQuery.Begin failed")
	}
	session.TransactionId = txInfo.TransactionId
	query := proto.Query{
		Sql:           executeSql,
		BindVariables: nil,
		SessionId:     session.SessionId,
		TransactionId: session.TransactionId,
	}
	sendReply := func(*mproto.QueryResult) error { return nil }
	if err := sqlQuery.StreamExecute(ctx, &query, sendReply); err == nil {
		t.Fatalf("SqlQuery.StreamExecute should fail: %s", query.Sql)
	}
	if err := sqlQuery.Rollback(ctx, &session); err != nil {
		t.Fatalf("call SqlQuery.Rollback failed")
	}
	query.TransactionId = 0
	if err := sqlQuery.StreamExecute(ctx, &query, sendReply); err != nil {
		t.Fatalf("SqlQuery.StreamExecute should success: %s, but get error: %v",
			query.Sql, err)
	}
}

func TestSqlQueryExecuteBatch(t *testing.T) {
	db := setUpSqlQueryTest()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &mproto.QueryResult{}
	expanedSql := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSql, sqlResult)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           sql,
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           "commit",
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
			*sqlResult,
			mproto.QueryResult{},
		},
	}
	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err != nil {
		t.Fatalf("SqlQuery.ExecuteBatch should success: %v, but get error: %v",
			query, err)
	}
}

func TestSqlQueryExecuteBatchFailEmptyQueryList(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries:   []proto.BoundQuery{},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{},
	}
	err = sqlQuery.ExecuteBatch(ctx, &query, &reply)
	verifyTabletError(t, err, ErrFail)
}

func TestSqlQueryExecuteBatchBeginFail(t *testing.T) {
	db := setUpSqlQueryTest()
	// make "begin" query fail
	db.AddRejectedQuery("begin")
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
		},
	}
	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.ExecuteBatch should fail")
	}
}

func TestSqlQueryExecuteBatchCommitFail(t *testing.T) {
	db := setUpSqlQueryTest()
	// make "commit" query fail
	db.AddRejectedQuery("commit")
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           "commit",
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
			mproto.QueryResult{},
		},
	}
	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.ExecuteBatch should fail")
	}
}

func TestSqlQueryExecuteBatchSqlExecFailInTransaction(t *testing.T) {
	db := setUpSqlQueryTest()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &mproto.QueryResult{}
	expanedSql := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSql, sqlResult)

	// make this query fail
	db.AddRejectedQuery(sql)
	db.AddRejectedQuery(expanedSql)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           sql,
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           "commit",
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
			*sqlResult,
			mproto.QueryResult{},
		},
	}

	if db.GetQueryCalledNum("rollback") != 0 {
		t.Fatalf("rollback should not be executed.")
	}

	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.ExecuteBatch should fail")
	}

	if db.GetQueryCalledNum("rollback") != 1 {
		t.Fatalf("rollback should be executed only once.")
	}
}

func TestSqlQueryExecuteBatchFailBeginWithoutCommit(t *testing.T) {
	db := setUpSqlQueryTest()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &mproto.QueryResult{}
	expanedSql := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSql, sqlResult)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           sql,
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
			*sqlResult,
		},
	}

	if db.GetQueryCalledNum("rollback") != 0 {
		t.Fatalf("rollback should not be executed.")
	}

	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.ExecuteBatch should fail")
	}

	if db.GetQueryCalledNum("rollback") != 1 {
		t.Fatalf("rollback should be executed only once.")
	}
}

func TestSqlQueryExecuteBatchSqlExecFailNotInTransaction(t *testing.T) {
	db := setUpSqlQueryTest()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &mproto.QueryResult{}
	expanedSql := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSql, sqlResult)

	// cause execution error for this particular sql query
	db.AddRejectedQuery(sql)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           sql,
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			*sqlResult,
		},
	}
	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.ExecuteBatch should fail")
	}
}

func TestSqlQueryExecuteBatchCallCommitWithoutABegin(t *testing.T) {
	setUpSqlQueryTest()
	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")
	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "commit",
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
		},
	}
	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.ExecuteBatch should fail")
	}
}

func TestExecuteBatchNestedTransaction(t *testing.T) {
	db := setUpSqlQueryTest()
	sql := "insert into test_table values (1, 2)"
	sqlResult := &mproto.QueryResult{}
	expanedSql := "insert into test_table values (1, 2) /* _stream test_table (pk ) (1 ); */"

	db.AddQuery(sql, sqlResult)
	db.AddQuery(expanedSql, sqlResult)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.QueryList{
		Queries: []proto.BoundQuery{
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           "begin",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           sql,
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           "commit",
				BindVariables: nil,
			},
			proto.BoundQuery{
				Sql:           "commit",
				BindVariables: nil,
			},
		},
		SessionId: sqlQuery.sessionID,
	}

	reply := proto.QueryResultList{
		List: []mproto.QueryResult{
			mproto.QueryResult{},
			mproto.QueryResult{},
			*sqlResult,
			mproto.QueryResult{},
			mproto.QueryResult{},
		},
	}
	if err := sqlQuery.ExecuteBatch(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.Execute should fail because of nested transaction")
	}
	sqlQuery.qe.txPool.SetTimeout(10)
}

func TestSqlQuerySplitQuery(t *testing.T) {
	setUpSqlQueryTest()

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.SplitQueryRequest{
		Query: proto.BoundQuery{
			Sql:           "select * from test_table where count > :count",
			BindVariables: nil,
		},
		SplitCount: 10,
		SessionID:  sqlQuery.sessionID,
	}

	reply := proto.SplitQueryResult{
		Queries: []proto.QuerySplit{
			proto.QuerySplit{
				Query: proto.BoundQuery{
					Sql:           "",
					BindVariables: nil,
				},
				RowCount: 10,
			},
		},
	}
	if err := sqlQuery.SplitQuery(ctx, &query, &reply); err != nil {
		t.Fatalf("SqlQuery.SplitQuery should success: %v, but get error: %v",
			query, err)
	}
}

func TestSqlQuerySplitQueryInvalidQuery(t *testing.T) {
	setUpSqlQueryTest()

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.SplitQueryRequest{
		Query: proto.BoundQuery{
			// add limit clause to make SplitQuery fail
			Sql:           "select * from test_table where count > :count limit 1000",
			BindVariables: nil,
		},
		SplitCount: 10,
		SessionID:  sqlQuery.sessionID,
	}

	reply := proto.SplitQueryResult{
		Queries: []proto.QuerySplit{
			proto.QuerySplit{
				Query: proto.BoundQuery{
					Sql:           "",
					BindVariables: nil,
				},
				RowCount: 10,
			},
		},
	}
	if err := sqlQuery.SplitQuery(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.SplitQuery should fail")
	}
}

func TestSqlQuerySplitQueryInvalidMinMax(t *testing.T) {
	db := setUpSqlQueryTest()
	pkMinMaxQuery := "SELECT MIN(pk), MAX(pk) FROM test_table"
	pkMinMaxQueryResp := &mproto.QueryResult{
		Fields: []mproto.Field{
			mproto.Field{Name: "pk", Type: mproto.VT_LONG},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			// this make SplitQueryFail
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("invalid")),
				sqltypes.MakeString([]byte("invalid")),
			},
		},
	}
	db.AddQuery(pkMinMaxQuery, pkMinMaxQueryResp)

	config := newTestSqlQueryConfig()
	sqlQuery := NewSqlQuery(config, "")

	dbconfigs := newTestDBConfigs()
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("allowQueries failed: %v", err)
	}
	defer sqlQuery.disallowQueries()
	ctx := context.Background()
	query := proto.SplitQueryRequest{
		Query: proto.BoundQuery{
			Sql:           "select * from test_table where count > :count",
			BindVariables: nil,
		},
		SplitCount: 10,
		SessionID:  sqlQuery.sessionID,
	}

	reply := proto.SplitQueryResult{
		Queries: []proto.QuerySplit{
			proto.QuerySplit{
				Query: proto.BoundQuery{
					Sql:           "",
					BindVariables: nil,
				},
				RowCount: 10,
			},
		},
	}
	if err := sqlQuery.SplitQuery(ctx, &query, &reply); err == nil {
		t.Fatalf("SqlQuery.SplitQuery should fail")
	}
}

func TestHandleExecUnknownError(t *testing.T) {
	ctx := context.Background()
	logStats := newSqlQueryStats("TestHandleExecError", ctx)
	query := proto.Query{
		Sql:           "select * from test_table",
		BindVariables: nil,
	}
	var err error
	sq := &SqlQuery{}
	defer sq.handleExecError(&query, &err, logStats)
	panic("unknown exec error")
}

func TestHandleExecTabletError(t *testing.T) {
	ctx := context.Background()
	logStats := newSqlQueryStats("TestHandleExecError", ctx)
	query := proto.Query{
		Sql:           "select * from test_table",
		BindVariables: nil,
	}
	var err error
	defer func() {
		want := "fatal: tablet error"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want '%s'", err, want)
		}
	}()
	sq := &SqlQuery{}
	defer sq.handleExecError(&query, &err, logStats)
	panic(NewTabletError(ErrFatal, "tablet error"))
}

func TestTerseErrors1(t *testing.T) {
	ctx := context.Background()
	logStats := newSqlQueryStats("TestHandleExecError", ctx)
	query := proto.Query{
		Sql:           "select * from test_table",
		BindVariables: nil,
	}
	var err error
	defer func() {
		want := "fatal: tablet error"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want '%s'", err, want)
		}
	}()
	sq := &SqlQuery{}
	sq.config.TerseErrors = true
	defer sq.handleExecError(&query, &err, logStats)
	panic(NewTabletError(ErrFatal, "tablet error"))
}

func TestTerseErrors2(t *testing.T) {
	ctx := context.Background()
	logStats := newSqlQueryStats("TestHandleExecError", ctx)
	query := proto.Query{
		Sql:           "select * from test_table",
		BindVariables: nil,
	}
	var err error
	defer func() {
		want := "error: (errno 10) during query: select * from test_table"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want '%s'", err, want)
		}
	}()
	sq := &SqlQuery{}
	sq.config.TerseErrors = true
	defer sq.handleExecError(&query, &err, logStats)
	panic(&TabletError{
		ErrorType: ErrFail,
		Message:   "msg",
		SqlError:  10,
	})
}

func setUpSqlQueryTest() *fakesqldb.DB {
	db := fakesqldb.Register()
	for query, result := range getSupportedQueries() {
		db.AddQuery(query, result)
	}
	return db
}

func newTestSqlQueryConfig() Config {
	randID := rand.Int63()
	config := DefaultQsConfig
	config.StatsPrefix = fmt.Sprintf("Stats-%d-", randID)
	config.DebugURLPrefix = fmt.Sprintf("/debug-%d-", randID)
	config.RowCache.StatsPrefix = fmt.Sprintf("Stats-%d-", randID)
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	config.StrictMode = true
	config.RowCache.Binary = "ls"
	config.RowCache.Connections = 100
	return config
}

func newMysqld(dbconfigs *dbconfigs.DBConfigs) *mysqlctl.Mysqld {
	randID := rand.Int63()
	return mysqlctl.NewMysqld(
		fmt.Sprintf("Dba_%d", randID),
		fmt.Sprintf("App_%d", randID),
		mysqlctl.NewMycnf(0, 6802),
		&dbconfigs.Dba,
		&dbconfigs.App.ConnParams,
		&dbconfigs.Repl,
	)
}

func newTestDBConfigs() dbconfigs.DBConfigs {
	appDBConfig := dbconfigs.DBConfig{
		ConnParams:        sqldb.ConnParams{},
		Keyspace:          "test_keyspace",
		Shard:             "0",
		EnableRowcache:    false,
		EnableInvalidator: false,
	}
	return dbconfigs.DBConfigs{
		App: appDBConfig,
	}
}

func checkSqlQueryState(t *testing.T, sqlQuery *SqlQuery, expectState string) {
	if sqlQuery.GetState() != expectState {
		t.Fatalf("sqlquery should in state: %s, but get state: %s", expectState, sqlQuery.GetState())
	}
}

func getSupportedQueries() map[string]*mproto.QueryResult {
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
		"select * from test_table where 1 != 1": &mproto.QueryResult{
			Fields: getTestTableFields(),
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
		"begin":    &mproto.QueryResult{},
		"commit":   &mproto.QueryResult{},
		"rollback": &mproto.QueryResult{},
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
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
