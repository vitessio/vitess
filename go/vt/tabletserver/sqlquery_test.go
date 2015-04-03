// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
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

var testSqlQuery *SqlQuery

func TestSqlQueryAllowQueries(t *testing.T) {
	fakesqldb.Register(nil, false)
	sqlQuery := getSqlQuery()
	if sqlQuery.GetState() != "NOT_SERVING" {
		t.Fatalf("sqlquery should in state: NOT_SERVING, but get state: %s", sqlQuery.GetState())
	}
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
	sqlQuery.setState(StateServing)
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("SqlQuery.allowQueries should success, but get error: %v", err)
	}
	sqlQuery.setState(StateShuttingTx)
	err = sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err == nil {
		t.Fatalf("SqlQuery.allowQueries should fail")
	}
	sqlQuery.setState(StateNotServing)
	fakesqldb.Register(nil, true)
	err = sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	if err == nil {
		t.Fatalf("SqlQuery.allowQueries should fail because of failed to create a new connection")
	}
}

func TestCheckMysql(t *testing.T) {
	fakesqldb.Register(getSupportedQueries(), true)
	sqlQuery := getSqlQuery()
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
	sqlQuery.setState(StateServing)
	err := sqlQuery.allowQueries(&dbconfigs, []SchemaOverride{}, newMysqld(&dbconfigs))
	defer sqlQuery.disallowQueries()
	if err != nil {
		t.Fatalf("SqlQuery.allowQueries should success but get error: %v", err)
	}
	sqlQuery.checkMySQL()
}

func TestGetSessionId(t *testing.T) {
	fakesqldb.Register(getSupportedQueries(), false)
	sqlQuery := getSqlQuery()
	if err := sqlQuery.GetSessionId(nil, nil); err == nil {
		t.Fatalf("call GetSessionId should get an error")
	}
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
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

func TestTransactionCommit(t *testing.T) {
	// sql that will be executed in this test
	executeSql := "select * from test_table"
	executeSqlResult := &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("row01"))},
		},
	}
	supportedQueries := getSupportedQueries()
	supportedQueries[executeSql] = executeSqlResult
	fakesqldb.Register(supportedQueries, false)
	sqlQuery := getSqlQuery()
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
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

func TestTransactionRollback(t *testing.T) {
	// sql that will be executed in this test
	executeSql := "select * from test_table"
	executeSqlResult := &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("row01"))},
		},
	}
	supportedQueries := getSupportedQueries()
	supportedQueries[executeSql] = executeSqlResult
	fakesqldb.Register(supportedQueries, false)
	sqlQuery := getSqlQuery()
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
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

func TestStreamExecute(t *testing.T) {
	// sql that will be executed in this test
	executeSql := "select * from test_table"
	executeSqlResult := &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("row01"))},
		},
	}
	supportedQueries := getSupportedQueries()
	supportedQueries[executeSql] = executeSqlResult
	fakesqldb.Register(supportedQueries, false)
	sqlQuery := getSqlQuery()
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
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

func TestExecuteBatch(t *testing.T) {
	sql := "INSERT INTO test_table VALUES(1, 2)"
	sqlResult := &mproto.QueryResult{
		RowsAffected: 1,
	}
	supportedQueries := getSupportedQueries()
	supportedQueries[sql] = sqlResult
	fakesqldb.Register(supportedQueries, false)
	sqlQuery := getSqlQuery()
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
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
		t.Fatalf("SqlQuery.Execute should success: %v, but get error: %v",
			query, err)
	}
}

func TestExecuteBatchNestedTransaction(t *testing.T) {
	sql := "INSERT INTO test_table VALUES(1, 2)"
	sqlResult := &mproto.QueryResult{
		RowsAffected: 1,
	}
	supportedQueries := getSupportedQueries()
	supportedQueries[sql] = sqlResult
	fakesqldb.Register(supportedQueries, false)
	sqlQuery := getSqlQuery()
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
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
	sql := "INSERT INTO test_table VALUES(1, 2)"
	sqlResult := &mproto.QueryResult{
		RowsAffected: 1,
	}
	supportedQueries := getSupportedQueries()
	supportedQueries[sql] = sqlResult
	fakesqldb.Register(supportedQueries, false)
	sqlQuery := getSqlQuery()
	keyspace := "test_keyspace"
	shard := "0"
	dbconfigs := getTestDBConfigs(keyspace, shard)
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

func getSqlQuery() *SqlQuery {
	if testSqlQuery == nil {
		config := DefaultQsConfig
		config.StrictMode = false
		testSqlQuery = NewSqlQuery(config)
	}
	// make sure SqlQuery is in StateNotServing state
	testSqlQuery.disallowQueries()
	return testSqlQuery
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

func getTestDBConfigs(keyspace string, shard string) dbconfigs.DBConfigs {
	appDBConfig := dbconfigs.DBConfig{
		ConnParams:        sqldb.ConnParams{},
		Keyspace:          keyspace,
		Shard:             shard,
		EnableRowcache:    false,
		EnableInvalidator: false,
	}
	return dbconfigs.DBConfigs{
		App: appDBConfig,
	}
}

// getSupportedQueries returns a list of queries along with their expected responses
// that a fake sqldb.Conn should support in order to unit test SqlQuery.
func getSupportedQueries() map[string]*mproto.QueryResult {
	return map[string]*mproto.QueryResult{
		// queries for schema info
		"select unix_timestamp()": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{sqltypes.MakeString([]byte("1427325875"))},
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
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("column_01")),
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
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("column_01")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
