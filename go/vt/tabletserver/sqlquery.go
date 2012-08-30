// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"expvar"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/vt/sqlparser"
)

const (
	MAX_RESULT_NAME = "_vtMaxResultSize"
)

const (
	INIT_FAILED   = -1
	CLOSED        = 0
	SHUTTING_DOWN = 1
	OPEN          = 2
)

//-----------------------------------------------
// RPC API
type SqlQuery struct {
	mu            sync.RWMutex
	state         int32 // Use sync/atomic to acces this variable
	sessionId     int64
	cachePool     *CachePool
	schemaInfo    *SchemaInfo
	connPool      *ConnectionPool
	reservedPool  *ReservedPool
	txPool        *ConnectionPool
	activeTxPool  *ActiveTxPool
	activePool    *ActivePool
	consolidator  *Consolidator
	maxResultSize int32 // Use sync/atomic

	// Vars for handling invalidations
	dbName string
}

// stats are globals to allow anybody to set them
var queryStats, waitStats *stats.Timings
var killStats, errorStats *stats.Counters
var resultStats *stats.Histogram

var resultBuckets = []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}

// CacheInvalidator provides the abstraction needed for an instant invalidation
// vs. delayed invalidation in the case of in-transaction dmls
type CacheInvalidator interface {
	Delete(key string) bool
}

func NewSqlQuery(config Config) *SqlQuery {
	sq := &SqlQuery{}
	sq.cachePool = NewCachePool(config.CachePoolCap, time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	sq.schemaInfo = NewSchemaInfo(config.QueryCacheSize, time.Duration(config.SchemaReloadTime*1e9))
	sq.connPool = NewConnectionPool(config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	sq.reservedPool = NewReservedPool()
	sq.txPool = NewConnectionPool(config.TransactionCap, time.Duration(config.IdleTimeout*1e9)) // connections in pool has to be > transactionCap
	sq.activeTxPool = NewActiveTxPool(time.Duration(config.TransactionTimeout * 1e9))
	sq.activePool = NewActivePool(time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	sq.consolidator = NewConsolidator()
	sq.maxResultSize = int32(config.MaxResultSize)
	expvar.Publish("Voltron", stats.StrFunc(func() string { return sq.statsJSON() }))
	queryStats = stats.NewTimings("Queries")
	stats.NewRates("QPS", queryStats, 15, 60e9)
	waitStats = stats.NewTimings("Waits")
	killStats = stats.NewCounters("Kills")
	errorStats = stats.NewCounters("Errors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	return sq
}

type CompiledPlan struct {
	Query string
	*ExecPlan
	BindVars      map[string]interface{}
	TransactionId int64
	ConnectionId  int64
}

func (sq *SqlQuery) allowQueries(dbconfig DBConfig) {
	connFactory := GenericConnectionCreator(dbconfig.MysqlParams())
	cacheFactory := CacheCreator(dbconfig)
	sq.mu.Lock()
	defer sq.mu.Unlock()
	atomic.StoreInt32(&sq.state, INIT_FAILED)

	start := time.Now().UnixNano()
	sq.cachePool.Open(cacheFactory)
	sq.schemaInfo.Open(connFactory, sq.cachePool)
	relog.Info("Time taken to load the schema: %v ms", (time.Now().UnixNano()-start)/1e6)
	sq.connPool.Open(connFactory)
	sq.reservedPool.Open(connFactory)
	sq.txPool.Open(connFactory)
	sq.activeTxPool.Open()
	sq.activePool.Open(connFactory)
	sq.sessionId = Rand()
	relog.Info("Session id: %d", sq.sessionId)
	atomic.StoreInt32(&sq.state, OPEN)
	sq.dbName = dbconfig.Dbname
}

func (sq *SqlQuery) disallowQueries() {
	// set this before obtaining lock so new incoming requests
	// can serve "unavailable" immediately
	atomic.StoreInt32(&sq.state, SHUTTING_DOWN)
	relog.Info("Stopping query service: %d", sq.sessionId)
	sq.activeTxPool.WaitForEmpty()

	sq.mu.Lock()
	defer sq.mu.Unlock()
	atomic.StoreInt32(&sq.state, CLOSED)
	sq.activePool.Close()
	sq.schemaInfo.Close()
	sq.activeTxPool.Close()
	sq.txPool.Close()
	sq.reservedPool.Close()
	sq.connPool.Close()
	sq.sessionId = 0
	sq.dbName = ""
}

func (sq *SqlQuery) checkState(sessionId int64, allowShutdown bool) {
	switch atomic.LoadInt32(&sq.state) {
	case INIT_FAILED:
		panic(NewTabletError(FATAL, "init failed"))
	case CLOSED:
		panic(NewTabletError(RETRY, "unavailable"))
	case SHUTTING_DOWN:
		if !allowShutdown {
			panic(NewTabletError(RETRY, "unavailable"))
		}
	}
	if sessionId != sq.sessionId {
		panic(NewTabletError(RETRY, "Invalid session Id %v", sessionId))
	}
}

type SessionParams struct {
	DbName string
}

type SessionInfo struct {
	SessionId int64
}

func (sq *SqlQuery) GetSessionId(sessionParams *SessionParams, sessionInfo *SessionInfo) error {
	if sessionParams.DbName != sq.dbName {
		return NewTabletError(FATAL, "db name mismatch, expecting %v, received %v", sq.dbName, sessionParams.DbName)
	}
	sessionInfo.SessionId = sq.sessionId
	return nil
}

func (sq *SqlQuery) Begin(session *Session, transactionId *int64) (err error) {
	defer handleError(&err)
	sq.checkState(session.SessionId, false)
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	var conn PoolConnection
	if session.ConnectionId != 0 {
		conn = sq.reservedPool.Get(session.ConnectionId)
	} else if conn = sq.txPool.TryGet(); conn == nil {
		panic(NewTabletError(FAIL, "Transaction pool connection limit exceeded"))
	}
	if *transactionId, err = sq.activeTxPool.SafeBegin(conn); err != nil {
		conn.Recycle()
		return err
	}
	return nil
}

func (sq *SqlQuery) Commit(session *Session, noOutput *string) (err error) {
	defer handleError(&err)
	sq.checkState(session.SessionId, true)
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	*noOutput = ""
	dirtyTables, err := sq.activeTxPool.SafeCommit(session.TransactionId)
	sq.invalidateRows(dirtyTables)
	return err
}

func (sq *SqlQuery) invalidateRows(dirtyTables map[string]DirtyKeys) {
	for tableName, invalidList := range dirtyTables {
		tableInfo := sq.schemaInfo.GetTable(tableName)
		if tableInfo == nil {
			continue
		}
		invalidations := int64(0)
		for key := range invalidList {
			tableInfo.Cache.Delete(key)
			invalidations++
		}
		atomic.AddInt64(&tableInfo.invalidations, invalidations)
	}
}

func (sq *SqlQuery) Rollback(session *Session, noOutput *string) (err error) {
	defer handleError(&err)
	sq.checkState(session.SessionId, true)
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	*noOutput = ""
	sq.activeTxPool.Rollback(session.TransactionId)
	return nil
}

type ConnectionInfo struct {
	ConnectionId int64
}

func (sq *SqlQuery) CreateReserved(session *Session, connectionInfo *ConnectionInfo) (err error) {
	defer handleError(&err)
	sq.checkState(session.SessionId, false)
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	connectionInfo.ConnectionId = sq.reservedPool.CreateConnection()
	return nil
}

func (sq *SqlQuery) CloseReserved(session *Session, noOutput *string) (err error) {
	defer handleError(&err)
	sq.checkState(session.SessionId, false)
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	*noOutput = ""
	sq.reservedPool.CloseConnection(session.ConnectionId)
	return nil
}

func handleExecError(query *Query, err *error) {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			relog.Error("Uncaught panic for %v", query)
			panic(x)
		}
		*err = terr
		terr.RecordStats()
		if terr.ErrorType == RETRY || terr.SqlError == DUPLICATE_KEY { // suppress these errors in logs
			return
		}
		relog.Error("%s: %v", terr.Message, query)
	}
}

func (sq *SqlQuery) Execute(query *Query, reply *QueryResult) (err error) {
	defer handleExecError(query, &err)

	// allow shutdown state if we're in a transaction
	allowShutdown := (query.TransactionId != 0)
	sq.checkState(query.SessionId, allowShutdown)

	sq.mu.RLock()
	defer sq.mu.RUnlock()

	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)
	basePlan := sq.schemaInfo.GetPlan(query.Sql, len(query.BindVariables) != 0)
	if basePlan.PlanId == sqlparser.PLAN_DDL {
		defer queryStats.Record("DDL", time.Now())
		*reply = *sq.execDDL(query.Sql)
		return nil
	}

	plan := &CompiledPlan{query.Sql, basePlan, query.BindVariables, query.TransactionId, query.ConnectionId}
	if query.TransactionId != 0 {
		// Need upfront connection for DMLs and transactions
		conn := sq.activeTxPool.Get(query.TransactionId)
		defer conn.Recycle()
		var invalidator CacheInvalidator
		if plan.TableInfo != nil && plan.TableInfo.CacheType != 0 {
			invalidator = conn.DirtyKeys(plan.TableName)
		}
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_DML:
			if plan.TableInfo != nil && plan.TableInfo.CacheType != 0 {
				panic(NewTabletError(FAIL, "DML too complex for cached table"))
			}
			defer queryStats.Record("PASS_DML", time.Now())
			*reply = *sq.directFetch(conn, plan.FullQuery, plan.BindVars, nil, nil)
		case sqlparser.PLAN_INSERT_PK:
			defer queryStats.Record("PLAN_INSERT_PK", time.Now())
			*reply = *sq.execInsertPK(conn, plan, invalidator)
		case sqlparser.PLAN_INSERT_SUBQUERY:
			defer queryStats.Record("PLAN_INSERT_SUBQUERY", time.Now())
			*reply = *sq.execInsertSubquery(conn, plan, invalidator)
		case sqlparser.PLAN_DML_PK:
			defer queryStats.Record("DML_PK", time.Now())
			*reply = *sq.execDMLPK(conn, plan, invalidator)
		case sqlparser.PLAN_DML_SUBQUERY:
			defer queryStats.Record("DML_SUBQUERY", time.Now())
			*reply = *sq.execDMLSubquery(conn, plan, invalidator)
		default: // select or set in a transaction, just count as select
			defer queryStats.Record("PASS_SELECT", time.Now())
			*reply = *sq.fullFetch(conn, plan.FullQuery, plan.BindVars, nil, nil)
		}
	} else {
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_SELECT:
			if plan.Reason == sqlparser.REASON_FOR_UPDATE {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			defer queryStats.Record("PASS_SELECT", time.Now())
			*reply = *sq.execSelect(plan)
		case sqlparser.PLAN_SELECT_PK:
			defer queryStats.Record("SELECT_PK", time.Now())
			*reply = *sq.execPK(plan)
		case sqlparser.PLAN_SELECT_SUBQUERY:
			defer queryStats.Record("SELECT_SUBQUERY", time.Now())
			*reply = *sq.execSubquery(plan)
		case sqlparser.PLAN_SELECT_CACHE_RESULT:
			defer queryStats.Record("SELECT_CACHE_RESULT", time.Now())
			// It may not be worth caching the results. So, just pass through.
			*reply = *sq.execSelect(plan)
		case sqlparser.PLAN_SET:
			defer queryStats.Record("SET", time.Now())
			*reply = *sq.execSet(plan)
		default:
			panic(NewTabletError(FAIL, "DMLs not allowed outside of transactions"))
		}
	}
	if plan.PlanId.IsSelect() {
		resultStats.Add(int64(reply.RowsAffected))
	}
	return nil
}

// the first QueryResult will have Fields set (and Rows nil)
// the subsequent QueryResult will have Rows set (and Fields nil)
func (sq *SqlQuery) StreamExecute(query *Query, sendReply func(reply interface{}) error) (err error) {

	defer handleExecError(query, &err)

	// check cases we don't handle yet
	if query.TransactionId != 0 {
		return NewTabletError(FAIL, "Transactions not supported with streaming")
	}
	if query.ConnectionId != 0 {
		return NewTabletError(FAIL, "Persistent connections not supported with streaming")
	}

	// allow shutdown state if we're in a transaction
	allowShutdown := (query.TransactionId != 0)
	sq.checkState(query.SessionId, allowShutdown)

	sq.mu.RLock()
	defer sq.mu.RUnlock()

	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)
	fullQuery := sq.schemaInfo.GetStreamPlan(query.Sql)
	defer queryStats.Record("SELECT_STREAM", time.Now())

	// does the real work: first get a connection
	conn := sq.connPool.Get()
	defer conn.Recycle()

	// then setup the callback and stream!
	callback := func(sqr *QueryResult) (err error) {
		err = sendReply(sqr)
		if err != nil {
			return err
		}
		return nil
	}
	err = sq.fullStreamFetch(conn, fullQuery, query.BindVariables, nil, nil, callback)
	if err != nil {
		return err
	}

	return nil
}

type QueryList struct {
	List []Query
}

type QueryResultList struct {
	List []QueryResult
}

func (sq *SqlQuery) ExecuteBatch(queryList *QueryList, reply *QueryResultList) (err error) {
	defer handleError(&err)
	ql := queryList.List
	if len(ql) == 0 {
		panic(NewTabletError(FAIL, "Empty query list"))
	}
	sq.checkState(ql[0].SessionId, false)
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	begin_called := false
	var noOutput string
	session := Session{
		TransactionId: ql[0].TransactionId,
		ConnectionId:  ql[0].ConnectionId,
		SessionId:     ql[0].SessionId,
	}
	reply.List = make([]QueryResult, 0, len(ql))
	for _, query := range ql {
		trimmed := strings.ToLower(strings.Trim(query.Sql, " \t\r\n"))
		switch trimmed {
		case "begin":
			if session.TransactionId != 0 {
				panic(NewTabletError(FAIL, "Nested transactions disallowed"))
			}
			if err = sq.Begin(&session, &session.TransactionId); err != nil {
				return err
			}
			begin_called = true
			reply.List = append(reply.List, QueryResult{})
		case "commit":
			if !begin_called {
				panic(NewTabletError(FAIL, "Cannot commit without begin"))
			}
			if err = sq.Commit(&session, &noOutput); err != nil {
				return err
			}
			session.TransactionId = 0
			begin_called = false
			reply.List = append(reply.List, QueryResult{})
		default:
			query.TransactionId = session.TransactionId
			query.ConnectionId = session.ConnectionId
			query.SessionId = session.SessionId
			var localReply QueryResult
			if err = sq.Execute(&query, &localReply); err != nil {
				if begin_called {
					sq.Rollback(&session, &noOutput)
				}
				return err
			}
			reply.List = append(reply.List, localReply)
		}
	}
	if begin_called {
		sq.Rollback(&session, &noOutput)
		panic(NewTabletError(FAIL, "begin called with no commit"))
	}
	return nil
}

type SlaveTxCommand struct {
	Command string
}

type CacheInvalidate struct {
	Database string
	Dmls     []struct {
		Table string
		Keys  []interface{}
	}
}

func (sq *SqlQuery) Invalidate(cacheInvalidate *CacheInvalidate, noOutput *string) (err error) {
	defer handleError(&err)
	sq.checkState(sq.sessionId, false)
	*noOutput = ""
	sq.mu.RLock()
	defer sq.mu.RUnlock()

	if sq.cachePool.IsClosed() || cacheInvalidate.Database != sq.dbName {
		return nil
	}
	for _, dml := range cacheInvalidate.Dmls {
		invalidations := int64(0)
		tableInfo := sq.schemaInfo.GetTable(dml.Table)
		if tableInfo == nil {
			return NewTabletError(FAIL, "Table %s not found", dml.Table)
		}
		if tableInfo.CacheType == 0 {
			break
		}
		for _, val := range dml.Keys {
			newKey := validateKey(tableInfo, val.(string))
			if newKey != "" {
				tableInfo.Cache.Delete(newKey)
			}
			invalidations++
		}
		atomic.AddInt64(&tableInfo.invalidations, invalidations)
	}
	return nil
}

type DDLInvalidate struct {
	Database string
	DDL      string
}

func (sq *SqlQuery) InvalidateForDDL(ddl *DDLInvalidate, noOutput *string) (err error) {
	defer handleError(&err)
	sq.checkState(sq.sessionId, true) // Accept DDLs in shut down mode
	*noOutput = ""
	sq.mu.RLock()
	defer sq.mu.RUnlock()

	if ddl.Database != sq.dbName {
		return nil
	}

	ddlDecoded := base64Decode([]byte(ddl.DDL))
	ddlPlan := sqlparser.DDLParse(ddlDecoded)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}
	sq.schemaInfo.DropTable(ddlPlan.TableName)
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		sq.schemaInfo.CreateTable(ddlPlan.NewName)
	}
	return nil
}

func (sq *SqlQuery) Ping(query *string, reply *string) error {
	*reply = "pong: " + *query
	return nil
}

//-----------------------------------------------
// DDL

func (sq *SqlQuery) execDDL(ddl string) *QueryResult {
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	ddlPlan := sqlparser.DDLParse(ddl)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}

	// Stolen from Begin
	conn := sq.txPool.TryGet()
	if conn == nil {
		panic(NewTabletError(FAIL, "Transaction pool connection limit exceeded"))
	}
	txid, err := sq.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	// Stolen from Commit
	defer sq.activeTxPool.SafeCommit(txid)

	// Stolen from Execute
	conn = sq.activeTxPool.Get(txid)
	defer conn.Recycle()
	result, err := sq.executeSql(conn, []byte(ddl), false)
	if err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}

	sq.schemaInfo.DropTable(ddlPlan.TableName)
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		sq.schemaInfo.CreateTable(ddlPlan.NewName)
	}
	return result
}

//-----------------------------------------------
// Execution

func (sq *SqlQuery) execPK(plan *CompiledPlan) (result *QueryResult) {
	pkRows := buildValueList(plan.PKValues, plan.BindVars)
	return sq.fetchPKRows(plan, pkRows)
}

func (sq *SqlQuery) execSubquery(plan *CompiledPlan) (result *QueryResult) {
	innerResult := sq.qFetch(plan, plan.Subquery, nil)
	return sq.fetchPKRows(plan, copyRows(innerResult.Rows))
}

func (sq *SqlQuery) fetchPKRows(plan *CompiledPlan, pkRows [][]interface{}) (result *QueryResult) {
	result = &QueryResult{}
	tableInfo := plan.TableInfo
	if plan.Fields == nil {
		panic("unexpected")
	}
	result.Fields = plan.Fields
	normalizePKRows(plan.TableInfo, pkRows)
	rows := make([][]interface{}, 0, len(pkRows))
	var hits, absent, misses int64
	for _, pk := range pkRows {
		key := buildKey(tableInfo, pk)
		if cacheRow, cas := tableInfo.Cache.Get(key); cacheRow != nil {
			/*if dbrow := sq.validateRow(plan, cacheRow, pk); dbrow != nil {
				rows = append(rows, applyFilter(plan.ColumnNumbers, dbrow))
			}*/
			rows = append(rows, applyFilter(plan.ColumnNumbers, cacheRow))
			hits++
		} else {
			resultFromdb := sq.qFetch(plan, plan.OuterQuery, pk)
			if len(resultFromdb.Rows) == 0 {
				absent++
				continue
			}
			row := resultFromdb.Rows[0]
			pkRow := applyFilter(tableInfo.PKColumns, row)
			newKey := buildKey(tableInfo, pkRow)
			if newKey != key {
				relog.Warning("Key mismatch for query %s. computed: %s, fetched: %s", plan.FullQuery.Query, key, newKey)
			}
			tableInfo.Cache.Set(newKey, row, cas)
			rows = append(rows, applyFilter(plan.ColumnNumbers, row))
			misses++
		}
	}
	atomic.AddInt64(&tableInfo.hits, hits)
	atomic.AddInt64(&tableInfo.absent, absent)
	atomic.AddInt64(&tableInfo.misses, misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	return result
}

func (sq *SqlQuery) validateRow(plan *CompiledPlan, cacheRow []interface{}, pk []interface{}) (dbrow []interface{}) {
	resultFromdb := sq.qFetch(plan, plan.OuterQuery, pk)
	if len(resultFromdb.Rows) != 1 {
		relog.Warning("unexpected number of rows for %v: %d", pk, len(resultFromdb.Rows))
		return nil
	}
	dbrow = resultFromdb.Rows[0]
	for i := 0; i < len(cacheRow); i++ {
		if cacheRow[i] == nil && dbrow[i] == nil {
			continue
		}
		if (cacheRow[i] == nil && dbrow[i] != nil) || (cacheRow[i] != nil && dbrow[i] == nil) || string(cacheRow[i].([]byte)) != dbrow[i] {
			relog.Warning("query: %v", plan.FullQuery)
			relog.Warning("mismatch for: %v, column: %v cache: %s, db: %s", pk, i, cacheRow[i], dbrow[i])
			return dbrow
		}
	}
	return dbrow
}

func (sq *SqlQuery) execSelect(plan *CompiledPlan) (result *QueryResult) {
	if plan.Fields != nil {
		result = sq.qFetch(plan, plan.FullQuery, nil)
		result.Fields = plan.Fields
		return
	}
	var conn PoolConnection
	if plan.ConnectionId != 0 {
		conn = sq.reservedPool.Get(plan.ConnectionId)
	} else {
		conn = sq.connPool.Get()
	}
	defer conn.Recycle()
	result = sq.fullFetch(conn, plan.FullQuery, plan.BindVars, nil, nil)
	sq.schemaInfo.SetFields(plan.Query, plan.ExecPlan, result.Fields)
	return result
}

func (sq *SqlQuery) execInsertPK(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	pkRows := buildValueList(plan.PKValues, plan.BindVars)
	normalizePKRows(plan.TableInfo, pkRows)
	return sq.execInsertPKRows(conn, plan, pkRows, invalidator)
}

func (sq *SqlQuery) execInsertSubquery(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	innerResult := sq.directFetch(conn, plan.Subquery, plan.BindVars, nil, nil)
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &QueryResult{RowsAffected: 0}
	}
	if len(plan.ColumnNumbers) != len(innerRows[0]) {
		panic(NewTabletError(FAIL, "Subquery length does not match column list"))
	}
	normalizeRows(plan.TableInfo, plan.ColumnNumbers, innerRows)
	pkRows := make([][]interface{}, len(innerRows))
	for i, innerRow := range innerRows {
		pkRows[i] = applyFilter(plan.SubqueryPKColumns, innerRow)
	}
	plan.BindVars["_rowValues"] = innerRows
	return sq.execInsertPKRows(conn, plan, pkRows, invalidator)
}

func (sq *SqlQuery) execInsertPKRows(conn PoolConnection, plan *CompiledPlan, pkRows [][]interface{}, invalidator CacheInvalidator) (result *QueryResult) {
	fillPKDefaults(plan.TableInfo, pkRows)
	secondaryList := buildSecondaryList(pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = sq.directFetch(conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	// TODO: We need to do this only if insert has on duplicate key clause
	if invalidator != nil {
		for _, pk := range pkRows {
			if key := buildKey(plan.TableInfo, pk); key != "" {
				invalidator.Delete(key)
			}
		}
	}
	return result
}

func (sq *SqlQuery) execDMLPK(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	pkRows := buildValueList(plan.PKValues, plan.BindVars)
	normalizePKRows(plan.TableInfo, pkRows)
	secondaryList := buildSecondaryList(pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = sq.directFetch(conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	if invalidator != nil {
		for _, pk := range pkRows {
			key := buildKey(plan.TableInfo, pk)
			invalidator.Delete(key)
		}
	}
	return result
}

func (sq *SqlQuery) execDMLSubquery(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	innerResult := sq.directFetch(conn, plan.Subquery, plan.BindVars, nil, nil)
	return sq.execDMLPKRows(conn, plan, innerResult.Rows, invalidator)
}

func (sq *SqlQuery) execDMLPKRows(conn PoolConnection, plan *CompiledPlan, pkRows [][]interface{}, invalidator CacheInvalidator) (result *QueryResult) {
	if len(pkRows) == 0 {
		return &QueryResult{RowsAffected: 0}
	}
	normalizePKRows(plan.TableInfo, pkRows)
	rowsAffected := uint64(0)
	singleRow := make([][]interface{}, 1)
	for _, pkRow := range pkRows {
		singleRow[0] = pkRow
		secondaryList := buildSecondaryList(singleRow, plan.SecondaryPKValues, plan.BindVars)
		bsc := buildStreamComment(plan.TableInfo, singleRow, secondaryList)
		rowsAffected += sq.directFetch(conn, plan.OuterQuery, plan.BindVars, pkRow, bsc).RowsAffected
		if invalidator != nil {
			key := buildKey(plan.TableInfo, pkRow)
			invalidator.Delete(key)
		}
	}
	return &QueryResult{RowsAffected: rowsAffected}
}

func (sq *SqlQuery) execSet(plan *CompiledPlan) (result *QueryResult) {
	switch plan.SetKey {
	case "vt_pool_size":
		sq.connPool.SetCapacity(int(plan.SetValue.(float64)))
		return &QueryResult{}
	case "vt_transaction_cap":
		sq.txPool.SetCapacity(int(plan.SetValue.(float64)))
		return &QueryResult{}
	case "vt_transaction_timeout":
		sq.activeTxPool.SetTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &QueryResult{}
	case "vt_schema_reload_time":
		sq.schemaInfo.SetReloadTime(time.Duration(plan.SetValue.(float64) * 1e9))
		return &QueryResult{}
	case "vt_query_cache_size":
		sq.schemaInfo.SetQueryCacheSize(int(plan.SetValue.(float64)))
		return &QueryResult{}
	case "vt_max_result_size":
		val := int32(plan.SetValue.(float64))
		if val < 1 {
			panic(NewTabletError(FAIL, "max result size out of range %v", val))
		}
		atomic.StoreInt32(&sq.maxResultSize, val)
		return &QueryResult{}
	case "vt_query_timeout":
		sq.activePool.SetTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &QueryResult{}
	case "vt_idle_timeout":
		sq.connPool.SetIdleTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		sq.txPool.SetIdleTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		sq.activePool.SetIdleTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &QueryResult{}
	}
	return sq.qFetch(plan, plan.FullQuery, nil)
}

func (sq *SqlQuery) qFetch(plan *CompiledPlan, parsed_query *sqlparser.ParsedQuery, listVars []interface{}) (result *QueryResult) {
	sql := sq.generateFinalSql(parsed_query, plan.BindVars, listVars, nil)
	q, ok := sq.consolidator.Create(string(sql))
	if ok {
		var conn PoolConnection
		if plan.ConnectionId != 0 {
			conn = sq.reservedPool.Get(plan.ConnectionId)
		} else {
			conn = sq.connPool.Get()
		}
		defer conn.Recycle()
		q.Result, q.Err = sq.executeSql(conn, sql, false)
		q.Broadcast()
	} else {
		q.Wait()
	}
	if q.Err != nil {
		panic(q.Err)
	}
	return q.Result
}

func (sq *SqlQuery) directFetch(conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []interface{}, buildStreamComment []byte) (result *QueryResult) {
	sql := sq.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := sq.executeSql(conn, sql, false)
	if err != nil {
		panic(err)
	}
	return result
}

// fullFetch also fetches field info
func (sq *SqlQuery) fullFetch(conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []interface{}, buildStreamComment []byte) (result *QueryResult) {
	sql := sq.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := sq.executeSql(conn, sql, true)
	if err != nil {
		panic(err)
	}
	return result
}

func (sq *SqlQuery) fullStreamFetch(conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []interface{}, buildStreamComment []byte, callback func(*QueryResult) error) error {
	sql := sq.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	return sq.executeStreamSql(conn, sql, callback)
}

func (sq *SqlQuery) generateFinalSql(parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []interface{}, buildStreamComment []byte) []byte {
	bindVars[MAX_RESULT_NAME] = atomic.LoadInt32(&sq.maxResultSize) + 1
	sql, err := parsed_query.GenerateQuery(bindVars, listVars)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	// undo hack done by stripTrailing
	sql = restoreTrailing(sql, bindVars)
	return sql
}

func (sq *SqlQuery) executeSql(conn PoolConnection, sql []byte, wantfields bool) (*QueryResult, error) {
	connid := conn.Id()
	sq.activePool.Put(connid)
	defer sq.activePool.Remove(connid)
	result, err := conn.ExecuteFetch(sql, int(atomic.LoadInt32(&sq.maxResultSize)), wantfields)
	if err != nil {
		return nil, NewTabletErrorSql(FAIL, err)
	}
	return result, nil
}

func (sq *SqlQuery) executeStreamSql(conn PoolConnection, sql []byte, callback func(*QueryResult) error) error {
	connid := conn.Id()
	sq.activePool.Put(connid)
	defer sq.activePool.Remove(connid)
	err := conn.ExecuteStreamFetch(sql, callback)
	if err != nil {
		return NewTabletErrorSql(FAIL, err)
	}
	return nil
}

func (sq *SqlQuery) statsJSON() string {
	sq.mu.RLock()
	defer sq.mu.RUnlock()

	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	fmt.Fprintf(buf, "\n \"IsOpen\": %v,", atomic.LoadInt32(&sq.state))
	fmt.Fprintf(buf, "\n \"CachePool\": %v,", sq.cachePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"QueryCache\": %v,", sq.schemaInfo.queries.StatsJSON())
	fmt.Fprintf(buf, "\n \"ConnPool\": %v,", sq.connPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"TxPool\": %v,", sq.txPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActiveTxPool\": %v,", sq.activeTxPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActivePool\": %v,", sq.activePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"MaxResultSize\": %v,", atomic.LoadInt32(&sq.maxResultSize))
	fmt.Fprintf(buf, "\n \"ReservedPool\": %v", sq.reservedPool.StatsJSON())
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

func Rand() int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63()
}
