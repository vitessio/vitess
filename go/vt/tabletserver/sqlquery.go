/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package tabletserver

import (
	"bytes"
	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/vt/sqlparser"
	"expvar"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

func NewSqlQuery(cachePoolCap, poolSize, transactionCap int, transactionTimeout float64, maxResultSize, queryCacheSize int, queryTimeout, idleTimeout float64) *SqlQuery {
	self := &SqlQuery{}
	self.cachePool = NewCachePool(cachePoolCap, time.Duration(idleTimeout*1e9))
	self.schemaInfo = NewSchemaInfo(queryCacheSize)
	self.connPool = NewConnectionPool(poolSize, time.Duration(idleTimeout*1e9))
	self.reservedPool = NewReservedPool()
	self.txPool = NewConnectionPool(transactionCap, time.Duration(idleTimeout*1e9)) // connections in pool has to be > transactionCap
	self.activeTxPool = NewActiveTxPool(time.Duration(transactionTimeout * 1e9))
	self.activePool = NewActivePool(time.Duration(queryTimeout*1e9), time.Duration(idleTimeout*1e9))
	self.consolidator = NewConsolidator()
	self.maxResultSize = int32(maxResultSize)
	expvar.Publish("Voltron", stats.StrFunc(func() string { return self.statsJSON() }))
	queryStats = stats.NewTimings("Queries")
	stats.NewRates("QPS", queryStats, 15, 60e9)
	waitStats = stats.NewTimings("Waits")
	killStats = stats.NewCounters("Kills")
	errorStats = stats.NewCounters("Errors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	return self
}

type CompiledPlan struct {
	*sqlparser.ExecPlan
	TableInfo     *TableInfo
	BindVars      map[string]interface{}
	TransactionId int64
	ConnectionId  int64
}

func (self *SqlQuery) allowQueries(dbconfig map[string]interface{}) {
	connFactory := GenericConnectionCreator(dbconfig)
	cacheFactory := CacheCreator(dbconfig)
	self.mu.Lock()
	defer self.mu.Unlock()
	atomic.StoreInt32(&self.state, INIT_FAILED)

	start := time.Now().UnixNano()
	self.cachePool.Open(cacheFactory)
	self.schemaInfo.Open(connFactory, self.cachePool)
	relog.Info("Time taken to load the schema: %v ms", (time.Now().UnixNano()-start)/1e6)
	self.connPool.Open(connFactory)
	self.reservedPool.Open(connFactory)
	self.txPool.Open(connFactory)
	self.activeTxPool.Open()
	self.activePool.Open(connFactory)
	self.sessionId = Rand()
	relog.Info("Session id: %d", self.sessionId)
	atomic.StoreInt32(&self.state, OPEN)
}

func (self *SqlQuery) disallowQueries() {
	// set this before obtaining lock so new incoming requests
	// can serve "unavailable" immediately
	atomic.StoreInt32(&self.state, SHUTTING_DOWN)
	relog.Info("Stopping query service: %d", self.sessionId)
	self.activeTxPool.WaitForEmpty()

	self.mu.Lock()
	defer self.mu.Unlock()
	atomic.StoreInt32(&self.state, CLOSED)
	self.activePool.Close()
	self.schemaInfo.Close()
	self.activeTxPool.Close()
	self.txPool.Close()
	self.reservedPool.Close()
	self.connPool.Close()
	self.sessionId = 0
}

func (self *SqlQuery) checkState(sessionId int64, allowShutdown bool) {
	switch atomic.LoadInt32(&self.state) {
	case INIT_FAILED:
		panic(NewTabletError(FATAL, "init failed"))
	case CLOSED:
		panic(NewTabletError(RETRY, "unavailable"))
	case SHUTTING_DOWN:
		if !allowShutdown {
			panic(NewTabletError(RETRY, "unavailable"))
		}
	}
	if sessionId != self.sessionId {
		panic(NewTabletError(RETRY, "Invalid session Id %v", sessionId))
	}
}

func (self *SqlQuery) Begin(session *Session, transactionId *int64) (err error) {
	defer handleError(&err)
	self.checkState(session.SessionId, false)
	self.mu.RLock()
	defer self.mu.RUnlock()
	var conn PoolConnection
	if session.ConnectionId != 0 {
		conn = self.reservedPool.Get(session.ConnectionId)
	} else if conn = self.txPool.TryGet(); conn == nil {
		panic(NewTabletError(FAIL, "Transaction pool connection limit exceeded"))
	}
	if *transactionId, err = self.activeTxPool.SafeBegin(conn); err != nil {
		conn.Recycle()
		return err
	}
	return nil
}

func (self *SqlQuery) Commit(session *Session, noOutput *string) (err error) {
	defer handleError(&err)
	self.checkState(session.SessionId, true)
	self.mu.RLock()
	defer self.mu.RUnlock()
	*noOutput = ""
	self.activeTxPool.Commit(session.TransactionId, self.schemaInfo)
	return nil
}

func (self *SqlQuery) Rollback(session *Session, noOutput *string) (err error) {
	defer handleError(&err)
	self.checkState(session.SessionId, true)
	self.mu.RLock()
	defer self.mu.RUnlock()
	*noOutput = ""
	self.activeTxPool.Rollback(session.TransactionId)
	return nil
}

func (self *SqlQuery) CreateReserved(session *Session, connectionId *int64) (err error) {
	defer handleError(&err)
	self.checkState(session.SessionId, false)
	self.mu.RLock()
	defer self.mu.RUnlock()
	*connectionId = self.reservedPool.CreateConnection()
	return nil
}

func (self *SqlQuery) CloseReserved(session *Session, noOutput *string) (err error) {
	defer handleError(&err)
	self.checkState(session.SessionId, false)
	self.mu.RLock()
	defer self.mu.RUnlock()
	*noOutput = ""
	self.reservedPool.CloseConnection(session.ConnectionId)
	return nil
}

func (self *SqlQuery) Execute(query *Query, reply *QueryResult) (err error) {
	defer func() {
		if x := recover(); x != nil {
			terr := x.(*TabletError)
			err = terr
			terr.RecordStats()
			if terr.ErrorType == RETRY || terr.SqlError == DUPLICATE_KEY { // suppress these errors in logs
				return
			}
			relog.Error("%s: %v", terr.Message, query)
		}
	}()
	// allow shutdown state if we're in a transaction
	allowShutdown := (query.TransactionId != 0)
	self.checkState(query.SessionId, allowShutdown)

	self.mu.RLock()
	defer self.mu.RUnlock()

	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)
	basePlan, tableInfo := self.schemaInfo.GetPlan(query.Sql, len(query.BindVariables) != 0)
	defer self.schemaInfo.Put(tableInfo)
	if basePlan.PlanId == sqlparser.PLAN_DDL {
		defer queryStats.Record("DDL", time.Now())
		*reply = *self.execDDL(query.Sql)
		return nil
	}

	plan := &CompiledPlan{basePlan, tableInfo, query.BindVariables, query.TransactionId, query.ConnectionId}
	if query.TransactionId != 0 {
		// Need upfront connection for DMLs and transactions
		conn := self.activeTxPool.Get(query.TransactionId)
		defer conn.Recycle()
		var invalidator CacheInvalidator
		if tableInfo != nil && tableInfo.CacheType != 0 {
			invalidator = conn.DirtyKeys(plan.TableName)
		}
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_DML:
			if tableInfo != nil && tableInfo.CacheType != 0 {
				panic(NewTabletError(FAIL, "DML too complex for cached table"))
			}
			defer queryStats.Record("PASS_DML", time.Now())
			*reply = *self.directFetch(conn, plan.FullQuery, plan.BindVars, nil, nil)
		case sqlparser.PLAN_INSERT_PK:
			defer queryStats.Record("PLAN_INSERT_PK", time.Now())
			*reply = *self.execInsertPK(conn, plan, invalidator)
		case sqlparser.PLAN_INSERT_SUBQUERY:
			defer queryStats.Record("PLAN_INSERT_SUBQUERY", time.Now())
			*reply = *self.execInsertSubquery(conn, plan, invalidator)
		case sqlparser.PLAN_DML_PK:
			defer queryStats.Record("DML_PK", time.Now())
			*reply = *self.execDMLPK(conn, plan, invalidator)
		case sqlparser.PLAN_DML_SUBQUERY:
			defer queryStats.Record("DML_SUBQUERY", time.Now())
			*reply = *self.execDMLSubquery(conn, plan, invalidator)
		default: // select or set in a transaction, just count as select
			defer queryStats.Record("PASS_SELECT", time.Now())
			*reply = *self.directFetch(conn, plan.FullQuery, plan.BindVars, nil, nil)
		}
	} else {
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_SELECT:
			if plan.Reason == sqlparser.REASON_FOR_UPDATE {
				panic(NewTabletError(FAIL, "Disallowed outside transaction: %s", query.Sql))
			}
			defer queryStats.Record("PASS_SELECT", time.Now())
			*reply = *self.qFetch(plan, plan.FullQuery, nil)
		case sqlparser.PLAN_SELECT_PK:
			defer queryStats.Record("SELECT_PK", time.Now())
			*reply = *self.execPK(plan)
		case sqlparser.PLAN_SELECT_SUBQUERY:
			defer queryStats.Record("SELECT_SUBQUERY", time.Now())
			*reply = *self.execSubquery(plan)
		case sqlparser.PLAN_SELECT_CACHE_RESULT:
			defer queryStats.Record("SELECT_CACHE_RESULT", time.Now())
			*reply = *self.execCacheResult(plan)
		case sqlparser.PLAN_SET:
			defer queryStats.Record("SET", time.Now())
			*reply = *self.execSet(plan)
		default:
			panic(NewTabletError(FAIL, "DMLs not allowed outside of transactions"))
		}
	}
	if plan.PlanId.IsSelect() {
		resultStats.Add(int64(reply.RowsAffected))
	}
	return nil
}

type QueryList []Query

func (self *SqlQuery) ExecuteBatch(queryList *QueryList, reply *QueryResult) (err error) {
	defer handleError(&err)
	ql := *queryList
	if len(ql) == 0 {
		panic(NewTabletError(FAIL, "Empty query list"))
	}
	self.checkState(ql[0].SessionId, false)
	self.mu.RLock()
	defer self.mu.RUnlock()
	begin_called := false
	var noOutput string
	session := Session{
		TransactionId: ql[0].TransactionId,
		ConnectionId:  ql[0].ConnectionId,
		SessionId:     ql[0].SessionId,
	}
	for _, query := range ql {
		trimmed := strings.ToLower(strings.Trim(query.Sql, " \t\n"))
		switch trimmed {
		case "begin":
			if session.TransactionId != 0 {
				panic(NewTabletError(FAIL, "Nested transactions disallowed"))
			}
			if err = self.Begin(&session, &session.TransactionId); err != nil {
				return err
			}
			begin_called = true
		case "commit":
			if !begin_called {
				panic(NewTabletError(FAIL, "Cannot commit without begin"))
			}
			if err = self.Commit(&session, &noOutput); err != nil {
				return err
			}
			session.TransactionId = 0
			begin_called = false
		default:
			query.TransactionId = session.TransactionId
			query.ConnectionId = session.ConnectionId
			query.SessionId = session.SessionId
			var localReply QueryResult
			if err = self.Execute(&query, &localReply); err != nil {
				if begin_called {
					self.Rollback(&session, &noOutput)
				}
				return err
			}
			reply.RowsAffected += localReply.RowsAffected
		}
	}
	if begin_called {
		self.Rollback(&session, &noOutput)
		panic(NewTabletError(FAIL, "begin called with no commit"))
	}
	return nil
}

type CacheInvalidate struct {
	Table string
	Keys  []interface{}
}

func (self *SqlQuery) Invalidate(cacheInvalidate *CacheInvalidate, noOutput *string) (err error) {
	defer handleError(&err)
	self.checkState(self.sessionId, false)
	*noOutput = ""
	self.mu.RLock()
	defer self.mu.RUnlock()
	tableInfo := self.schemaInfo.GetTable(cacheInvalidate.Table)
	if tableInfo == nil {
		return NewTabletError(FAIL, "Table %s not found", cacheInvalidate.Table)
	}
	defer self.schemaInfo.Put(tableInfo)
	if tableInfo != nil && tableInfo.Cache != nil {
		for i, val := range cacheInvalidate.Keys {
			switch v := val.(type) {
			case string:
				cacheInvalidate.Keys[i] = base64Decode([]byte(v))
			case []byte:
				cacheInvalidate.Keys[i] = base64Decode(v)
			}
		}
		key := buildKey(tableInfo, cacheInvalidate.Keys)
		tableInfo.Cache.Delete(key)
	}
	return nil
}

func (self *SqlQuery) Ping(query *string, reply *string) error {
	*reply = "pong: " + *query
	return nil
}

//-----------------------------------------------
// DDL

func (self *SqlQuery) execDDL(ddl string) *QueryResult {
	self.mu.RLock()
	defer self.mu.RUnlock()
	ddlPlan := sqlparser.DDLParse(ddl)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}

	// Stolen from Begin
	conn := self.txPool.TryGet()
	if conn == nil {
		panic(NewTabletError(FAIL, "Transaction pool connection limit exceeded"))
	}
	txid, err := self.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	// Stolen from Commit
	defer self.activeTxPool.Commit(txid, self.schemaInfo)

	// Stolen from Execute
	conn = self.activeTxPool.Get(txid)
	defer conn.Recycle()
	result, err := self.executeSql(conn, []byte(ddl))
	if err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}

	if ddlPlan.Action != sqlparser.CREATE { // ALTER, RENAME, DROP
		self.schemaInfo.DropTable(ddlPlan.TableName)
	}
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		self.schemaInfo.CreateTable(ddlPlan.NewName)
	}
	return result
}

//-----------------------------------------------
// Execution

func (self *SqlQuery) execPK(plan *CompiledPlan) (result *QueryResult) {
	pkRows := buildValueList(plan.PKValues, plan.BindVars)
	return self.fetchPKRows(plan, pkRows)
}

func (self *SqlQuery) execSubquery(plan *CompiledPlan) (result *QueryResult) {
	innerResult := self.qFetch(plan, plan.Subquery, nil)
	return self.fetchPKRows(plan, innerResult.Rows)
}

func (self *SqlQuery) fetchPKRows(plan *CompiledPlan, pkRows [][]interface{}) (result *QueryResult) {
	result = &QueryResult{}
	tableInfo := plan.TableInfo
	result.Fields = applyFieldFilter(plan.ColumnNumbers, tableInfo.Fields)
	normalizePKRows(plan.TableInfo, pkRows)
	rows := make([][]interface{}, 0, len(pkRows))
	var hits, misses int64
	readTime := time.Now()
	for _, pk := range pkRows {
		key := buildKey(tableInfo, pk)
		if cacheRow := tableInfo.Cache.Get(key); cacheRow != nil {
			rows = append(rows, applyFilter(plan.ColumnNumbers, cacheRow))
			hits++
		} else {
			resultFromdb := self.qFetch(plan, plan.OuterQuery, pk)
			for _, row := range resultFromdb.Rows {
				pkRow := applyFilter(tableInfo.PKColumns, row)
				key := buildKey(tableInfo, pkRow)
				tableInfo.Cache.Set(key, row, readTime)
				rows = append(rows, applyFilter(plan.ColumnNumbers, row))
			}
			misses++
		}
	}
	atomic.AddInt64(&tableInfo.hits, hits)
	atomic.AddInt64(&tableInfo.misses, misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	return result
}

func (self *SqlQuery) execCacheResult(plan *CompiledPlan) (result *QueryResult) {
	readTime := time.Now()
	result = self.qFetch(plan, plan.OuterQuery, nil)
	tableInfo := plan.TableInfo
	result.Fields = applyFieldFilter(plan.ColumnNumbers, result.Fields)
	for i, row := range result.Rows {
		pkRow := applyFilter(tableInfo.PKColumns, row)
		key := buildKey(tableInfo, pkRow)
		tableInfo.Cache.Set(key, row, readTime)
		result.Rows[i] = applyFilter(plan.ColumnNumbers, row)
	}
	return result
}

func (self *SqlQuery) execInsertPK(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	pkRows := buildValueList(plan.PKValues, plan.BindVars)
	normalizePKRows(plan.TableInfo, pkRows)
	return self.execInsertPKRows(conn, plan, pkRows, invalidator)
}

func (self *SqlQuery) execInsertSubquery(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	innerResult := self.directFetch(conn, plan.Subquery, plan.BindVars, nil, nil)
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
	return self.execInsertPKRows(conn, plan, pkRows, invalidator)
}

func (self *SqlQuery) execInsertPKRows(conn PoolConnection, plan *CompiledPlan, pkRows [][]interface{}, invalidator CacheInvalidator) (result *QueryResult) {
	secondaryList := buildSecondaryList(pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = self.directFetch(conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	// TODO: We need to do this only if insert has on duplicate key clause
	if invalidator != nil {
		for _, pk := range pkRows {
			key := buildKey(plan.TableInfo, pk)
			invalidator.Delete(key)
		}
	}
	return result
}

func (self *SqlQuery) execDMLPK(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	pkRows := buildValueList(plan.PKValues, plan.BindVars)
	normalizePKRows(plan.TableInfo, pkRows)
	secondaryList := buildSecondaryList(pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = self.directFetch(conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	if invalidator != nil {
		for _, pk := range pkRows {
			key := buildKey(plan.TableInfo, pk)
			invalidator.Delete(key)
		}
	}
	return result
}

func (self *SqlQuery) execDMLSubquery(conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *QueryResult) {
	innerResult := self.directFetch(conn, plan.Subquery, plan.BindVars, nil, nil)
	return self.execDMLPKRows(conn, plan, innerResult.Rows, invalidator)
}

func (self *SqlQuery) execDMLPKRows(conn PoolConnection, plan *CompiledPlan, pkRows [][]interface{}, invalidator CacheInvalidator) (result *QueryResult) {
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
		rowsAffected += self.directFetch(conn, plan.OuterQuery, plan.BindVars, pkRow, bsc).RowsAffected
		if invalidator != nil {
			key := buildKey(plan.TableInfo, pkRow)
			invalidator.Delete(key)
		}
	}
	return &QueryResult{RowsAffected: rowsAffected}
}

func (self *SqlQuery) execSet(plan *CompiledPlan) (result *QueryResult) {
	switch plan.SetKey {
	case "vt_pool_size":
		self.connPool.SetCapacity(int(plan.SetValue.(float64)))
		return &QueryResult{}
	case "vt_transaction_cap":
		self.txPool.SetCapacity(int(plan.SetValue.(float64)))
		return &QueryResult{}
	case "vt_transaction_timeout":
		self.activeTxPool.SetTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &QueryResult{}
	case "vt_query_cache_size":
		self.schemaInfo.SetQueryCacheSize(int(plan.SetValue.(float64)))
		return &QueryResult{}
	case "vt_max_result_size":
		val := int32(plan.SetValue.(float64))
		if val < 1 {
			panic(NewTabletError(FAIL, "max result size out of range %v", val))
		}
		atomic.StoreInt32(&self.maxResultSize, val)
		return &QueryResult{}
	case "vt_query_timeout":
		self.activePool.SetTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &QueryResult{}
	case "vt_idle_timeout":
		self.connPool.SetIdleTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		self.txPool.SetIdleTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		self.activePool.SetIdleTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &QueryResult{}
	}
	return self.qFetch(plan, plan.FullQuery, nil)
}

func (self *SqlQuery) qFetch(plan *CompiledPlan, parsed_query *sqlparser.ParsedQuery, listVars []interface{}) (result *QueryResult) {
	sql := self.generateFinalSql(parsed_query, plan.BindVars, listVars, nil)
	q, ok := self.consolidator.Create(string(sql))
	if ok {
		var conn PoolConnection
		if plan.ConnectionId != 0 {
			conn = self.reservedPool.Get(plan.ConnectionId)
		} else {
			conn = self.connPool.Get()
		}
		defer conn.Recycle()
		q.Result, q.Err = self.executeSql(conn, sql)
		q.Broadcast()
	} else {
		q.Wait()
	}
	if q.Err != nil {
		panic(q.Err)
	}
	return q.Result
}

func (self *SqlQuery) directFetch(conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []interface{}, buildStreamComment []byte) (result *QueryResult) {
	sql := self.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := self.executeSql(conn, sql)
	if err != nil {
		panic(err)
	}
	return result
}

func (self *SqlQuery) generateFinalSql(parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []interface{}, buildStreamComment []byte) []byte {
	bindVars[MAX_RESULT_NAME] = atomic.LoadInt32(&self.maxResultSize) + 1
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

func (self *SqlQuery) executeSql(conn PoolConnection, sql []byte) (*QueryResult, error) {
	connid := conn.Id()
	self.activePool.Put(connid)
	defer self.activePool.Remove(connid)
	result, err := conn.ExecuteFetch(sql, int(atomic.LoadInt32(&self.maxResultSize)))
	if err != nil {
		return nil, NewTabletErrorSql(FAIL, err)
	}
	return result, nil
}

func (self *SqlQuery) statsJSON() string {
	self.mu.RLock()
	defer self.mu.RUnlock()

	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	fmt.Fprintf(buf, "\n \"IsOpen\": %v,", atomic.LoadInt32(&self.state))
	fmt.Fprintf(buf, "\n \"CachePool\": %v,", self.cachePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"QueryCache\": %v,", self.schemaInfo.queries.StatsJSON())
	fmt.Fprintf(buf, "\n \"ConnPool\": %v,", self.connPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"TxPool\": %v,", self.txPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActiveTxPool\": %v,", self.activeTxPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActivePool\": %v,", self.activePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"MaxResultSize\": %v,", atomic.LoadInt32(&self.maxResultSize))
	fmt.Fprintf(buf, "\n \"ReservedPool\": %v", self.reservedPool.StatsJSON())
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

func applyFieldFilter(columnNumbers []int, input []mysql.Field) (output []mysql.Field) {
	output = make([]mysql.Field, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		}
		output[colIndex] = input[colPointer]
	}
	return output
}

func applyFilter(columnNumbers []int, input []interface{}) (output []interface{}) {
	output = make([]interface{}, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		}
	}
	return output
}

func Rand() int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63()
}
