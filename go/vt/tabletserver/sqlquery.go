// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
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

	// Vars for handling invalidations
	slaveDirtyRows map[string]DirtyKeys
	dbName         string
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
	self := &SqlQuery{}
	self.cachePool = NewCachePool(config.CachePoolCap, time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	self.schemaInfo = NewSchemaInfo(config.QueryCacheSize, time.Duration(config.SchemaReloadTime*1e9))
	self.connPool = NewConnectionPool(config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	self.reservedPool = NewReservedPool()
	self.txPool = NewConnectionPool(config.TransactionCap, time.Duration(config.IdleTimeout*1e9)) // connections in pool has to be > transactionCap
	self.activeTxPool = NewActiveTxPool(time.Duration(config.TransactionTimeout * 1e9))
	self.activePool = NewActivePool(time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	self.consolidator = NewConsolidator()
	self.maxResultSize = int32(config.MaxResultSize)
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
	Query string
	*ExecPlan
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
	self.dbName = dbconfig["dbname"].(string)
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
	self.dbName = ""
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

func (self *SqlQuery) GetSessionId(dbName *string, sessionId *int64) error {
	if *dbName != self.dbName {
		return NewTabletError(FATAL, "db name mismatch, expecting %v, received %v", self.dbName, *dbName)
	}
	*sessionId = self.sessionId
	return nil
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
	dirtyTables, err := self.activeTxPool.SafeCommit(session.TransactionId)
	self.invalidateRows(dirtyTables)
	return err
}

func (self *SqlQuery) invalidateRows(dirtyTables map[string]DirtyKeys) {
	for tableName, invalidList := range dirtyTables {
		tableInfo := self.schemaInfo.GetTable(tableName)
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

func (self *SqlQuery) Execute(query *Query, reply *QueryResult) (err error) {
	defer handleExecError(query, &err)

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
	basePlan := self.schemaInfo.GetPlan(query.Sql, len(query.BindVariables) != 0)
	if basePlan.PlanId == sqlparser.PLAN_DDL {
		defer queryStats.Record("DDL", time.Now())
		*reply = *self.execDDL(query.Sql)
		return nil
	}

	plan := &CompiledPlan{query.Sql, basePlan, query.BindVariables, query.TransactionId, query.ConnectionId}
	if query.TransactionId != 0 {
		// Need upfront connection for DMLs and transactions
		conn := self.activeTxPool.Get(query.TransactionId)
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
			*reply = *self.fullFetch(conn, plan.FullQuery, plan.BindVars, nil, nil)
		}
	} else {
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_SELECT:
			if plan.Reason == sqlparser.REASON_FOR_UPDATE {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			defer queryStats.Record("PASS_SELECT", time.Now())
			*reply = *self.execSelect(plan)
		case sqlparser.PLAN_SELECT_PK:
			defer queryStats.Record("SELECT_PK", time.Now())
			*reply = *self.execPK(plan)
		case sqlparser.PLAN_SELECT_SUBQUERY:
			defer queryStats.Record("SELECT_SUBQUERY", time.Now())
			*reply = *self.execSubquery(plan)
		case sqlparser.PLAN_SELECT_CACHE_RESULT:
			defer queryStats.Record("SELECT_CACHE_RESULT", time.Now())
			// It may not be worth caching the results. So, just pass through.
			*reply = *self.execSelect(plan)
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
type QueryResultList []QueryResult

func (self *SqlQuery) ExecuteBatch(queryList *QueryList, reply *QueryResultList) (err error) {
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
	*reply = make([]QueryResult, 0, len(ql))
	for _, query := range ql {
		trimmed := strings.ToLower(strings.Trim(query.Sql, " \t\r\n"))
		switch trimmed {
		case "begin":
			if session.TransactionId != 0 {
				panic(NewTabletError(FAIL, "Nested transactions disallowed"))
			}
			if err = self.Begin(&session, &session.TransactionId); err != nil {
				return err
			}
			begin_called = true
			*reply = append(*reply, QueryResult{})
		case "commit":
			if !begin_called {
				panic(NewTabletError(FAIL, "Cannot commit without begin"))
			}
			if err = self.Commit(&session, &noOutput); err != nil {
				return err
			}
			session.TransactionId = 0
			begin_called = false
			*reply = append(*reply, QueryResult{})
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
			*reply = append(*reply, localReply)
		}
	}
	if begin_called {
		self.Rollback(&session, &noOutput)
		panic(NewTabletError(FAIL, "begin called with no commit"))
	}
	return nil
}

type SlaveTxCommand struct {
	Command  string
}

func (self *SqlQuery) SlaveTx(cmd *SlaveTxCommand, noOutput *string) (err error) {
	defer handleError(&err)
	allowShutdown := (cmd.Command == "commit")
	self.checkState(self.sessionId, allowShutdown)
	*noOutput = ""
	self.mu.RLock()
	defer self.mu.RUnlock()

	if self.cachePool.IsClosed() {
		return nil
	}
	switch cmd.Command {
	case "begin":
		self.slaveDirtyRows = make(map[string]DirtyKeys)
	case "commit":
		self.invalidateRows(self.slaveDirtyRows)
		self.slaveDirtyRows = nil
	case "rollback":
		self.slaveDirtyRows = nil
	default:
		panic(NewTabletError(FAIL, "Unknown tx command: %s", cmd.Command))
	}
	return nil
}

type CacheInvalidate struct {
	Database string
	Table    string
	Keys     []interface{}
}

func (self *SqlQuery) Invalidate(cacheInvalidate *CacheInvalidate, noOutput *string) (err error) {
	defer handleError(&err)
	self.checkState(self.sessionId, false)
	*noOutput = ""
	self.mu.RLock()
	defer self.mu.RUnlock()

	if self.cachePool.IsClosed() || cacheInvalidate.Database != self.dbName {
		return nil
	}
	tableInfo := self.schemaInfo.GetTable(cacheInvalidate.Table)
	if tableInfo == nil {
		return NewTabletError(FAIL, "Table %s not found", cacheInvalidate.Table)
	}
	if tableInfo.CacheType == 0 {
		return nil
	}
	if self.slaveDirtyRows == nil {
		return NewTabletError(FAIL, "Not in transaction")
	}
	invalidations := int64(0)
	for _, val := range cacheInvalidate.Keys {
		newKey := validateKey(tableInfo, val.(string))
		if newKey != "" {
			tableInfo.Cache.Delete(newKey)
		}
		invalidations++
	}
	atomic.AddInt64(&tableInfo.invalidations, invalidations)
	return nil
}

type DDLInvalidate struct {
	Database string
	DDL      string
}

func (self *SqlQuery) InvalidateForDDL(ddl *DDLInvalidate, noOutput *string) (err error) {
	defer handleError(&err)
	self.checkState(self.sessionId, true) // Accept DDLs in shut down mode
	*noOutput = ""
	self.mu.RLock()
	defer self.mu.RUnlock()

	if ddl.Database != self.dbName {
		return nil
	}

	ddlDecoded := base64Decode([]byte(ddl.DDL))
	ddlPlan := sqlparser.DDLParse(ddlDecoded)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}
	self.schemaInfo.DropTable(ddlPlan.TableName)
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		self.schemaInfo.CreateTable(ddlPlan.NewName)
	}
	// DDL == auto-commit
	self.invalidateRows(self.slaveDirtyRows)
	self.slaveDirtyRows = nil
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
	defer self.activeTxPool.SafeCommit(txid)

	// Stolen from Execute
	conn = self.activeTxPool.Get(txid)
	defer conn.Recycle()
	result, err := self.executeSql(conn, []byte(ddl), false)
	if err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}

	self.schemaInfo.DropTable(ddlPlan.TableName)
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
	return self.fetchPKRows(plan, copyRows(innerResult.Rows))
}

func (self *SqlQuery) fetchPKRows(plan *CompiledPlan, pkRows [][]interface{}) (result *QueryResult) {
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
			/*if dbrow := self.validateRow(plan, cacheRow, pk); dbrow != nil {
				rows = append(rows, applyFilter(plan.ColumnNumbers, dbrow))
			}*/
			rows = append(rows, applyFilter(plan.ColumnNumbers, cacheRow))
			hits++
		} else {
			resultFromdb := self.qFetch(plan, plan.OuterQuery, pk)
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

func (self *SqlQuery) validateRow(plan *CompiledPlan, cacheRow []interface{}, pk []interface{}) (dbrow []interface{}) {
	resultFromdb := self.qFetch(plan, plan.OuterQuery, pk)
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

func (self *SqlQuery) execSelect(plan *CompiledPlan) (result *QueryResult) {
	if plan.Fields != nil {
		result = self.qFetch(plan, plan.FullQuery, nil)
		result.Fields = plan.Fields
		return
	}
	var conn PoolConnection
	if plan.ConnectionId != 0 {
		conn = self.reservedPool.Get(plan.ConnectionId)
	} else {
		conn = self.connPool.Get()
	}
	defer conn.Recycle()
	result = self.fullFetch(conn, plan.FullQuery, plan.BindVars, nil, nil)
	self.schemaInfo.SetFields(plan.Query, plan.ExecPlan, result.Fields)
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
	fillPKDefaults(plan.TableInfo, pkRows)
	secondaryList := buildSecondaryList(pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = self.directFetch(conn, plan.OuterQuery, plan.BindVars, nil, bsc)
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
	case "vt_schema_reload_time":
		self.schemaInfo.SetReloadTime(time.Duration(plan.SetValue.(float64) * 1e9))
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
		q.Result, q.Err = self.executeSql(conn, sql, false)
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
	result, err := self.executeSql(conn, sql, false)
	if err != nil {
		panic(err)
	}
	return result
}

// fullFetch also fetches field info
func (self *SqlQuery) fullFetch(conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []interface{}, buildStreamComment []byte) (result *QueryResult) {
	sql := self.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := self.executeSql(conn, sql, true)
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

func (self *SqlQuery) executeSql(conn PoolConnection, sql []byte, wantfields bool) (*QueryResult, error) {
	connid := conn.Id()
	self.activePool.Put(connid)
	defer self.activePool.Remove(connid)
	result, err := conn.ExecuteFetch(sql, int(atomic.LoadInt32(&self.maxResultSize)), wantfields)
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

func Rand() int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63()
}
