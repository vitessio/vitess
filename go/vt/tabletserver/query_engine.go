// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"
	"time"

	"code.google.com/p/vitess/go/hack"
	mproto "code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/sync2"
	"code.google.com/p/vitess/go/vt/dbconfigs"
	"code.google.com/p/vitess/go/vt/schema"
	"code.google.com/p/vitess/go/vt/sqlparser"
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

const (
	MAX_RESULT_NAME                = "_vtMaxResultSize"
	ROWCACHE_INVALIDATION_POSITION = "ROWCACHE_INVALIDATION_POSITION"
)

//-----------------------------------------------
type QueryEngine struct {
	// Obtain read lock on mu to execute queries
	// Obtain write lock to start/stop query service
	mu sync.RWMutex

	cachePool *CachePool
	//this is used to access administrative keys in row-cache using the same.
	adminCache     *GenericCache
	schemaInfo     *SchemaInfo
	connPool       *ConnectionPool
	streamConnPool *ConnectionPool
	reservedPool   *ReservedPool
	txPool         *ConnectionPool
	activeTxPool   *ActiveTxPool
	activePool     *ActivePool
	consolidator   *Consolidator

	maxResultSize    sync2.AtomicInt32
	streamBufferSize sync2.AtomicInt32
}

type CompiledPlan struct {
	Query string
	*ExecPlan
	BindVars      map[string]interface{}
	TransactionId int64
	ConnectionId  int64
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

func NewQueryEngine(config Config) *QueryEngine {
	qe := &QueryEngine{}
	qe.cachePool = NewCachePool(config.CachePoolCap, time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	qe.schemaInfo = NewSchemaInfo(config.QueryCacheSize, time.Duration(config.SchemaReloadTime*1e9), time.Duration(config.IdleTimeout*1e9))
	qe.adminCache = NewGenericCache(qe.cachePool)
	qe.connPool = NewConnectionPool(config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.streamConnPool = NewConnectionPool(config.StreamPoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.reservedPool = NewReservedPool()
	qe.txPool = NewConnectionPool(config.TransactionCap, time.Duration(config.IdleTimeout*1e9)) // connections in pool has to be > transactionCap
	qe.activeTxPool = NewActiveTxPool(time.Duration(config.TransactionTimeout * 1e9))
	qe.activePool = NewActivePool(time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	qe.consolidator = NewConsolidator()
	qe.maxResultSize = sync2.AtomicInt32(config.MaxResultSize)
	qe.streamBufferSize = sync2.AtomicInt32(config.StreamBufferSize)
	queryStats = stats.NewTimings("Queries")
	stats.NewRates("QPS", queryStats, 15, 60e9)
	waitStats = stats.NewTimings("Waits")
	killStats = stats.NewCounters("Kills")
	errorStats = stats.NewCounters("Errors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	return qe
}

func (qe *QueryEngine) Open(dbconfig dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules) {
	// Wait for Close, in case it's running
	qe.mu.Lock()
	defer qe.mu.Unlock()

	connFactory := GenericConnectionCreator(dbconfig.MysqlParams())
	cacheFactory := CacheCreator(dbconfig)

	start := time.Now().UnixNano()
	qe.cachePool.Open(cacheFactory)
	qe.schemaInfo.Open(connFactory, schemaOverrides, qe.cachePool, qrs)
	relog.Info("Time taken to load the schema: %v ms", (time.Now().UnixNano()-start)/1e6)
	qe.connPool.Open(connFactory)
	qe.streamConnPool.Open(connFactory)
	qe.reservedPool.Open(connFactory)
	qe.txPool.Open(connFactory)
	qe.activeTxPool.Open()
	qe.activePool.Open(connFactory)
}

func (qe *QueryEngine) Close(forRestart bool) {
	qe.activeTxPool.WaitForEmpty()
	// Ensure all read locks are released (no more queries being served)
	qe.mu.Lock()
	defer qe.mu.Unlock()

	qe.activePool.Close()
	qe.schemaInfo.Close()
	qe.activeTxPool.Close()
	qe.txPool.Close()
	qe.reservedPool.Close()
	qe.streamConnPool.Close()
	qe.connPool.Close()
}

func (qe *QueryEngine) Begin(logStats *sqlQueryStats, connectionId int64) (transactionId int64) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	var conn PoolConnection
	if connectionId != 0 {
		conn = qe.reservedPool.Get(connectionId)
	} else if conn = qe.txPool.TryGet(); conn == nil {
		panic(NewTabletError(TX_POOL_FULL, "Transaction pool connection limit exceeded"))
	}
	transactionId, err := qe.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	return transactionId
}

func (qe *QueryEngine) Commit(logStats *sqlQueryStats, transactionId int64) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	dirtyTables, err := qe.activeTxPool.SafeCommit(transactionId)
	qe.invalidateRows(logStats, dirtyTables)
	if err != nil {
		panic(err)
	}
}

func (qe *QueryEngine) invalidateRows(logStats *sqlQueryStats, dirtyTables map[string]DirtyKeys) {
	for tableName, invalidList := range dirtyTables {
		tableInfo := qe.schemaInfo.GetTable(tableName)
		if tableInfo == nil {
			continue
		}
		invalidations := int64(0)
		for key := range invalidList {
			tableInfo.Cache.Delete(key)
			invalidations++
		}
		logStats.CacheInvalidations += invalidations
		tableInfo.invalidations.Add(invalidations)
	}
}

func (qe *QueryEngine) Rollback(logStats *sqlQueryStats, transactionId int64) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	qe.activeTxPool.Rollback(transactionId)
}

func (qe *QueryEngine) CreateReserved() (connectionId int64) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	return qe.reservedPool.CreateConnection()
}

func (qe *QueryEngine) CloseReserved(connectionId int64) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	qe.reservedPool.CloseConnection(connectionId)
}

func (qe *QueryEngine) Execute(logStats *sqlQueryStats, query *proto.Query) (reply *mproto.QueryResult) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	logStats.BindVariables = query.BindVariables
	logStats.OriginalSql = query.Sql
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)
	basePlan := qe.schemaInfo.GetPlan(logStats, query.Sql)
	planName := basePlan.PlanId.String()
	logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		queryStats.Add(planName, duration)
		if reply == nil {
			basePlan.AddStats(1, duration, 0, 1)
		} else {
			basePlan.AddStats(1, duration, int64(len(reply.Rows)), 0)
		}
	}(time.Now())

	// Run it by the rules engine
	action, desc := basePlan.Rules.getAction(logStats.RemoteAddr(), logStats.Username(), query.BindVariables)
	if action == QR_FAIL_QUERY {
		panic(NewTabletError(FAIL, "Query disallowed due to rule: %s", desc))
	}

	if basePlan.PlanId == sqlparser.PLAN_DDL {
		return qe.execDDL(logStats, query.Sql)
	}

	plan := &CompiledPlan{query.Sql, basePlan, query.BindVariables, query.TransactionId, query.ConnectionId}
	if query.TransactionId != 0 {
		// Need upfront connection for DMLs and transactions
		conn := qe.activeTxPool.Get(query.TransactionId)
		defer conn.Recycle()
		conn.RecordQuery(plan.Query)
		var invalidator CacheInvalidator
		if plan.TableInfo != nil && plan.TableInfo.CacheType != schema.CACHE_NONE {
			invalidator = conn.DirtyKeys(plan.TableName)
		}
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_DML:
			if plan.TableInfo != nil && plan.TableInfo.CacheType != schema.CACHE_NONE {
				panic(NewTabletError(FAIL, "DML too complex for cached table"))
			}
			reply = qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		case sqlparser.PLAN_INSERT_PK:
			reply = qe.execInsertPK(logStats, conn, plan, invalidator)
		case sqlparser.PLAN_INSERT_SUBQUERY:
			reply = qe.execInsertSubquery(logStats, conn, plan, invalidator)
		case sqlparser.PLAN_DML_PK:
			reply = qe.execDMLPK(logStats, conn, plan, invalidator)
		case sqlparser.PLAN_DML_SUBQUERY:
			reply = qe.execDMLSubquery(logStats, conn, plan, invalidator)
		default: // select or set in a transaction, just count as select
			reply = qe.execDirect(logStats, plan, conn)
		}
	} else if plan.ConnectionId != 0 {
		conn := qe.reservedPool.Get(plan.ConnectionId)
		defer conn.Recycle()
		if plan.PlanId.IsSelect() {
			reply = qe.execDirect(logStats, plan, conn)
		} else if plan.PlanId == sqlparser.PLAN_SET {
			reply = qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		} else {
			panic(NewTabletError(FAIL, "DMLs not allowed outside of transactions"))
		}
	} else {
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_SELECT:
			if plan.Reason == sqlparser.REASON_FOR_UPDATE {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			reply = qe.execSelect(logStats, plan)
		case sqlparser.PLAN_SELECT_PK:
			reply = qe.execPK(logStats, plan)
		case sqlparser.PLAN_SELECT_SUBQUERY:
			reply = qe.execSubquery(logStats, plan)
		case sqlparser.PLAN_SET:
			waitingForConnectionStart := time.Now()
			conn := qe.connPool.Get()
			logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
			defer conn.Recycle()
			reply = qe.execSet(logStats, conn, plan)
		default:
			panic(NewTabletError(FAIL, "DMLs not allowed outside of transactions"))
		}
	}
	if plan.PlanId.IsSelect() {
		logStats.RowsAffected = int(reply.RowsAffected)
		resultStats.Add(int64(reply.RowsAffected))
		logStats.Rows = reply.Rows
	}

	return reply
}

// the first QueryResult will have Fields set (and Rows nil)
// the subsequent QueryResult will have Rows set (and Fields nil)
func (qe *QueryEngine) StreamExecute(logStats *sqlQueryStats, query *proto.Query, sendReply func(reply interface{}) error) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	logStats.BindVariables = query.BindVariables
	logStats.OriginalSql = query.Sql
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)

	fullQuery := qe.schemaInfo.GetStreamPlan(query.Sql)
	logStats.PlanType = "SELECT_STREAM"
	defer queryStats.Record("SELECT_STREAM", time.Now())

	// does the real work: first get a connection
	waitingForConnectionStart := time.Now()
	conn := qe.streamConnPool.Get()
	logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()

	// then let's stream!
	qe.fullStreamFetch(logStats, conn, fullQuery, query.BindVariables, nil, nil, sendReply)
}

func (qe *QueryEngine) getCurrentInvalidationPosition() (invalidationPosition []byte, err error) {
	return qe.adminCache.Get(ROWCACHE_INVALIDATION_POSITION)
}

func (qe *QueryEngine) purgeRowCache() {
	qe.adminCache.PurgeCache()
}

func (qe *QueryEngine) Invalidate(cacheInvalidate *proto.CacheInvalidate) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	if qe.cachePool.IsClosed() {
		return
	}
	for _, dml := range cacheInvalidate.Dmls {
		invalidations := int64(0)
		tableInfo := qe.schemaInfo.GetTable(dml.Table)
		if tableInfo == nil {
			panic(NewTabletError(FAIL, "Table %s not found", dml.Table))
		}
		if tableInfo.CacheType == schema.CACHE_NONE {
			break
		}
		for _, val := range dml.Keys {
			newKey := validateKey(tableInfo, val.(string))
			if newKey != "" {
				tableInfo.Cache.Delete(newKey)
			}
			invalidations++
		}
		tableInfo.invalidations.Add(invalidations)
	}
	qe.adminCache.Set(ROWCACHE_INVALIDATION_POSITION, 0, 0, []byte(cacheInvalidate.Position))
}

func (qe *QueryEngine) InvalidateForDDL(ddlInvalidate *proto.DDLInvalidate) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	ddlPlan := sqlparser.DDLParse(ddlInvalidate.DDL)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}
	qe.schemaInfo.DropTable(ddlPlan.TableName)
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		qe.schemaInfo.CreateTable(ddlPlan.NewName)
	}
	qe.adminCache.Set(ROWCACHE_INVALIDATION_POSITION, 0, 0, []byte(ddlInvalidate.Position))
}

//-----------------------------------------------
// DDL

func (qe *QueryEngine) execDDL(logStats *sqlQueryStats, ddl string) *mproto.QueryResult {
	ddlPlan := sqlparser.DDLParse(ddl)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}

	// Stolen from Begin
	conn := qe.txPool.Get()
	txid, err := qe.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	// Stolen from Commit
	defer qe.activeTxPool.SafeCommit(txid)

	// Stolen from Execute
	conn = qe.activeTxPool.Get(txid)
	defer conn.Recycle()
	result, err := qe.executeSql(logStats, conn, ddl, false)
	if err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}

	qe.schemaInfo.DropTable(ddlPlan.TableName)
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		qe.schemaInfo.CreateTable(ddlPlan.NewName)
	}
	return result
}

//-----------------------------------------------
// Execution

func (qe *QueryEngine) execPK(logStats *sqlQueryStats, plan *CompiledPlan) (result *mproto.QueryResult) {
	pkRows := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	return qe.fetchPKRows(logStats, plan, pkRows)
}

func (qe *QueryEngine) execSubquery(logStats *sqlQueryStats, plan *CompiledPlan) (result *mproto.QueryResult) {
	innerResult := qe.qFetch(logStats, plan, plan.Subquery, nil)
	// no need to validate innerResult
	return qe.fetchPKRows(logStats, plan, innerResult.Rows)
}

func (qe *QueryEngine) fetchPKRows(logStats *sqlQueryStats, plan *CompiledPlan, pkRows [][]sqltypes.Value) (result *mproto.QueryResult) {
	result = &mproto.QueryResult{}
	tableInfo := plan.TableInfo
	if plan.Fields == nil {
		panic("unexpected")
	}

	// Fetch from cache first
	keys := make([]string, len(pkRows))
	for i, pk := range pkRows {
		keys[i] = buildKey(pk)
	}
	rcresults := tableInfo.Cache.Get(keys)

	result.Fields = plan.Fields
	rows := make([][]sqltypes.Value, 0, len(pkRows))
	var hits, absent, misses int64
	for i, pk := range pkRows {
		rcresult := rcresults[keys[i]]
		if rcresult.Row != nil {
			/*if dbrow := qe.compareRow(logStats, plan, rcresult.Row, pk); dbrow != nil {
				rows = append(rows, applyFilter(plan.ColumnNumbers, dbrow))
			}*/
			rows = append(rows, applyFilter(plan.ColumnNumbers, rcresult.Row))
			hits++
		} else {
			resultFromdb := qe.qFetch(logStats, plan, plan.OuterQuery, pk)
			if len(resultFromdb.Rows) == 0 {
				absent++
				continue
			}
			row := resultFromdb.Rows[0]
			pkRow := applyFilter(tableInfo.PKColumns, row)
			newKey := buildKey(pkRow)
			if newKey != keys[i] {
				relog.Warning("Key mismatch for query %s. computed: %s, fetched: %s", plan.FullQuery.Query, keys[i], newKey)
			}
			tableInfo.Cache.Set(newKey, row, rcresult.Cas)
			rows = append(rows, applyFilter(plan.ColumnNumbers, row))
			misses++
		}
	}

	logStats.CacheHits = hits
	logStats.CacheAbsent = absent
	logStats.CacheMisses = misses

	logStats.QuerySources |= QUERY_SOURCE_ROWCACHE

	tableInfo.hits.Add(hits)
	tableInfo.absent.Add(absent)
	tableInfo.misses.Add(misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	return result
}

func (qe *QueryEngine) compareRow(logStats *sqlQueryStats, plan *CompiledPlan, cacheRow []sqltypes.Value, pk []sqltypes.Value) (dbrow []sqltypes.Value) {
	rowsAreEquql := func(row1, row2 []sqltypes.Value) bool {
		if len(row1) != len(row2) {
			return false
		}
		for i := 0; i < len(row1); i++ {
			if row1[i].IsNull() && row2[i].IsNull() {
				continue
			}
			if (row1[i].IsNull() && !row2[i].IsNull()) || (!row1[i].IsNull() && row2[i].IsNull()) || row1[i].String() != row2[i].String() {
				return false
			}
		}
		return true
	}
	reloadFromCache := func(pk []sqltypes.Value) (newRow []sqltypes.Value) {
		keys := make([]string, 1)
		keys[0] = buildKey(pk)
		rcresults := plan.TableInfo.Cache.Get(keys)
		if len(rcresults) == 0 {
			return nil
		}
		return rcresults[keys[0]].Row
	}

	resultFromdb := qe.qFetch(logStats, plan, plan.OuterQuery, pk)
	if len(resultFromdb.Rows) == 0 {
		// Reload from cache for verification
		if reloadFromCache(pk) == nil {
			return nil
		}
		relog.Warning("unexpected number of rows for %v", pk)
		errorStats.Add("Mismatch", 1)
		return nil
	}
	dbrow = resultFromdb.Rows[0]
	if !rowsAreEquql(cacheRow, dbrow) {
		// Reload from cache for verification
		newRow := reloadFromCache(pk)
		if newRow == nil {
			return
		}
		if !rowsAreEquql(newRow, dbrow) {
			relog.Warning("query: %v", plan.FullQuery)
			relog.Warning("mismatch for: %v, cache: %v, db: %v", pk, newRow, dbrow)
			errorStats.Add("Mismatch", 1)
		}
	}
	return dbrow
}

// execDirect always sends the query to mysql
func (qe *QueryEngine) execDirect(logStats *sqlQueryStats, plan *CompiledPlan, conn PoolConnection) (result *mproto.QueryResult) {
	if plan.Fields != nil {
		result = qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		result.Fields = plan.Fields
		return
	}
	result = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
	return
}

// execSelect sends a query to mysql only if another identical query is not running. Otherwise, it waits and
// reuses the result. If the plan is missng field info, it sends the query to mysql requesting full info.
func (qe *QueryEngine) execSelect(logStats *sqlQueryStats, plan *CompiledPlan) (result *mproto.QueryResult) {
	if plan.Fields != nil {
		result = qe.qFetch(logStats, plan, plan.FullQuery, nil)
		result.Fields = plan.Fields
		return
	}
	waitingForConnectionStart := time.Now()
	conn := qe.connPool.Get()
	logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()
	result = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
	return
}

func (qe *QueryEngine) execInsertPK(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	pkRows := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	return qe.execInsertPKRows(logStats, conn, plan, pkRows, invalidator)
}

func (qe *QueryEngine) execInsertSubquery(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	innerResult := qe.directFetch(logStats, conn, plan.Subquery, plan.BindVars, nil, nil)
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}
	}
	if len(plan.ColumnNumbers) != len(innerRows[0]) {
		panic(NewTabletError(FAIL, "Subquery length does not match column list"))
	}
	pkRows := make([][]sqltypes.Value, len(innerRows))
	for i, innerRow := range innerRows {
		pkRows[i] = applyFilterWithPKDefaults(plan.TableInfo, plan.SubqueryPKColumns, innerRow)
	}
	// Validating first row is sufficient
	validateRow(plan.TableInfo, plan.TableInfo.PKColumns, pkRows[0])
	plan.BindVars["_rowValues"] = innerRows
	return qe.execInsertPKRows(logStats, conn, plan, pkRows, invalidator)
}

func (qe *QueryEngine) execInsertPKRows(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	secondaryList := buildSecondaryList(plan.TableInfo, pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	// TODO: We need to do this only if insert has on duplicate key clause
	if invalidator != nil {
		for _, pk := range pkRows {
			if key := buildKey(pk); key != "" {
				invalidator.Delete(key)
			}
		}
	}
	return result
}

func (qe *QueryEngine) execDMLPK(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	pkRows := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	secondaryList := buildSecondaryList(plan.TableInfo, pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	if invalidator != nil {
		for _, pk := range pkRows {
			key := buildKey(pk)
			invalidator.Delete(key)
		}
	}
	return result
}

func (qe *QueryEngine) execDMLSubquery(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	innerResult := qe.directFetch(logStats, conn, plan.Subquery, plan.BindVars, nil, nil)
	// no need to validate innerResult
	return qe.execDMLPKRows(logStats, conn, plan, innerResult.Rows, invalidator)
}

func (qe *QueryEngine) execDMLPKRows(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	if len(pkRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}
	}
	rowsAffected := uint64(0)
	singleRow := make([][]sqltypes.Value, 1)
	for _, pkRow := range pkRows {
		singleRow[0] = pkRow
		secondaryList := buildSecondaryList(plan.TableInfo, singleRow, plan.SecondaryPKValues, plan.BindVars)
		bsc := buildStreamComment(plan.TableInfo, singleRow, secondaryList)
		rowsAffected += qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, pkRow, bsc).RowsAffected
		if invalidator != nil {
			key := buildKey(pkRow)
			invalidator.Delete(key)
		}
	}
	return &mproto.QueryResult{RowsAffected: rowsAffected}
}

func (qe *QueryEngine) execSet(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan) (result *mproto.QueryResult) {
	switch plan.SetKey {
	case "vt_pool_size":
		qe.connPool.SetCapacity(int(plan.SetValue.(float64)))
		return &mproto.QueryResult{}
	case "vt_stream_pool_size":
		qe.streamConnPool.SetCapacity(int(plan.SetValue.(float64)))
		return &mproto.QueryResult{}
	case "vt_transaction_cap":
		qe.txPool.SetCapacity(int(plan.SetValue.(float64)))
		return &mproto.QueryResult{}
	case "vt_transaction_timeout":
		qe.activeTxPool.SetTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &mproto.QueryResult{}
	case "vt_schema_reload_time":
		qe.schemaInfo.SetReloadTime(time.Duration(plan.SetValue.(float64) * 1e9))
		return &mproto.QueryResult{}
	case "vt_query_cache_size":
		qe.schemaInfo.SetQueryCacheSize(int(plan.SetValue.(float64)))
		return &mproto.QueryResult{}
	case "vt_max_result_size":
		val := int32(plan.SetValue.(float64))
		if val < 1 {
			panic(NewTabletError(FAIL, "max result size out of range %v", val))
		}
		qe.maxResultSize.Set(val)
		return &mproto.QueryResult{}
	case "vt_stream_buffer_size":
		val := int32(plan.SetValue.(float64))
		if val < 1024 {
			panic(NewTabletError(FAIL, "stream buffer size out of range %v", val))
		}
		qe.streamBufferSize.Set(val)
		return &mproto.QueryResult{}
	case "vt_query_timeout":
		qe.activePool.SetTimeout(time.Duration(plan.SetValue.(float64) * 1e9))
		return &mproto.QueryResult{}
	case "vt_idle_timeout":
		t := plan.SetValue.(float64) * 1e9
		qe.connPool.SetIdleTimeout(time.Duration(t))
		qe.streamConnPool.SetIdleTimeout(time.Duration(t))
		qe.txPool.SetIdleTimeout(time.Duration(t))
		qe.activePool.SetIdleTimeout(time.Duration(t))
		return &mproto.QueryResult{}
	}
	return qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
}

func (qe *QueryEngine) qFetch(logStats *sqlQueryStats, plan *CompiledPlan, parsed_query *sqlparser.ParsedQuery, listVars []sqltypes.Value) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, plan.BindVars, listVars, nil)
	q, ok := qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
		waitingForConnectionStart := time.Now()
		conn, err := qe.connPool.SafeGet()
		logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		if err != nil {
			q.Err = NewTabletErrorSql(FATAL, err)
		} else {
			defer conn.Recycle()
			q.Result, q.Err = qe.executeSql(logStats, conn, sql, false)
		}
	} else {
		logStats.QuerySources |= QUERY_SOURCE_CONSOLIDATOR
		q.Wait()
	}
	if q.Err != nil {
		panic(q.Err)
	}
	return q.Result
}

func (qe *QueryEngine) directFetch(logStats *sqlQueryStats, conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := qe.executeSql(logStats, conn, sql, false)
	if err != nil {
		panic(err)
	}
	return result
}

// fullFetch also fetches field info
func (qe *QueryEngine) fullFetch(logStats *sqlQueryStats, conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := qe.executeSql(logStats, conn, sql, true)
	if err != nil {
		panic(err)
	}
	return result
}

func (qe *QueryEngine) fullStreamFetch(logStats *sqlQueryStats, conn PoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte, callback func(interface{}) error) error {
	sql := qe.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	return qe.executeStreamSql(logStats, conn, sql, callback)
}

func (qe *QueryEngine) generateFinalSql(parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) string {
	bindVars[MAX_RESULT_NAME] = qe.maxResultSize.Get() + 1
	sql, err := parsed_query.GenerateQuery(bindVars, listVars)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	// undo hack done by stripTrailing
	sql = restoreTrailing(sql, bindVars)
	return hack.String(sql)
}

func (qe *QueryEngine) executeSql(logStats *sqlQueryStats, conn PoolConnection, sql string, wantfields bool) (*mproto.QueryResult, error) {
	connid := conn.Id()
	qe.activePool.Put(connid)
	defer qe.activePool.Remove(connid)

	logStats.QuerySources |= QUERY_SOURCE_MYSQL
	logStats.NumberOfQueries += 1
	logStats.AddRewrittenSql(sql)

	// NOTE(szopa): I am not doing this measurement inside
	// conn.ExecuteFetch because that would require changing the
	// PoolConnection interface. Same applies to executeStreamSql.
	fetchStart := time.Now()
	result, err := conn.ExecuteFetch(sql, int(qe.maxResultSize.Get()), wantfields)
	logStats.MysqlResponseTime += time.Now().Sub(fetchStart)

	if err != nil {
		return nil, NewTabletErrorSql(FAIL, err)
	}
	return result, nil
}

func (qe *QueryEngine) executeStreamSql(logStats *sqlQueryStats, conn PoolConnection, sql string, callback func(interface{}) error) error {
	logStats.QuerySources |= QUERY_SOURCE_MYSQL
	logStats.NumberOfQueries += 1
	logStats.AddRewrittenSql(sql)
	fetchStart := time.Now()
	err := conn.ExecuteStreamFetch(sql, callback, int(qe.streamBufferSize.Get()))
	logStats.MysqlResponseTime += time.Now().Sub(fetchStart)
	if err != nil {
		return NewTabletErrorSql(FAIL, err)
	}
	return nil
}
