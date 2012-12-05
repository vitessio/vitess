// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"
	"sync/atomic"
	"time"

	mproto "code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/vt/sqlparser"
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

const (
	MAX_RESULT_NAME = "_vtMaxResultSize"
)

//-----------------------------------------------
type QueryEngine struct {
	// Obtain read lock on mu to execute queries
	// Obtain write lock to start/stop query service
	mu sync.RWMutex

	cachePool      *CachePool
	schemaInfo     *SchemaInfo
	connPool       *ConnectionPool
	streamConnPool *ConnectionPool
	reservedPool   *ReservedPool
	txPool         *ConnectionPool
	activeTxPool   *ActiveTxPool
	activePool     *ActivePool
	consolidator   *Consolidator

	maxResultSize    int32 // Use atomic
	streamBufferSize int32 // Use atomic
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
	qe.connPool = NewConnectionPool(config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.streamConnPool = NewConnectionPool(config.StreamPoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.reservedPool = NewReservedPool()
	qe.txPool = NewConnectionPool(config.TransactionCap, time.Duration(config.IdleTimeout*1e9)) // connections in pool has to be > transactionCap
	qe.activeTxPool = NewActiveTxPool(time.Duration(config.TransactionTimeout * 1e9))
	qe.activePool = NewActivePool(time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	qe.consolidator = NewConsolidator()
	qe.maxResultSize = int32(config.MaxResultSize)
	qe.streamBufferSize = int32(config.StreamBufferSize)
	queryStats = stats.NewTimings("Queries")
	stats.NewRates("QPS", queryStats, 15, 60e9)
	waitStats = stats.NewTimings("Waits")
	killStats = stats.NewCounters("Kills")
	errorStats = stats.NewCounters("Errors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	return qe
}

func (qe *QueryEngine) Open(dbconfig DBConfig) {
	// Wait for Close, in case it's running
	qe.mu.Lock()
	defer qe.mu.Unlock()

	connFactory := GenericConnectionCreator(dbconfig.MysqlParams())
	cacheFactory := CacheCreator(dbconfig)

	start := time.Now().UnixNano()
	qe.cachePool.Open(cacheFactory)
	qe.schemaInfo.Open(connFactory, qe.cachePool)
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
	} else {
		conn = qe.txPool.Get()
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
		atomic.AddInt64(&tableInfo.invalidations, invalidations)
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
	if basePlan.PlanId == sqlparser.PLAN_DDL {
		defer queryStats.Record("DDL", time.Now())
		return qe.execDDL(logStats, query.Sql)
	}

	plan := &CompiledPlan{query.Sql, basePlan, query.BindVariables, query.TransactionId, query.ConnectionId}
	if query.TransactionId != 0 {
		// Need upfront connection for DMLs and transactions
		conn := qe.activeTxPool.Get(query.TransactionId)
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
			logStats.PlanType = "PASS_DML"
			defer queryStats.Record("PASS_DML", time.Now())
			reply = qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		case sqlparser.PLAN_INSERT_PK:
			logStats.PlanType = "INSERT_PK"
			defer queryStats.Record("INSERT_PK", time.Now())
			reply = qe.execInsertPK(logStats, conn, plan, invalidator)
		case sqlparser.PLAN_INSERT_SUBQUERY:
			logStats.PlanType = "INSERT_SUBQUERY"
			defer queryStats.Record("INSERT_SUBQUERY", time.Now())
			reply = qe.execInsertSubquery(logStats, conn, plan, invalidator)
		case sqlparser.PLAN_DML_PK:
			logStats.PlanType = "DML_PK"
			defer queryStats.Record("DML_PK", time.Now())
			reply = qe.execDMLPK(logStats, conn, plan, invalidator)
		case sqlparser.PLAN_DML_SUBQUERY:
			logStats.PlanType = "DML_SUBQUERY"
			defer queryStats.Record("DML_SUBQUERY", time.Now())
			reply = qe.execDMLSubquery(logStats, conn, plan, invalidator)
		default: // select or set in a transaction, just count as select
			logStats.PlanType = "PASS_SELECT"
			defer queryStats.Record("PASS_SELECT", time.Now())
			reply = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		}
	} else if plan.ConnectionId != 0 {
		conn := qe.reservedPool.Get(plan.ConnectionId)
		defer conn.Recycle()
		if plan.PlanId.IsSelect() {
			logStats.PlanType = "PASS_SELECT"
			defer queryStats.Record("PASS_SELECT", time.Now())
			reply = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		} else if plan.PlanId == sqlparser.PLAN_SET {
			logStats.PlanType = "SET"
			defer queryStats.Record("SET", time.Now())
			reply = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		} else {
			panic(NewTabletError(FAIL, "DMLs not allowed outside of transactions"))
		}
	} else {
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_SELECT:
			if plan.Reason == sqlparser.REASON_FOR_UPDATE {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			logStats.PlanType = "PASS_SELECT"
			defer queryStats.Record("PASS_SELECT", time.Now())
			reply = qe.execSelect(logStats, plan)
		case sqlparser.PLAN_SELECT_PK:
			logStats.PlanType = "SELECT_PK"
			defer queryStats.Record("SELECT_PK", time.Now())
			reply = qe.execPK(logStats, plan)
		case sqlparser.PLAN_SELECT_SUBQUERY:
			logStats.PlanType = "SELECT_SUBQUERY"
			defer queryStats.Record("SELECT_SUBQUERY", time.Now())
			reply = qe.execSubquery(logStats, plan)
		case sqlparser.PLAN_SELECT_CACHE_RESULT:
			logStats.PlanType = "SELECT_CACHE_RESULT"
			defer queryStats.Record("SELECT_CACHE_RESULT", time.Now())
			// It may not be worth caching the results. So, just pass through.
			reply = qe.execSelect(logStats, plan)
		case sqlparser.PLAN_SET:
			waitingForConnectionStart := time.Now()
			conn := qe.connPool.Get()
			logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
			defer conn.Recycle()
			logStats.PlanType = "SET"
			defer queryStats.Record("SET", time.Now())
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
}

func (qe *QueryEngine) InvalidateForDDL(ddl string) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	ddlPlan := sqlparser.DDLParse(ddl)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}
	qe.schemaInfo.DropTable(ddlPlan.TableName)
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		qe.schemaInfo.CreateTable(ddlPlan.NewName)
	}
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
	result, err := qe.executeSql(logStats, conn, []byte(ddl), false)
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
	result.Fields = plan.Fields
	rows := make([][]sqltypes.Value, 0, len(pkRows))
	var hits, absent, misses int64
	for _, pk := range pkRows {
		key := buildKey(tableInfo, pk)
		if cacheRow, cas := tableInfo.Cache.Get(key); cacheRow != nil {
			/*if dbrow := qe.compareRow(plan, cacheRow, pk); dbrow != nil {
				rows = append(rows, applyFilter(plan.ColumnNumbers, dbrow))
			}*/
			rows = append(rows, applyFilter(plan.ColumnNumbers, cacheRow))
			hits++
		} else {
			resultFromdb := qe.qFetch(logStats, plan, plan.OuterQuery, pk)
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

	logStats.CacheHits = hits
	logStats.CacheAbsent = absent
	logStats.CacheMisses = misses

	logStats.QuerySources |= QUERY_SOURCE_ROWCACHE

	atomic.AddInt64(&tableInfo.hits, hits)
	atomic.AddInt64(&tableInfo.absent, absent)
	atomic.AddInt64(&tableInfo.misses, misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	return result
}

func (qe *QueryEngine) compareRow(logStats *sqlQueryStats, plan *CompiledPlan, cacheRow []sqltypes.Value, pk []sqltypes.Value) (dbrow []sqltypes.Value) {
	resultFromdb := qe.qFetch(logStats, plan, plan.OuterQuery, pk)
	if len(resultFromdb.Rows) != 1 {
		relog.Warning("unexpected number of rows for %v: %d", pk, len(resultFromdb.Rows))
		return nil
	}
	dbrow = resultFromdb.Rows[0]
	for i := 0; i < len(cacheRow); i++ {
		if cacheRow[i].IsNull() && dbrow[i].IsNull() {
			continue
		}
		if (cacheRow[i].IsNull() && !dbrow[i].IsNull()) || (!cacheRow[i].IsNull() && dbrow[i].IsNull()) || cacheRow[i].String() != dbrow[i].String() {
			relog.Warning("query: %v", plan.FullQuery)
			relog.Warning("mismatch for: %v, column: %v cache: %s, db: %s", pk, i, cacheRow[i], dbrow[i])
			return dbrow
		}
	}
	return dbrow
}

func (qe *QueryEngine) execSelect(logStats *sqlQueryStats, plan *CompiledPlan) (result *mproto.QueryResult) {
	result = qe.qFetch(logStats, plan, plan.FullQuery, nil)
	result.Fields = plan.Fields
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
			if key := buildKey(plan.TableInfo, pk); key != "" {
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
			key := buildKey(plan.TableInfo, pk)
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
			key := buildKey(plan.TableInfo, pkRow)
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
		atomic.StoreInt32(&qe.maxResultSize, val)
		return &mproto.QueryResult{}
	case "vt_stream_buffer_size":
		val := int32(plan.SetValue.(float64))
		if val < 1024 {
			panic(NewTabletError(FAIL, "stream buffer size out of range %v", val))
		}
		atomic.StoreInt32(&qe.streamBufferSize, val)
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
	return qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
}

func (qe *QueryEngine) qFetch(logStats *sqlQueryStats, plan *CompiledPlan, parsed_query *sqlparser.ParsedQuery, listVars []sqltypes.Value) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, plan.BindVars, listVars, nil)
	q, ok := qe.consolidator.Create(string(sql))
	if ok {
		waitingForConnectionStart := time.Now()
		conn := qe.connPool.Get()
		logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		defer conn.Recycle()
		q.Result, q.Err = qe.executeSql(logStats, conn, sql, false)
		q.Broadcast()
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

func (qe *QueryEngine) generateFinalSql(parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) []byte {
	bindVars[MAX_RESULT_NAME] = atomic.LoadInt32(&qe.maxResultSize) + 1
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

func (qe *QueryEngine) executeSql(logStats *sqlQueryStats, conn PoolConnection, sql []byte, wantfields bool) (*mproto.QueryResult, error) {
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
	result, err := conn.ExecuteFetch(sql, int(atomic.LoadInt32(&qe.maxResultSize)), wantfields)
	logStats.MysqlResponseTime += time.Now().Sub(fetchStart)

	if err != nil {
		return nil, NewTabletErrorSql(FAIL, err)
	}
	return result, nil
}

func (qe *QueryEngine) executeStreamSql(logStats *sqlQueryStats, conn PoolConnection, sql []byte, callback func(interface{}) error) error {
	logStats.QuerySources |= QUERY_SOURCE_MYSQL
	logStats.NumberOfQueries += 1
	logStats.AddRewrittenSql(sql)
	fetchStart := time.Now()
	err := conn.ExecuteStreamFetch(sql, callback, int(atomic.LoadInt32(&qe.streamBufferSize)))
	logStats.MysqlResponseTime += time.Now().Sub(fetchStart)
	if err != nil {
		return NewTabletErrorSql(FAIL, err)
	}
	return nil
}
