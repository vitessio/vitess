// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/hack"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

const (
	MAX_RESULT_NAME                = "_vtMaxResultSize"
	ROWCACHE_INVALIDATION_POSITION = "ROWCACHE_INVALIDATION_POSITION"

	// SPOT_CHECK_MULTIPLIER determines the precision of the
	// spot check ratio: 1e6 == 6 digits
	SPOT_CHECK_MULTIPLIER = 1e6
)

// QueryEngine implements the core functionality of tabletserver.
// It assumes that no requests will be sent to it before Open is
// called and succeeds.
// Shutdown is done in the following order:
//
// WaitForTxEmpty: There should be no more new calls to Begin
// once this function is called. This will return when there
// are no more pending transactions.
//
// Close: There should be no more pending queries when this
// function is called.
//
// Functions of QueryEngine do not return errors. They instead
// panic with NewTabletError as the error type.
// TODO(sougou): Switch to error return scheme.
type QueryEngine struct {
	schemaInfo *SchemaInfo

	// Pools
	cachePool      *CachePool
	connPool       *dbconnpool.ConnectionPool
	streamConnPool *dbconnpool.ConnectionPool
	txPool         *dbconnpool.ConnectionPool

	// Services
	activeTxPool *ActiveTxPool
	activePool   *ActivePool
	consolidator *Consolidator
	invalidator  *RowcacheInvalidator
	streamQList  *QueryList

	// Vars
	spotCheckFreq    sync2.AtomicInt64
	strictMode       sync2.AtomicInt64
	maxResultSize    sync2.AtomicInt64
	streamBufferSize sync2.AtomicInt64
}

type compiledPlan struct {
	Query string
	*ExecPlan
	BindVars      map[string]interface{}
	TransactionId int64
}

// stats are globals to allow anybody to set them
var (
	mysqlStats     *stats.Timings
	queryStats     *stats.Timings
	waitStats      *stats.Timings
	killStats      *stats.Counters
	infoErrors     *stats.Counters
	errorStats     *stats.Counters
	internalErrors *stats.Counters
	resultStats    *stats.Histogram
	spotCheckCount *stats.Int
	QPSRates       *stats.Rates
)

var resultBuckets = []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}

// CacheInvalidator provides the abstraction needed for an instant invalidation
// vs. delayed invalidation in the case of in-transaction dmls
type CacheInvalidator interface {
	Delete(key string) bool
}

var connPoolClosedErr = NewTabletError(FATAL, "connection pool is closed")

// Helper method for conn pools to convert errors
func getOrPanic(pool *dbconnpool.ConnectionPool) dbconnpool.PoolConnection {
	conn, err := pool.Get()
	if err == nil {
		return conn
	}
	if err == dbconnpool.CONN_POOL_CLOSED_ERR {
		panic(connPoolClosedErr)
	}
	panic(NewTabletErrorSql(FATAL, err))
}

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(config Config) *QueryEngine {
	qe := &QueryEngine{}
	qe.schemaInfo = NewSchemaInfo(config.QueryCacheSize, time.Duration(config.SchemaReloadTime*1e9), time.Duration(config.IdleTimeout*1e9), config.SensitiveMode)

	mysqlStats = stats.NewTimings("Mysql")

	// Pools
	qe.cachePool = NewCachePool("Rowcache", config.RowCache, time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	qe.connPool = dbconnpool.NewConnectionPool("ConnPool", config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.streamConnPool = dbconnpool.NewConnectionPool("StreamConnPool", config.StreamPoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.txPool = dbconnpool.NewConnectionPool("TransactionPool", config.TransactionCap, time.Duration(config.IdleTimeout*1e9)) // connections in pool has to be > transactionCap

	// Services
	qe.activeTxPool = NewActiveTxPool("ActiveTransactionPool", time.Duration(config.TransactionTimeout*1e9))
	qe.activePool = NewActivePool("ActivePool", time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	qe.consolidator = NewConsolidator()
	qe.invalidator = NewRowcacheInvalidator(qe)

	// Vars
	qe.spotCheckFreq = sync2.AtomicInt64(config.SpotCheckRatio * SPOT_CHECK_MULTIPLIER)
	if config.StrictMode {
		qe.strictMode.Set(1)
	}
	qe.maxResultSize = sync2.AtomicInt64(config.MaxResultSize)
	qe.streamBufferSize = sync2.AtomicInt64(config.StreamBufferSize)
	qe.streamQList = NewQueryList()

	// Stats
	stats.Publish("MaxResultSize", stats.IntFunc(qe.maxResultSize.Get))
	stats.Publish("StreamBufferSize", stats.IntFunc(qe.streamBufferSize.Get))
	queryStats = stats.NewTimings("Queries")
	QPSRates = stats.NewRates("QPS", queryStats, 15, 60*time.Second)
	waitStats = stats.NewTimings("Waits")
	killStats = stats.NewCounters("Kills")
	infoErrors = stats.NewCounters("InfoErrors")
	errorStats = stats.NewCounters("Errors")
	internalErrors = stats.NewCounters("InternalErrors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	stats.Publish("RowcacheSpotCheckRatio", stats.FloatFunc(func() float64 {
		return float64(qe.spotCheckFreq.Get()) / SPOT_CHECK_MULTIPLIER
	}))
	spotCheckCount = stats.NewInt("RowcacheSpotCheckCount")

	return qe
}

// Open must be called before sending requests to QueryEngine.
func (qe *QueryEngine) Open(dbconfig *dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules, mysqld *mysqlctl.Mysqld) {
	connFactory := dbconnpool.DBConnectionCreator(&dbconfig.ConnectionParams, mysqlStats)

	strictMode := false
	if qe.strictMode.Get() != 0 {
		strictMode = true
	}
	if !strictMode && dbconfig.EnableRowcache {
		panic(NewTabletError(FATAL, "Rowcache cannot be enabled when queryserver-config-strict-mode is false"))
	}
	if dbconfig.EnableRowcache {
		qe.cachePool.Open()
		log.Infof("rowcache is enabled")
	} else {
		// Invalidator should not be enabled if rowcache is not enabled.
		dbconfig.EnableInvalidator = false
		log.Infof("rowcache is not enabled")
	}

	start := time.Now()
	// schemaInfo depends on cachePool. Every table that has a rowcache
	// points to the cachePool.
	qe.schemaInfo.Open(connFactory, schemaOverrides, qe.cachePool, qrs, strictMode)
	log.Infof("Time taken to load the schema: %v", time.Now().Sub(start))

	// Start the invalidator only after schema is loaded.
	// This will allow qe to find the table info
	// for the invalidation events that will start coming
	// immediately.
	if dbconfig.EnableInvalidator {
		qe.invalidator.Open(dbconfig.DbName, mysqld)
	}
	qe.connPool.Open(connFactory)
	qe.streamConnPool.Open(connFactory)
	qe.txPool.Open(connFactory)
	qe.activeTxPool.Open()
	qe.activePool.Open(connFactory)
}

// WaitForTxEmpty must be called before calling Close.
// Before calling WaitForTxEmpty, you must ensure that there
// will be no more calls to Begin.
func (qe *QueryEngine) WaitForTxEmpty() {
	qe.activeTxPool.WaitForEmpty()
}

// Close must be called to shut down QueryEngine.
// You must ensure that no more queries will be sent
// before calling Close.
func (qe *QueryEngine) Close() {
	// Close in reverse order of Open.
	qe.activePool.Close()
	qe.activeTxPool.Close()
	qe.txPool.Close()
	qe.streamConnPool.Close()
	qe.connPool.Close()
	qe.invalidator.Close()
	qe.schemaInfo.Close()
	qe.cachePool.Close()
}

// Begin begins a transaction.
func (qe *QueryEngine) Begin(logStats *SQLQueryStats) int64 {
	defer queryStats.Record("BEGIN", time.Now())

	conn, err := qe.txPool.TryGet()
	if err == dbconnpool.CONN_POOL_CLOSED_ERR {
		panic(connPoolClosedErr)
	}
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	if conn == nil {
		panic(NewTabletError(TX_POOL_FULL, "Transaction pool connection limit exceeded"))
	}
	transactionId, err := qe.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	return transactionId
}

// Commit commits the specified transaction.
func (qe *QueryEngine) Commit(logStats *SQLQueryStats, transactionId int64) {
	defer queryStats.Record("COMMIT", time.Now())
	dirtyTables, err := qe.activeTxPool.SafeCommit(transactionId)
	qe.invalidateRows(logStats, dirtyTables)
	if err != nil {
		panic(err)
	}
}

func (qe *QueryEngine) invalidateRows(logStats *SQLQueryStats, dirtyTables map[string]DirtyKeys) {
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

// Rollback rolls back the specified transaction.
func (qe *QueryEngine) Rollback(logStats *SQLQueryStats, transactionId int64) {
	defer queryStats.Record("ROLLBACK", time.Now())
	qe.activeTxPool.Rollback(transactionId)
}

// Execute executes the specified query and returns its result.
func (qe *QueryEngine) Execute(logStats *SQLQueryStats, query *proto.Query) (reply *mproto.QueryResult) {
	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	logStats.BindVariables = query.BindVariables
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)
	basePlan := qe.schemaInfo.GetPlan(logStats, query.Sql)
	planName := basePlan.PlanId.String()
	logStats.PlanType = planName
	logStats.OriginalSql = basePlan.DisplayQuery
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
	switch action {
	case QR_FAIL:
		panic(NewTabletError(FAIL, "Query disallowed due to rule: %s", desc))
	case QR_FAIL_RETRY:
		panic(NewTabletError(RETRY, "Query disallowed due to rule: %s", desc))
	}

	if basePlan.PlanId == sqlparser.PLAN_DDL {
		return qe.execDDL(logStats, query.Sql)
	}

	plan := &compiledPlan{
		Query:         query.Sql,
		ExecPlan:      basePlan,
		BindVars:      query.BindVariables,
		TransactionId: query.TransactionId,
	}
	if query.TransactionId != 0 {
		// Need upfront connection for DMLs and transactions
		conn := qe.activeTxPool.Get(query.TransactionId)
		defer conn.Recycle()
		conn.RecordQuery(plan.DisplayQuery)
		var invalidator CacheInvalidator
		if plan.TableInfo != nil && plan.TableInfo.CacheType != schema.CACHE_NONE {
			invalidator = conn.DirtyKeys(plan.TableName)
		}
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_DML:
			if qe.strictMode.Get() != 0 {
				panic(NewTabletError(FAIL, "DML too complex"))
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
	} else {
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_SELECT:
			if plan.Reason == sqlparser.REASON_LOCK {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			reply = qe.execSelect(logStats, plan)
		case sqlparser.PLAN_PK_EQUAL:
			reply = qe.execPKEqual(logStats, plan)
		case sqlparser.PLAN_PK_IN:
			reply = qe.execPKIN(logStats, plan)
		case sqlparser.PLAN_SELECT_SUBQUERY:
			reply = qe.execSubquery(logStats, plan)
		case sqlparser.PLAN_SET:
			waitingForConnectionStart := time.Now()
			conn := getOrPanic(qe.connPool)
			logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
			defer conn.Recycle()
			reply = qe.execSet(logStats, conn, plan)
		default:
			panic(NewTabletError(NOT_IN_TX, "DMLs not allowed outside of transactions"))
		}
	}
	if plan.PlanId.IsSelect() {
		logStats.RowsAffected = int(reply.RowsAffected)
		resultStats.Add(int64(reply.RowsAffected))
		logStats.Rows = reply.Rows
	}

	return reply
}

// StreamExecute executes the query and streams its result.
// The first QueryResult will have Fields set (and Rows nil)
// The subsequent QueryResult will have Rows set (and Fields nil)
func (qe *QueryEngine) StreamExecute(logStats *SQLQueryStats, query *proto.Query, sendReply func(*mproto.QueryResult) error) {
	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	logStats.BindVariables = query.BindVariables
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)

	plan := qe.schemaInfo.GetStreamPlan(query.Sql)
	logStats.PlanType = "SELECT_STREAM"
	logStats.OriginalSql = plan.DisplayQuery
	defer queryStats.Record("SELECT_STREAM", time.Now())

	// does the real work: first get a connection
	waitingForConnectionStart := time.Now()
	conn := getOrPanic(qe.streamConnPool)
	logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()

	qd := NewQueryDetail(query, logStats.context, conn.Id())
	qe.streamQList.Add(qd)
	defer qe.streamQList.Remove(qd)
	// then let's stream!
	qe.fullStreamFetch(logStats, conn, plan.FullQuery, query.BindVariables, nil, nil, sendReply)
}

// InvalidateForDml performs rowcache invalidations for the dml.
func (qe *QueryEngine) InvalidateForDml(dml *proto.DmlType) {
	if qe.cachePool.IsClosed() {
		return
	}
	invalidations := int64(0)
	tableInfo := qe.schemaInfo.GetTable(dml.Table)
	if tableInfo == nil {
		panic(NewTabletError(FAIL, "Table %s not found", dml.Table))
	}
	if tableInfo.CacheType == schema.CACHE_NONE {
		return
	}
	for _, val := range dml.Keys {
		newKey := validateKey(tableInfo, val)
		if newKey != "" {
			tableInfo.Cache.Delete(newKey)
		}
		invalidations++
	}
	tableInfo.invalidations.Add(invalidations)
}

// InvalidateForDDL performs schema and rowcache changes for the ddl.
func (qe *QueryEngine) InvalidateForDDL(ddlInvalidate *proto.DDLInvalidate) {
	ddlPlan := sqlparser.DDLParse(ddlInvalidate.DDL)
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

func (qe *QueryEngine) execDDL(logStats *SQLQueryStats, ddl string) *mproto.QueryResult {
	ddlPlan := sqlparser.DDLParse(ddl)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}

	// Stolen from Begin
	conn := getOrPanic(qe.txPool)
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

func (qe *QueryEngine) execPKEqual(logStats *SQLQueryStats, plan *compiledPlan) (result *mproto.QueryResult) {
	pkRows, err := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	if err != nil {
		panic(err)
	}
	if len(pkRows) != 1 || plan.Fields == nil {
		panic("unexpected")
	}
	row := qe.fetchOne(logStats, plan, pkRows[0])
	result = &mproto.QueryResult{}
	result.Fields = plan.Fields
	if row == nil {
		return
	}
	result.Rows = make([][]sqltypes.Value, 1)
	result.Rows[0] = applyFilter(plan.ColumnNumbers, row)
	result.RowsAffected = 1
	return
}

func (qe *QueryEngine) fetchOne(logStats *SQLQueryStats, plan *compiledPlan, pk []sqltypes.Value) (row []sqltypes.Value) {
	logStats.QuerySources |= QUERY_SOURCE_ROWCACHE
	tableInfo := plan.TableInfo
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	rcresults := tableInfo.Cache.Get(keys)
	rcresult := rcresults[keys[0]]
	if rcresult.Row != nil {
		if qe.mustVerify() {
			qe.spotCheck(logStats, plan, rcresult, pk)
		}
		logStats.CacheHits++
		tableInfo.hits.Add(1)
		return rcresult.Row
	}
	resultFromdb := qe.qFetch(logStats, plan.OuterQuery, plan.BindVars, pk)
	if len(resultFromdb.Rows) == 0 {
		logStats.CacheAbsent++
		tableInfo.absent.Add(1)
		return nil
	}
	row = resultFromdb.Rows[0]
	tableInfo.Cache.Set(keys[0], row, rcresult.Cas)
	logStats.CacheMisses++
	tableInfo.misses.Add(1)
	return row
}

func (qe *QueryEngine) execPKIN(logStats *SQLQueryStats, plan *compiledPlan) (result *mproto.QueryResult) {
	pkRows, err := buildINValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	if err != nil {
		panic(err)
	}
	return qe.fetchMulti(logStats, plan, pkRows)
}

func (qe *QueryEngine) execSubquery(logStats *SQLQueryStats, plan *compiledPlan) (result *mproto.QueryResult) {
	innerResult := qe.qFetch(logStats, plan.Subquery, plan.BindVars, nil)
	return qe.fetchMulti(logStats, plan, innerResult.Rows)
}

func (qe *QueryEngine) fetchMulti(logStats *SQLQueryStats, plan *compiledPlan, pkRows [][]sqltypes.Value) (result *mproto.QueryResult) {
	result = &mproto.QueryResult{}
	if len(pkRows) == 0 {
		return
	}
	if len(pkRows[0]) != 1 || plan.Fields == nil {
		panic("unexpected")
	}

	tableInfo := plan.TableInfo
	keys := make([]string, len(pkRows))
	for i, pk := range pkRows {
		keys[i] = buildKey(pk)
	}
	rcresults := tableInfo.Cache.Get(keys)

	result.Fields = plan.Fields
	rows := make([][]sqltypes.Value, 0, len(pkRows))
	missingRows := make([]sqltypes.Value, 0, len(pkRows))
	var hits, absent, misses int64
	for i, pk := range pkRows {
		rcresult := rcresults[keys[i]]
		if rcresult.Row != nil {
			if qe.mustVerify() {
				qe.spotCheck(logStats, plan, rcresult, pk)
			}
			rows = append(rows, applyFilter(plan.ColumnNumbers, rcresult.Row))
			hits++
		} else {
			missingRows = append(missingRows, pk[0])
		}
	}
	if len(missingRows) != 0 {
		resultFromdb := qe.qFetch(logStats, plan.OuterQuery, plan.BindVars, missingRows)
		misses = int64(len(resultFromdb.Rows))
		absent = int64(len(pkRows)) - hits - misses
		for _, row := range resultFromdb.Rows {
			rows = append(rows, applyFilter(plan.ColumnNumbers, row))
			key := buildKey(applyFilter(plan.TableInfo.PKColumns, row))
			tableInfo.Cache.Set(key, row, rcresults[key].Cas)
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

func (qe *QueryEngine) mustVerify() bool {
	return (Rand() % SPOT_CHECK_MULTIPLIER) < qe.spotCheckFreq.Get()
}

func (qe *QueryEngine) spotCheck(logStats *SQLQueryStats, plan *compiledPlan, rcresult RCResult, pk []sqltypes.Value) {
	spotCheckCount.Add(1)
	resultFromdb := qe.qFetch(logStats, plan.OuterQuery, plan.BindVars, pk)
	var dbrow []sqltypes.Value
	if len(resultFromdb.Rows) != 0 {
		dbrow = resultFromdb.Rows[0]
	}
	if dbrow == nil || !rowsAreEqual(rcresult.Row, dbrow) {
		qe.recheckLater(plan, rcresult, dbrow, pk)
	}
}

func (qe *QueryEngine) recheckLater(plan *compiledPlan, rcresult RCResult, dbrow []sqltypes.Value, pk []sqltypes.Value) {
	if qe.cachePool.IsClosed() {
		return
	}

	time.Sleep(10 * time.Second)
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	reloaded := plan.TableInfo.Cache.Get(keys)[keys[0]]
	// If reloaded row is absent or has changed, we're good
	if reloaded.Row == nil || reloaded.Cas != rcresult.Cas {
		return
	}
	log.Warningf("query: %v", plan.FullQuery)
	log.Warningf("mismatch for: %v\ncache: %v\ndb:    %v", pk, rcresult.Row, dbrow)
	internalErrors.Add("Mismatch", 1)
}

// execDirect always sends the query to mysql
func (qe *QueryEngine) execDirect(logStats *SQLQueryStats, plan *compiledPlan, conn dbconnpool.PoolConnection) (result *mproto.QueryResult) {
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
func (qe *QueryEngine) execSelect(logStats *SQLQueryStats, plan *compiledPlan) (result *mproto.QueryResult) {
	if plan.Fields != nil {
		result = qe.qFetch(logStats, plan.FullQuery, plan.BindVars, nil)
		result.Fields = plan.Fields
		return
	}
	waitingForConnectionStart := time.Now()
	conn := getOrPanic(qe.connPool)
	logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()
	result = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
	return
}

func (qe *QueryEngine) execInsertPK(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, plan *compiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	pkRows, err := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	if err != nil {
		panic(err)
	}
	return qe.execInsertPKRows(logStats, conn, plan, pkRows, invalidator)
}

func (qe *QueryEngine) execInsertSubquery(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, plan *compiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
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
	if err := validateRow(plan.TableInfo, plan.TableInfo.PKColumns, pkRows[0]); err != nil {
		panic(err)
	}

	plan.BindVars["_rowValues"] = innerRows
	return qe.execInsertPKRows(logStats, conn, plan, pkRows, invalidator)
}

func (qe *QueryEngine) execInsertPKRows(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, plan *compiledPlan, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	secondaryList, err := buildSecondaryList(plan.TableInfo, pkRows, plan.SecondaryPKValues, plan.BindVars)
	if err != nil {
		panic(err)
	}
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	return result
}

func (qe *QueryEngine) execDMLPK(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, plan *compiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	pkRows, err := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	if err != nil {
		panic(err)
	}
	secondaryList, err := buildSecondaryList(plan.TableInfo, pkRows, plan.SecondaryPKValues, plan.BindVars)
	if err != nil {
		panic(err)
	}

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

func (qe *QueryEngine) execDMLSubquery(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, plan *compiledPlan, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	innerResult := qe.directFetch(logStats, conn, plan.Subquery, plan.BindVars, nil, nil)
	// no need to validate innerResult
	return qe.execDMLPKRows(logStats, conn, plan, innerResult.Rows, invalidator)
}

func (qe *QueryEngine) execDMLPKRows(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, plan *compiledPlan, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	if len(pkRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}
	}
	rowsAffected := uint64(0)
	singleRow := make([][]sqltypes.Value, 1)
	for _, pkRow := range pkRows {
		singleRow[0] = pkRow
		secondaryList, err := buildSecondaryList(plan.TableInfo, singleRow, plan.SecondaryPKValues, plan.BindVars)
		if err != nil {
			panic(err)
		}

		bsc := buildStreamComment(plan.TableInfo, singleRow, secondaryList)
		rowsAffected += qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, pkRow, bsc).RowsAffected
		if invalidator != nil {
			key := buildKey(pkRow)
			invalidator.Delete(key)
		}
	}
	return &mproto.QueryResult{RowsAffected: rowsAffected}
}

func (qe *QueryEngine) execSet(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, plan *compiledPlan) (result *mproto.QueryResult) {
	switch plan.SetKey {
	case "vt_pool_size":
		qe.connPool.SetCapacity(int(getInt64(plan.SetValue)))
	case "vt_stream_pool_size":
		qe.streamConnPool.SetCapacity(int(getInt64(plan.SetValue)))
	case "vt_transaction_cap":
		qe.txPool.SetCapacity(int(getInt64(plan.SetValue)))
	case "vt_transaction_timeout":
		qe.activeTxPool.SetTimeout(getDuration(plan.SetValue))
	case "vt_schema_reload_time":
		qe.schemaInfo.SetReloadTime(getDuration(plan.SetValue))
	case "vt_query_cache_size":
		qe.schemaInfo.SetQueryCacheSize(int(getInt64(plan.SetValue)))
	case "vt_max_result_size":
		val := getInt64(plan.SetValue)
		if val < 1 {
			panic(NewTabletError(FAIL, "max result size out of range %v", val))
		}
		qe.maxResultSize.Set(val)
	case "vt_stream_buffer_size":
		val := getInt64(plan.SetValue)
		if val < 1024 {
			panic(NewTabletError(FAIL, "stream buffer size out of range %v", val))
		}
		qe.streamBufferSize.Set(val)
	case "vt_query_timeout":
		qe.activePool.SetTimeout(getDuration(plan.SetValue))
	case "vt_idle_timeout":
		t := getDuration(plan.SetValue)
		qe.connPool.SetIdleTimeout(t)
		qe.streamConnPool.SetIdleTimeout(t)
		qe.txPool.SetIdleTimeout(t)
		qe.activePool.SetIdleTimeout(t)
	case "vt_spot_check_ratio":
		qe.spotCheckFreq.Set(int64(getFloat64(plan.SetValue) * SPOT_CHECK_MULTIPLIER))
	case "vt_strict_mode":
		qe.strictMode.Set(getInt64(plan.SetValue))
	default:
		return qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
	}
	return &mproto.QueryResult{}
}

func getInt64(v interface{}) int64 {
	if ival, ok := v.(int64); ok {
		return ival
	}
	panic(NewTabletError(FAIL, "expecting int"))
}

func getFloat64(v interface{}) float64 {
	if ival, ok := v.(int64); ok {
		return float64(ival)
	}
	if fval, ok := v.(float64); ok {
		return fval
	}
	panic(NewTabletError(FAIL, "expecting number"))
}

func getDuration(v interface{}) time.Duration {
	return time.Duration(getFloat64(v) * 1e9)
}

func (qe *QueryEngine) qFetch(logStats *SQLQueryStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsedQuery, bindVars, listVars, nil)
	q, ok := qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
		waitingForConnectionStart := time.Now()
		conn, err := qe.connPool.Get()
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

func (qe *QueryEngine) directFetch(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsedQuery, bindVars, listVars, buildStreamComment)
	result, err := qe.executeSql(logStats, conn, sql, false)
	if err != nil {
		panic(err)
	}
	return result
}

// fullFetch also fetches field info
func (qe *QueryEngine) fullFetch(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsedQuery, bindVars, listVars, buildStreamComment)
	result, err := qe.executeSql(logStats, conn, sql, true)
	if err != nil {
		panic(err)
	}
	return result
}

func (qe *QueryEngine) fullStreamFetch(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte, callback func(*mproto.QueryResult) error) {
	sql := qe.generateFinalSql(parsedQuery, bindVars, listVars, buildStreamComment)
	qe.executeStreamSql(logStats, conn, sql, callback)
}

func (qe *QueryEngine) generateFinalSql(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) string {
	bindVars[MAX_RESULT_NAME] = qe.maxResultSize.Get() + 1
	sql, err := parsedQuery.GenerateQuery(bindVars, listVars)
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

func (qe *QueryEngine) executeSql(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, sql string, wantfields bool) (*mproto.QueryResult, error) {
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

func (qe *QueryEngine) executeStreamSql(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, sql string, callback func(*mproto.QueryResult) error) {
	logStats.QuerySources |= QUERY_SOURCE_MYSQL
	logStats.NumberOfQueries += 1
	logStats.AddRewrittenSql(sql)
	fetchStart := time.Now()
	err := conn.ExecuteStreamFetch(sql, callback, int(qe.streamBufferSize.Get()))
	logStats.MysqlResponseTime += time.Now().Sub(fetchStart)
	if err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}
}

func rowsAreEqual(row1, row2 []sqltypes.Value) bool {
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
