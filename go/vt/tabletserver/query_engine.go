// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/hack"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
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
	dbconfig   *dbconfigs.DBConfig

	// Pools
	cachePool      *CachePool
	connPool       *dbconnpool.ConnectionPool
	streamConnPool *dbconnpool.ConnectionPool
	txPool         *dbconnpool.ConnectionPool

	// Services
	activeTxPool *ActiveTxPool
	consolidator *Consolidator
	invalidator  *RowcacheInvalidator
	streamQList  *QueryList
	connKiller   *ConnectionKiller

	// Vars
	queryTimeout     sync2.AtomicDuration
	spotCheckFreq    sync2.AtomicInt64
	strictMode       sync2.AtomicInt64
	maxResultSize    sync2.AtomicInt64
	streamBufferSize sync2.AtomicInt64
	strictTableAcl   bool

	// loggers
	accessCheckerLogger *logutil.ThrottledLogger
}

type compiledPlan struct {
	Query string
	*ExecPlan
	BindVars      map[string]interface{}
	TransactionID int64
}

var (
	// stats are globals to allow anybody to set them
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

	resultBuckets = []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}

	connPoolClosedErr = NewTabletError(FATAL, "connection pool is closed")
)

// CacheInvalidator provides the abstraction needed for an instant invalidation
// vs. delayed invalidation in the case of in-transaction dmls
type CacheInvalidator interface {
	Delete(key string)
}

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
	qe.schemaInfo = NewSchemaInfo(config.QueryCacheSize, time.Duration(config.SchemaReloadTime*1e9), time.Duration(config.IdleTimeout*1e9))

	mysqlStats = stats.NewTimings("Mysql")

	// Pools
	qe.cachePool = NewCachePool("Rowcache", config.RowCache, time.Duration(config.QueryTimeout*1e9), time.Duration(config.IdleTimeout*1e9))
	qe.connPool = dbconnpool.NewConnectionPool("ConnPool", config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.streamConnPool = dbconnpool.NewConnectionPool("StreamConnPool", config.StreamPoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.txPool = dbconnpool.NewConnectionPool("TransactionPool", config.TransactionCap, time.Duration(config.IdleTimeout*1e9)) // connections in pool has to be > transactionCap

	// Services
	qe.activeTxPool = NewActiveTxPool("ActiveTransactionPool", time.Duration(config.TransactionTimeout*1e9))
	qe.connKiller = NewConnectionKiller(1, time.Duration(config.IdleTimeout*1e9))
	qe.consolidator = NewConsolidator()
	qe.invalidator = NewRowcacheInvalidator(qe)
	qe.streamQList = NewQueryList(qe.connKiller)

	// Vars
	qe.queryTimeout.Set(time.Duration(config.QueryTimeout * 1e9))
	qe.spotCheckFreq = sync2.AtomicInt64(config.SpotCheckRatio * SPOT_CHECK_MULTIPLIER)
	if config.StrictMode {
		qe.strictMode.Set(1)
	}
	qe.strictTableAcl = config.StrictTableAcl
	qe.maxResultSize = sync2.AtomicInt64(config.MaxResultSize)
	qe.streamBufferSize = sync2.AtomicInt64(config.StreamBufferSize)

	// loggers
	qe.accessCheckerLogger = logutil.NewThrottledLogger("accessChecker", 1*time.Second)

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
	qe.dbconfig = dbconfig
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
	qe.connKiller.Open(connFactory)
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
	qe.connKiller.Close()
	qe.activeTxPool.Close()
	qe.txPool.Close()
	qe.streamConnPool.Close()
	qe.connPool.Close()
	qe.invalidator.Close()
	qe.schemaInfo.Close()
	qe.cachePool.Close()
	qe.dbconfig = nil
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
		qe.activeTxPool.LogActive()
		panic(NewTabletError(TX_POOL_FULL, "Transaction pool connection limit exceeded"))
	}
	transactionID, err := qe.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	return transactionID
}

// Commit commits the specified transaction.
func (qe *QueryEngine) Commit(logStats *SQLQueryStats, transactionID int64) {
	defer queryStats.Record("COMMIT", time.Now())
	dirtyTables, err := qe.activeTxPool.SafeCommit(transactionID)
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
func (qe *QueryEngine) Rollback(logStats *SQLQueryStats, transactionID int64) {
	defer queryStats.Record("ROLLBACK", time.Now())
	qe.activeTxPool.Rollback(transactionID)
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
	logStats.PlanType = plan.PlanId.String()
	logStats.OriginalSql = query.Sql
	defer queryStats.Record(plan.PlanId.String(), time.Now())

	authorized := tableacl.Authorized(plan.TableName, plan.PlanId.MinRole())
	qe.checkTableAcl(plan.TableName, plan.PlanId, authorized, logStats.context.GetUsername())

	// does the real work: first get a connection
	waitingForConnectionStart := time.Now()
	conn := getOrPanic(qe.streamConnPool)
	logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()

	qd := NewQueryDetail(query, logStats.context, conn.Id())
	qe.streamQList.Add(qd)
	defer qe.streamQList.Remove(qd)

	// then let's stream! Wrap callback function to return an
	// error on query termination to stop further streaming
	qe.fullStreamFetch(logStats, conn, plan.FullQuery, query.BindVariables, nil, nil, sendReply)
}

func (qe *QueryEngine) checkTableAcl(table string, planId planbuilder.PlanType, authorized tableacl.ACL, user string) {
	if !authorized.IsMember(user) {
		err := fmt.Sprintf("table acl error: %v cannot run %v on table %v", user, planId, table)
		if qe.strictTableAcl {
			panic(NewTabletError(FAIL, err))
		}
		qe.accessCheckerLogger.Errorf(err)
	}
}

// InvalidateForDml performs rowcache invalidations for the dml.
func (qe *QueryEngine) InvalidateForDml(table string, keys []string) {
	if qe.cachePool.IsClosed() {
		return
	}
	invalidations := int64(0)
	tableInfo := qe.schemaInfo.GetTable(table)
	if tableInfo == nil {
		panic(NewTabletError(FAIL, "Table %s not found", table))
	}
	if tableInfo.CacheType == schema.CACHE_NONE {
		return
	}
	for _, val := range keys {
		newKey := validateKey(tableInfo, val)
		if newKey != "" {
			tableInfo.Cache.Delete(newKey)
		}
		invalidations++
	}
	tableInfo.invalidations.Add(invalidations)
}

// InvalidateForDDL performs schema and rowcache changes for the ddl.
func (qe *QueryEngine) InvalidateForDDL(ddl string) {
	ddlPlan := planbuilder.DDLParse(ddl)
	if ddlPlan.Action == "" {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}
	if ddlPlan.TableName != "" && ddlPlan.TableName != ddlPlan.NewName {
		// It's a drop or rename.
		qe.schemaInfo.DropTable(ddlPlan.TableName)
	}
	if ddlPlan.NewName != "" {
		qe.schemaInfo.CreateOrUpdateTable(ddlPlan.NewName)
	}
}

// InvalidateForUnrecognized performs best effort rowcache invalidation
// for unrecognized statements.
func (qe *QueryEngine) InvalidateForUnrecognized(sql string) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		log.Errorf("Error: %v: %s", err, sql)
		internalErrors.Add("Invalidation", 1)
		return
	}
	var table *sqlparser.TableName
	switch stmt := statement.(type) {
	case *sqlparser.Insert:
		// Inserts don't affect rowcache.
		return
	case *sqlparser.Update:
		table = stmt.Table
	case *sqlparser.Delete:
		table = stmt.Table
	default:
		log.Errorf("Unrecognized: %s", sql)
		internalErrors.Add("Invalidation", 1)
		return
	}

	// Ignore cross-db statements.
	if table.Qualifier != nil && string(table.Qualifier) != qe.dbconfig.DbName {
		return
	}

	// Ignore if it's an uncached table.
	tableName := string(table.Name)
	tableInfo := qe.schemaInfo.GetTable(tableName)
	if tableInfo == nil {
		log.Errorf("Table %s not found: %s", tableName, sql)
		internalErrors.Add("Invalidation", 1)
		return
	}
	if tableInfo.CacheType == schema.CACHE_NONE {
		return
	}

	// Treat the statement as a DDL.
	// It will conservatively invalidate all rows of the table.
	log.Warningf("Treating '%s' as DDL for table %s", sql, tableName)
	qe.schemaInfo.CreateOrUpdateTable(tableName)
}

//-----------------------------------------------
// DDL

func (qe *QueryEngine) execDDL(logStats *SQLQueryStats, ddl string) *mproto.QueryResult {
	ddlPlan := planbuilder.DDLParse(ddl)
	if ddlPlan.Action == "" {
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
	result := qe.execSQL(logStats, conn, ddl, false)
	if ddlPlan.TableName != "" && ddlPlan.TableName != ddlPlan.NewName {
		// It's a drop or rename.
		qe.schemaInfo.DropTable(ddlPlan.TableName)
	}
	if ddlPlan.NewName != "" {
		qe.schemaInfo.CreateOrUpdateTable(ddlPlan.NewName)
	}
	return result
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
			q.Result, q.Err = qe.execSQLNoPanic(logStats, conn, sql, false)
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
	return qe.execSQL(logStats, conn, sql, false)
}

// fullFetch also fetches field info
func (qe *QueryEngine) fullFetch(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := qe.generateFinalSql(parsedQuery, bindVars, listVars, buildStreamComment)
	return qe.execSQL(logStats, conn, sql, true)
}

func (qe *QueryEngine) fullStreamFetch(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte, callback func(*mproto.QueryResult) error) {
	sql := qe.generateFinalSql(parsedQuery, bindVars, listVars, buildStreamComment)
	qe.execStreamSQL(logStats, conn, sql, callback)
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

func (qe *QueryEngine) execSQL(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, sql string, wantfields bool) *mproto.QueryResult {
	result, err := qe.execSQLNoPanic(logStats, conn, sql, true)
	if err != nil {
		panic(err)
	}
	return result
}

func (qe *QueryEngine) execSQLNoPanic(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, sql string, wantfields bool) (*mproto.QueryResult, error) {
	if timeout := qe.queryTimeout.Get(); timeout != 0 {
		qd := qe.connKiller.SetDeadline(conn.Id(), time.Now().Add(timeout))
		defer qd.Done()
	}

	logStats.QuerySources |= QUERY_SOURCE_MYSQL
	logStats.NumberOfQueries++
	logStats.AddRewrittenSql(sql)

	fetchStart := time.Now()
	result, err := conn.ExecuteFetch(sql, int(qe.maxResultSize.Get()), wantfields)
	logStats.MysqlResponseTime += time.Now().Sub(fetchStart)

	if err != nil {
		return nil, NewTabletErrorSql(FAIL, err)
	}
	return result, nil
}

func (qe *QueryEngine) execStreamSQL(logStats *SQLQueryStats, conn dbconnpool.PoolConnection, sql string, callback func(*mproto.QueryResult) error) {
	logStats.QuerySources |= QUERY_SOURCE_MYSQL
	logStats.NumberOfQueries++
	logStats.AddRewrittenSql(sql)
	fetchStart := time.Now()
	err := conn.ExecuteStreamFetch(sql, callback, int(qe.streamBufferSize.Get()))
	logStats.MysqlResponseTime += time.Now().Sub(fetchStart)
	if err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}
}

// SplitQuery uses QuerySplitter to split a BoundQuery into smaller queries
// that return a subset of rows from the original query.
func (qe *QueryEngine) SplitQuery(logStats *SQLQueryStats, query *proto.BoundQuery, splitCount int) ([]proto.QuerySplit, error) {
	splitter := NewQuerySplitter(query, splitCount, qe.schemaInfo)
	err := splitter.validateQuery()
	if err != nil {
		return nil, NewTabletError(FAIL, "query validation error: %s", err)
	}
	conn := getOrPanic(qe.connPool)
	// TODO: For fetching pkMinMax, include where clauses on the
	// primary key, if any, in the original query which might give a narrower
	// range of PKs to work with.
	minMaxSql := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v", splitter.pkCol, splitter.pkCol, splitter.tableName)
	pkMinMax := qe.execSQL(logStats, conn, minMaxSql, true)
	return splitter.split(pkMinMax), nil
}
