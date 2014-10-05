package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

type RequestContext struct {
	ctx      context.Context
	logStats *SQLQueryStats
	sq       *SqlQuery
	qe       *QueryEngine
}

type requestExecutor struct {
	query         string
	bindVars      map[string]interface{}
	transactionID int64
	plan          *ExecPlan
	rq            RequestContext
}

// ExecuteQuery executes a single query request. It fetches (or builds) the plan
// for the query and executes accordingly.
func ExecuteQuery(ctx context.Context, logStats *SQLQueryStats, sq *SqlQuery, query *proto.Query) *mproto.QueryResult {
	// TODO(sougou): Change usage such that we don't have to do this.
	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	rqe := &requestExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          sq.qe.schemaInfo.GetPlan(logStats, query.Sql),
		rq: RequestContext{
			ctx:      ctx,
			logStats: logStats,
			sq:       sq,
			qe:       sq.qe,
		},
	}
	return rqe.execute()
}

// ExecuteStreamQuery executes the query and streams its result.
// The first QueryResult will have Fields set (and Rows nil)
// The subsequent QueryResult will have Rows set (and Fields nil)
func ExecuteStreamQuery(ctx context.Context, logStats *SQLQueryStats, sq *SqlQuery, query *proto.Query, sendReply func(*mproto.QueryResult) error) {
	// TODO(sougou): Change usage such that we don't have to do this.
	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	rqe := &requestExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          sq.qe.schemaInfo.GetStreamPlan(query.Sql),
		rq: RequestContext{
			ctx:      ctx,
			logStats: logStats,
			sq:       sq,
			qe:       sq.qe,
		},
	}
	rqe.stream(sendReply)
}

func (rqe *requestExecutor) stream(sendReply func(*mproto.QueryResult) error) {
	rqe.rq.logStats.OriginalSql = rqe.query
	rqe.rq.logStats.PlanType = rqe.plan.PlanId.String()
	defer queryStats.Record(rqe.plan.PlanId.String(), time.Now())

	rqe.checkPermissions()

	waitingForConnectionStart := time.Now()
	conn := getOrPanic(rqe.rq.qe.streamConnPool)
	rqe.rq.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()

	qd := NewQueryDetail(rqe.query, rqe.rq.logStats.context, conn.Id())
	rqe.rq.qe.streamQList.Add(qd)
	defer rqe.rq.qe.streamQList.Remove(qd)

	rqe.rq.qe.fullStreamFetch(rqe.rq.logStats, conn, rqe.plan.FullQuery, rqe.bindVars, nil, nil, sendReply)
}

func (rqe *requestExecutor) execute() (reply *mproto.QueryResult) {
	rqe.rq.logStats.OriginalSql = rqe.query
	rqe.rq.logStats.BindVariables = rqe.bindVars
	rqe.rq.logStats.TransactionID = rqe.transactionID
	planName := rqe.plan.PlanId.String()
	rqe.rq.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		queryStats.Add(planName, duration)
		if reply == nil {
			rqe.plan.AddStats(1, duration, 0, 1)
		} else {
			rqe.plan.AddStats(1, duration, int64(len(reply.Rows)), 0)
		}
	}(time.Now())

	rqe.checkPermissions()

	if rqe.plan.PlanId == planbuilder.PLAN_DDL {
		return rqe.rq.qe.execDDL(rqe.rq.logStats, rqe.query)
	}

	if rqe.transactionID != 0 {
		// Need upfront connection for DMLs and transactions
		conn := rqe.rq.qe.activeTxPool.Get(rqe.transactionID)
		defer conn.Recycle()
		conn.RecordQuery(rqe.query)
		var invalidator CacheInvalidator
		if rqe.plan.TableInfo != nil && rqe.plan.TableInfo.CacheType != schema.CACHE_NONE {
			invalidator = conn.DirtyKeys(rqe.plan.TableName)
		}
		switch rqe.plan.PlanId {
		case planbuilder.PLAN_PASS_DML:
			if rqe.rq.qe.strictMode.Get() != 0 {
				panic(NewTabletError(FAIL, "DML too complex"))
			}
			reply = rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.FullQuery, rqe.bindVars, nil, nil)
		case planbuilder.PLAN_INSERT_PK:
			reply = rqe.execInsertPK(conn)
		case planbuilder.PLAN_INSERT_SUBQUERY:
			reply = rqe.execInsertSubquery(conn)
		case planbuilder.PLAN_DML_PK:
			reply = rqe.execDMLPK(conn, invalidator)
		case planbuilder.PLAN_DML_SUBQUERY:
			reply = rqe.execDMLSubquery(conn, invalidator)
		case planbuilder.PLAN_OTHER:
			reply = rqe.rq.qe.execSQL(rqe.rq.logStats, conn, rqe.query, true)
		default: // select or set in a transaction, just count as select
			reply = rqe.execDirect(conn)
		}
	} else {
		switch rqe.plan.PlanId {
		case planbuilder.PLAN_PASS_SELECT:
			if rqe.plan.Reason == planbuilder.REASON_LOCK {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			reply = rqe.execSelect()
		case planbuilder.PLAN_PK_EQUAL:
			reply = rqe.execPKEqual()
		case planbuilder.PLAN_PK_IN:
			reply = rqe.execPKIN()
		case planbuilder.PLAN_SELECT_SUBQUERY:
			reply = rqe.execSubquery()
		case planbuilder.PLAN_SET:
			reply = rqe.execSet()
		case planbuilder.PLAN_OTHER:
			waitingForConnectionStart := time.Now()
			conn := getOrPanic(rqe.rq.qe.connPool)
			rqe.rq.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
			defer conn.Recycle()
			reply = rqe.rq.qe.execSQL(rqe.rq.logStats, conn, rqe.query, true)
		default:
			panic(NewTabletError(NOT_IN_TX, "DMLs not allowed outside of transactions"))
		}
	}
	if rqe.plan.PlanId.IsSelect() {
		rqe.rq.logStats.RowsAffected = int(reply.RowsAffected)
		resultStats.Add(int64(reply.RowsAffected))
		rqe.rq.logStats.Rows = reply.Rows
	}

	return reply
}

func (rqe *requestExecutor) checkPermissions() {
	// Blacklist
	action, desc := rqe.plan.Rules.getAction(rqe.rq.ctx.GetRemoteAddr(), rqe.rq.ctx.GetUsername(), rqe.bindVars)
	switch action {
	case QR_FAIL:
		panic(NewTabletError(FAIL, "Query disallowed due to rule: %s", desc))
	case QR_FAIL_RETRY:
		panic(NewTabletError(RETRY, "Query disallowed due to rule: %s", desc))
	}

	// ACLs
	if !rqe.plan.Authorized.IsMember(rqe.rq.ctx.GetUsername()) {
		errStr := fmt.Sprintf("table acl error: %v cannot run %v on table %v", rqe.rq.ctx.GetUsername(), rqe.plan.PlanId, rqe.plan.TableName)
		if rqe.rq.qe.strictTableAcl {
			panic(NewTabletError(FAIL, "%s", errStr))
		}
		rqe.rq.qe.accessCheckerLogger.Errorf("%s", errStr)
	}
}

func (rqe *requestExecutor) execPKEqual() (result *mproto.QueryResult) {
	pkRows, err := buildValueList(rqe.plan.TableInfo, rqe.plan.PKValues, rqe.bindVars)
	if err != nil {
		panic(err)
	}
	if len(pkRows) != 1 || rqe.plan.Fields == nil {
		panic("unexpected")
	}
	row := rqe.fetchOne(pkRows[0])
	result = &mproto.QueryResult{}
	result.Fields = rqe.plan.Fields
	if row == nil {
		return
	}
	result.Rows = make([][]sqltypes.Value, 1)
	result.Rows[0] = applyFilter(rqe.plan.ColumnNumbers, row)
	result.RowsAffected = 1
	return
}

func (rqe *requestExecutor) fetchOne(pk []sqltypes.Value) (row []sqltypes.Value) {
	rqe.rq.logStats.QuerySources |= QUERY_SOURCE_ROWCACHE
	tableInfo := rqe.plan.TableInfo
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	rcresults := tableInfo.Cache.Get(keys)
	rcresult := rcresults[keys[0]]
	if rcresult.Row != nil {
		if rqe.mustVerify() {
			rqe.spotCheck(rcresult, pk)
		}
		rqe.rq.logStats.CacheHits++
		tableInfo.hits.Add(1)
		return rcresult.Row
	}
	resultFromdb := rqe.rq.qe.qFetch(rqe.rq.logStats, rqe.plan.OuterQuery, rqe.bindVars, pk)
	if len(resultFromdb.Rows) == 0 {
		rqe.rq.logStats.CacheAbsent++
		tableInfo.absent.Add(1)
		return nil
	}
	row = resultFromdb.Rows[0]
	tableInfo.Cache.Set(keys[0], row, rcresult.Cas)
	rqe.rq.logStats.CacheMisses++
	tableInfo.misses.Add(1)
	return row
}

func (rqe *requestExecutor) execPKIN() (result *mproto.QueryResult) {
	pkRows, err := buildINValueList(rqe.plan.TableInfo, rqe.plan.PKValues, rqe.bindVars)
	if err != nil {
		panic(err)
	}
	return rqe.fetchMulti(pkRows)
}

func (rqe *requestExecutor) execSubquery() (result *mproto.QueryResult) {
	innerResult := rqe.rq.qe.qFetch(rqe.rq.logStats, rqe.plan.Subquery, rqe.bindVars, nil)
	return rqe.fetchMulti(innerResult.Rows)
}

func (rqe *requestExecutor) fetchMulti(pkRows [][]sqltypes.Value) (result *mproto.QueryResult) {
	result = &mproto.QueryResult{}
	if len(pkRows) == 0 {
		return
	}
	if len(pkRows[0]) != 1 || rqe.plan.Fields == nil {
		panic("unexpected")
	}

	tableInfo := rqe.plan.TableInfo
	keys := make([]string, len(pkRows))
	for i, pk := range pkRows {
		keys[i] = buildKey(pk)
	}
	rcresults := tableInfo.Cache.Get(keys)

	result.Fields = rqe.plan.Fields
	rows := make([][]sqltypes.Value, 0, len(pkRows))
	missingRows := make([]sqltypes.Value, 0, len(pkRows))
	var hits, absent, misses int64
	for i, pk := range pkRows {
		rcresult := rcresults[keys[i]]
		if rcresult.Row != nil {
			if rqe.mustVerify() {
				rqe.spotCheck(rcresult, pk)
			}
			rows = append(rows, applyFilter(rqe.plan.ColumnNumbers, rcresult.Row))
			hits++
		} else {
			missingRows = append(missingRows, pk[0])
		}
	}
	if len(missingRows) != 0 {
		resultFromdb := rqe.rq.qe.qFetch(rqe.rq.logStats, rqe.plan.OuterQuery, rqe.bindVars, missingRows)
		misses = int64(len(resultFromdb.Rows))
		absent = int64(len(pkRows)) - hits - misses
		for _, row := range resultFromdb.Rows {
			rows = append(rows, applyFilter(rqe.plan.ColumnNumbers, row))
			key := buildKey(applyFilter(rqe.plan.TableInfo.PKColumns, row))
			tableInfo.Cache.Set(key, row, rcresults[key].Cas)
		}
	}

	rqe.rq.logStats.CacheHits = hits
	rqe.rq.logStats.CacheAbsent = absent
	rqe.rq.logStats.CacheMisses = misses

	rqe.rq.logStats.QuerySources |= QUERY_SOURCE_ROWCACHE

	tableInfo.hits.Add(hits)
	tableInfo.absent.Add(absent)
	tableInfo.misses.Add(misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	return result
}

func (rqe *requestExecutor) mustVerify() bool {
	return (Rand() % SPOT_CHECK_MULTIPLIER) < rqe.rq.qe.spotCheckFreq.Get()
}

func (rqe *requestExecutor) spotCheck(rcresult RCResult, pk []sqltypes.Value) {
	spotCheckCount.Add(1)
	resultFromdb := rqe.rq.qe.qFetch(rqe.rq.logStats, rqe.plan.OuterQuery, rqe.bindVars, pk)
	var dbrow []sqltypes.Value
	if len(resultFromdb.Rows) != 0 {
		dbrow = resultFromdb.Rows[0]
	}
	if dbrow == nil || !rowsAreEqual(rcresult.Row, dbrow) {
		rqe.recheckLater(rcresult, dbrow, pk)
	}
}

func (rqe *requestExecutor) recheckLater(rcresult RCResult, dbrow []sqltypes.Value, pk []sqltypes.Value) {
	time.Sleep(10 * time.Second)
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	reloaded := rqe.plan.TableInfo.Cache.Get(keys)[keys[0]]
	// If reloaded row is absent or has changed, we're good
	if reloaded.Row == nil || reloaded.Cas != rcresult.Cas {
		return
	}
	log.Warningf("query: %v", rqe.plan.FullQuery)
	log.Warningf("mismatch for: %v\ncache: %v\ndb:    %v", pk, rcresult.Row, dbrow)
	internalErrors.Add("Mismatch", 1)
}

// execDirect always sends the query to mysql
func (rqe *requestExecutor) execDirect(conn dbconnpool.PoolConnection) (result *mproto.QueryResult) {
	if rqe.plan.Fields != nil {
		result = rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.FullQuery, rqe.bindVars, nil, nil)
		result.Fields = rqe.plan.Fields
		return
	}
	result = rqe.rq.qe.fullFetch(rqe.rq.logStats, conn, rqe.plan.FullQuery, rqe.bindVars, nil, nil)
	return
}

// execSelect sends a query to mysql only if another identical query is not running. Otherwise, it waits and
// reuses the result. If the plan is missng field info, it sends the query to mysql requesting full info.
func (rqe *requestExecutor) execSelect() (result *mproto.QueryResult) {
	if rqe.plan.Fields != nil {
		result = rqe.rq.qe.qFetch(rqe.rq.logStats, rqe.plan.FullQuery, rqe.bindVars, nil)
		result.Fields = rqe.plan.Fields
		return
	}
	waitingForConnectionStart := time.Now()
	conn := getOrPanic(rqe.rq.qe.connPool)
	rqe.rq.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()
	result = rqe.rq.qe.fullFetch(rqe.rq.logStats, conn, rqe.plan.FullQuery, rqe.bindVars, nil, nil)
	return
}

func (rqe *requestExecutor) execInsertPK(conn dbconnpool.PoolConnection) (result *mproto.QueryResult) {
	pkRows, err := buildValueList(rqe.plan.TableInfo, rqe.plan.PKValues, rqe.bindVars)
	if err != nil {
		panic(err)
	}
	return rqe.execInsertPKRows(conn, pkRows)
}

func (rqe *requestExecutor) execInsertSubquery(conn dbconnpool.PoolConnection) (result *mproto.QueryResult) {
	innerResult := rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.Subquery, rqe.bindVars, nil, nil)
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}
	}
	if len(rqe.plan.ColumnNumbers) != len(innerRows[0]) {
		panic(NewTabletError(FAIL, "Subquery length does not match column list"))
	}
	pkRows := make([][]sqltypes.Value, len(innerRows))
	for i, innerRow := range innerRows {
		pkRows[i] = applyFilterWithPKDefaults(rqe.plan.TableInfo, rqe.plan.SubqueryPKColumns, innerRow)
	}
	// Validating first row is sufficient
	if err := validateRow(rqe.plan.TableInfo, rqe.plan.TableInfo.PKColumns, pkRows[0]); err != nil {
		panic(err)
	}

	rqe.bindVars["_rowValues"] = innerRows
	return rqe.execInsertPKRows(conn, pkRows)
}

func (rqe *requestExecutor) execInsertPKRows(conn dbconnpool.PoolConnection, pkRows [][]sqltypes.Value) (result *mproto.QueryResult) {
	secondaryList, err := buildSecondaryList(rqe.plan.TableInfo, pkRows, rqe.plan.SecondaryPKValues, rqe.bindVars)
	if err != nil {
		panic(err)
	}
	bsc := buildStreamComment(rqe.plan.TableInfo, pkRows, secondaryList)
	result = rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.OuterQuery, rqe.bindVars, nil, bsc)
	return result
}

func (rqe *requestExecutor) execDMLPK(conn dbconnpool.PoolConnection, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	pkRows, err := buildValueList(rqe.plan.TableInfo, rqe.plan.PKValues, rqe.bindVars)
	if err != nil {
		panic(err)
	}
	secondaryList, err := buildSecondaryList(rqe.plan.TableInfo, pkRows, rqe.plan.SecondaryPKValues, rqe.bindVars)
	if err != nil {
		panic(err)
	}

	bsc := buildStreamComment(rqe.plan.TableInfo, pkRows, secondaryList)
	result = rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.OuterQuery, rqe.bindVars, nil, bsc)
	if invalidator == nil {
		return result
	}
	for _, pk := range pkRows {
		key := buildKey(pk)
		invalidator.Delete(key)
	}
	return result
}

func (rqe *requestExecutor) execDMLSubquery(conn dbconnpool.PoolConnection, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	innerResult := rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.Subquery, rqe.bindVars, nil, nil)
	// no need to validate innerResult
	return rqe.execDMLPKRows(conn, innerResult.Rows, invalidator)
}

func (rqe *requestExecutor) execDMLPKRows(conn dbconnpool.PoolConnection, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	if len(pkRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}
	}
	rowsAffected := uint64(0)
	singleRow := make([][]sqltypes.Value, 1)
	for _, pkRow := range pkRows {
		singleRow[0] = pkRow
		secondaryList, err := buildSecondaryList(rqe.plan.TableInfo, singleRow, rqe.plan.SecondaryPKValues, rqe.bindVars)
		if err != nil {
			panic(err)
		}

		bsc := buildStreamComment(rqe.plan.TableInfo, singleRow, secondaryList)
		rowsAffected += rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.OuterQuery, rqe.bindVars, pkRow, bsc).RowsAffected
		if invalidator != nil {
			key := buildKey(pkRow)
			invalidator.Delete(key)
		}
	}
	return &mproto.QueryResult{RowsAffected: rowsAffected}
}

func (rqe *requestExecutor) execSet() (result *mproto.QueryResult) {
	switch rqe.plan.SetKey {
	case "vt_pool_size":
		rqe.rq.qe.connPool.SetCapacity(int(getInt64(rqe.plan.SetValue)))
	case "vt_stream_pool_size":
		rqe.rq.qe.streamConnPool.SetCapacity(int(getInt64(rqe.plan.SetValue)))
	case "vt_transaction_cap":
		rqe.rq.qe.txPool.SetCapacity(int(getInt64(rqe.plan.SetValue)))
	case "vt_transaction_timeout":
		rqe.rq.qe.activeTxPool.SetTimeout(getDuration(rqe.plan.SetValue))
	case "vt_schema_reload_time":
		rqe.rq.qe.schemaInfo.SetReloadTime(getDuration(rqe.plan.SetValue))
	case "vt_query_cache_size":
		rqe.rq.qe.schemaInfo.SetQueryCacheSize(int(getInt64(rqe.plan.SetValue)))
	case "vt_max_result_size":
		val := getInt64(rqe.plan.SetValue)
		if val < 1 {
			panic(NewTabletError(FAIL, "max result size out of range %v", val))
		}
		rqe.rq.qe.maxResultSize.Set(val)
	case "vt_stream_buffer_size":
		val := getInt64(rqe.plan.SetValue)
		if val < 1024 {
			panic(NewTabletError(FAIL, "stream buffer size out of range %v", val))
		}
		rqe.rq.qe.streamBufferSize.Set(val)
	case "vt_query_timeout":
		rqe.rq.qe.queryTimeout.Set(getDuration(rqe.plan.SetValue))
	case "vt_idle_timeout":
		t := getDuration(rqe.plan.SetValue)
		rqe.rq.qe.connPool.SetIdleTimeout(t)
		rqe.rq.qe.streamConnPool.SetIdleTimeout(t)
		rqe.rq.qe.txPool.SetIdleTimeout(t)
		rqe.rq.qe.connKiller.SetIdleTimeout(t)
	case "vt_spot_check_ratio":
		rqe.rq.qe.spotCheckFreq.Set(int64(getFloat64(rqe.plan.SetValue) * SPOT_CHECK_MULTIPLIER))
	case "vt_strict_mode":
		rqe.rq.qe.strictMode.Set(getInt64(rqe.plan.SetValue))
	default:
		waitingForConnectionStart := time.Now()
		conn := getOrPanic(rqe.rq.qe.connPool)
		rqe.rq.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		defer conn.Recycle()
		return rqe.rq.qe.directFetch(rqe.rq.logStats, conn, rqe.plan.FullQuery, rqe.bindVars, nil, nil)
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
