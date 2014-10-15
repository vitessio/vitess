package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
)

// QueryExecutor is used for executing a query request.
type QueryExecutor struct {
	query         string
	bindVars      map[string]interface{}
	transactionID int64
	plan          *ExecPlan
	RequestContext
}

// Execute performs a non-streaming query execution.
func (qre *QueryExecutor) Execute() (reply *mproto.QueryResult) {
	qre.logStats.OriginalSql = qre.query
	qre.logStats.BindVariables = qre.bindVars
	qre.logStats.TransactionID = qre.transactionID
	planName := qre.plan.PlanId.String()
	qre.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		queryStats.Add(planName, duration)
		if reply == nil {
			qre.plan.AddStats(1, duration, 0, 1)
		} else {
			qre.plan.AddStats(1, duration, int64(len(reply.Rows)), 0)
		}
	}(time.Now())

	qre.checkPermissions()

	if qre.plan.PlanId == planbuilder.PLAN_DDL {
		return qre.execDDL()
	}

	if qre.transactionID != 0 {
		// Need upfront connection for DMLs and transactions
		conn := qre.qe.txPool.Get(qre.transactionID)
		defer conn.Recycle()
		conn.RecordQuery(qre.query)
		var invalidator CacheInvalidator
		if qre.plan.TableInfo != nil && qre.plan.TableInfo.CacheType != schema.CACHE_NONE {
			invalidator = conn.DirtyKeys(qre.plan.TableName)
		}
		switch qre.plan.PlanId {
		case planbuilder.PLAN_PASS_DML:
			if qre.qe.strictMode.Get() != 0 {
				panic(NewTabletError(FAIL, "DML too complex"))
			}
			reply = qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil)
		case planbuilder.PLAN_INSERT_PK:
			reply = qre.execInsertPK(conn)
		case planbuilder.PLAN_INSERT_SUBQUERY:
			reply = qre.execInsertSubquery(conn)
		case planbuilder.PLAN_DML_PK:
			reply = qre.execDMLPK(conn, invalidator)
		case planbuilder.PLAN_DML_SUBQUERY:
			reply = qre.execDMLSubquery(conn, invalidator)
		case planbuilder.PLAN_OTHER:
			reply = qre.execSQL(conn, qre.query, true)
		default: // select or set in a transaction, just count as select
			reply = qre.execDirect(conn)
		}
	} else {
		switch qre.plan.PlanId {
		case planbuilder.PLAN_PASS_SELECT:
			if qre.plan.Reason == planbuilder.REASON_LOCK {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			reply = qre.execSelect()
		case planbuilder.PLAN_PK_EQUAL:
			reply = qre.execPKEqual()
		case planbuilder.PLAN_PK_IN:
			reply = qre.execPKIN()
		case planbuilder.PLAN_SELECT_SUBQUERY:
			reply = qre.execSubquery()
		case planbuilder.PLAN_SET:
			reply = qre.execSet()
		case planbuilder.PLAN_OTHER:
			waitingForConnectionStart := time.Now()
			conn := getOrPanic(qre.qe.connPool)
			qre.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
			defer conn.Recycle()
			reply = qre.execSQL(conn, qre.query, true)
		default:
			panic(NewTabletError(NOT_IN_TX, "DMLs not allowed outside of transactions"))
		}
	}
	if qre.plan.PlanId.IsSelect() {
		qre.logStats.RowsAffected = int(reply.RowsAffected)
		resultStats.Add(int64(reply.RowsAffected))
		qre.logStats.Rows = reply.Rows
	}

	return reply
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(sendReply func(*mproto.QueryResult) error) {
	qre.logStats.OriginalSql = qre.query
	qre.logStats.PlanType = qre.plan.PlanId.String()
	defer queryStats.Record(qre.plan.PlanId.String(), time.Now())

	qre.checkPermissions()

	waitingForConnectionStart := time.Now()
	conn := getOrPanic(qre.qe.streamConnPool)
	qre.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()

	qd := NewQueryDetail(qre.query, qre.logStats.context, conn.Id())
	qre.qe.streamQList.Add(qd)
	defer qre.qe.streamQList.Remove(qd)

	qre.fullStreamFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil, sendReply)
}

func (qre *QueryExecutor) checkPermissions() {
	// Blacklist
	action, desc := qre.plan.Rules.getAction(qre.ctx.GetRemoteAddr(), qre.ctx.GetUsername(), qre.bindVars)
	switch action {
	case QR_FAIL:
		panic(NewTabletError(FAIL, "Query disallowed due to rule: %s", desc))
	case QR_FAIL_RETRY:
		panic(NewTabletError(RETRY, "Query disallowed due to rule: %s", desc))
	}

	// ACLs
	if !qre.plan.Authorized.IsMember(qre.ctx.GetUsername()) {
		errStr := fmt.Sprintf("table acl error: %v cannot run %v on table %v", qre.ctx.GetUsername(), qre.plan.PlanId, qre.plan.TableName)
		if qre.qe.strictTableAcl {
			panic(NewTabletError(FAIL, "%s", errStr))
		}
		qre.qe.accessCheckerLogger.Errorf("%s", errStr)
	}
}

func (qre *QueryExecutor) execDDL() *mproto.QueryResult {
	ddlPlan := planbuilder.DDLParse(qre.query)
	if ddlPlan.Action == "" {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}

	txid := qre.qe.txPool.Begin()
	defer qre.qe.txPool.SafeCommit(txid)

	// Stolen from Execute
	conn := qre.qe.txPool.Get(txid)
	defer conn.Recycle()
	result := qre.execSQL(conn, qre.query, false)

	if ddlPlan.TableName != "" && ddlPlan.TableName != ddlPlan.NewName {
		// It's a drop or rename.
		qre.qe.schemaInfo.DropTable(ddlPlan.TableName)
	}
	if ddlPlan.NewName != "" {
		qre.qe.schemaInfo.CreateOrUpdateTable(ddlPlan.NewName)
	}
	return result
}

func (qre *QueryExecutor) execPKEqual() (result *mproto.QueryResult) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		panic(err)
	}
	if len(pkRows) != 1 || qre.plan.Fields == nil {
		panic("unexpected")
	}
	row := qre.fetchOne(pkRows[0])
	result = &mproto.QueryResult{}
	result.Fields = qre.plan.Fields
	if row == nil {
		return
	}
	result.Rows = make([][]sqltypes.Value, 1)
	result.Rows[0] = applyFilter(qre.plan.ColumnNumbers, row)
	result.RowsAffected = 1
	return
}

func (qre *QueryExecutor) fetchOne(pk []sqltypes.Value) (row []sqltypes.Value) {
	qre.logStats.QuerySources |= QUERY_SOURCE_ROWCACHE
	tableInfo := qre.plan.TableInfo
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	rcresults := tableInfo.Cache.Get(keys)
	rcresult := rcresults[keys[0]]
	if rcresult.Row != nil {
		if qre.mustVerify() {
			qre.spotCheck(rcresult, pk)
		}
		qre.logStats.CacheHits++
		tableInfo.hits.Add(1)
		return rcresult.Row
	}
	resultFromdb := qre.qFetch(qre.logStats, qre.plan.OuterQuery, qre.bindVars, pk)
	if len(resultFromdb.Rows) == 0 {
		qre.logStats.CacheAbsent++
		tableInfo.absent.Add(1)
		return nil
	}
	row = resultFromdb.Rows[0]
	tableInfo.Cache.Set(keys[0], row, rcresult.Cas)
	qre.logStats.CacheMisses++
	tableInfo.misses.Add(1)
	return row
}

func (qre *QueryExecutor) execPKIN() (result *mproto.QueryResult) {
	pkRows, err := buildINValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		panic(err)
	}
	return qre.fetchMulti(pkRows)
}

func (qre *QueryExecutor) execSubquery() (result *mproto.QueryResult) {
	innerResult := qre.qFetch(qre.logStats, qre.plan.Subquery, qre.bindVars, nil)
	return qre.fetchMulti(innerResult.Rows)
}

func (qre *QueryExecutor) fetchMulti(pkRows [][]sqltypes.Value) (result *mproto.QueryResult) {
	result = &mproto.QueryResult{}
	if len(pkRows) == 0 {
		return
	}
	if len(pkRows[0]) != 1 || qre.plan.Fields == nil {
		panic("unexpected")
	}

	tableInfo := qre.plan.TableInfo
	keys := make([]string, len(pkRows))
	for i, pk := range pkRows {
		keys[i] = buildKey(pk)
	}
	rcresults := tableInfo.Cache.Get(keys)

	result.Fields = qre.plan.Fields
	rows := make([][]sqltypes.Value, 0, len(pkRows))
	missingRows := make([]sqltypes.Value, 0, len(pkRows))
	var hits, absent, misses int64
	for i, pk := range pkRows {
		rcresult := rcresults[keys[i]]
		if rcresult.Row != nil {
			if qre.mustVerify() {
				qre.spotCheck(rcresult, pk)
			}
			rows = append(rows, applyFilter(qre.plan.ColumnNumbers, rcresult.Row))
			hits++
		} else {
			missingRows = append(missingRows, pk[0])
		}
	}
	if len(missingRows) != 0 {
		resultFromdb := qre.qFetch(qre.logStats, qre.plan.OuterQuery, qre.bindVars, missingRows)
		misses = int64(len(resultFromdb.Rows))
		absent = int64(len(pkRows)) - hits - misses
		for _, row := range resultFromdb.Rows {
			rows = append(rows, applyFilter(qre.plan.ColumnNumbers, row))
			key := buildKey(applyFilter(qre.plan.TableInfo.PKColumns, row))
			tableInfo.Cache.Set(key, row, rcresults[key].Cas)
		}
	}

	qre.logStats.CacheHits = hits
	qre.logStats.CacheAbsent = absent
	qre.logStats.CacheMisses = misses

	qre.logStats.QuerySources |= QUERY_SOURCE_ROWCACHE

	tableInfo.hits.Add(hits)
	tableInfo.absent.Add(absent)
	tableInfo.misses.Add(misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	return result
}

func (qre *QueryExecutor) mustVerify() bool {
	return (Rand() % SPOT_CHECK_MULTIPLIER) < qre.qe.spotCheckFreq.Get()
}

func (qre *QueryExecutor) spotCheck(rcresult RCResult, pk []sqltypes.Value) {
	spotCheckCount.Add(1)
	resultFromdb := qre.qFetch(qre.logStats, qre.plan.OuterQuery, qre.bindVars, pk)
	var dbrow []sqltypes.Value
	if len(resultFromdb.Rows) != 0 {
		dbrow = resultFromdb.Rows[0]
	}
	if dbrow == nil || !rowsAreEqual(rcresult.Row, dbrow) {
		qre.qe.Launch(func() { qre.recheckLater(rcresult, dbrow, pk) })
	}
}

func (qre *QueryExecutor) recheckLater(rcresult RCResult, dbrow []sqltypes.Value, pk []sqltypes.Value) {
	time.Sleep(10 * time.Second)
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	reloaded := qre.plan.TableInfo.Cache.Get(keys)[keys[0]]
	// If reloaded row is absent or has changed, we're good
	if reloaded.Row == nil || reloaded.Cas != rcresult.Cas {
		return
	}
	log.Warningf("query: %v", qre.plan.FullQuery)
	log.Warningf("mismatch for: %v\ncache: %v\ndb:    %v", pk, rcresult.Row, dbrow)
	internalErrors.Add("Mismatch", 1)
}

// execDirect always sends the query to mysql
func (qre *QueryExecutor) execDirect(conn dbconnpool.PoolConnection) (result *mproto.QueryResult) {
	if qre.plan.Fields != nil {
		result = qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil)
		result.Fields = qre.plan.Fields
		return
	}
	result = qre.fullFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil)
	return
}

// execSelect sends a query to mysql only if another identical query is not running. Otherwise, it waits and
// reuses the result. If the plan is missng field info, it sends the query to mysql requesting full info.
func (qre *QueryExecutor) execSelect() (result *mproto.QueryResult) {
	if qre.plan.Fields != nil {
		result = qre.qFetch(qre.logStats, qre.plan.FullQuery, qre.bindVars, nil)
		result.Fields = qre.plan.Fields
		return
	}
	waitingForConnectionStart := time.Now()
	conn := getOrPanic(qre.qe.connPool)
	qre.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()
	result = qre.fullFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil)
	return
}

func (qre *QueryExecutor) execInsertPK(conn dbconnpool.PoolConnection) (result *mproto.QueryResult) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		panic(err)
	}
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertSubquery(conn dbconnpool.PoolConnection) (result *mproto.QueryResult) {
	innerResult := qre.directFetch(conn, qre.plan.Subquery, qre.bindVars, nil, nil)
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}
	}
	if len(qre.plan.ColumnNumbers) != len(innerRows[0]) {
		panic(NewTabletError(FAIL, "Subquery length does not match column list"))
	}
	pkRows := make([][]sqltypes.Value, len(innerRows))
	for i, innerRow := range innerRows {
		pkRows[i] = applyFilterWithPKDefaults(qre.plan.TableInfo, qre.plan.SubqueryPKColumns, innerRow)
	}
	// Validating first row is sufficient
	if err := validateRow(qre.plan.TableInfo, qre.plan.TableInfo.PKColumns, pkRows[0]); err != nil {
		panic(err)
	}

	qre.bindVars["_rowValues"] = innerRows
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertPKRows(conn dbconnpool.PoolConnection, pkRows [][]sqltypes.Value) (result *mproto.QueryResult) {
	secondaryList, err := buildSecondaryList(qre.plan.TableInfo, pkRows, qre.plan.SecondaryPKValues, qre.bindVars)
	if err != nil {
		panic(err)
	}
	bsc := buildStreamComment(qre.plan.TableInfo, pkRows, secondaryList)
	result = qre.directFetch(conn, qre.plan.OuterQuery, qre.bindVars, nil, bsc)
	return result
}

func (qre *QueryExecutor) execDMLPK(conn dbconnpool.PoolConnection, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		panic(err)
	}
	secondaryList, err := buildSecondaryList(qre.plan.TableInfo, pkRows, qre.plan.SecondaryPKValues, qre.bindVars)
	if err != nil {
		panic(err)
	}

	bsc := buildStreamComment(qre.plan.TableInfo, pkRows, secondaryList)
	result = qre.directFetch(conn, qre.plan.OuterQuery, qre.bindVars, nil, bsc)
	if invalidator == nil {
		return result
	}
	for _, pk := range pkRows {
		key := buildKey(pk)
		invalidator.Delete(key)
	}
	return result
}

func (qre *QueryExecutor) execDMLSubquery(conn dbconnpool.PoolConnection, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	innerResult := qre.directFetch(conn, qre.plan.Subquery, qre.bindVars, nil, nil)
	// no need to validate innerResult
	return qre.execDMLPKRows(conn, innerResult.Rows, invalidator)
}

func (qre *QueryExecutor) execDMLPKRows(conn dbconnpool.PoolConnection, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *mproto.QueryResult) {
	if len(pkRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}
	}
	rowsAffected := uint64(0)
	singleRow := make([][]sqltypes.Value, 1)
	for _, pkRow := range pkRows {
		singleRow[0] = pkRow
		secondaryList, err := buildSecondaryList(qre.plan.TableInfo, singleRow, qre.plan.SecondaryPKValues, qre.bindVars)
		if err != nil {
			panic(err)
		}

		bsc := buildStreamComment(qre.plan.TableInfo, singleRow, secondaryList)
		rowsAffected += qre.directFetch(conn, qre.plan.OuterQuery, qre.bindVars, pkRow, bsc).RowsAffected
		if invalidator != nil {
			key := buildKey(pkRow)
			invalidator.Delete(key)
		}
	}
	return &mproto.QueryResult{RowsAffected: rowsAffected}
}

func (qre *QueryExecutor) execSet() (result *mproto.QueryResult) {
	switch qre.plan.SetKey {
	case "vt_pool_size":
		qre.qe.connPool.SetCapacity(int(getInt64(qre.plan.SetValue)))
	case "vt_stream_pool_size":
		qre.qe.streamConnPool.SetCapacity(int(getInt64(qre.plan.SetValue)))
	case "vt_transaction_cap":
		qre.qe.txPool.pool.SetCapacity(int(getInt64(qre.plan.SetValue)))
	case "vt_transaction_timeout":
		qre.qe.txPool.SetTimeout(getDuration(qre.plan.SetValue))
	case "vt_schema_reload_time":
		qre.qe.schemaInfo.SetReloadTime(getDuration(qre.plan.SetValue))
	case "vt_query_cache_size":
		qre.qe.schemaInfo.SetQueryCacheSize(int(getInt64(qre.plan.SetValue)))
	case "vt_max_result_size":
		val := getInt64(qre.plan.SetValue)
		if val < 1 {
			panic(NewTabletError(FAIL, "max result size out of range %v", val))
		}
		qre.qe.maxResultSize.Set(val)
	case "vt_stream_buffer_size":
		val := getInt64(qre.plan.SetValue)
		if val < 1024 {
			panic(NewTabletError(FAIL, "stream buffer size out of range %v", val))
		}
		qre.qe.streamBufferSize.Set(val)
	case "vt_query_timeout":
		qre.qe.queryTimeout.Set(getDuration(qre.plan.SetValue))
	case "vt_idle_timeout":
		t := getDuration(qre.plan.SetValue)
		qre.qe.connPool.SetIdleTimeout(t)
		qre.qe.streamConnPool.SetIdleTimeout(t)
		qre.qe.txPool.pool.SetIdleTimeout(t)
		qre.qe.connKiller.SetIdleTimeout(t)
	case "vt_spot_check_ratio":
		qre.qe.spotCheckFreq.Set(int64(getFloat64(qre.plan.SetValue) * SPOT_CHECK_MULTIPLIER))
	case "vt_strict_mode":
		qre.qe.strictMode.Set(getInt64(qre.plan.SetValue))
	default:
		waitingForConnectionStart := time.Now()
		conn := getOrPanic(qre.qe.connPool)
		qre.logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		defer conn.Recycle()
		return qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil)
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
