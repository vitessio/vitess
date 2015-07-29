// Copyright 2014, Google Inc. All rights reserved.
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
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"golang.org/x/net/context"
)

// QueryExecutor is used for executing a query request.
type QueryExecutor struct {
	query         string
	bindVars      map[string]interface{}
	transactionID int64
	plan          *ExecPlan
	ctx           context.Context
	logStats      *SQLQueryStats
	qe            *QueryEngine
}

// poolConn is the interface implemented by users of this specialized pool.
type poolConn interface {
	Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*mproto.QueryResult, error)
}

// Execute performs a non-streaming query execution.
func (qre *QueryExecutor) Execute() (reply *mproto.QueryResult, err error) {
	qre.logStats.OriginalSql = qre.query
	qre.logStats.BindVariables = qre.bindVars
	qre.logStats.TransactionID = qre.transactionID
	planName := qre.plan.PlanId.String()
	qre.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		qre.qe.queryServiceStats.QueryStats.Add(planName, duration)
		if reply == nil {
			qre.plan.AddStats(1, duration, 0, 1)
			return
		}
		qre.plan.AddStats(1, duration, int64(reply.RowsAffected), 0)
		qre.logStats.RowsAffected = int(reply.RowsAffected)
		qre.logStats.Rows = reply.Rows
		qre.qe.queryServiceStats.ResultStats.Add(int64(len(reply.Rows)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return nil, err
	}

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
				return nil, NewTabletError(ErrFail, "DML too complex")
			}
			reply, err = qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
		case planbuilder.PLAN_INSERT_PK:
			reply, err = qre.execInsertPK(conn)
		case planbuilder.PLAN_INSERT_SUBQUERY:
			reply, err = qre.execInsertSubquery(conn)
		case planbuilder.PLAN_DML_PK:
			reply, err = qre.execDMLPK(conn, invalidator)
		case planbuilder.PLAN_DML_SUBQUERY:
			reply, err = qre.execDMLSubquery(conn, invalidator)
		case planbuilder.PLAN_OTHER:
			reply, err = qre.execSQL(conn, qre.query, true)
		default: // select or set in a transaction, just count as select
			reply, err = qre.execDirect(conn)
		}
	} else {
		switch qre.plan.PlanId {
		case planbuilder.PLAN_PASS_SELECT:
			if qre.plan.Reason == planbuilder.REASON_LOCK {
				return nil, NewTabletError(ErrFail, "Disallowed outside transaction")
			}
			reply, err = qre.execSelect()
		case planbuilder.PLAN_PK_IN:
			reply, err = qre.execPKIN()
		case planbuilder.PLAN_SELECT_SUBQUERY:
			reply, err = qre.execSubquery()
		case planbuilder.PLAN_SET:
			reply, err = qre.execSet()
		case planbuilder.PLAN_OTHER:
			conn, connErr := qre.getConn(qre.qe.connPool)
			if connErr != nil {
				return nil, connErr
			}
			defer conn.Recycle()
			reply, err = qre.execSQL(conn, qre.query, true)
		default:
			if !qre.qe.enableAutoCommit {
				return nil, NewTabletError(ErrFatal, "unsupported query: %s", qre.query)
			}
			reply, err = qre.execDmlAutoCommit()
		}
	}
	return reply, err
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(sendReply func(*mproto.QueryResult) error) error {
	qre.logStats.OriginalSql = qre.query
	qre.logStats.PlanType = qre.plan.PlanId.String()
	defer qre.qe.queryServiceStats.QueryStats.Record(qre.plan.PlanId.String(), time.Now())

	if err := qre.checkPermissions(); err != nil {
		return err
	}

	conn, err := qre.getConn(qre.qe.streamConnPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	qd := NewQueryDetail(qre.logStats.ctx, conn)
	qre.qe.streamQList.Add(qd)
	defer qre.qe.streamQList.Remove(qd)

	return qre.fullStreamFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, sendReply)
}

func (qre *QueryExecutor) execDmlAutoCommit() (reply *mproto.QueryResult, err error) {
	transactionID := qre.qe.txPool.Begin(qre.ctx)
	qre.logStats.AddRewrittenSql("begin", time.Now())
	defer func() {
		// TxPool.Get may panic
		if panicErr := recover(); panicErr != nil {
			err = fmt.Errorf("DML autocommit got panic: %v", panicErr)
		}
		if err != nil {
			qre.qe.txPool.Rollback(qre.ctx, transactionID)
			qre.logStats.AddRewrittenSql("rollback", time.Now())
		} else {
			qre.qe.Commit(qre.ctx, qre.logStats, transactionID)
			qre.logStats.AddRewrittenSql("commit", time.Now())
		}
	}()
	conn := qre.qe.txPool.Get(transactionID)
	defer conn.Recycle()
	var invalidator CacheInvalidator
	if qre.plan.TableInfo != nil && qre.plan.TableInfo.CacheType != schema.CACHE_NONE {
		invalidator = conn.DirtyKeys(qre.plan.TableName)
	}
	switch qre.plan.PlanId {
	case planbuilder.PLAN_PASS_DML:
		if qre.qe.strictMode.Get() != 0 {
			return nil, NewTabletError(ErrFail, "DML too complex")
		}
		reply, err = qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
	case planbuilder.PLAN_INSERT_PK:
		reply, err = qre.execInsertPK(conn)
	case planbuilder.PLAN_INSERT_SUBQUERY:
		reply, err = qre.execInsertSubquery(conn)
	case planbuilder.PLAN_DML_PK:
		reply, err = qre.execDMLPK(conn, invalidator)
	case planbuilder.PLAN_DML_SUBQUERY:
		reply, err = qre.execDMLSubquery(conn, invalidator)
	default:
		return nil, NewTabletError(ErrFatal, "unsupported query: %s", qre.query)
	}
	return reply, err
}

// checkPermissions
func (qre *QueryExecutor) checkPermissions() error {
	// Skip permissions check if we have a background context.
	if qre.ctx == context.Background() {
		return nil
	}

	// Blacklist
	remoteAddr := ""
	username := ""
	ci, ok := callinfo.FromContext(qre.ctx)
	if ok {
		remoteAddr = ci.RemoteAddr()
		username = ci.Username()
	}
	action, desc := qre.plan.Rules.getAction(remoteAddr, username, qre.bindVars)
	switch action {
	case QR_FAIL:
		return NewTabletError(ErrFail, "Query disallowed due to rule: %s", desc)
	case QR_FAIL_RETRY:
		return NewTabletError(ErrRetry, "Query disallowed due to rule: %s", desc)
	}

	// a superuser that exempts from table ACL checking.
	if qre.qe.exemptACL == username {
		qre.qe.tableaclExemptCount.Add(1)
		return nil
	}
	tableACLStatsKey := []string{
		qre.plan.TableName,
		// TODO(shengzhe): use table group instead of username.
		username,
		qre.plan.PlanId.String(),
		username,
	}
	if qre.plan.Authorized == nil {
		return NewTabletError(ErrFail, "table acl error: nil acl")
	}
	// perform table ACL check if it is enabled.
	if !qre.plan.Authorized.IsMember(username) {
		if qre.qe.enableTableAclDryRun {
			qre.qe.tableaclPseudoDenied.Add(tableACLStatsKey, 1)
			return nil
		}
		// raise error if in strictTableAcl mode, else just log an error.
		if qre.qe.strictTableAcl {
			errStr := fmt.Sprintf("table acl error: %q cannot run %v on table %q", username, qre.plan.PlanId, qre.plan.TableName)
			qre.qe.tableaclDenied.Add(tableACLStatsKey, 1)
			qre.qe.accessCheckerLogger.Errorf("%s", errStr)
			return NewTabletError(ErrFail, "%s", errStr)
		}
		return nil
	}
	qre.qe.tableaclAllowed.Add(tableACLStatsKey, 1)
	return nil
}

func (qre *QueryExecutor) execDDL() (*mproto.QueryResult, error) {
	ddlPlan := planbuilder.DDLParse(qre.query)
	if ddlPlan.Action == "" {
		return nil, NewTabletError(ErrFail, "DDL is not understood")
	}

	txid := qre.qe.txPool.Begin(qre.ctx)
	defer qre.qe.txPool.SafeCommit(qre.ctx, txid)

	// Stolen from Execute
	conn := qre.qe.txPool.Get(txid)
	defer conn.Recycle()
	result, err := qre.execSQL(conn, qre.query, false)
	if err != nil {
		return nil, err
	}
	if ddlPlan.TableName != "" && ddlPlan.TableName != ddlPlan.NewName {
		// It's a drop or rename.
		qre.qe.schemaInfo.DropTable(ddlPlan.TableName)
	}
	if ddlPlan.NewName != "" {
		qre.qe.schemaInfo.CreateOrUpdateTable(qre.ctx, ddlPlan.NewName)
	}
	return result, nil
}

func (qre *QueryExecutor) execPKIN() (*mproto.QueryResult, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	limit, err := getLimit(qre.plan.Limit, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.fetchMulti(pkRows, limit)
}

func (qre *QueryExecutor) execSubquery() (*mproto.QueryResult, error) {
	innerResult, err := qre.qFetch(qre.logStats, qre.plan.Subquery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.fetchMulti(innerResult.Rows, -1)
}

func (qre *QueryExecutor) fetchMulti(pkRows [][]sqltypes.Value, limit int64) (*mproto.QueryResult, error) {
	if qre.plan.Fields == nil {
		return nil, NewTabletError(ErrFatal, "query plan.Fields is empty")
	}
	result := &mproto.QueryResult{Fields: qre.plan.Fields}
	if len(pkRows) == 0 || limit == 0 {
		return result, nil
	}
	tableInfo := qre.plan.TableInfo
	keys := make([]string, len(pkRows))
	for i, pk := range pkRows {
		keys[i] = buildKey(pk)
	}
	rcresults := tableInfo.Cache.Get(qre.ctx, keys)
	rows := make([][]sqltypes.Value, 0, len(pkRows))
	missingRows := make([][]sqltypes.Value, 0, len(pkRows))
	var hits, absent, misses int64
	for i, pk := range pkRows {
		rcresult := rcresults[keys[i]]
		if rcresult.Row != nil {
			if qre.mustVerify() {
				err := qre.spotCheck(rcresult, pk)
				if err != nil {
					return nil, err
				}
			}
			rows = append(rows, applyFilter(qre.plan.ColumnNumbers, rcresult.Row))
			hits++
		} else {
			missingRows = append(missingRows, pk)
		}
	}
	if len(missingRows) != 0 {
		bv := map[string]interface{}{
			"#pk": sqlparser.TupleEqualityList{
				Columns: qre.plan.TableInfo.Indexes[0].Columns,
				Rows:    missingRows,
			},
		}
		resultFromdb, err := qre.qFetch(qre.logStats, qre.plan.OuterQuery, bv)
		if err != nil {
			return nil, err
		}
		misses = int64(len(resultFromdb.Rows))
		absent = int64(len(pkRows)) - hits - misses
		for _, row := range resultFromdb.Rows {
			rows = append(rows, applyFilter(qre.plan.ColumnNumbers, row))
			key := buildKey(applyFilter(qre.plan.TableInfo.PKColumns, row))
			tableInfo.Cache.Set(qre.ctx, key, row, rcresults[key].Cas)
		}
	}

	qre.logStats.CacheHits = hits
	qre.logStats.CacheAbsent = absent
	qre.logStats.CacheMisses = misses

	qre.logStats.QuerySources |= QuerySourceRowcache

	tableInfo.hits.Add(hits)
	tableInfo.absent.Add(absent)
	tableInfo.misses.Add(misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	// limit == 0 is already addressed upfront.
	if limit > 0 && len(result.Rows) > int(limit) {
		result.Rows = result.Rows[:limit]
		result.RowsAffected = uint64(limit)
	}
	return result, nil
}

func (qre *QueryExecutor) mustVerify() bool {
	return (Rand() % spotCheckMultiplier) < qre.qe.spotCheckFreq.Get()
}

func (qre *QueryExecutor) spotCheck(rcresult RCResult, pk []sqltypes.Value) error {
	qre.qe.queryServiceStats.SpotCheckCount.Add(1)
	bv := map[string]interface{}{
		"#pk": sqlparser.TupleEqualityList{
			Columns: qre.plan.TableInfo.Indexes[0].Columns,
			Rows:    [][]sqltypes.Value{pk},
		},
	}
	resultFromdb, err := qre.qFetch(qre.logStats, qre.plan.OuterQuery, bv)
	if err != nil {
		return err
	}
	var dbrow []sqltypes.Value
	if len(resultFromdb.Rows) != 0 {
		dbrow = resultFromdb.Rows[0]
	}
	if dbrow == nil || !rowsAreEqual(rcresult.Row, dbrow) {
		qre.qe.Launch(func() { qre.recheckLater(rcresult, dbrow, pk) })
	}
	return nil
}

func (qre *QueryExecutor) recheckLater(rcresult RCResult, dbrow []sqltypes.Value, pk []sqltypes.Value) {
	time.Sleep(10 * time.Second)
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	reloaded := qre.plan.TableInfo.Cache.Get(context.Background(), keys)[keys[0]]
	// If reloaded row is absent or has changed, we're good
	if reloaded.Row == nil || reloaded.Cas != rcresult.Cas {
		return
	}
	log.Warningf("query: %v", qre.plan.FullQuery)
	log.Warningf("mismatch for: %v\ncache: %v\ndb:    %v", pk, rcresult.Row, dbrow)
	qre.qe.queryServiceStats.InternalErrors.Add("Mismatch", 1)
}

// execDirect always sends the query to mysql
func (qre *QueryExecutor) execDirect(conn poolConn) (*mproto.QueryResult, error) {
	if qre.plan.Fields != nil {
		result, err := qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
		if err != nil {
			return nil, err
		}
		result.Fields = qre.plan.Fields
		return result, nil
	}
	return qre.fullFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
}

// execSelect sends a query to mysql only if another identical query is not running. Otherwise, it waits and
// reuses the result. If the plan is missng field info, it sends the query to mysql requesting full info.
func (qre *QueryExecutor) execSelect() (*mproto.QueryResult, error) {
	if qre.plan.Fields != nil {
		result, err := qre.qFetch(qre.logStats, qre.plan.FullQuery, qre.bindVars)
		if err != nil {
			return nil, err
		}
		result.Fields = qre.plan.Fields
		return result, nil
	}
	conn, err := qre.getConn(qre.qe.connPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.fullFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
}

func (qre *QueryExecutor) execInsertPK(conn poolConn) (*mproto.QueryResult, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertSubquery(conn poolConn) (*mproto.QueryResult, error) {
	innerResult, err := qre.directFetch(conn, qre.plan.Subquery, qre.bindVars, nil)
	if err != nil {
		return nil, err
	}
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}, nil
	}
	if len(qre.plan.ColumnNumbers) != len(innerRows[0]) {
		return nil, NewTabletError(ErrFail, "Subquery length does not match column list")
	}
	pkRows := make([][]sqltypes.Value, len(innerRows))
	for i, innerRow := range innerRows {
		pkRows[i] = applyFilterWithPKDefaults(qre.plan.TableInfo, qre.plan.SubqueryPKColumns, innerRow)
	}
	// Validating first row is sufficient
	if err := validateRow(qre.plan.TableInfo, qre.plan.TableInfo.PKColumns, pkRows[0]); err != nil {
		return nil, err
	}

	qre.bindVars["#values"] = innerRows
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertPKRows(conn poolConn, pkRows [][]sqltypes.Value) (*mproto.QueryResult, error) {
	secondaryList, err := buildSecondaryList(qre.plan.TableInfo, pkRows, qre.plan.SecondaryPKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	bsc := buildStreamComment(qre.plan.TableInfo, pkRows, secondaryList)
	return qre.directFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc)
}

func (qre *QueryExecutor) execDMLPK(conn poolConn, invalidator CacheInvalidator) (*mproto.QueryResult, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execDMLPKRows(conn, pkRows, invalidator)
}

func (qre *QueryExecutor) execDMLSubquery(conn poolConn, invalidator CacheInvalidator) (*mproto.QueryResult, error) {
	innerResult, err := qre.directFetch(conn, qre.plan.Subquery, qre.bindVars, nil)
	if err != nil {
		return nil, err
	}
	return qre.execDMLPKRows(conn, innerResult.Rows, invalidator)
}

func (qre *QueryExecutor) execDMLPKRows(conn poolConn, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (*mproto.QueryResult, error) {
	if len(pkRows) == 0 {
		return &mproto.QueryResult{RowsAffected: 0}, nil
	}
	secondaryList, err := buildSecondaryList(qre.plan.TableInfo, pkRows, qre.plan.SecondaryPKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}

	result := &mproto.QueryResult{}
	maxRows := int(qre.qe.maxDMLRows.Get())
	for i := 0; i < len(pkRows); i += maxRows {
		end := i + maxRows
		if end >= len(pkRows) {
			end = len(pkRows)
		}
		pkRows := pkRows[i:end]
		secondaryList := secondaryList
		if secondaryList != nil {
			secondaryList = secondaryList[i:end]
		}
		bsc := buildStreamComment(qre.plan.TableInfo, pkRows, secondaryList)
		qre.bindVars["#pk"] = sqlparser.TupleEqualityList{
			Columns: qre.plan.TableInfo.Indexes[0].Columns,
			Rows:    pkRows,
		}
		r, err := qre.directFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc)
		if err != nil {
			return nil, err
		}
		// DMLs should only return RowsAffected.
		result.RowsAffected += r.RowsAffected
	}
	if invalidator == nil {
		return result, nil
	}
	for _, pk := range pkRows {
		key := buildKey(pk)
		invalidator.Delete(key)
	}
	return result, nil
}

func (qre *QueryExecutor) execSet() (*mproto.QueryResult, error) {
	switch qre.plan.SetKey {
	case "vt_pool_size":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_pool_size = %v, want to int64", err)
		}
		qre.qe.connPool.SetCapacity(int(val))
	case "vt_stream_pool_size":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_stream_pool_size = %v, want int64", err)
		}
		qre.qe.streamConnPool.SetCapacity(int(val))
	case "vt_transaction_cap":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_transaction_cap = %v, want int64", err)
		}
		qre.qe.txPool.pool.SetCapacity(int(val))
	case "vt_transaction_timeout":
		val, err := parseDuration(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_transaction_timeout = %v, want int64 or float64", err)
		}
		qre.qe.txPool.SetTimeout(val)
	case "vt_schema_reload_time":
		val, err := parseDuration(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_schema_reload_time = %v, want int64 or float64", err)
		}
		qre.qe.schemaInfo.SetReloadTime(val)
	case "vt_query_cache_size":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_query_cache_size = %v, want int64", err)
		}
		qre.qe.schemaInfo.SetQueryCacheSize(int(val))
	case "vt_max_result_size":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_max_result_size = %v, want int64", err)
		}
		if val < 1 {
			return nil, NewTabletError(ErrFail, "vt_max_result_size out of range %v", val)
		}
		qre.qe.maxResultSize.Set(val)
	case "vt_max_dml_rows":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_max_dml_rows = %v, want to int64", err)
		}
		if val < 1 {
			return nil, NewTabletError(ErrFail, "vt_max_dml_rows out of range %v", val)
		}
		qre.qe.maxDMLRows.Set(val)
	case "vt_stream_buffer_size":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_stream_buffer_size = %v, want int64", err)
		}

		if val < 1024 {
			return nil, NewTabletError(ErrFail, "vt_stream_buffer_size out of range %v", val)
		}
		qre.qe.streamBufferSize.Set(val)
	case "vt_query_timeout":
		val, err := parseDuration(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_query_timeout = %v, want int64 or float64", err)
		}
		qre.qe.queryTimeout.Set(val)
	case "vt_idle_timeout":
		val, err := parseDuration(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_idle_timeout = %v, want int64 or float64", err)
		}
		qre.qe.connPool.SetIdleTimeout(val)
		qre.qe.streamConnPool.SetIdleTimeout(val)
		qre.qe.txPool.pool.SetIdleTimeout(val)
	case "vt_spot_check_ratio":
		val, err := parseFloat64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_spot_check_ratio = %v, want float64", err)
		}
		qre.qe.spotCheckFreq.Set(int64(val * spotCheckMultiplier))
	case "vt_strict_mode":
		val, err := parseInt64(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_strict_mode = %v, want to int64", err)
		}
		qre.qe.strictMode.Set(val)
	case "vt_txpool_timeout":
		val, err := parseDuration(qre.plan.SetValue)
		if err != nil {
			return nil, NewTabletError(ErrFail, "got set vt_txpool_timeout = %v, want int64 or float64", err)
		}
		qre.qe.txPool.SetPoolTimeout(val)
	default:
		conn, err := qre.getConn(qre.qe.connPool)
		if err != nil {
			return nil, err
		}
		defer conn.Recycle()
		return qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
	}
	return &mproto.QueryResult{}, nil
}

func parseInt64(v interface{}) (int64, error) {
	if ival, ok := v.(int64); ok {
		return ival, nil
	}
	return -1, NewTabletError(ErrFail, "got %v, want int64", v)
}

func parseFloat64(v interface{}) (float64, error) {
	if ival, ok := v.(int64); ok {
		return float64(ival), nil
	}
	if fval, ok := v.(float64); ok {
		return fval, nil
	}
	return -1, NewTabletError(ErrFail, "got %v, want int64 or float64", v)
}

func parseDuration(v interface{}) (time.Duration, error) {
	val, err := parseFloat64(v)
	if err != nil {
		return 0, err
	}
	// time.Duration is an int64, have to multiple by 1e9 because
	// val might be in range (0, 1)
	return time.Duration(val * 1e9), nil
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

func (qre *QueryExecutor) getConn(pool *ConnPool) (*DBConn, error) {
	start := time.Now()
	conn, err := pool.Get(qre.ctx)
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Now().Sub(start)
		return conn, nil
	case ErrConnPoolClosed:
		return nil, err
	}
	return nil, NewTabletErrorSql(ErrFatal, err)
}

func (qre *QueryExecutor) qFetch(logStats *SQLQueryStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}) (*mproto.QueryResult, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, nil)
	if err != nil {
		return nil, err
	}
	q, ok := qre.qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
		waitingForConnectionStart := time.Now()
		conn, err := qre.qe.connPool.Get(qre.ctx)
		logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		if err != nil {
			q.Err = NewTabletErrorSql(ErrFatal, err)
		} else {
			defer conn.Recycle()
			q.Result, q.Err = qre.execSQL(conn, sql, false)
		}
	} else {
		logStats.QuerySources |= QuerySourceConsolidator
		startTime := time.Now()
		q.Wait()
		qre.qe.queryServiceStats.WaitStats.Record("Consolidations", startTime)
	}
	if q.Err != nil {
		return nil, q.Err
	}
	return q.Result.(*mproto.QueryResult), nil
}

func (qre *QueryExecutor) directFetch(conn poolConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (*mproto.QueryResult, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return nil, err
	}
	return qre.execSQL(conn, sql, false)
}

// fullFetch also fetches field info
func (qre *QueryExecutor) fullFetch(conn poolConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (*mproto.QueryResult, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return nil, err
	}
	return qre.execSQL(conn, sql, true)
}

func (qre *QueryExecutor) fullStreamFetch(conn *DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, callback func(*mproto.QueryResult) error) error {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return err
	}
	return qre.execStreamSQL(conn, sql, callback)
}

func (qre *QueryExecutor) generateFinalSQL(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (string, error) {
	bindVars["#maxLimit"] = qre.qe.maxResultSize.Get() + 1
	sql, err := parsedQuery.GenerateQuery(bindVars)
	if err != nil {
		return "", NewTabletError(ErrFail, "%s", err)
	}
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	// undo hack done by stripTrailing
	sql = restoreTrailing(sql, bindVars)
	return hack.String(sql), nil
}

func (qre *QueryExecutor) execSQL(conn poolConn, sql string, wantfields bool) (*mproto.QueryResult, error) {
	defer qre.logStats.AddRewrittenSql(sql, time.Now())
	return conn.Exec(qre.ctx, sql, int(qre.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStreamSQL(conn *DBConn, sql string, callback func(*mproto.QueryResult) error) error {
	start := time.Now()
	err := conn.Stream(qre.ctx, sql, callback, int(qre.qe.streamBufferSize.Get()))
	qre.logStats.AddRewrittenSql(sql, start)
	if err != nil {
		return NewTabletErrorSql(ErrFail, err)
	}
	return nil
}
