// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
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
	logStats      *LogStats
	qe            *QueryEngine
}

// poolConn is the interface implemented by users of this specialized pool.
type poolConn interface {
	Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
}

func addUserTableQueryStats(queryServiceStats *QueryServiceStats, ctx context.Context, tableName string, queryType string, duration int64) {
	username := callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(ctx))
	if username == "" {
		username = callerid.GetUsername(callerid.ImmediateCallerIDFromContext(ctx))
	}
	queryServiceStats.UserTableQueryCount.Add([]string{tableName, username, queryType}, 1)
	queryServiceStats.UserTableQueryTimesNs.Add([]string{tableName, username, queryType}, int64(duration))
}

// Execute performs a non-streaming query execution.
func (qre *QueryExecutor) Execute() (reply *sqltypes.Result, err error) {
	qre.logStats.OriginalSQL = qre.query
	qre.logStats.BindVariables = qre.bindVars
	qre.logStats.TransactionID = qre.transactionID
	planName := qre.plan.PlanID.String()
	qre.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		qre.qe.queryServiceStats.QueryStats.Add(planName, duration)
		addUserTableQueryStats(qre.qe.queryServiceStats, qre.ctx, qre.plan.TableName, "Execute", int64(duration))

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

	if qre.plan.PlanID == planbuilder.PlanDDL {
		return qre.execDDL()
	}

	if qre.transactionID != 0 {
		// Need upfront connection for DMLs and transactions
		conn := qre.qe.txPool.Get(qre.transactionID)
		defer conn.Recycle()
		conn.RecordQuery(qre.query)
		var invalidator CacheInvalidator
		if qre.plan.TableInfo != nil && qre.plan.TableInfo.CacheType != schema.CacheNone {
			invalidator = conn.DirtyKeys(qre.plan.TableName)
		}
		switch qre.plan.PlanID {
		case planbuilder.PlanPassDML:
			if qre.qe.strictMode.Get() != 0 {
				return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "DML too complex")
			}
			reply, err = qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
		case planbuilder.PlanInsertPK:
			reply, err = qre.execInsertPK(conn)
		case planbuilder.PlanInsertSubquery:
			reply, err = qre.execInsertSubquery(conn)
		case planbuilder.PlanDMLPK:
			reply, err = qre.execDMLPK(conn, invalidator)
		case planbuilder.PlanDMLSubquery:
			reply, err = qre.execDMLSubquery(conn, invalidator)
		case planbuilder.PlanOther:
			reply, err = qre.execSQL(conn, qre.query, true)
		case planbuilder.PlanUpsertPK:
			reply, err = qre.execUpsertPK(conn, invalidator)
		default: // select or set in a transaction, just count as select
			reply, err = qre.execDirect(conn)
		}
	} else {
		switch qre.plan.PlanID {
		case planbuilder.PlanPassSelect:
			if qre.plan.Reason == planbuilder.ReasonLock {
				return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "Disallowed outside transaction")
			}
			reply, err = qre.execSelect()
		case planbuilder.PlanPKIn:
			reply, err = qre.execPKIN()
		case planbuilder.PlanSelectSubquery:
			reply, err = qre.execSubquery()
		case planbuilder.PlanSet:
			reply, err = qre.execSet()
		case planbuilder.PlanOther:
			conn, connErr := qre.getConn(qre.qe.connPool)
			if connErr != nil {
				return nil, connErr
			}
			defer conn.Recycle()
			reply, err = qre.execSQL(conn, qre.query, true)
		default:
			if qre.qe.autoCommit.Get() == 0 {
				return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT,
					"unsupported query outside transaction: %s", qre.query)
			}
			reply, err = qre.execDmlAutoCommit()
		}
	}
	return reply, err
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(sendReply func(*sqltypes.Result) error) error {
	qre.logStats.OriginalSQL = qre.query
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		qre.qe.queryServiceStats.QueryStats.Record(qre.plan.PlanID.String(), start)
		addUserTableQueryStats(qre.qe.queryServiceStats, qre.ctx, qre.plan.TableName, "Stream", int64(time.Now().Sub(start)))
	}(time.Now())

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

func (qre *QueryExecutor) execDmlAutoCommit() (reply *sqltypes.Result, err error) {
	transactionID := qre.qe.txPool.Begin(qre.ctx)
	qre.logStats.AddRewrittenSQL("begin", time.Now())
	defer func() {
		// TxPool.Get may panic
		if panicErr := recover(); panicErr != nil {
			err = fmt.Errorf("DML autocommit got panic: %v", panicErr)
		}
		if err != nil {
			qre.qe.txPool.Rollback(qre.ctx, transactionID)
			qre.logStats.AddRewrittenSQL("rollback", time.Now())
		} else {
			qre.qe.Commit(qre.ctx, qre.logStats, transactionID)
			qre.logStats.AddRewrittenSQL("commit", time.Now())
		}
	}()
	conn := qre.qe.txPool.Get(transactionID)
	defer conn.Recycle()
	conn.RecordQuery(qre.query)
	var invalidator CacheInvalidator
	if qre.plan.TableInfo != nil && qre.plan.TableInfo.CacheType != schema.CacheNone {
		invalidator = conn.DirtyKeys(qre.plan.TableName)
	}
	switch qre.plan.PlanID {
	case planbuilder.PlanPassDML:
		if qre.qe.strictMode.Get() != 0 {
			return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "DML too complex")
		}
		reply, err = qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
	case planbuilder.PlanInsertPK:
		reply, err = qre.execInsertPK(conn)
	case planbuilder.PlanInsertSubquery:
		reply, err = qre.execInsertSubquery(conn)
	case planbuilder.PlanDMLPK:
		reply, err = qre.execDMLPK(conn, invalidator)
	case planbuilder.PlanDMLSubquery:
		reply, err = qre.execDMLSubquery(conn, invalidator)
	case planbuilder.PlanUpsertPK:
		reply, err = qre.execUpsertPK(conn, invalidator)
	default:
		return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "unsupported query: %s", qre.query)
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
	case QRFail:
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "Query disallowed due to rule: %s", desc)
	case QRFailRetry:
		return NewTabletError(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "Query disallowed due to rule: %s", desc)
	}

	// Check for SuperUser calling directly to VTTablet (e.g. VTWorker)
	if qre.qe.exemptACL != nil && qre.qe.exemptACL.IsMember(username) {
		qre.qe.tableaclExemptCount.Add(1)
		return nil
	}

	callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
	if callerID == nil {
		if qre.qe.strictTableAcl {
			return NewTabletError(ErrFail, vtrpcpb.ErrorCode_UNAUTHENTICATED, "missing caller id")
		}
		return nil
	}

	// a superuser that exempts from table ACL checking.
	if qre.qe.exemptACL != nil && qre.qe.exemptACL.IsMember(callerID.Username) {
		qre.qe.tableaclExemptCount.Add(1)
		return nil
	}

	// empty table name, do not need a table ACL check.
	if qre.plan.TableName == "" {
		return nil
	}

	if qre.plan.Authorized == nil {
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_PERMISSION_DENIED, "table acl error: nil acl")
	}
	tableACLStatsKey := []string{
		qre.plan.TableName,
		qre.plan.Authorized.GroupName,
		qre.plan.PlanID.String(),
		callerID.Username,
	}
	// perform table ACL check if it is enabled.
	if !qre.plan.Authorized.IsMember(callerID.Username) {
		if qre.qe.enableTableAclDryRun {
			qre.qe.tableaclPseudoDenied.Add(tableACLStatsKey, 1)
			return nil
		}
		// raise error if in strictTableAcl mode, else just log an error.
		if qre.qe.strictTableAcl {
			errStr := fmt.Sprintf("table acl error: %q cannot run %v on table %q", callerID.Username, qre.plan.PlanID, qre.plan.TableName)
			qre.qe.tableaclDenied.Add(tableACLStatsKey, 1)
			qre.qe.accessCheckerLogger.Errorf("%s", errStr)
			return NewTabletError(ErrFail, vtrpcpb.ErrorCode_PERMISSION_DENIED, "%s", errStr)
		}
		return nil
	}
	qre.qe.tableaclAllowed.Add(tableACLStatsKey, 1)
	return nil
}

func (qre *QueryExecutor) execDDL() (*sqltypes.Result, error) {
	ddlPlan := planbuilder.DDLParse(qre.query)
	if ddlPlan.Action == "" {
		return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "DDL is not understood")
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

func (qre *QueryExecutor) execPKIN() (*sqltypes.Result, error) {
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

func (qre *QueryExecutor) execSubquery() (*sqltypes.Result, error) {
	innerResult, err := qre.qFetch(qre.logStats, qre.plan.Subquery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.fetchMulti(innerResult.Rows, -1)
}

func (qre *QueryExecutor) fetchMulti(pkRows [][]sqltypes.Value, limit int64) (*sqltypes.Result, error) {
	if qre.plan.Fields == nil {
		// TODO(aaijazi): Is this due to a bad query, or an internal error? We might want to change
		// this to ErrFail and ErrorCode_BAD_INPUT instead.
		return nil, NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "query plan.Fields is empty")
	}
	result := &sqltypes.Result{Fields: qre.plan.Fields}
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
func (qre *QueryExecutor) execDirect(conn poolConn) (*sqltypes.Result, error) {
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
func (qre *QueryExecutor) execSelect() (*sqltypes.Result, error) {
	if qre.plan.Fields != nil {
		result, err := qre.qFetch(qre.logStats, qre.plan.FullQuery, qre.bindVars)
		if err != nil {
			return nil, err
		}
		// result is read-only. So, let's copy it before modifying.
		newResult := *result
		newResult.Fields = qre.plan.Fields
		return &newResult, nil
	}
	conn, err := qre.getConn(qre.qe.connPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.fullFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
}

func (qre *QueryExecutor) execInsertPK(conn poolConn) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertSubquery(conn poolConn) (*sqltypes.Result, error) {
	innerResult, err := qre.directFetch(conn, qre.plan.Subquery, qre.bindVars, nil)
	if err != nil {
		return nil, err
	}
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &sqltypes.Result{RowsAffected: 0}, nil
	}
	if len(qre.plan.ColumnNumbers) != len(innerRows[0]) {
		return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "Subquery length does not match column list")
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

func (qre *QueryExecutor) execInsertPKRows(conn poolConn, pkRows [][]sqltypes.Value) (*sqltypes.Result, error) {
	bsc := buildStreamComment(qre.plan.TableInfo, pkRows, nil)
	return qre.directFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc)
}

func (qre *QueryExecutor) execUpsertPK(conn poolConn, invalidator CacheInvalidator) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	bsc := buildStreamComment(qre.plan.TableInfo, pkRows, nil)
	result, err := qre.directFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc)
	if err == nil {
		return result, nil
	}
	terr, ok := err.(*TabletError)
	if !ok {
		return result, err
	}
	if terr.SQLError != mysql.ErrDupEntry {
		return nil, err
	}
	// If the error didn't match pk, just return the error without updating.
	if !strings.Contains(terr.Message, "'PRIMARY'") {
		return nil, err
	}
	// At this point, we know the insert failed due to a duplicate pk row.
	// So, we just update the row.
	result, err = qre.execDMLPKRows(conn, qre.plan.UpsertQuery, pkRows, invalidator)
	if err != nil {
		return nil, err
	}
	// Follow MySQL convention. RowsAffected must be 2 if a row was updated.
	if result.RowsAffected == 1 {
		result.RowsAffected = 2
	}
	return result, err
}

func (qre *QueryExecutor) execDMLPK(conn poolConn, invalidator CacheInvalidator) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execDMLPKRows(conn, qre.plan.OuterQuery, pkRows, invalidator)
}

func (qre *QueryExecutor) execDMLSubquery(conn poolConn, invalidator CacheInvalidator) (*sqltypes.Result, error) {
	innerResult, err := qre.directFetch(conn, qre.plan.Subquery, qre.bindVars, nil)
	if err != nil {
		return nil, err
	}
	return qre.execDMLPKRows(conn, qre.plan.OuterQuery, innerResult.Rows, invalidator)
}

func (qre *QueryExecutor) execDMLPKRows(conn poolConn, query *sqlparser.ParsedQuery, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (*sqltypes.Result, error) {
	if len(pkRows) == 0 {
		return &sqltypes.Result{RowsAffected: 0}, nil
	}
	secondaryList, err := buildSecondaryList(qre.plan.TableInfo, pkRows, qre.plan.SecondaryPKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{}
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
		r, err := qre.directFetch(conn, query, qre.bindVars, bsc)
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

func (qre *QueryExecutor) execSet() (*sqltypes.Result, error) {
	conn, err := qre.getConn(qre.qe.connPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.directFetch(conn, qre.plan.FullQuery, qre.bindVars, nil)
}

func parseInt64(v interface{}) (int64, error) {
	if ival, ok := v.(int64); ok {
		return ival, nil
	}
	return -1, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "got %v, want int64", v)
}

func parseFloat64(v interface{}) (float64, error) {
	if ival, ok := v.(int64); ok {
		return float64(ival), nil
	}
	if fval, ok := v.(float64); ok {
		return fval, nil
	}
	return -1, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "got %v, want int64 or float64", v)
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
	span := trace.NewSpanFromContext(qre.ctx)
	span.StartLocal("QueryExecutor.getConn")
	defer span.Finish()

	start := time.Now()
	conn, err := pool.Get(qre.ctx)
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Now().Sub(start)
		return conn, nil
	case ErrConnPoolClosed:
		return nil, err
	}
	return nil, NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
}

func (qre *QueryExecutor) qFetch(logStats *LogStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}) (*sqltypes.Result, error) {
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
			q.Err = NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
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
	return q.Result.(*sqltypes.Result), nil
}

func (qre *QueryExecutor) directFetch(conn poolConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (*sqltypes.Result, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return nil, err
	}
	return qre.execSQL(conn, sql, false)
}

// fullFetch also fetches field info
func (qre *QueryExecutor) fullFetch(conn poolConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (*sqltypes.Result, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return nil, err
	}
	return qre.execSQL(conn, sql, true)
}

func (qre *QueryExecutor) fullStreamFetch(conn *DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, callback func(*sqltypes.Result) error) error {
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
		return "", NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "%s", err)
	}
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	// undo hack done by stripTrailing
	sql = restoreTrailing(sql, bindVars)
	return hack.String(sql), nil
}

func (qre *QueryExecutor) execSQL(conn poolConn, sql string, wantfields bool) (*sqltypes.Result, error) {
	defer qre.logStats.AddRewrittenSQL(sql, time.Now())
	return conn.Exec(qre.ctx, sql, int(qre.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStreamSQL(conn *DBConn, sql string, callback func(*sqltypes.Result) error) error {
	start := time.Now()
	err := conn.Stream(qre.ctx, sql, callback, int(qre.qe.streamBufferSize.Get()))
	qre.logStats.AddRewrittenSQL(sql, start)
	if err != nil {
		// MySQL error that isn't due to a connection issue
		return NewTabletErrorSQL(ErrFail, vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	return nil
}
