// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
	te            *TxEngine
	messager      *MessagerEngine
}

var sequenceFields = []*querypb.Field{
	{
		Name: "nextval",
		Type: sqltypes.Int64,
	},
}

func addUserTableQueryStats(queryServiceStats *QueryServiceStats, ctx context.Context, tableName sqlparser.TableIdent, queryType string, duration int64) {
	username := callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(ctx))
	if username == "" {
		username = callerid.GetUsername(callerid.ImmediateCallerIDFromContext(ctx))
	}
	queryServiceStats.UserTableQueryCount.Add([]string{tableName.String(), username, queryType}, 1)
	queryServiceStats.UserTableQueryTimesNs.Add([]string{tableName.String(), username, queryType}, int64(duration))
}

// Execute performs a non-streaming query execution.
func (qre *QueryExecutor) Execute() (reply *sqltypes.Result, err error) {
	qre.logStats.TransactionID = qre.transactionID
	planName := qre.plan.PlanID.String()
	qre.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		qre.qe.queryServiceStats.QueryStats.Add(planName, duration)
		addUserTableQueryStats(qre.qe.queryServiceStats, qre.ctx, qre.plan.TableName, "Execute", int64(duration))

		if reply == nil {
			qre.plan.AddStats(1, duration, qre.logStats.MysqlResponseTime, 0, 1)
			return
		}
		qre.plan.AddStats(1, duration, qre.logStats.MysqlResponseTime, int64(reply.RowsAffected), 0)
		qre.logStats.RowsAffected = int(reply.RowsAffected)
		qre.logStats.Rows = reply.Rows
		qre.qe.queryServiceStats.ResultStats.Add(int64(len(reply.Rows)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return nil, err
	}

	switch qre.plan.PlanID {
	case planbuilder.PlanDDL:
		return qre.execDDL()
	case planbuilder.PlanNextval:
		return qre.execNextval()
	}

	if qre.transactionID != 0 {
		// Need upfront connection for DMLs and transactions
		conn, err := qre.te.txPool.Get(qre.transactionID, "for query")
		if err != nil {
			return nil, err
		}
		defer conn.Recycle()
		switch qre.plan.PlanID {
		case planbuilder.PlanPassDML:
			if qre.qe.strictMode.Get() != 0 {
				return nil, NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "DML too complex")
			}
			return qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, false, true)
		case planbuilder.PlanInsertPK:
			return qre.execInsertPK(conn)
		case planbuilder.PlanInsertMessage:
			return qre.execInsertMessage(conn)
		case planbuilder.PlanInsertSubquery:
			return qre.execInsertSubquery(conn)
		case planbuilder.PlanDMLPK:
			return qre.execDMLPK(conn)
		case planbuilder.PlanDMLSubquery:
			return qre.execDMLSubquery(conn)
		case planbuilder.PlanOther:
			return qre.execSQL(conn, qre.query, true)
		case planbuilder.PlanUpsertPK:
			return qre.execUpsertPK(conn)
		case planbuilder.PlanSet:
			return qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, false, true)
		default:
			return qre.execDirect(conn)
		}
	} else {
		switch qre.plan.PlanID {
		case planbuilder.PlanPassSelect:
			return qre.execSelect()
		case planbuilder.PlanSelectLock:
			return nil, NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "Disallowed outside transaction")
		case planbuilder.PlanSet:
			return qre.execSet()
		case planbuilder.PlanOther:
			conn, connErr := qre.getConn(qre.qe.connPool)
			if connErr != nil {
				return nil, connErr
			}
			defer conn.Recycle()
			return qre.execSQL(conn, qre.query, true)
		default:
			if qre.qe.autoCommit.Get() == 0 {
				return nil, NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT,
					"unsupported query outside transaction: %s", qre.query)
			}
			return qre.execDmlAutoCommit()
		}
	}
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(includedFields querypb.ExecuteOptions_IncludedFields, sendReply func(*sqltypes.Result) error) error {
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

	return qre.streamFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, includedFields, sendReply)
}

func (qre *QueryExecutor) execDmlAutoCommit() (reply *sqltypes.Result, err error) {
	return qre.execAsTransaction(func(conn *TxConnection) (reply *sqltypes.Result, err error) {
		switch qre.plan.PlanID {
		case planbuilder.PlanPassDML:
			if qre.qe.strictMode.Get() != 0 {
				return nil, NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "DML too complex")
			}
			reply, err = qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, false, true)
		case planbuilder.PlanInsertPK:
			reply, err = qre.execInsertPK(conn)
		case planbuilder.PlanInsertMessage:
			return qre.execInsertMessage(conn)
		case planbuilder.PlanInsertSubquery:
			reply, err = qre.execInsertSubquery(conn)
		case planbuilder.PlanDMLPK:
			reply, err = qre.execDMLPK(conn)
		case planbuilder.PlanDMLSubquery:
			reply, err = qre.execDMLSubquery(conn)
		case planbuilder.PlanUpsertPK:
			reply, err = qre.execUpsertPK(conn)
		default:
			return nil, NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "unsupported query: %s", qre.query)
		}
		return reply, err
	})
}

func (qre *QueryExecutor) execAsTransaction(f func(conn *TxConnection) (*sqltypes.Result, error)) (reply *sqltypes.Result, err error) {
	conn, err := qre.te.txPool.LocalBegin(qre.ctx)
	if err != nil {
		return nil, err
	}
	defer qre.te.txPool.LocalConclude(qre.ctx, conn)
	qre.logStats.AddRewrittenSQL("begin", time.Now())

	reply, err = f(conn)

	if err != nil {
		qre.te.txPool.LocalConclude(qre.ctx, conn)
		qre.logStats.AddRewrittenSQL("rollback", time.Now())
		return nil, err
	}
	err = qre.te.txPool.LocalCommit(qre.ctx, conn, qre.messager)
	if err != nil {
		return nil, err
	}
	qre.logStats.AddRewrittenSQL("commit", time.Now())
	return reply, nil
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
		return NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "Query disallowed due to rule: %s", desc)
	case QRFailRetry:
		return NewTabletError(vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "Query disallowed due to rule: %s", desc)
	}

	// Check for SuperUser calling directly to VTTablet (e.g. VTWorker)
	if qre.qe.exemptACL != nil && qre.qe.exemptACL.IsMember(username) {
		qre.qe.tableaclExemptCount.Add(1)
		return nil
	}

	callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
	if callerID == nil {
		if qre.qe.strictTableAcl {
			return NewTabletError(vtrpcpb.ErrorCode_UNAUTHENTICATED, "missing caller id")
		}
		return nil
	}

	// a superuser that exempts from table ACL checking.
	if qre.qe.exemptACL != nil && qre.qe.exemptACL.IsMember(callerID.Username) {
		qre.qe.tableaclExemptCount.Add(1)
		return nil
	}

	// empty table name, do not need a table ACL check.
	if qre.plan.TableName.IsEmpty() {
		return nil
	}

	if qre.plan.Authorized == nil {
		return NewTabletError(vtrpcpb.ErrorCode_PERMISSION_DENIED, "table acl error: nil acl")
	}
	tableACLStatsKey := []string{
		qre.plan.TableName.String(),
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
			qre.qe.accessCheckerLogger.Infof("%s", errStr)
			return NewTabletError(vtrpcpb.ErrorCode_PERMISSION_DENIED, "%s", errStr)
		}
		return nil
	}
	qre.qe.tableaclAllowed.Add(tableACLStatsKey, 1)
	return nil
}

func (qre *QueryExecutor) execDDL() (*sqltypes.Result, error) {
	ddlPlan := planbuilder.DDLParse(qre.query)
	if ddlPlan.Action == "" {
		return nil, NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "DDL is not understood")
	}

	conn, err := qre.te.txPool.LocalBegin(qre.ctx)
	if err != nil {
		return nil, err
	}
	defer qre.te.txPool.LocalCommit(qre.ctx, conn, qre.messager)

	result, err := qre.execSQL(conn, qre.query, false)
	if err != nil {
		return nil, err
	}
	if !ddlPlan.TableName.IsEmpty() && ddlPlan.TableName != ddlPlan.NewName {
		// It's a drop or rename.
		qre.qe.schemaInfo.DropTable(ddlPlan.TableName)
	}
	if !ddlPlan.NewName.IsEmpty() {
		if err := qre.qe.schemaInfo.CreateOrUpdateTable(qre.ctx, ddlPlan.NewName.String()); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (qre *QueryExecutor) execNextval() (*sqltypes.Result, error) {
	inc, err := resolveNumber(qre.plan.PKValues[0], qre.bindVars)
	if err != nil {
		return nil, err
	}
	if inc < 1 {
		return nil, fmt.Errorf("invalid increment for sequence %s: %d", qre.plan.TableName, inc)
	}

	t := qre.plan.TableInfo
	t.Seq.Lock()
	defer t.Seq.Unlock()
	if t.NextVal == 0 || t.NextVal+inc > t.LastVal {
		_, err := qre.execAsTransaction(func(conn *TxConnection) (*sqltypes.Result, error) {
			query := fmt.Sprintf("select next_id, cache from %s where id = 0 for update", sqlparser.String(qre.plan.TableName))
			qr, err := qre.execSQL(conn, query, false)
			if err != nil {
				return nil, err
			}
			if len(qr.Rows) != 1 {
				return nil, fmt.Errorf("unexpected rows from reading sequence %s (possible mis-route): %d", qre.plan.TableName, len(qr.Rows))
			}
			nextID, err := qr.Rows[0][0].ParseInt64()
			if err != nil {
				return nil, fmt.Errorf("error loading sequence %s: %v", qre.plan.TableName, err)
			}
			// Initialize NextVal if it wasn't already.
			if t.NextVal == 0 {
				t.NextVal = nextID
			}
			cache, err := qr.Rows[0][1].ParseInt64()
			if err != nil {
				return nil, fmt.Errorf("error loading sequence %s: %v", qre.plan.TableName, err)
			}
			if cache < 1 {
				return nil, fmt.Errorf("invalid cache value for sequence %s: %d", qre.plan.TableName, cache)
			}
			newLast := nextID + cache
			for newLast <= t.NextVal+inc {
				newLast += cache
			}
			query = fmt.Sprintf("update %s set next_id = %d where id = 0", sqlparser.String(qre.plan.TableName), newLast)
			conn.RecordQuery(query)
			_, err = qre.execSQL(conn, query, false)
			if err != nil {
				return nil, err
			}
			t.LastVal = newLast
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
	}
	ret := t.NextVal
	t.NextVal += inc
	return &sqltypes.Result{
		Fields: sequenceFields,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, ret, 10)),
		}},
		RowsAffected: 1,
	}, nil
}

// execDirect is for reads inside transactions. Always send to MySQL.
func (qre *QueryExecutor) execDirect(conn *TxConnection) (*sqltypes.Result, error) {
	if qre.plan.Fields != nil {
		result, err := qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, false, false)
		if err != nil {
			return nil, err
		}
		result.Fields = qre.plan.Fields
		return result, nil
	}
	return qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, true, false)
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
	return qre.dbConnFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, true)
}

func (qre *QueryExecutor) execInsertPK(conn *TxConnection) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertMessage(conn *TxConnection) (*sqltypes.Result, error) {
	qre.bindVars["#time_now"] = time.Now().UnixNano()
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	qr, err := qre.execInsertPKRows(conn, pkRows)
	if err != nil {
		return nil, err
	}
	bv := map[string]interface{}{
		"#pk": sqlparser.TupleEqualityList{
			Columns: qre.plan.TableInfo.Indexes[0].Columns,
			Rows:    pkRows,
		},
	}
	readback, err := qre.txFetch(conn, qre.plan.MessageReloaderQuery, bv, nil, false, false)
	if err != nil {
		return nil, err
	}
	mrs := conn.NewMessages[qre.plan.TableInfo.Name.String()]
	for _, row := range readback.Rows {
		mr, err := BuildMessageRow(row)
		if err != nil {
			return nil, err
		}
		mrs = append(mrs, mr)
	}
	conn.NewMessages[qre.plan.TableInfo.Name.String()] = mrs
	return qr, nil
}

func (qre *QueryExecutor) execInsertSubquery(conn *TxConnection) (*sqltypes.Result, error) {
	innerResult, err := qre.txFetch(conn, qre.plan.Subquery, qre.bindVars, nil, false, false)
	if err != nil {
		return nil, err
	}
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &sqltypes.Result{RowsAffected: 0}, nil
	}
	if len(qre.plan.ColumnNumbers) != len(innerRows[0]) {
		return nil, NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "Subquery length does not match column list")
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

func (qre *QueryExecutor) execInsertPKRows(conn *TxConnection, pkRows [][]sqltypes.Value) (*sqltypes.Result, error) {
	bsc := buildStreamComment(qre.plan.TableInfo, pkRows, nil)
	return qre.txFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc, false, true)
}

func (qre *QueryExecutor) execUpsertPK(conn *TxConnection) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	bsc := buildStreamComment(qre.plan.TableInfo, pkRows, nil)
	result, err := qre.txFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc, false, true)
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
	result, err = qre.execDMLPKRows(conn, qre.plan.UpsertQuery, pkRows)
	if err != nil {
		return nil, err
	}
	// Follow MySQL convention. RowsAffected must be 2 if a row was updated.
	if result.RowsAffected == 1 {
		result.RowsAffected = 2
	}
	return result, err
}

func (qre *QueryExecutor) execDMLPK(conn *TxConnection) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.TableInfo, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execDMLPKRows(conn, qre.plan.OuterQuery, pkRows)
}

func (qre *QueryExecutor) execDMLSubquery(conn *TxConnection) (*sqltypes.Result, error) {
	innerResult, err := qre.txFetch(conn, qre.plan.Subquery, qre.bindVars, nil, false, false)
	if err != nil {
		return nil, err
	}
	return qre.execDMLPKRows(conn, qre.plan.OuterQuery, innerResult.Rows)
}

func (qre *QueryExecutor) execDMLPKRows(conn *TxConnection, query *sqlparser.ParsedQuery, pkRows [][]sqltypes.Value) (*sqltypes.Result, error) {
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
		r, err := qre.txFetch(conn, query, qre.bindVars, bsc, false, true)
		if err != nil {
			return nil, err
		}
		// DMLs should only return RowsAffected.
		result.RowsAffected += r.RowsAffected
	}
	if qre.plan.TableInfo.Type == schema.Message {
		ids := conn.ChangedMessages[qre.plan.TableInfo.Name.String()]
		for _, pkrow := range pkRows {
			ids = append(ids, pkrow[qre.plan.TableInfo.IDPKIndex].String())
		}
		conn.ChangedMessages[qre.plan.TableInfo.Name.String()] = ids
	}
	return result, nil
}

func (qre *QueryExecutor) execSet() (*sqltypes.Result, error) {
	conn, err := qre.getConn(qre.qe.connPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.dbConnFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, false)
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
	return nil, NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
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
			q.Err = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
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

// txFetch fetches from a TxConnection.
func (qre *QueryExecutor) txFetch(conn *TxConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, wantfields, record bool) (*sqltypes.Result, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return nil, err
	}
	qr, err := qre.execSQL(conn, sql, wantfields)
	if err != nil {
		return nil, err
	}
	// Only record successful queries.
	if record {
		conn.RecordQuery(sql)
	}
	return qr, nil
}

// dbConnFetch fetches from a DBConn.
func (qre *QueryExecutor) dbConnFetch(conn *DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, wantfields bool) (*sqltypes.Result, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return nil, err
	}
	return qre.execSQL(conn, sql, wantfields)
}

// streamFetch performs a streaming fetch.
func (qre *QueryExecutor) streamFetch(conn *DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, includedFields querypb.ExecuteOptions_IncludedFields, callback func(*sqltypes.Result) error) error {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return err
	}
	return qre.execStreamSQL(conn, sql, includedFields, callback)
}

func (qre *QueryExecutor) generateFinalSQL(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (string, error) {
	bindVars["#maxLimit"] = qre.qe.maxResultSize.Get() + 1
	sql, err := parsedQuery.GenerateQuery(bindVars)
	if err != nil {
		return "", NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "%s", err)
	}
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	sql = restoreTrailing(sql, bindVars)
	return hack.String(sql), nil
}

// poolConn is an abstraction for reusing code in execSQL.
type poolConn interface {
	Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
}

func (qre *QueryExecutor) execSQL(conn poolConn, sql string, wantfields bool) (*sqltypes.Result, error) {
	defer qre.logStats.AddRewrittenSQL(sql, time.Now())
	return conn.Exec(qre.ctx, sql, int(qre.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStreamSQL(conn *DBConn, sql string, includedFields querypb.ExecuteOptions_IncludedFields, callback func(*sqltypes.Result) error) error {
	start := time.Now()
	err := conn.Stream(qre.ctx, sql, callback, int(qre.qe.streamBufferSize.Get()), includedFields)
	qre.logStats.AddRewrittenSQL(sql, start)
	if err != nil {
		// MySQL error that isn't due to a connection issue
		return NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	return nil
}
