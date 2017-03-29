// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/messager"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// QueryExecutor is used for executing a query request.
type QueryExecutor struct {
	query         string
	bindVars      map[string]interface{}
	transactionID int64
	plan          *TabletPlan
	ctx           context.Context
	logStats      *tabletenv.LogStats
	tsv           *TabletServer
}

var sequenceFields = []*querypb.Field{
	{
		Name: "nextval",
		Type: sqltypes.Int64,
	},
}

// Execute performs a non-streaming query execution.
func (qre *QueryExecutor) Execute() (reply *sqltypes.Result, err error) {
	qre.logStats.TransactionID = qre.transactionID
	planName := qre.plan.PlanID.String()
	qre.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		tabletenv.QueryStats.Add(planName, duration)
		tabletenv.RecordUserQuery(qre.ctx, qre.plan.TableName(), "Execute", int64(duration))

		if reply == nil {
			qre.plan.AddStats(1, duration, qre.logStats.MysqlResponseTime, 0, 1)
			return
		}
		qre.plan.AddStats(1, duration, qre.logStats.MysqlResponseTime, int64(reply.RowsAffected), 0)
		qre.logStats.RowsAffected = int(reply.RowsAffected)
		qre.logStats.Rows = reply.Rows
		tabletenv.ResultStats.Add(int64(len(reply.Rows)))
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
		conn, err := qre.tsv.te.txPool.Get(qre.transactionID, "for query")
		if err != nil {
			return nil, err
		}
		defer conn.Recycle()
		switch qre.plan.PlanID {
		case planbuilder.PlanPassDML:
			if qre.tsv.qe.strictMode.Get() {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "DML too complex")
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
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed outside transaction")
		case planbuilder.PlanSet:
			return qre.execSet()
		case planbuilder.PlanOther:
			conn, connErr := qre.getConn(qre.tsv.qe.conns)
			if connErr != nil {
				return nil, connErr
			}
			defer conn.Recycle()
			return qre.execSQL(conn, qre.query, true)
		default:
			if !qre.tsv.qe.autoCommit.Get() {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed outside transaction")
			}
			return qre.execDmlAutoCommit()
		}
	}
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(includedFields querypb.ExecuteOptions_IncludedFields, callback func(*sqltypes.Result) error) error {
	qre.logStats.OriginalSQL = qre.query
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		tabletenv.QueryStats.Record(qre.plan.PlanID.String(), start)
		tabletenv.RecordUserQuery(qre.ctx, qre.plan.TableName(), "Stream", int64(time.Now().Sub(start)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return err
	}

	conn, err := qre.getConn(qre.tsv.qe.streamConns)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	qre.tsv.qe.streamQList.Add(qd)
	defer qre.tsv.qe.streamQList.Remove(qd)

	return qre.streamFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, includedFields, callback)
}

func (qre *QueryExecutor) execDmlAutoCommit() (reply *sqltypes.Result, err error) {
	return qre.execAsTransaction(func(conn *TxConnection) (reply *sqltypes.Result, err error) {
		switch qre.plan.PlanID {
		case planbuilder.PlanPassDML:
			if qre.tsv.qe.strictMode.Get() {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "DML too complex")
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
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported query: %s", qre.query)
		}
		return reply, err
	})
}

func (qre *QueryExecutor) execAsTransaction(f func(conn *TxConnection) (*sqltypes.Result, error)) (reply *sqltypes.Result, err error) {
	conn, err := qre.tsv.te.txPool.LocalBegin(qre.ctx)
	if err != nil {
		return nil, err
	}
	defer qre.tsv.te.txPool.LocalConclude(qre.ctx, conn)
	qre.logStats.AddRewrittenSQL("begin", time.Now())

	reply, err = f(conn)

	start := time.Now()
	if err != nil {
		qre.tsv.te.txPool.LocalConclude(qre.ctx, conn)
		qre.logStats.AddRewrittenSQL("rollback", start)
		return nil, err
	}
	err = qre.tsv.te.txPool.LocalCommit(qre.ctx, conn, qre.tsv.messager)
	qre.logStats.AddRewrittenSQL("commit", start)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// checkPermissions
func (qre *QueryExecutor) checkPermissions() error {
	// Skip permissions check if the context is local.
	if tabletenv.IsLocalContext(qre.ctx) {
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
	action, desc := qre.plan.Rules.GetAction(remoteAddr, username, qre.bindVars)
	switch action {
	case rules.QRFail:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed due to rule: %s", desc)
	case rules.QRFailRetry:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: %s", desc)
	}

	// Check for SuperUser calling directly to VTTablet (e.g. VTWorker)
	if qre.tsv.qe.exemptACL != nil && qre.tsv.qe.exemptACL.IsMember(&querypb.VTGateCallerID{Username: username}) {
		qre.tsv.qe.tableaclExemptCount.Add(1)
		return nil
	}

	callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
	if callerID == nil {
		if qre.tsv.qe.strictTableACL {
			return vterrors.Errorf(vtrpcpb.Code_UNAUTHENTICATED, "missing caller id")
		}
		return nil
	}

	// a superuser that exempts from table ACL checking.
	if qre.tsv.qe.exemptACL != nil && qre.tsv.qe.exemptACL.IsMember(callerID) {
		qre.tsv.qe.tableaclExemptCount.Add(1)
		return nil
	}

	// empty table name, do not need a table ACL check.
	if qre.plan.TableName().IsEmpty() {
		return nil
	}

	if qre.plan.Authorized == nil {
		return vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "table acl error: nil acl")
	}
	tableACLStatsKey := []string{
		qre.plan.TableName().String(),
		qre.plan.Authorized.GroupName,
		qre.plan.PlanID.String(),
		callerID.Username,
	}
	// perform table ACL check if it is enabled.
	if !qre.plan.Authorized.IsMember(callerID) {
		if qre.tsv.qe.enableTableACLDryRun {
			tabletenv.TableaclPseudoDenied.Add(tableACLStatsKey, 1)
			return nil
		}
		// raise error if in strictTableAcl mode, else just log an error.
		if qre.tsv.qe.strictTableACL {
			errStr := fmt.Sprintf("table acl error: %q cannot run %v on table %q", callerID.Username, qre.plan.PlanID, qre.plan.TableName())
			tabletenv.TableaclDenied.Add(tableACLStatsKey, 1)
			qre.tsv.qe.accessCheckerLogger.Infof("%s", errStr)
			return vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "%s", errStr)
		}
		return nil
	}
	tabletenv.TableaclAllowed.Add(tableACLStatsKey, 1)
	return nil
}

func (qre *QueryExecutor) execDDL() (*sqltypes.Result, error) {
	ddlPlan := planbuilder.DDLParse(qre.query)
	if ddlPlan.Action == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "DDL is not understood")
	}

	defer qre.tsv.se.Reload(qre.ctx)

	if qre.transactionID != 0 {
		conn, err := qre.tsv.te.txPool.Get(qre.transactionID, "DDL begin again")
		if err != nil {
			return nil, err
		}
		defer conn.Recycle()
		result, err := qre.execSQL(conn, qre.query, false)
		if err != nil {
			return nil, err
		}
		err = conn.BeginAgain(qre.ctx)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	result, err := qre.execAsTransaction(func(conn *TxConnection) (*sqltypes.Result, error) {
		return qre.execSQL(conn, qre.query, false)
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (qre *QueryExecutor) execNextval() (*sqltypes.Result, error) {
	inc, err := resolveNumber(qre.plan.PKValues[0], qre.bindVars)
	if err != nil {
		return nil, err
	}
	tableName := qre.plan.TableName()
	if inc < 1 {
		return nil, fmt.Errorf("invalid increment for sequence %s: %d", tableName, inc)
	}

	t := qre.plan.Table
	t.SequenceInfo.Lock()
	defer t.SequenceInfo.Unlock()
	if t.SequenceInfo.NextVal == 0 || t.SequenceInfo.NextVal+inc > t.SequenceInfo.LastVal {
		_, err := qre.execAsTransaction(func(conn *TxConnection) (*sqltypes.Result, error) {
			query := fmt.Sprintf("select next_id, cache from %s where id = 0 for update", sqlparser.String(tableName))
			qr, err := qre.execSQL(conn, query, false)
			if err != nil {
				return nil, err
			}
			if len(qr.Rows) != 1 {
				return nil, fmt.Errorf("unexpected rows from reading sequence %s (possible mis-route): %d", tableName, len(qr.Rows))
			}
			nextID, err := qr.Rows[0][0].ParseInt64()
			if err != nil {
				return nil, fmt.Errorf("error loading sequence %s: %v", tableName, err)
			}
			// Initialize SequenceInfo.NextVal if it wasn't already.
			if t.SequenceInfo.NextVal == 0 {
				t.SequenceInfo.NextVal = nextID
			}
			cache, err := qr.Rows[0][1].ParseInt64()
			if err != nil {
				return nil, fmt.Errorf("error loading sequence %s: %v", tableName, err)
			}
			if cache < 1 {
				return nil, fmt.Errorf("invalid cache value for sequence %s: %d", tableName, cache)
			}
			newLast := nextID + cache
			for newLast <= t.SequenceInfo.NextVal+inc {
				newLast += cache
			}
			query = fmt.Sprintf("update %s set next_id = %d where id = 0", sqlparser.String(tableName), newLast)
			conn.RecordQuery(query)
			_, err = qre.execSQL(conn, query, false)
			if err != nil {
				return nil, err
			}
			t.SequenceInfo.LastVal = newLast
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
	}
	ret := t.SequenceInfo.NextVal
	t.SequenceInfo.NextVal += inc
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
	conn, err := qre.getConn(qre.tsv.qe.conns)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.dbConnFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, true)
}

func (qre *QueryExecutor) execInsertPK(conn *TxConnection) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.Table, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertMessage(conn *TxConnection) (*sqltypes.Result, error) {
	qre.bindVars["#time_now"] = time.Now().UnixNano()
	pkRows, err := buildValueList(qre.plan.Table, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	qr, err := qre.execInsertPKRows(conn, pkRows)
	if err != nil {
		return nil, err
	}
	bv := map[string]interface{}{
		"#pk": sqlparser.TupleEqualityList{
			Columns: qre.plan.Table.Indexes[0].Columns,
			Rows:    pkRows,
		},
	}
	readback, err := qre.txFetch(conn, qre.plan.MessageReloaderQuery, bv, nil, false, false)
	if err != nil {
		return nil, err
	}
	mrs := conn.NewMessages[qre.plan.Table.Name.String()]
	for _, row := range readback.Rows {
		mr, err := messager.BuildMessageRow(row)
		if err != nil {
			return nil, err
		}
		mrs = append(mrs, mr)
	}
	conn.NewMessages[qre.plan.Table.Name.String()] = mrs
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
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Subquery length does not match column list")
	}
	pkRows := make([][]sqltypes.Value, len(innerRows))
	for i, innerRow := range innerRows {
		pkRows[i] = applyFilterWithPKDefaults(qre.plan.Table, qre.plan.SubqueryPKColumns, innerRow)
	}
	// Validating first row is sufficient
	if err := validateRow(qre.plan.Table, qre.plan.Table.PKColumns, pkRows[0]); err != nil {
		return nil, err
	}

	qre.bindVars["#values"] = innerRows
	return qre.execInsertPKRows(conn, pkRows)
}

func (qre *QueryExecutor) execInsertPKRows(conn *TxConnection, pkRows [][]sqltypes.Value) (*sqltypes.Result, error) {
	bsc := buildStreamComment(qre.plan.Table, pkRows, nil)
	return qre.txFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc, false, true)
}

func (qre *QueryExecutor) execUpsertPK(conn *TxConnection) (*sqltypes.Result, error) {
	pkRows, err := buildValueList(qre.plan.Table, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	bsc := buildStreamComment(qre.plan.Table, pkRows, nil)
	result, err := qre.txFetch(conn, qre.plan.OuterQuery, qre.bindVars, bsc, false, true)
	if err == nil {
		return result, nil
	}
	sqlErr, ok := err.(*sqldb.SQLError)
	if !ok {
		return result, err
	}
	if sqlErr.Number() != mysqlconn.ERDupEntry {
		return nil, err
	}
	// If the error didn't match pk, just return the error without updating.
	if !strings.Contains(sqlErr.Error(), "'PRIMARY'") {
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
	pkRows, err := buildValueList(qre.plan.Table, qre.plan.PKValues, qre.bindVars)
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
	secondaryList, err := buildSecondaryList(qre.plan.Table, pkRows, qre.plan.SecondaryPKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{}
	maxRows := int(qre.tsv.qe.maxDMLRows.Get())
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
		bsc := buildStreamComment(qre.plan.Table, pkRows, secondaryList)
		qre.bindVars["#pk"] = sqlparser.TupleEqualityList{
			Columns: qre.plan.Table.Indexes[0].Columns,
			Rows:    pkRows,
		}
		r, err := qre.txFetch(conn, query, qre.bindVars, bsc, false, true)
		if err != nil {
			return nil, err
		}
		// DMLs should only return RowsAffected.
		result.RowsAffected += r.RowsAffected
	}
	if qre.plan.Table.Type == schema.Message {
		ids := conn.ChangedMessages[qre.plan.Table.Name.String()]
		for _, pkrow := range pkRows {
			ids = append(ids, pkrow[qre.plan.Table.MessageInfo.IDPKIndex].String())
		}
		conn.ChangedMessages[qre.plan.Table.Name.String()] = ids
	}
	return result, nil
}

func (qre *QueryExecutor) execSet() (*sqltypes.Result, error) {
	conn, err := qre.getConn(qre.tsv.qe.conns)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.dbConnFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, false)
}

func (qre *QueryExecutor) getConn(pool *connpool.Pool) (*connpool.DBConn, error) {
	span := trace.NewSpanFromContext(qre.ctx)
	span.StartLocal("QueryExecutor.getConn")
	defer span.Finish()

	start := time.Now()
	conn, err := pool.Get(qre.ctx)
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Now().Sub(start)
		return conn, nil
	case connpool.ErrConnPoolClosed:
		return nil, err
	}
	return nil, err
}

func (qre *QueryExecutor) qFetch(logStats *tabletenv.LogStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, nil)
	if err != nil {
		return nil, err
	}
	q, ok := qre.tsv.qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
		waitingForConnectionStart := time.Now()
		conn, err := qre.tsv.qe.conns.Get(qre.ctx)
		logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		if err != nil {
			q.Err = err
		} else {
			defer conn.Recycle()
			q.Result, q.Err = qre.execSQL(conn, sql, false)
		}
	} else {
		logStats.QuerySources |= tabletenv.QuerySourceConsolidator
		startTime := time.Now()
		q.Wait()
		tabletenv.WaitStats.Record("Consolidations", startTime)
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

// dbConnFetch fetches from a connpool.DBConn.
func (qre *QueryExecutor) dbConnFetch(conn *connpool.DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, wantfields bool) (*sqltypes.Result, error) {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return nil, err
	}
	return qre.execSQL(conn, sql, wantfields)
}

// streamFetch performs a streaming fetch.
func (qre *QueryExecutor) streamFetch(conn *connpool.DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, includedFields querypb.ExecuteOptions_IncludedFields, callback func(*sqltypes.Result) error) error {
	sql, err := qre.generateFinalSQL(parsedQuery, bindVars, buildStreamComment)
	if err != nil {
		return err
	}
	return qre.execStreamSQL(conn, sql, includedFields, callback)
}

func (qre *QueryExecutor) generateFinalSQL(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (string, error) {
	bindVars["#maxLimit"] = qre.tsv.qe.maxResultSize.Get() + 1
	sql, err := parsedQuery.GenerateQuery(bindVars)
	if err != nil {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s", err)
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
	return conn.Exec(qre.ctx, sql, int(qre.tsv.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStreamSQL(conn *connpool.DBConn, sql string, includedFields querypb.ExecuteOptions_IncludedFields, callback func(*sqltypes.Result) error) error {
	start := time.Now()
	err := conn.Stream(qre.ctx, sql, callback, int(qre.tsv.qe.streamBufferSize.Get()), includedFields)
	qre.logStats.AddRewrittenSQL(sql, start)
	if err != nil {
		// MySQL error that isn't due to a connection issue
		return err
	}
	return nil
}
