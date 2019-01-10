/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletserver

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/messager"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TODO(sougou): remove after affected parties have transitioned to new behavior.
var legacyTableACL = flag.Bool("legacy-table-acl", false, "deprecated: this flag can be used to revert to the older table ACL behavior, which checked access for at most one table")

// QueryExecutor is used for executing a query request.
type QueryExecutor struct {
	query          string
	marginComments sqlparser.MarginComments
	bindVars       map[string]*querypb.BindVariable
	transactionID  int64
	options        *querypb.ExecuteOptions
	plan           *TabletPlan
	ctx            context.Context
	logStats       *tabletenv.LogStats
	tsv            *TabletServer
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

		mysqlTime := qre.logStats.MysqlResponseTime
		tableName := qre.plan.TableName().String()
		if tableName == "" {
			tableName = "Join"
		}

		if reply == nil {
			qre.tsv.qe.AddStats(planName, tableName, 1, duration, mysqlTime, 0, 1)
			qre.plan.AddStats(1, duration, mysqlTime, 0, 1)
			return
		}
		qre.tsv.qe.AddStats(planName, tableName, 1, duration, mysqlTime, int64(reply.RowsAffected), 0)
		qre.plan.AddStats(1, duration, mysqlTime, int64(reply.RowsAffected), 0)
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
			if !qre.tsv.qe.allowUnsafeDMLs && (qre.tsv.qe.binlogFormat != connpool.BinlogFormatRow) {
				return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cannot identify primary key of statement")
			}
			return qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil, true, true)
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
		case planbuilder.PlanOtherRead, planbuilder.PlanOtherAdmin:
			return qre.execSQL(conn, qre.query, true)
		case planbuilder.PlanUpsertPK:
			return qre.execUpsertPK(conn)
		case planbuilder.PlanSet:
			return qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil, true, true)
		case planbuilder.PlanPassSelect, planbuilder.PlanSelectLock:
			return qre.execDirect(conn)
		default:
			// handled above:
			// planbuilder.PlanNextval
			// planbuilder.PlanDDL

			// not valid for Execute:
			// planbuilder.PlanSelectStream
			// planbuilder.PlanMessageStream:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s unexpected plan type", qre.plan.PlanID.String())
		}
	} else {
		switch qre.plan.PlanID {
		case planbuilder.PlanPassSelect:
			return qre.execSelect()
		case planbuilder.PlanSelectLock:
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s disallowed outside transaction", qre.plan.PlanID.String())
		case planbuilder.PlanSet:
			return qre.execSet()
		case planbuilder.PlanOtherRead:
			conn, connErr := qre.getConn()
			if connErr != nil {
				return nil, connErr
			}
			defer conn.Recycle()
			return qre.execSQL(conn, qre.query, true)

		case planbuilder.PlanPassDML:
			fallthrough
		case planbuilder.PlanInsertPK:
			fallthrough
		case planbuilder.PlanInsertMessage:
			fallthrough
		case planbuilder.PlanInsertSubquery:
			fallthrough
		case planbuilder.PlanDMLPK:
			fallthrough
		case planbuilder.PlanDMLSubquery:
			fallthrough
		case planbuilder.PlanUpsertPK:
			if !qre.tsv.qe.autoCommit.Get() {
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s disallowed outside transaction", qre.plan.PlanID.String())
			}
			return qre.execDmlAutoCommit()
		default:
			// handled above:
			// planbuilder.PlanNextval
			// planbuilder.PlanDDL

			// not valid for Execute:
			// planbuilder.PlanSelectStream
			// planbuilder.PlanMessageStream:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s unexpected plan type", qre.plan.PlanID.String())
		}
	}
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(callback func(*sqltypes.Result) error) error {
	qre.logStats.OriginalSQL = qre.query
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		tabletenv.QueryStats.Record(qre.plan.PlanID.String(), start)
		tabletenv.RecordUserQuery(qre.ctx, qre.plan.TableName(), "Stream", int64(time.Now().Sub(start)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return err
	}

	conn, err := qre.getStreamConn()
	if err != nil {
		return err
	}
	defer conn.Recycle()

	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	qre.tsv.qe.streamQList.Add(qd)
	defer qre.tsv.qe.streamQList.Remove(qd)

	return qre.streamFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, callback)
}

// MessageStream streams messages from a message table.
func (qre *QueryExecutor) MessageStream(callback func(*sqltypes.Result) error) error {
	qre.logStats.OriginalSQL = qre.query
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		tabletenv.QueryStats.Record(qre.plan.PlanID.String(), start)
		tabletenv.RecordUserQuery(qre.ctx, qre.plan.TableName(), "MessageStream", int64(time.Now().Sub(start)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return err
	}

	done, err := qre.tsv.messager.Subscribe(qre.ctx, qre.plan.TableName().String(), func(r *sqltypes.Result) error {
		select {
		case <-qre.ctx.Done():
			return io.EOF
		default:
		}
		return callback(r)
	})
	if err != nil {
		return err
	}
	<-done
	return nil
}

func (qre *QueryExecutor) execDmlAutoCommit() (reply *sqltypes.Result, err error) {
	return qre.execAsTransaction(func(conn *TxConnection) (reply *sqltypes.Result, err error) {
		switch qre.plan.PlanID {
		case planbuilder.PlanPassDML:
			if !qre.tsv.qe.allowUnsafeDMLs && (qre.tsv.qe.binlogFormat != connpool.BinlogFormatRow) {
				return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cannot identify primary key of statement")
			}
			reply, err = qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil, true, true)
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
	conn, err := qre.tsv.te.txPool.LocalBegin(qre.ctx, qre.options)
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

// checkPermissions returns an error if the query does not pass all checks
// (query blacklisting, table ACL).
func (qre *QueryExecutor) checkPermissions() error {
	// Skip permissions check if the context is local.
	if tabletenv.IsLocalContext(qre.ctx) {
		return nil
	}

	// Check if the query is blacklisted.
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

	// Skip ACL check for queries against the dummy dual table
	if qre.plan.TableName().String() == "dual" {
		return nil
	}

	// Skip the ACL check if the connecting user is an exempted superuser.
	// Necessary to whitelist e.g. direct vtworker access.
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

	// Skip the ACL check if the caller id is an exempted superuser.
	if qre.tsv.qe.exemptACL != nil && qre.tsv.qe.exemptACL.IsMember(callerID) {
		qre.tsv.qe.tableaclExemptCount.Add(1)
		return nil
	}

	if *legacyTableACL {
		if !qre.plan.TableName().IsEmpty() {
			return qre.checkAccess(qre.plan.LegacyAuthorized, qre.plan.TableName().String(), callerID)
		}
	} else {
		for i, auth := range qre.plan.Authorized {
			if err := qre.checkAccess(auth, qre.plan.Permissions[i].TableName, callerID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (qre *QueryExecutor) checkAccess(authorized *tableacl.ACLResult, tableName string, callerID *querypb.VTGateCallerID) error {
	statsKey := []string{tableName, authorized.GroupName, qre.plan.PlanID.String(), callerID.Username}
	if !authorized.IsMember(callerID) {
		if qre.tsv.qe.enableTableACLDryRun {
			tabletenv.TableaclPseudoDenied.Add(statsKey, 1)
			return nil
		}
		if qre.tsv.qe.strictTableACL {
			errStr := fmt.Sprintf("table acl error: %q %v cannot run %v on table %q", callerID.Username, callerID.Groups, qre.plan.PlanID, tableName)
			tabletenv.TableaclDenied.Add(statsKey, 1)
			qre.tsv.qe.accessCheckerLogger.Infof("%s", errStr)
			return vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "%s", errStr)
		}
		return nil
	}
	tabletenv.TableaclAllowed.Add(statsKey, 1)
	return nil
}

func (qre *QueryExecutor) execDDL() (*sqltypes.Result, error) {
	sql := qre.query
	var err error
	if qre.plan.FullQuery != nil {
		sql, _, err = qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	ddlPlan := planbuilder.DDLParse(sql)
	if ddlPlan.Action == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "DDL is not understood")
	}

	defer func() {
		err := qre.tsv.se.Reload(qre.ctx)
		if err != nil {
			log.Errorf("failed to reload schema %v", err)
		}
	}()

	if qre.transactionID != 0 {
		conn, err := qre.tsv.te.txPool.Get(qre.transactionID, "DDL begin again")
		if err != nil {
			return nil, err
		}
		defer conn.Recycle()
		result, err := qre.execSQL(conn, sql, true)
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
		return qre.execSQL(conn, sql, true)
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
			nextID, err := sqltypes.ToInt64(qr.Rows[0][0])
			if err != nil {
				return nil, vterrors.Wrapf(err, "error loading sequence %s", tableName)
			}
			// Initialize SequenceInfo.NextVal if it wasn't already.
			if t.SequenceInfo.NextVal == 0 {
				t.SequenceInfo.NextVal = nextID
			}
			cache, err := sqltypes.ToInt64(qr.Rows[0][1])
			if err != nil {
				return nil, vterrors.Wrapf(err, "error loading sequence %s", tableName)
			}
			if cache < 1 {
				return nil, fmt.Errorf("invalid cache value for sequence %s: %d", tableName, cache)
			}
			newLast := nextID + cache
			for newLast < t.SequenceInfo.NextVal+inc {
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
			sqltypes.NewInt64(ret),
		}},
		RowsAffected: 1,
	}, nil
}

// execDirect is for reads inside transactions. Always send to MySQL.
func (qre *QueryExecutor) execDirect(conn *TxConnection) (*sqltypes.Result, error) {
	if qre.plan.Fields != nil {
		result, err := qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil, true, false)
		if err != nil {
			return nil, err
		}
		result.Fields = qre.plan.Fields
		return result, nil
	}
	return qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil, true, false)
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
	conn, err := qre.getConn()
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
	return qre.execInsertPKRows(conn, nil, pkRows)
}

func (qre *QueryExecutor) execInsertMessage(conn *TxConnection) (*sqltypes.Result, error) {
	qre.bindVars["#time_now"] = sqltypes.Int64BindVariable(time.Now().UnixNano())
	pkRows, err := buildValueList(qre.plan.Table, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	qr, err := qre.execInsertPKRows(conn, nil, pkRows)
	if err != nil {
		return nil, err
	}

	// If an auto-inc value was generated, it could be the id column.
	// If so, we have to populate it.
	if qr.InsertID != 0 {
		id := int64(qr.InsertID)
		idPKIndex := qre.plan.Table.MessageInfo.IDPKIndex
		for _, row := range pkRows {
			if !row[idPKIndex].IsNull() {
				// If a value was supplied, either it was not the auto-inc column
				// or values were partially supplied for an auto-inc column.
				// If it's the former, there is nothing more to do.
				// If it's the latter, we cannot predict the values for subsequent
				// rows. So, we still break out of this loop, and will rely on
				// the poller to eventually pick up the rows from the database.
				break
			}
			row[idPKIndex] = sqltypes.NewInt64(id)
			id++
		}
	}

	// Re-read the inserted rows to prime the cache.
	extras := map[string]sqlparser.Encodable{
		"#pk": &sqlparser.TupleEqualityList{
			Columns: qre.plan.Table.Indexes[0].Columns,
			Rows:    pkRows,
		},
	}
	tableName := qre.plan.Table.Name.String()
	loadMessages, err := qre.tsv.messager.GenerateLoadMessagesQuery(tableName)
	if err != nil {
		return nil, err
	}
	readback, err := qre.txFetch(conn, loadMessages, qre.bindVars, extras, nil, true, false)
	if err != nil {
		return nil, err
	}

	// Append to the list of pending rows to be sent
	// to the cache on successful commit.
	mrs := conn.NewMessages[tableName]
	for _, row := range readback.Rows {
		mr, err := messager.BuildMessageRow(row)
		if err != nil {
			return nil, err
		}
		mrs = append(mrs, mr)
	}
	conn.NewMessages[tableName] = mrs
	return qr, nil
}

func (qre *QueryExecutor) execInsertSubquery(conn *TxConnection) (*sqltypes.Result, error) {
	innerResult, err := qre.txFetch(conn, qre.plan.Subquery, qre.bindVars, nil, nil, true, false)
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

	extras := map[string]sqlparser.Encodable{
		"#values": sqlparser.InsertValues(innerRows),
	}
	return qre.execInsertPKRows(conn, extras, pkRows)
}

func (qre *QueryExecutor) execInsertPKRows(conn *TxConnection, extras map[string]sqlparser.Encodable, pkRows [][]sqltypes.Value) (*sqltypes.Result, error) {
	var bsc []byte
	// Build comments only if we're not in RBR mode.
	if qre.tsv.qe.binlogFormat != connpool.BinlogFormatRow {
		secondaryList, err := buildSecondaryList(qre.plan.Table, pkRows, qre.plan.SecondaryPKValues, qre.bindVars)
		if err != nil {
			return nil, err
		}
		bsc = buildStreamComment(qre.plan.Table, pkRows, secondaryList)
	}
	return qre.txFetch(conn, qre.plan.OuterQuery, qre.bindVars, extras, bsc, true, true)
}

func (qre *QueryExecutor) execUpsertPK(conn *TxConnection) (*sqltypes.Result, error) {
	// For RBR, upserts are passed through.
	if qre.tsv.qe.binlogFormat == connpool.BinlogFormatRow {
		return qre.txFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, nil, true, true)
	}

	// For statement or mixed mode, we have to split into two ops.
	pkRows, err := buildValueList(qre.plan.Table, qre.plan.PKValues, qre.bindVars)
	if err != nil {
		return nil, err
	}
	// We do not need to build the secondary list for the insert part.
	// But the part that updates will build it if it gets executed,
	// because it's the one that can change the primary keys.
	bsc := buildStreamComment(qre.plan.Table, pkRows, nil)
	result, err := qre.txFetch(conn, qre.plan.OuterQuery, qre.bindVars, nil, bsc, true, true)
	if err == nil {
		return result, nil
	}
	sqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		return result, err
	}
	if sqlErr.Number() != mysql.ERDupEntry {
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
	innerResult, err := qre.txFetch(conn, qre.plan.Subquery, qre.bindVars, nil, nil, true, false)
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
		var bsc []byte
		// Build comments only if we're not in RBR mode.
		if qre.tsv.qe.binlogFormat != connpool.BinlogFormatRow {
			bsc = buildStreamComment(qre.plan.Table, pkRows, secondaryList)
		}
		extras := map[string]sqlparser.Encodable{
			"#pk": &sqlparser.TupleEqualityList{
				Columns: qre.plan.Table.Indexes[0].Columns,
				Rows:    pkRows,
			},
		}
		r, err := qre.txFetch(conn, query, qre.bindVars, extras, bsc, true, true)
		if err != nil {
			return nil, err
		}

		// UPDATEs can return InsertID when LAST_INSERT_ID(expr) is used. In
		// this case it should be the same for all rows.
		result.InsertID = r.InsertID

		// DMLs should all return RowsAffected.
		result.RowsAffected += r.RowsAffected
	}
	if qre.plan.Table.Type == schema.Message {
		ids := conn.ChangedMessages[qre.plan.Table.Name.String()]
		for _, pkrow := range pkRows {
			ids = append(ids, pkrow[qre.plan.Table.MessageInfo.IDPKIndex].ToString())
		}
		conn.ChangedMessages[qre.plan.Table.Name.String()] = ids
	}
	return result, nil
}

func (qre *QueryExecutor) execSet() (*sqltypes.Result, error) {
	conn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.dbConnFetch(conn, qre.plan.FullQuery, qre.bindVars, nil, false)
}

func (qre *QueryExecutor) getConn() (*connpool.DBConn, error) {
	span := trace.NewSpanFromContext(qre.ctx)
	span.StartLocal("QueryExecutor.getConn")
	defer span.Finish()

	start := time.Now()
	conn, err := qre.tsv.qe.getQueryConn(qre.ctx)
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Now().Sub(start)
		return conn, nil
	case connpool.ErrConnPoolClosed:
		return nil, err
	}
	return nil, err
}

func (qre *QueryExecutor) getStreamConn() (*connpool.DBConn, error) {
	span := trace.NewSpanFromContext(qre.ctx)
	span.StartLocal("QueryExecutor.getStreamConn")
	defer span.Finish()

	start := time.Now()
	conn, err := qre.tsv.qe.streamConns.Get(qre.ctx)
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Now().Sub(start)
		return conn, nil
	case connpool.ErrConnPoolClosed:
		return nil, err
	}
	return nil, err
}

func (qre *QueryExecutor) qFetch(logStats *tabletenv.LogStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	sql, sqlWithoutComments, err := qre.generateFinalSQL(parsedQuery, bindVars, nil, nil)
	if err != nil {
		return nil, err
	}
	if qre.tsv.qe.enableConsolidator {
		q, original := qre.tsv.qe.consolidator.Create(string(sqlWithoutComments))
		if original {
			defer q.Broadcast()
			conn, err := qre.getConn()

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
	conn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	res, err := qre.execSQL(conn, sql, false)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// txFetch fetches from a TxConnection.
func (qre *QueryExecutor) txFetch(conn *TxConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable, extras map[string]sqlparser.Encodable, buildStreamComment []byte, wantfields, record bool) (*sqltypes.Result, error) {
	sql, _, err := qre.generateFinalSQL(parsedQuery, bindVars, extras, buildStreamComment)
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
func (qre *QueryExecutor) dbConnFetch(conn *connpool.DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable, buildStreamComment []byte, wantfields bool) (*sqltypes.Result, error) {
	sql, _, err := qre.generateFinalSQL(parsedQuery, bindVars, nil, buildStreamComment)
	if err != nil {
		return nil, err
	}
	return qre.execSQL(conn, sql, wantfields)
}

// streamFetch performs a streaming fetch.
func (qre *QueryExecutor) streamFetch(conn *connpool.DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable, buildStreamComment []byte, callback func(*sqltypes.Result) error) error {
	sql, _, err := qre.generateFinalSQL(parsedQuery, bindVars, nil, buildStreamComment)
	if err != nil {
		return err
	}
	return qre.execStreamSQL(conn, sql, callback)
}

func (qre *QueryExecutor) generateFinalSQL(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable, extras map[string]sqlparser.Encodable, buildStreamComment []byte) (string, string, error) {
	bindVars["#maxLimit"] = sqltypes.Int64BindVariable(qre.getLimit(parsedQuery))

	var sql []byte
	sql = append(sql, qre.marginComments.Leading...)

	query, err := parsedQuery.GenerateQuery(bindVars, extras)
	if err != nil {
		return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s", err)
	}
	sql = append(sql, query...)
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	fullSQL := append(sql, qre.marginComments.Trailing...)
	return hack.String(fullSQL), hack.String(sql), nil
}

func (qre *QueryExecutor) getLimit(query *sqlparser.ParsedQuery) int64 {
	maxRows := qre.tsv.qe.maxResultSize.Get()
	sqlLimit := qre.options.GetSqlSelectLimit()
	if sqlLimit > 0 && sqlLimit < maxRows && strings.HasPrefix(sqlparser.StripLeadingComments(query.Query), "select") {
		return sqlLimit
	}
	return maxRows + 1
}

// poolConn is an abstraction for reusing code in execSQL.
type poolConn interface {
	Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
}

func (qre *QueryExecutor) execSQL(conn poolConn, sql string, wantfields bool) (*sqltypes.Result, error) {
	defer qre.logStats.AddRewrittenSQL(sql, time.Now())
	res, err := conn.Exec(qre.ctx, sql, int(qre.tsv.qe.maxResultSize.Get()), wantfields)
	warnThreshold := qre.tsv.qe.warnResultSize.Get()
	if res != nil && warnThreshold > 0 && int64(len(res.Rows)) > warnThreshold {
		callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
		tabletenv.Warnings.Add("ResultsExceeded", 1)
		log.Warningf("CallerID: %s Results returned (%v) exceeds warning threshold (%v): %q", callerID.Username, len(res.Rows), warnThreshold, sql)
	}
	return res, err
}

func (qre *QueryExecutor) execStreamSQL(conn *connpool.DBConn, sql string, callback func(*sqltypes.Result) error) error {
	start := time.Now()
	err := conn.Stream(qre.ctx, sql, callback, int(qre.tsv.qe.streamBufferSize.Get()), sqltypes.IncludeFieldsOrDefault(qre.options))
	qre.logStats.AddRewrittenSQL(sql, start)
	if err != nil {
		// MySQL error that isn't due to a connection issue
		return err
	}
	return nil
}
