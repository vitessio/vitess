/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"io"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

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
	p "vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// QueryExecutor is used for executing a query request.
type QueryExecutor struct {
	query          string
	marginComments sqlparser.MarginComments
	bindVars       map[string]*querypb.BindVariable
	connID         int64
	options        *querypb.ExecuteOptions
	plan           *TabletPlan
	ctx            context.Context
	logStats       *tabletenv.LogStats
	tsv            *TabletServer
	tabletType     topodatapb.TabletType
}

var sequenceFields = []*querypb.Field{
	{
		Name: "nextval",
		Type: sqltypes.Int64,
	},
}

// Execute performs a non-streaming query execution.
func (qre *QueryExecutor) Execute() (reply *sqltypes.Result, err error) {
	planName := qre.plan.PlanID.String()
	qre.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Since(start)
		qre.tsv.stats.QueryTimings.Add(planName, duration)
		qre.recordUserQuery("Execute", int64(duration))

		mysqlTime := qre.logStats.MysqlResponseTime
		tableName := qre.plan.TableName().String()
		if tableName == "" {
			tableName = "Join"
		}

		if reply == nil {
			qre.tsv.qe.AddStats(planName, tableName, 1, duration, mysqlTime, 0, 1)
			qre.plan.AddStats(1, duration, mysqlTime, 0, 0, 1)
			return
		}
		qre.tsv.qe.AddStats(planName, tableName, 1, duration, mysqlTime, int64(reply.RowsAffected), 0)
		qre.plan.AddStats(1, duration, mysqlTime, reply.RowsAffected, uint64(len(reply.Rows)), 0)
		qre.logStats.RowsAffected = int(reply.RowsAffected)
		qre.logStats.Rows = reply.Rows
		qre.tsv.Stats().ResultHistogram.Add(int64(len(reply.Rows)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return nil, err
	}

	switch qre.plan.PlanID {
	case p.PlanNextval:
		return qre.execNextval()
	case p.PlanSelectImpossible:
		// If the fields did not get cached, we have send the query
		// to mysql, which you can see below.
		if qre.plan.Fields != nil {
			return &sqltypes.Result{
				Fields: qre.plan.Fields,
			}, nil
		}
	}

	if qre.connID != 0 {
		// Need upfront connection for DMLs and transactions
		conn, err := qre.tsv.te.txPool.GetAndLock(qre.connID, "for query")
		if err != nil {
			return nil, err
		}
		defer conn.Unlock()
		return qre.txConnExec(conn)
	}

	switch qre.plan.PlanID {
	case p.PlanSelect, p.PlanSelectImpossible, p.PlanShow:
		maxrows := qre.getSelectLimit()
		qre.bindVars["#maxLimit"] = sqltypes.Int64BindVariable(maxrows + 1)
		if qre.bindVars[sqltypes.BvReplaceSchemaName] != nil {
			qre.bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(qre.tsv.config.DB.DBName)
		}
		qr, err := qre.execSelect()
		if err != nil {
			return nil, err
		}
		if err := qre.verifyRowCount(int64(len(qr.Rows)), maxrows); err != nil {
			return nil, err
		}
		return qr, nil
	case p.PlanOtherRead, p.PlanOtherAdmin, p.PlanFlush:
		return qre.execOther()
	case p.PlanSavepoint, p.PlanRelease, p.PlanSRollback:
		return qre.execOther()
	case p.PlanInsert, p.PlanUpdate, p.PlanDelete, p.PlanInsertMessage, p.PlanDDL, p.PlanLoad:
		return qre.execAutocommit(qre.txConnExec)
	case p.PlanUpdateLimit, p.PlanDeleteLimit:
		return qre.execAsTransaction(qre.txConnExec)
	case p.PlanCallProc:
		return qre.execCallProc()
	case p.PlanAlterMigration:
		return qre.execAlterMigration()
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s unexpected plan type", qre.plan.PlanID.String())
}

func (qre *QueryExecutor) execAutocommit(f func(conn *StatefulConnection) (*sqltypes.Result, error)) (reply *sqltypes.Result, err error) {
	if qre.options == nil {
		qre.options = &querypb.ExecuteOptions{}
	}
	qre.options.TransactionIsolation = querypb.ExecuteOptions_AUTOCOMMIT

	conn, _, err := qre.tsv.te.txPool.Begin(qre.ctx, qre.options, false, 0, nil)

	if err != nil {
		return nil, err
	}
	defer qre.tsv.te.txPool.RollbackAndRelease(qre.ctx, conn)

	return f(conn)
}

func (qre *QueryExecutor) execAsTransaction(f func(conn *StatefulConnection) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	conn, beginSQL, err := qre.tsv.te.txPool.Begin(qre.ctx, qre.options, false, 0, nil)
	if err != nil {
		return nil, err
	}
	defer qre.tsv.te.txPool.RollbackAndRelease(qre.ctx, conn)
	qre.logStats.AddRewrittenSQL(beginSQL, time.Now())

	result, err := f(conn)
	if err != nil {
		// dbConn is nil, it means the transaction was aborted.
		// If so, we should not relog the rollback.
		// TODO(sougou): these txPool functions should take the logstats
		// and log any statements they issue. This needs to be done as
		// a separate refactor because it impacts lot of code.
		if conn.IsInTransaction() {
			defer qre.logStats.AddRewrittenSQL("rollback", time.Now())
		}
		return nil, err
	}

	defer qre.logStats.AddRewrittenSQL("commit", time.Now())
	if _, err := qre.tsv.te.txPool.Commit(qre.ctx, conn); err != nil {
		return nil, err
	}
	return result, nil
}

func (qre *QueryExecutor) txConnExec(conn *StatefulConnection) (*sqltypes.Result, error) {
	switch qre.plan.PlanID {
	case p.PlanInsert, p.PlanUpdate, p.PlanDelete, p.PlanSet:
		return qre.txFetch(conn, true)
	case p.PlanInsertMessage:
		qre.bindVars["#time_now"] = sqltypes.Int64BindVariable(time.Now().UnixNano())
		return qre.txFetch(conn, true)
	case p.PlanUpdateLimit, p.PlanDeleteLimit:
		return qre.execDMLLimit(conn)
	case p.PlanOtherRead, p.PlanOtherAdmin, p.PlanFlush:
		return qre.execStatefulConn(conn, qre.query, true)
	case p.PlanSavepoint, p.PlanRelease, p.PlanSRollback:
		return qre.execStatefulConn(conn, qre.query, true)
	case p.PlanSelect, p.PlanSelectImpossible, p.PlanShow:
		maxrows := qre.getSelectLimit()
		qre.bindVars["#maxLimit"] = sqltypes.Int64BindVariable(maxrows + 1)
		if qre.bindVars[sqltypes.BvReplaceSchemaName] != nil {
			qre.bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(qre.tsv.config.DB.DBName)
		}
		qr, err := qre.txFetch(conn, false)
		if err != nil {
			return nil, err
		}
		if err := qre.verifyRowCount(int64(len(qr.Rows)), maxrows); err != nil {
			return nil, err
		}
		return qr, nil
	case p.PlanDDL:
		return qre.execDDL(conn)
	case p.PlanLoad:
		return qre.execLoad(conn)
	case p.PlanCallProc:
		return qre.execProc(conn)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s unexpected plan type", qre.plan.PlanID.String())
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(callback func(*sqltypes.Result) error) error {
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		qre.tsv.stats.QueryTimings.Record(qre.plan.PlanID.String(), start)
		qre.recordUserQuery("Stream", int64(time.Since(start)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return err
	}

	// if we have a transaction id, let's use the txPool for this query
	var conn *connpool.DBConn
	if qre.connID != 0 {
		txConn, err := qre.tsv.te.txPool.GetAndLock(qre.connID, "for streaming query")
		if err != nil {
			return err
		}
		defer txConn.Unlock()
		conn = txConn.UnderlyingDBConn()
	} else {
		dbConn, err := qre.getStreamConn()
		if err != nil {
			return err
		}
		defer dbConn.Recycle()
		conn = dbConn
	}

	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return err
	}
	return qre.execStreamSQL(conn, sql, callback)
}

// MessageStream streams messages from a message table.
func (qre *QueryExecutor) MessageStream(callback func(*sqltypes.Result) error) error {
	qre.logStats.OriginalSQL = qre.query
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		qre.tsv.stats.QueryTimings.Record(qre.plan.PlanID.String(), start)
		qre.recordUserQuery("MessageStream", int64(time.Since(start)))
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

	for i, auth := range qre.plan.Authorized {
		if err := qre.checkAccess(auth, qre.plan.Permissions[i].TableName, callerID); err != nil {
			return err
		}
	}

	return nil
}

func (qre *QueryExecutor) checkAccess(authorized *tableacl.ACLResult, tableName string, callerID *querypb.VTGateCallerID) error {
	statsKey := []string{tableName, authorized.GroupName, qre.plan.PlanID.String(), callerID.Username}
	if !authorized.IsMember(callerID) {
		if qre.tsv.qe.enableTableACLDryRun {
			qre.tsv.Stats().TableaclPseudoDenied.Add(statsKey, 1)
			return nil
		}

		// Skip ACL check for queries against the dummy dual table
		if tableName == "dual" {
			return nil
		}

		if qre.tsv.qe.strictTableACL {
			errStr := fmt.Sprintf("table acl error: %q %v cannot run %v on table %q", callerID.Username, callerID.Groups, qre.plan.PlanID, tableName)
			qre.tsv.Stats().TableaclDenied.Add(statsKey, 1)
			qre.tsv.qe.accessCheckerLogger.Infof("%s", errStr)
			return vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "%s", errStr)
		}
		return nil
	}
	qre.tsv.Stats().TableaclAllowed.Add(statsKey, 1)
	return nil
}

func (qre *QueryExecutor) execDDL(conn *StatefulConnection) (*sqltypes.Result, error) {
	defer func() {
		if err := qre.tsv.se.Reload(qre.ctx); err != nil {
			log.Errorf("failed to reload schema %v", err)
		}
	}()

	sql := qre.query
	// If FullQuery is not nil, then the DDL query was fully parsed
	// and we should use the ast to generate the query instead.
	if qre.plan.FullQuery != nil {
		var err error
		sql, _, err = qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
		if err != nil {
			return nil, err
		}
	}
	result, err := qre.execStatefulConn(conn, sql, true)
	if err != nil {
		return nil, err
	}
	// Only perform this operation when the connection has transaction open.
	// TODO: This actually does not retain the old transaction. We should see how to provide correct behaviour to client.
	if conn.txProps != nil {
		err = qre.BeginAgain(qre.ctx, conn)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (qre *QueryExecutor) execLoad(conn *StatefulConnection) (*sqltypes.Result, error) {
	result, err := qre.execStatefulConn(conn, qre.query, true)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// BeginAgain commits the existing transaction and begins a new one
func (*QueryExecutor) BeginAgain(ctx context.Context, dc *StatefulConnection) error {
	if dc.IsClosed() || dc.TxProperties().Autocommit {
		return nil
	}
	if _, err := dc.Exec(ctx, "commit", 1, false); err != nil {
		return err
	}
	if _, err := dc.Exec(ctx, "begin", 1, false); err != nil {
		return err
	}
	return nil
}

func (qre *QueryExecutor) execNextval() (*sqltypes.Result, error) {
	inc, err := resolveNumber(qre.plan.NextCount, qre.bindVars)
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
		_, err := qre.execAsTransaction(func(conn *StatefulConnection) (*sqltypes.Result, error) {
			query := fmt.Sprintf("select next_id, cache from %s where id = 0 for update", sqlparser.String(tableName))
			qr, err := qre.execStatefulConn(conn, query, false)
			if err != nil {
				return nil, err
			}
			if len(qr.Rows) != 1 {
				return nil, fmt.Errorf("unexpected rows from reading sequence %s (possible mis-route): %d", tableName, len(qr.Rows))
			}
			nextID, err := evalengine.ToInt64(qr.Rows[0][0])
			if err != nil {
				return nil, vterrors.Wrapf(err, "error loading sequence %s", tableName)
			}
			// If LastVal does not match next ID, then either:
			// VTTablet just started, and we're initializing the cache, or
			// Someone reset the id underneath us.
			if t.SequenceInfo.LastVal != nextID {
				if nextID < t.SequenceInfo.LastVal {
					log.Warningf("Sequence next ID value %v is below the currently cached max %v, updating it to max", nextID, t.SequenceInfo.LastVal)
					nextID = t.SequenceInfo.LastVal
				}
				t.SequenceInfo.NextVal = nextID
				t.SequenceInfo.LastVal = nextID
			}
			cache, err := evalengine.ToInt64(qr.Rows[0][1])
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
			conn.TxProperties().RecordQuery(query)
			_, err = qre.execStatefulConn(conn, query, false)
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
	}, nil
}

// execSelect sends a query to mysql only if another identical query is not running. Otherwise, it waits and
// reuses the result. If the plan is missing field info, it sends the query to mysql requesting full info.
func (qre *QueryExecutor) execSelect() (*sqltypes.Result, error) {
	if qre.tsv.qe.enableQueryPlanFieldCaching && qre.plan.Fields != nil {
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

	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	return qre.execDBConn(conn, sql, true)
}

func (qre *QueryExecutor) execDMLLimit(conn *StatefulConnection) (*sqltypes.Result, error) {
	maxrows := qre.tsv.qe.maxResultSize.Get()
	qre.bindVars["#maxLimit"] = sqltypes.Int64BindVariable(maxrows + 1)
	result, err := qre.txFetch(conn, true)
	if err != nil {
		return nil, err
	}
	if err := qre.verifyRowCount(int64(result.RowsAffected), maxrows); err != nil {
		defer qre.logStats.AddRewrittenSQL("rollback", time.Now())
		_ = qre.tsv.te.txPool.Rollback(qre.ctx, conn)
		return nil, err
	}
	return result, nil
}

func (qre *QueryExecutor) verifyRowCount(count, maxrows int64) error {
	if count > maxrows {
		callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
		return mysql.NewSQLError(mysql.ERVitessMaxRowsExceeded, mysql.SSUnknownSQLState, "caller id: %s: row count exceeded %d", callerID.Username, maxrows)
	}
	warnThreshold := qre.tsv.qe.warnResultSize.Get()
	if warnThreshold > 0 && count > warnThreshold {
		callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
		qre.tsv.Stats().Warnings.Add("ResultsExceeded", 1)
		log.Warningf("caller id: %s row count %v exceeds warning threshold %v: %q", callerID.Username, count, warnThreshold, queryAsString(qre.plan.FullQuery.Query, qre.bindVars))
	}
	return nil
}

func (qre *QueryExecutor) execOther() (*sqltypes.Result, error) {
	conn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.execDBConn(conn, qre.query, true)
}

func (qre *QueryExecutor) getConn() (*connpool.DBConn, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.getConn")
	defer span.Finish()

	start := time.Now()
	conn, err := qre.tsv.qe.conns.Get(ctx)
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Since(start)
		return conn, nil
	case connpool.ErrConnPoolClosed:
		return nil, err
	}
	return nil, err
}

func (qre *QueryExecutor) getStreamConn() (*connpool.DBConn, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.getStreamConn")
	defer span.Finish()

	start := time.Now()
	conn, err := qre.tsv.qe.streamConns.Get(ctx)
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Since(start)
		return conn, nil
	case connpool.ErrConnPoolClosed:
		return nil, err
	}
	return nil, err
}

func (qre *QueryExecutor) qFetch(logStats *tabletenv.LogStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	sql, sqlWithoutComments, err := qre.generateFinalSQL(parsedQuery, bindVars)
	if err != nil {
		return nil, err
	}
	// Check tablet type.
	if cm := qre.tsv.qe.consolidatorMode.Get(); cm == tabletenv.Enable || (cm == tabletenv.NotOnMaster && qre.tabletType != topodatapb.TabletType_MASTER) {
		q, original := qre.tsv.qe.consolidator.Create(sqlWithoutComments)
		if original {
			defer q.Broadcast()
			conn, err := qre.getConn()

			if err != nil {
				q.Err = err
			} else {
				defer conn.Recycle()
				q.Result, q.Err = qre.execDBConn(conn, sql, false)
			}
		} else {
			logStats.QuerySources |= tabletenv.QuerySourceConsolidator
			startTime := time.Now()
			q.Wait()
			qre.tsv.stats.WaitTimings.Record("Consolidations", startTime)
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
	res, err := qre.execDBConn(conn, sql, false)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// txFetch fetches from a TxConnection.
func (qre *QueryExecutor) txFetch(conn *StatefulConnection, record bool) (*sqltypes.Result, error) {
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	qr, err := qre.execStatefulConn(conn, sql, true)
	if err != nil {
		return nil, err
	}
	// Only record successful queries.
	if record {
		conn.TxProperties().RecordQuery(sql)
	}
	return qr, nil
}

func (qre *QueryExecutor) generateFinalSQL(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (string, string, error) {
	var buf strings.Builder
	buf.WriteString(qre.marginComments.Leading)

	query, err := parsedQuery.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s", err)
	}
	withoutComments := query
	buf.WriteString(query)
	buf.WriteString(qre.marginComments.Trailing)
	fullSQL := buf.String()
	return fullSQL, withoutComments, nil
}

func rewriteOUTParamError(err error) error {
	sqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		return err
	}
	if sqlErr.Num == mysql.ErSPNotVarArg {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "OUT and INOUT parameters are not supported")
	}
	return err
}

func (qre *QueryExecutor) execCallProc() (*sqltypes.Result, error) {
	conn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}

	qr, err := qre.execDBConn(conn, sql, true)
	if err != nil {
		return nil, rewriteOUTParamError(err)
	}
	if !qr.IsMoreResultsExists() {
		if qr.IsInTransaction() {
			conn.Close()
			return nil, vterrors.New(vtrpcpb.Code_CANCELED, "Transaction not concluded inside the stored procedure, leaking transaction from stored procedure is not allowed")
		}
		return qr, nil
	}
	err = qre.drainResultSetOnConn(conn)
	if err != nil {
		return nil, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "Multi-Resultset not supported in stored procedure")
}

func (qre *QueryExecutor) execProc(conn *StatefulConnection) (*sqltypes.Result, error) {
	beforeInTx := conn.IsInTransaction()
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	qr, err := qre.execStatefulConn(conn, sql, true)
	if err != nil {
		return nil, rewriteOUTParamError(err)
	}
	if !qr.IsMoreResultsExists() {
		afterInTx := qr.IsInTransaction()
		if beforeInTx != afterInTx {
			conn.Close()
			return nil, vterrors.New(vtrpcpb.Code_CANCELED, "Transaction state change inside the stored procedure is not allowed")
		}
		return qr, nil
	}
	err = qre.drainResultSetOnConn(conn.UnderlyingDBConn())
	if err != nil {
		return nil, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "Multi-Resultset not supported in stored procedure")
}

func (qre *QueryExecutor) execAlterMigration() (*sqltypes.Result, error) {
	alterMigration, ok := qre.plan.FullStmt.(*sqlparser.AlterMigration)
	if !ok {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Expecting ALTER VITESS_MIGRATION plan")
	}
	switch alterMigration.Type {
	case sqlparser.RetryMigrationType:
		return qre.tsv.onlineDDLExecutor.RetryMigration(qre.ctx, alterMigration.UUID)
	case sqlparser.CompleteMigrationType:
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "ALTER VITESS_MIGRATION COMPLETE is not implemented yet")
	case sqlparser.CancelMigrationType:
		return qre.tsv.onlineDDLExecutor.CancelMigration(qre.ctx, alterMigration.UUID, true, "CANCEL issued by user")
	case sqlparser.CancelAllMigrationType:
		return qre.tsv.onlineDDLExecutor.CancelPendingMigrations(qre.ctx, "CANCEL ALL issued by user")
	}
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "ALTER VITESS_MIGRATION not implemented")
}

func (qre *QueryExecutor) drainResultSetOnConn(conn *connpool.DBConn) error {
	more := true
	for more {
		qr, err := conn.FetchNext(qre.ctx, int(qre.getSelectLimit()), true)
		if err != nil {
			return err
		}
		more = qr.IsMoreResultsExists()
	}
	return nil
}

func (qre *QueryExecutor) getSelectLimit() int64 {
	maxRows := qre.tsv.qe.maxResultSize.Get()
	sqlLimit := qre.options.GetSqlSelectLimit()
	if sqlLimit > 0 && sqlLimit < maxRows {
		return sqlLimit
	}
	return maxRows
}

func (qre *QueryExecutor) execDBConn(conn *connpool.DBConn, sql string, wantfields bool) (*sqltypes.Result, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.execDBConn")
	defer span.Finish()

	defer qre.logStats.AddRewrittenSQL(sql, time.Now())

	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	qre.tsv.statelessql.Add(qd)
	defer qre.tsv.statelessql.Remove(qd)

	return conn.Exec(ctx, sql, int(qre.tsv.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStatefulConn(conn *StatefulConnection, sql string, wantfields bool) (*sqltypes.Result, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.execStatefulConn")
	defer span.Finish()

	defer qre.logStats.AddRewrittenSQL(sql, time.Now())

	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	qre.tsv.statefulql.Add(qd)
	defer qre.tsv.statefulql.Remove(qd)

	return conn.Exec(ctx, sql, int(qre.tsv.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStreamSQL(conn *connpool.DBConn, sql string, callback func(*sqltypes.Result) error) error {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.execStreamSQL")
	trace.AnnotateSQL(span, sql)
	callBackClosingSpan := func(result *sqltypes.Result) error {
		defer span.Finish()
		return callback(result)
	}

	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	qre.tsv.olapql.Add(qd)
	defer qre.tsv.olapql.Remove(qd)

	start := time.Now()
	err := conn.Stream(ctx, sql, callBackClosingSpan, int(qre.tsv.qe.streamBufferSize.Get()), sqltypes.IncludeFieldsOrDefault(qre.options))
	qre.logStats.AddRewrittenSQL(sql, start)
	if err != nil {
		// MySQL error that isn't due to a connection issue
		return err
	}
	return nil
}

func (qre *QueryExecutor) recordUserQuery(queryType string, duration int64) {
	username := callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(qre.ctx))
	if username == "" {
		username = callerid.GetUsername(callerid.ImmediateCallerIDFromContext(qre.ctx))
	}
	tableName := qre.plan.TableName().String()
	qre.tsv.Stats().UserTableQueryCount.Add([]string{tableName, username, queryType}, 1)
	qre.tsv.Stats().UserTableQueryTimesNs.Add([]string{tableName, username, queryType}, duration)
}

// resolveNumber extracts a number from a bind variable or sql value.
func resolveNumber(pv sqltypes.PlanValue, bindVars map[string]*querypb.BindVariable) (int64, error) {
	v, err := pv.ResolveValue(bindVars)
	if err != nil {
		return 0, err
	}
	return evalengine.ToInt64(v)
}
