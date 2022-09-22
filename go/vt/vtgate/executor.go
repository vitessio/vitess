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

package vtgate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	errNoKeyspace     = vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)")
	defaultTabletType topodatapb.TabletType

	// TODO: @rafael - These two counters should be deprecated in favor of the ByTable ones. They are kept for now for backwards compatibility.
	queriesProcessed = stats.NewCountersWithSingleLabel("QueriesProcessed", "Queries processed at vtgate by plan type", "Plan")
	queriesRouted    = stats.NewCountersWithSingleLabel("QueriesRouted", "Queries routed from vtgate to vttablet by plan type", "Plan")

	queriesProcessedByTable = stats.NewCountersWithMultiLabels("QueriesProcessedByTable", "Queries processed at vtgate by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})
	queriesRoutedByTable    = stats.NewCountersWithMultiLabels("QueriesRoutedByTable", "Queries routed from vtgate to vttablet by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})
)

const (
	bindVarPrefix = "__vt"
)

func init() {
	topoproto.TabletTypeVar(&defaultTabletType, "default_tablet_type", topodatapb.TabletType_PRIMARY, "The default tablet type to set for queries, when one is not explicitly selected")
}

// Executor is the engine that executes queries by utilizing
// the abilities of the underlying vttablets.
type Executor struct {
	serv        srvtopo.Server
	cell        string
	resolver    *Resolver
	scatterConn *ScatterConn
	txConn      *TxConn

	mu           sync.Mutex
	vschema      *vindexes.VSchema
	streamSize   int
	plans        cache.Cache
	vschemaStats *VSchemaStats

	normalize       bool
	warnShardedOnly bool

	vm            *VSchemaManager
	schemaTracker SchemaInfo

	// allowScatter will fail planning if set to false and a plan contains any scatter queries
	allowScatter bool
}

var executorOnce sync.Once

const pathQueryPlans = "/debug/query_plans"
const pathScatterStats = "/debug/scatter_stats"
const pathVSchema = "/debug/vschema"

// NewExecutor creates a new Executor.
func NewExecutor(ctx context.Context, serv srvtopo.Server, cell string, resolver *Resolver, normalize, warnOnShardedOnly bool, streamSize int, cacheCfg *cache.Config, schemaTracker SchemaInfo, noScatter bool) *Executor {
	e := &Executor{
		serv:            serv,
		cell:            cell,
		resolver:        resolver,
		scatterConn:     resolver.scatterConn,
		txConn:          resolver.scatterConn.txConn,
		plans:           cache.NewDefaultCacheImpl(cacheCfg),
		normalize:       normalize,
		warnShardedOnly: warnOnShardedOnly,
		streamSize:      streamSize,
		schemaTracker:   schemaTracker,
		allowScatter:    !noScatter,
	}

	vschemaacl.Init()
	// we subscribe to update from the VSchemaManager
	e.vm = &VSchemaManager{
		subscriber: e.SaveVSchema,
		serv:       serv,
		cell:       cell,
		schema:     e.schemaTracker,
	}
	serv.WatchSrvVSchema(ctx, cell, e.vm.VSchemaUpdate)

	executorOnce.Do(func() {
		stats.NewGaugeFunc("QueryPlanCacheLength", "Query plan cache length", func() int64 {
			return int64(e.plans.Len())
		})
		stats.NewGaugeFunc("QueryPlanCacheSize", "Query plan cache size", func() int64 {
			return e.plans.UsedCapacity()
		})
		stats.NewGaugeFunc("QueryPlanCacheCapacity", "Query plan cache capacity", func() int64 {
			return e.plans.MaxCapacity()
		})
		stats.NewCounterFunc("QueryPlanCacheEvictions", "Query plan cache evictions", func() int64 {
			return e.plans.Evictions()
		})
		stats.NewCounterFunc("QueryPlanCacheHits", "Query plan cache hits", func() int64 {
			return e.plans.Hits()
		})
		stats.NewCounterFunc("QueryPlanCacheMisses", "Query plan cache misses", func() int64 {
			return e.plans.Misses()
		})
		http.Handle(pathQueryPlans, e)
		http.Handle(pathScatterStats, e)
		http.Handle(pathVSchema, e)
	})
	return e
}

// Execute executes a non-streaming query.
func (e *Executor) Execute(ctx context.Context, method string, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable) (result *sqltypes.Result, err error) {
	span, ctx := trace.NewSpan(ctx, "executor.Execute")
	span.Annotate("method", method)
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	logStats := NewLogStats(ctx, method, sql, bindVars)
	stmtType, result, err := e.execute(ctx, safeSession, sql, bindVars, logStats)
	logStats.Error = err
	if result == nil {
		saveSessionStats(safeSession, stmtType, 0, 0, 0, err)
	} else {
		saveSessionStats(safeSession, stmtType, result.RowsAffected, result.InsertID, len(result.Rows), err)
	}
	if result != nil && len(result.Rows) > *warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := sqlparser.RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		log.Warningf("%q exceeds warning threshold of max memory rows: %v", piiSafeSQL, *warnMemoryRows)
	}

	logStats.Send()
	return result, err
}

type streaminResultReceiver struct {
	mu           sync.Mutex
	stmtType     sqlparser.StatementType
	rowsAffected uint64
	rowsReturned int
	insertID     uint64
	callback     func(*sqltypes.Result) error
}

func (s *streaminResultReceiver) storeResultStats(typ sqlparser.StatementType, qr *sqltypes.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rowsAffected += qr.RowsAffected
	s.rowsReturned += len(qr.Rows)
	if qr.InsertID != 0 {
		s.insertID = qr.InsertID
	}
	s.stmtType = typ
	return s.callback(qr)
}

// StreamExecute executes a streaming query.
func (e *Executor) StreamExecute(
	ctx context.Context,
	method string,
	safeSession *SafeSession,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result) error,
) error {
	span, ctx := trace.NewSpan(ctx, "executor.StreamExecute")
	span.Annotate("method", method)
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	logStats := NewLogStats(ctx, method, sql, bindVars)
	srr := &streaminResultReceiver{callback: callback}
	var err error

	resultHandler := func(plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, execStart time.Time) error {
		var seenResults sync2.AtomicBool
		var resultMu sync.Mutex
		result := &sqltypes.Result{}
		if canReturnRows(plan.Type) {
			srr.callback = func(qr *sqltypes.Result) error {
				resultMu.Lock()
				defer resultMu.Unlock()
				// If the row has field info, send it separately.
				// TODO(sougou): this behavior is for handling tests because
				// the framework currently sends all results as one packet.
				byteCount := 0
				if len(qr.Fields) > 0 {
					qrfield := &sqltypes.Result{Fields: qr.Fields}
					if err := callback(qrfield); err != nil {
						return err
					}
					seenResults.Set(true)
				}

				for _, row := range qr.Rows {
					result.Rows = append(result.Rows, row)

					for _, col := range row {
						byteCount += col.Len()
					}

					if byteCount >= e.streamSize {
						err := callback(result)
						seenResults.Set(true)
						result = &sqltypes.Result{}
						byteCount = 0
						if err != nil {
							return err
						}
					}
				}
				return nil
			}
		}

		// 4: Execute!
		err := vc.StreamExecutePrimitive(plan.Instructions, bindVars, true, func(qr *sqltypes.Result) error {
			return srr.storeResultStats(plan.Type, qr)
		})

		// Check if there was partial DML execution. If so, rollback the effect of the partially executed query.
		if err != nil {
			if !canReturnRows(plan.Type) {
				return e.rollbackExecIfNeeded(ctx, safeSession, bindVars, logStats, err)
			}
			return err
		}

		if !canReturnRows(plan.Type) {
			return nil
		}

		// Send left-over rows if there is no error on execution.
		if len(result.Rows) > 0 || !seenResults.Get() {
			if err := callback(result); err != nil {
				return err
			}
		}

		// 5: Log and add statistics
		logStats.Keyspace = plan.Instructions.GetKeyspaceName()
		logStats.Table = plan.Instructions.GetTableName()
		logStats.TabletType = vc.TabletType().String()
		logStats.ExecuteTime = time.Since(execStart)

		e.updateQueryCounts(plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

		return err
	}

	err = e.newExecute(ctx, safeSession, sql, bindVars, logStats, resultHandler, srr.storeResultStats)

	logStats.Error = err
	saveSessionStats(safeSession, srr.stmtType, srr.rowsAffected, srr.insertID, srr.rowsReturned, err)
	if srr.rowsReturned > *warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := sqlparser.RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		log.Warningf("%q exceeds warning threshold of max memory rows: %v", piiSafeSQL, *warnMemoryRows)
	}

	logStats.Send()
	return err

}

func canReturnRows(stmtType sqlparser.StatementType) bool {
	switch stmtType {
	case sqlparser.StmtSelect, sqlparser.StmtShow, sqlparser.StmtExplain, sqlparser.StmtCallProc:
		return true
	default:
		return false
	}
}

func saveSessionStats(safeSession *SafeSession, stmtType sqlparser.StatementType, rowsAffected, insertID uint64, rowsReturned int, err error) {
	safeSession.RowCount = -1
	if err != nil {
		return
	}
	if !safeSession.foundRowsHandled {
		safeSession.FoundRows = uint64(rowsReturned)
	}
	if insertID > 0 {
		safeSession.LastInsertId = insertID
	}
	switch stmtType {
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		safeSession.RowCount = int64(rowsAffected)
	case sqlparser.StmtDDL, sqlparser.StmtSet, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtFlush:
		safeSession.RowCount = 0
	}
}

func (e *Executor) execute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (sqlparser.StatementType, *sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	var stmtType sqlparser.StatementType
	err = e.newExecute(ctx, safeSession, sql, bindVars, logStats, func(plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, time time.Time) error {
		stmtType = plan.Type
		qr, err = e.executePlan(ctx, safeSession, plan, vc, bindVars, logStats, time)
		return err
	}, func(typ sqlparser.StatementType, result *sqltypes.Result) error {
		stmtType = typ
		qr = result
		return nil
	})
	if err == planbuilder.ErrPlanNotSupported {
		return e.legacyExecute(ctx, safeSession, sql, bindVars, logStats)
	}
	return stmtType, qr, err
}

func (e *Executor) legacyExecute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (sqlparser.StatementType, *sqltypes.Result, error) {
	// Start an implicit transaction if necessary.
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession); err != nil {
			return 0, nil, err
		}
	}

	destKeyspace, destTabletType, dest, err := e.ParseDestinationTarget(safeSession.TargetString)
	if err != nil {
		return 0, nil, err
	}

	logStats.Keyspace = destKeyspace
	logStats.TabletType = destTabletType.String()
	// Legacy gateway allows transactions only on PRIMARY
	if UsingLegacyGateway() && safeSession.InTransaction() && destTabletType != topodatapb.TabletType_PRIMARY {
		return 0, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "transaction is supported only for primary tablet type, current type: %v", destTabletType)
	}
	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	stmtType := sqlparser.Preview(sql)
	logStats.StmtType = stmtType.String()

	// Mysql warnings are scoped to the current session, but are
	// cleared when a "non-diagnostic statement" is executed:
	// https://dev.mysql.com/doc/refman/8.0/en/show-warnings.html
	//
	// To emulate this behavior, clear warnings from the session
	// for all statements _except_ SHOW, so that SHOW WARNINGS
	// can actually return them.
	if stmtType != sqlparser.StmtShow {
		safeSession.ClearWarnings()
	}

	switch stmtType {
	case sqlparser.StmtSelect, sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate,
		sqlparser.StmtDelete, sqlparser.StmtDDL, sqlparser.StmtUse, sqlparser.StmtExplain, sqlparser.StmtOther, sqlparser.StmtFlush:
		return 0, nil, vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] not reachable, should be handled with plan execute")
	case sqlparser.StmtSet:
		qr, err := e.handleSet(ctx, sql, logStats)
		return sqlparser.StmtSet, qr, err
	case sqlparser.StmtShow:
		qr, err := e.handleShow(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
		return sqlparser.StmtShow, qr, err
	case sqlparser.StmtComment:
		// Effectively should be done through new plan.
		// There are some statements which are not planned for special comments.
		return sqlparser.StmtComment, &sqltypes.Result{}, nil
	}
	return 0, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] statement not handled: %s", sql)
}

// addNeededBindVars adds bind vars that are needed by the plan
func (e *Executor) addNeededBindVars(bindVarNeeds *sqlparser.BindVarNeeds, bindVars map[string]*querypb.BindVariable, session *SafeSession) error {
	for _, funcName := range bindVarNeeds.NeedFunctionResult {
		switch funcName {
		case sqlparser.DBVarName:
			bindVars[sqlparser.DBVarName] = sqltypes.StringBindVariable(session.TargetString)
		case sqlparser.LastInsertIDName:
			bindVars[sqlparser.LastInsertIDName] = sqltypes.Uint64BindVariable(session.GetLastInsertId())
		case sqlparser.FoundRowsName:
			bindVars[sqlparser.FoundRowsName] = sqltypes.Uint64BindVariable(session.FoundRows)
		case sqlparser.RowCountName:
			bindVars[sqlparser.RowCountName] = sqltypes.Int64BindVariable(session.RowCount)
		}
	}

	for _, sysVar := range bindVarNeeds.NeedSystemVariable {
		key := bindVarPrefix + sysVar
		switch sysVar {
		case sysvars.Autocommit.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.Autocommit)
		case sysvars.ClientFoundRows.Name:
			var v bool
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.ClientFoundRows
			})
			bindVars[key] = sqltypes.BoolBindVariable(v)
		case sysvars.SkipQueryPlanCache.Name:
			var v bool
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.ClientFoundRows
			})
			bindVars[key] = sqltypes.BoolBindVariable(v)
		case sysvars.SQLSelectLimit.Name:
			var v int64
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.SqlSelectLimit
			})
			bindVars[key] = sqltypes.Int64BindVariable(v)
		case sysvars.TransactionMode.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.TransactionMode.String())
		case sysvars.Workload.Name:
			var v string
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.GetWorkload().String()
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.DDLStrategy.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.DDLStrategy)
		case sysvars.SessionUUID.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.SessionUUID)
		case sysvars.SessionEnableSystemSettings.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.EnableSystemSettings)
		case sysvars.ReadAfterWriteGTID.Name:
			var v string
			ifReadAfterWriteExist(session, func(raw *vtgatepb.ReadAfterWrite) {
				v = raw.ReadAfterWriteGtid
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.ReadAfterWriteTimeOut.Name:
			var v float64
			ifReadAfterWriteExist(session, func(raw *vtgatepb.ReadAfterWrite) {
				v = raw.ReadAfterWriteTimeout
			})
			bindVars[key] = sqltypes.Float64BindVariable(v)
		case sysvars.SessionTrackGTIDs.Name:
			v := "off"
			ifReadAfterWriteExist(session, func(raw *vtgatepb.ReadAfterWrite) {
				if raw.SessionTrackGtids {
					v = "own_gtid"
				}
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.Version.Name:
			bindVars[key] = sqltypes.StringBindVariable(servenv.AppVersion.MySQLVersion())
		case sysvars.VersionComment.Name:
			bindVars[key] = sqltypes.StringBindVariable(servenv.AppVersion.String())
		case sysvars.Socket.Name:
			bindVars[key] = sqltypes.StringBindVariable(mysqlSocketPath())
		default:
			if value, hasSysVar := session.SystemVariables[sysVar]; hasSysVar {
				expr, err := sqlparser.ParseExpr(value)
				if err != nil {
					return err
				}

				evalExpr, err := evalengine.Translate(expr, nil)
				if err != nil {
					return err
				}
				evaluated, err := evalengine.EmptyExpressionEnv().Evaluate(evalExpr)
				if err != nil {
					return err
				}
				bindVars[key] = sqltypes.ValueBindVariable(evaluated.Value())
			}
		}
	}

	udvMap := session.UserDefinedVariables
	if udvMap == nil {
		udvMap = map[string]*querypb.BindVariable{}
	}
	for _, udv := range bindVarNeeds.NeedUserDefinedVariables {
		val := udvMap[udv]
		if val == nil {
			val = sqltypes.NullBindVariable
		}
		bindVars[sqlparser.UserDefinedVariableName+udv] = val
	}

	return nil
}

func ifOptionsExist(session *SafeSession, f func(*querypb.ExecuteOptions)) {
	options := session.GetOptions()
	if options != nil {
		f(options)
	}
}

func ifReadAfterWriteExist(session *SafeSession, f func(*vtgatepb.ReadAfterWrite)) {
	raw := session.ReadAfterWrite
	if raw != nil {
		f(raw)
	}
}

func (e *Executor) destinationExec(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats, ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	return e.resolver.Execute(ctx, sql, bindVars, destKeyspace, destTabletType, dest, safeSession, safeSession.Options, logStats, false /* canAutocommit */, ignoreMaxMemoryRows)
}

func (e *Executor) handleBegin(ctx context.Context, safeSession *SafeSession, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	err := e.txConn.Begin(ctx, safeSession)
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts("Begin", "", "", 0)

	return &sqltypes.Result{}, err
}

func (e *Executor) handleCommit(ctx context.Context, safeSession *SafeSession, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts("Commit", "", "", int64(logStats.ShardQueries))

	err := e.txConn.Commit(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

// Commit commits the existing transactions
func (e *Executor) Commit(ctx context.Context, safeSession *SafeSession) error {
	return e.txConn.Commit(ctx, safeSession)
}

func (e *Executor) handleRollback(ctx context.Context, safeSession *SafeSession, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts("Rollback", "", "", int64(logStats.ShardQueries))
	err := e.txConn.Rollback(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleSavepoint(ctx context.Context, safeSession *SafeSession, sql string, planType string, logStats *LogStats, nonTxResponse func(query string) (*sqltypes.Result, error), ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts(planType, "", "", int64(logStats.ShardQueries))
	defer func() {
		logStats.ExecuteTime = time.Since(execStart)
	}()

	// If no transaction exists on any of the shard sessions,
	// then savepoint does not need to be executed, it will be only stored in the session
	// and later will be executed when a transaction is started.
	if !safeSession.isTxOpen() {
		if safeSession.InTransaction() {
			// Storing, as this needs to be executed just after starting transaction on the shard.
			safeSession.StoreSavepoint(sql)
			return &sqltypes.Result{}, nil
		}
		return nonTxResponse(sql)
	}
	orig := safeSession.commitOrder
	qr, err := e.executeSPInAllSessions(ctx, safeSession, sql, ignoreMaxMemoryRows)
	safeSession.SetCommitOrder(orig)
	if err != nil {
		return nil, err
	}
	safeSession.StoreSavepoint(sql)
	return qr, nil
}

// executeSPInAllSessions function executes the savepoint query in all open shard sessions (pre, normal and post)
// which has non-zero transaction id (i.e. an open transaction on the shard connection).
func (e *Executor) executeSPInAllSessions(ctx context.Context, safeSession *SafeSession, sql string, ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	var errs []error
	for _, co := range []vtgatepb.CommitOrder{vtgatepb.CommitOrder_PRE, vtgatepb.CommitOrder_NORMAL, vtgatepb.CommitOrder_POST} {
		safeSession.SetCommitOrder(co)

		var rss []*srvtopo.ResolvedShard
		var queries []*querypb.BoundQuery
		for _, shardSession := range safeSession.getSessions() {
			// This will avoid executing savepoint on reserved connections
			// which has no open transaction.
			if shardSession.TransactionId == 0 {
				continue
			}
			rss = append(rss, &srvtopo.ResolvedShard{
				Target:  shardSession.Target,
				Gateway: e.resolver.resolver.GetGateway(),
			})
			queries = append(queries, &querypb.BoundQuery{Sql: sql})
		}
		qr, errs = e.ExecuteMultiShard(ctx, rss, queries, safeSession, false /*autocommit*/, ignoreMaxMemoryRows)
		err := vterrors.Aggregate(errs)
		if err != nil {
			return nil, err
		}
	}
	return qr, nil
}

// CloseSession releases the current connection, which rollbacks open transactions and closes reserved connections.
// It is called then the MySQL servers closes the connection to its client.
func (e *Executor) CloseSession(ctx context.Context, safeSession *SafeSession) error {
	return e.txConn.ReleaseAll(ctx, safeSession)
}

func (e *Executor) handleSet(ctx context.Context, sql string, logStats *LogStats) (*sqltypes.Result, error) {
	stmt, reserved, err := sqlparser.Parse2(sql)
	if err != nil {
		return nil, err
	}
	reservedVars := sqlparser.NewReservedVars("vtg", reserved)
	rewrittenAST, err := sqlparser.PrepareAST(stmt, reservedVars, nil, false, "", sqlparser.SQLSelectLimitUnset, "", nil)
	if err != nil {
		return nil, err
	}
	set, ok := rewrittenAST.AST.(*sqlparser.Set)
	if !ok {
		_, ok := rewrittenAST.AST.(*sqlparser.SetTransaction)
		if !ok {
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "unexpected statement type")
		}
		// Parser ensures set transaction is well-formed.

		// TODO: This is a NOP, modeled off of tx_isolation and tx_read_only.  It's incredibly
		// dangerous that it's a NOP, but fixing that is left to.
		return &sqltypes.Result{}, nil
	}

	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	defer func() {
		logStats.ExecuteTime = time.Since(execStart)
	}()

	var value interface{}
	for _, expr := range set.Exprs {
		// This is what correctly allows us to handle queries such as "set @@session.`autocommit`=1"
		// it will remove backticks and double quotes that might surround the part after the first period
		_, name := sqlparser.NewStringTokenizer(expr.Name.Lowered()).Scan()
		switch expr.Scope {
		case sqlparser.VitessMetadataScope:
			value, err = getValueFor(expr)
			if err != nil {
				return nil, err
			}
			val, ok := value.(string)
			if !ok {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "unexpected value type for '%s': %v", name, value)
			}
			_, err = e.handleSetVitessMetadata(ctx, name, val)
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable statement: %s", sql)

		}
		if err != nil {
			return nil, err
		}
	}

	return &sqltypes.Result{}, nil
}

func getValueFor(expr *sqlparser.SetExpr) (interface{}, error) {
	switch expr := expr.Expr.(type) {
	case *sqlparser.Literal:
		switch expr.Type {
		case sqlparser.StrVal:
			return strings.ToLower(expr.Val), nil
		case sqlparser.IntVal:
			num, err := strconv.ParseInt(expr.Val, 0, 64)
			if err != nil {
				return nil, err
			}
			return num, nil
		case sqlparser.FloatVal, sqlparser.DecimalVal:
			num, err := strconv.ParseFloat(expr.Val, 64)
			if err != nil {
				return nil, err
			}
			return num, nil
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid value type: %v", sqlparser.String(expr))
		}
	case sqlparser.BoolVal:
		var val int64
		if expr {
			val = 1
		}
		return val, nil
	case *sqlparser.NullVal:
		return nil, nil
	case *sqlparser.ColName:
		return expr.Name.String(), nil
	case *sqlparser.Default:
		return "default", nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid syntax: %s", sqlparser.String(expr))
	}
}

func (e *Executor) handleSetVitessMetadata(ctx context.Context, name, value string) (*sqltypes.Result, error) {
	// TODO(kalfonso): move to its own acl check and consolidate into an acl component that can handle multiple operations (vschema, metadata)
	user := callerid.ImmediateCallerIDFromContext(ctx)
	allowed := vschemaacl.Authorized(user)
	if !allowed {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_PERMISSION_DENIED, vterrors.AccessDeniedError, "User '%s' not authorized to perform vitess metadata operations", user.GetUsername())
	}

	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return nil, err
	}

	if value == "" {
		err = ts.DeleteMetadata(ctx, name)
	} else {
		err = ts.UpsertMetadata(ctx, name, value)
	}

	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{}, nil
}

func (e *Executor) handleShowVitessMetadata(ctx context.Context, opt *sqlparser.ShowTablesOpt) (*sqltypes.Result, error) {
	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return nil, err
	}

	var metadata map[string]string
	if opt.Filter == nil {
		metadata, err = ts.GetMetadata(ctx, "")
		if err != nil {
			return nil, err
		}
	} else {
		metadata, err = ts.GetMetadata(ctx, opt.Filter.Like)
		if err != nil {
			return nil, err
		}
	}

	rows := make([][]sqltypes.Value, 0, len(metadata))
	for k, v := range metadata {
		row := buildVarCharRow(k, v)
		rows = append(rows, row)
	}

	return &sqltypes.Result{
		Fields: buildVarCharFields("Key", "Value"),
		Rows:   rows,
	}, nil
}

func (e *Executor) handleShow(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	showOuter, ok := stmt.(*sqlparser.Show)
	if !ok {
		// This code is unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unrecognized SHOW statement: %v", sql)
	}
	show, ok := showOuter.Internal.(*sqlparser.ShowLegacy)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] This should only be SHOW Legacy statement type: %v", sql)
	}
	ignoreMaxMemoryRows := sqlparser.IgnoreMaxMaxMemoryRowsDirective(stmt)
	execStart := time.Now()
	defer func() { logStats.ExecuteTime = time.Since(execStart) }()
	switch strings.ToLower(show.Type) {
	case sqlparser.KeywordString(sqlparser.VARIABLES):
		if show.Scope == sqlparser.VitessMetadataScope {
			return e.handleShowVitessMetadata(ctx, show.ShowTablesOpt)
		}
	// for ENGINES, we want to return just InnoDB
	case sqlparser.KeywordString(sqlparser.ENGINES):
		rows := make([][]sqltypes.Value, 0, 6)
		row := buildVarCharRow(
			"InnoDB",
			"DEFAULT",
			"Supports transactions, row-level locking, and foreign keys",
			"YES",
			"YES",
			"YES")
		rows = append(rows, row)
		return &sqltypes.Result{
			Fields: buildVarCharFields("Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"),
			Rows:   rows,
		}, nil
	// for PLUGINS, return InnoDb + mysql_native_password
	case sqlparser.KeywordString(sqlparser.PLUGINS):
		rows := make([][]sqltypes.Value, 0, 5)
		row := buildVarCharRow(
			"InnoDB",
			"ACTIVE",
			"STORAGE ENGINE",
			"NULL",
			"GPL")
		rows = append(rows, row)
		return &sqltypes.Result{
			Fields: buildVarCharFields("Name", "Status", "Type", "Library", "License"),
			Rows:   rows,
		}, nil
	case sqlparser.KeywordString(sqlparser.VITESS_SHARDS):
		showVitessShardsFilters := func(show *sqlparser.ShowLegacy) ([]func(string) bool, []func(string, *topodatapb.ShardReference) bool) {
			keyspaceFilters := []func(string) bool{}
			shardFilters := []func(string, *topodatapb.ShardReference) bool{}

			if show.ShowTablesOpt == nil || show.ShowTablesOpt.Filter == nil {
				return keyspaceFilters, shardFilters
			}

			filter := show.ShowTablesOpt.Filter

			if filter.Like != "" {
				shardLikeRexep := sqlparser.LikeToRegexp(filter.Like)

				if strings.Contains(filter.Like, "/") {
					keyspaceLikeRexep := sqlparser.LikeToRegexp(strings.Split(filter.Like, "/")[0])
					keyspaceFilters = append(keyspaceFilters, func(ks string) bool {
						return keyspaceLikeRexep.MatchString(ks)
					})
				}
				shardFilters = append(shardFilters, func(ks string, shard *topodatapb.ShardReference) bool {
					return shardLikeRexep.MatchString(topoproto.KeyspaceShardString(ks, shard.Name))
				})

				return keyspaceFilters, shardFilters
			}

			if filter.Filter != nil {
				// TODO build a query planner I guess? lol that should be fun
				log.Infof("SHOW VITESS_SHARDS where clause %+v. Ignoring this (for now).", filter.Filter)
			}

			return keyspaceFilters, shardFilters
		}

		keyspaceFilters, shardFilters := showVitessShardsFilters(show)

		keyspaces, err := e.resolver.resolver.GetAllKeyspaces(ctx)
		if err != nil {
			return nil, err
		}

		var rows [][]sqltypes.Value
		for _, keyspace := range keyspaces {
			skipKeyspace := false
			for _, filter := range keyspaceFilters {
				if !filter(keyspace) {
					skipKeyspace = true
					break
				}
			}

			if skipKeyspace {
				continue
			}

			_, _, shards, err := e.resolver.resolver.GetKeyspaceShards(ctx, keyspace, destTabletType)
			if err != nil {
				// There might be a misconfigured keyspace or no shards in the keyspace.
				// Skip any errors and move on.
				continue
			}

			for _, shard := range shards {
				skipShard := false
				for _, filter := range shardFilters {
					if !filter(keyspace, shard) {
						skipShard = true
						break
					}
				}

				if skipShard {
					continue
				}

				rows = append(rows, buildVarCharRow(topoproto.KeyspaceShardString(keyspace, shard.Name)))
			}
		}

		return &sqltypes.Result{
			Fields: buildVarCharFields("Shards"),
			Rows:   rows,
		}, nil
	case sqlparser.KeywordString(sqlparser.VITESS_TABLETS):
		return e.showTablets(show)
	case sqlparser.KeywordString(sqlparser.VITESS_REPLICATION_STATUS):
		return e.showVitessReplicationStatus(ctx, show)
	case "vitess_target":
		var rows [][]sqltypes.Value
		rows = append(rows, buildVarCharRow(safeSession.TargetString))
		return &sqltypes.Result{
			Fields: buildVarCharFields("Target"),
			Rows:   rows,
		}, nil
	case "vschema tables":
		if destKeyspace == "" {
			return nil, errNoKeyspace
		}
		ks, ok := e.VSchema().Keyspaces[destKeyspace]
		if !ok {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadDb, "Unknown database '%s' in vschema", destKeyspace)
		}

		var tables []string
		for name := range ks.Tables {
			tables = append(tables, name)
		}
		sort.Strings(tables)

		rows := make([][]sqltypes.Value, len(tables))
		for i, v := range tables {
			rows[i] = buildVarCharRow(v)
		}

		return &sqltypes.Result{
			Fields: buildVarCharFields("Tables"),
			Rows:   rows,
		}, nil
	case "vschema vindexes":
		vschema := e.vm.GetCurrentSrvVschema()
		if vschema == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
		}

		rows := make([][]sqltypes.Value, 0, 16)

		if show.HasOnTable() {
			// If the table reference is not fully qualified, then
			// pull the keyspace from the session. Fail if the keyspace
			// isn't specified or isn't valid, or if the table isn't
			// known.
			ksName := show.OnTable.Qualifier.String()
			if ksName == "" {
				ksName = destKeyspace
			}

			ks, ok := vschema.Keyspaces[ksName]
			if !ok {
				return nil, errNoKeyspace
			}

			tableName := show.OnTable.Name.String()
			table, ok := ks.Tables[tableName]
			if !ok {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.NoSuchTable, "table '%s' does not exist in keyspace '%s'", tableName, ksName)
			}

			for _, colVindex := range table.ColumnVindexes {
				vindex, ok := ks.Vindexes[colVindex.GetName()]
				columns := colVindex.GetColumns()
				if len(columns) == 0 {
					columns = []string{colVindex.GetColumn()}
				}
				if ok {
					params := make([]string, 0, 4)
					for k, v := range vindex.GetParams() {
						params = append(params, fmt.Sprintf("%s=%s", k, v))
					}
					sort.Strings(params)
					rows = append(rows, buildVarCharRow(strings.Join(columns, ", "), colVindex.GetName(), vindex.GetType(), strings.Join(params, "; "), vindex.GetOwner()))
				} else {
					rows = append(rows, buildVarCharRow(strings.Join(columns, ", "), colVindex.GetName(), "", "", ""))
				}
			}

			return &sqltypes.Result{
				Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
				Rows:   rows,
			}, nil
		}

		// For the query interface to be stable we need to sort
		// for each of the map iterations
		ksNames := make([]string, 0, len(vschema.Keyspaces))
		for name := range vschema.Keyspaces {
			ksNames = append(ksNames, name)
		}
		sort.Strings(ksNames)
		for _, ksName := range ksNames {
			ks := vschema.Keyspaces[ksName]

			vindexNames := make([]string, 0, len(ks.Vindexes))
			for name := range ks.Vindexes {
				vindexNames = append(vindexNames, name)
			}
			sort.Strings(vindexNames)
			for _, vindexName := range vindexNames {
				vindex := ks.Vindexes[vindexName]

				params := make([]string, 0, 4)
				for k, v := range vindex.GetParams() {
					params = append(params, fmt.Sprintf("%s=%s", k, v))
				}
				sort.Strings(params)
				rows = append(rows, buildVarCharRow(ksName, vindexName, vindex.GetType(), strings.Join(params, "; "), vindex.GetOwner()))
			}
		}
		return &sqltypes.Result{
			Fields: buildVarCharFields("Keyspace", "Name", "Type", "Params", "Owner"),
			Rows:   rows,
		}, nil
	}

	// Any other show statement is passed through
	return e.handleOther(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats, ignoreMaxMemoryRows)
}

// (tablet, servingState, mtst) -> bool
type tabletFilter func(*topodatapb.Tablet, string, int64) bool

func (e *Executor) showTablets(show *sqlparser.ShowLegacy) (*sqltypes.Result, error) {
	getTabletFilters := func(show *sqlparser.ShowLegacy) []tabletFilter {
		filters := []tabletFilter{}

		if show.ShowTablesOpt == nil || show.ShowTablesOpt.Filter == nil {
			return filters
		}

		filter := show.ShowTablesOpt.Filter
		if filter.Like != "" {
			tabletRegexp := sqlparser.LikeToRegexp(filter.Like)

			f := func(tablet *topodatapb.Tablet, servingState string, PrimaryTermStartTime int64) bool {
				return tabletRegexp.MatchString(tablet.Hostname)
			}

			filters = append(filters, f)
			return filters
		}

		if filter.Filter != nil {
			log.Infof("SHOW VITESS_TABLETS where clause: %+v. Ignoring this (for now).", filter.Filter)
		}

		return filters
	}

	tabletFilters := getTabletFilters(show)

	rows := [][]sqltypes.Value{}
	if UsingLegacyGateway() {
		status := e.scatterConn.GetLegacyHealthCheckCacheStatus()
		for _, s := range status {
			for _, ts := range s.TabletsStats {
				state := "SERVING"
				if !ts.Serving {
					state = "NOT_SERVING"
				}
				mtst := ts.TabletExternallyReparentedTimestamp
				mtstStr := ""
				if mtst > 0 {
					// this code depends on the fact that TabletExternallyReparentedTimestamp is the seconds since epoch start
					mtstStr = time.Unix(mtst, 0).UTC().Format(time.RFC3339)
				}

				skipTablet := false
				for _, filter := range tabletFilters {
					if !filter(ts.Tablet, state, mtst) {
						skipTablet = true
						break
					}
				}

				if skipTablet {
					continue
				}

				rows = append(rows, buildVarCharRow(
					s.Cell,
					s.Target.Keyspace,
					s.Target.Shard,
					ts.Target.TabletType.String(),
					state,
					topoproto.TabletAliasString(ts.Tablet.Alias),
					ts.Tablet.Hostname,
					mtstStr,
				))
			}
		}
	} else {
		status := e.scatterConn.GetHealthCheckCacheStatus()
		for _, s := range status {
			for _, ts := range s.TabletsStats {
				state := "SERVING"
				if !ts.Serving {
					state = "NOT_SERVING"
				}
				mtst := ts.PrimaryTermStartTime
				mtstStr := ""
				if mtst > 0 {
					// this code depends on the fact that PrimaryTermStartTime is the seconds since epoch start
					mtstStr = time.Unix(mtst, 0).UTC().Format(time.RFC3339)
				}

				skipTablet := false
				for _, filter := range tabletFilters {
					if !filter(ts.Tablet, state, mtst) {
						skipTablet = true
						break
					}
				}

				if skipTablet {
					continue
				}

				rows = append(rows, buildVarCharRow(
					s.Cell,
					s.Target.Keyspace,
					s.Target.Shard,
					ts.Target.TabletType.String(),
					state,
					topoproto.TabletAliasString(ts.Tablet.Alias),
					ts.Tablet.Hostname,
					mtstStr,
				))
			}
		}
	}
	return &sqltypes.Result{
		Fields: buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "PrimaryTermStartTime"),
		Rows:   rows,
	}, nil
}

func (e *Executor) showVitessReplicationStatus(ctx context.Context, show *sqlparser.ShowLegacy) (*sqltypes.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, *HealthCheckTimeout)
	defer cancel()
	rows := [][]sqltypes.Value{}

	// This is only used for tests
	if UsingLegacyGateway() {
		status := e.scatterConn.GetLegacyHealthCheckCacheStatus()

		for _, s := range status {
			for _, ts := range s.TabletsStats {
				// We only want to show REPLICA and RDONLY tablets
				if ts.Tablet.Type != topodatapb.TabletType_REPLICA && ts.Tablet.Type != topodatapb.TabletType_RDONLY {
					continue
				}

				tabletHostPort := ts.GetTabletHostPort()
				throttlerStatus, err := getTabletThrottlerStatus(tabletHostPort)
				if err != nil {
					log.Warningf("Could not get throttler status from %s: %v", tabletHostPort, err)
				}

				replSourceHost := ""
				replSourcePort := int64(0)
				replIOThreadHealth := ""
				replSQLThreadHealth := ""
				replLastError := ""
				replLag := int64(-1)
				sql := "show slave status"
				results, err := e.txConn.gateway.Execute(ctx, ts.Target, sql, nil, 0, 0, nil)
				if err != nil {
					log.Warningf("Could not get replication status from %s: %v", tabletHostPort, err)
				} else if results != nil && len(results.Rows) == 1 {
					replSourceHost = results.Rows[0][1].ToString()
					replSourcePort, _ = results.Rows[0][3].ToInt64()
					replIOThreadHealth = results.Rows[0][10].ToString()
					replSQLThreadHealth = results.Rows[0][11].ToString()
					replLastError = results.Rows[0][19].ToString()
					if ts.Stats != nil {
						replLag = int64(ts.Stats.ReplicationLagSeconds)
					}
				}
				replicationHealth := fmt.Sprintf("{\"EventStreamRunning\":\"%s\",\"EventApplierRunning\":\"%s\",\"LastError\":\"%s\"}", replIOThreadHealth, replSQLThreadHealth, replLastError)

				rows = append(rows, buildVarCharRow(
					s.Target.Keyspace,
					s.Target.Shard,
					ts.Target.TabletType.String(),
					topoproto.TabletAliasString(ts.Tablet.Alias),
					ts.Tablet.Hostname,
					fmt.Sprintf("%s:%d", replSourceHost, replSourcePort),
					replicationHealth,
					fmt.Sprintf("%d", replLag),
					throttlerStatus,
				))
			}
		}
	} else {
		status := e.scatterConn.GetHealthCheckCacheStatus()

		for _, s := range status {
			for _, ts := range s.TabletsStats {
				// We only want to show REPLICA and RDONLY tablets
				if ts.Tablet.Type != topodatapb.TabletType_REPLICA && ts.Tablet.Type != topodatapb.TabletType_RDONLY {
					continue
				}

				// Allow people to filter by Keyspace and Shard using a LIKE clause
				if show.ShowTablesOpt != nil && show.ShowTablesOpt.Filter != nil {
					ksFilterRegex := sqlparser.LikeToRegexp(show.ShowTablesOpt.Filter.Like)
					keyspaceShardStr := fmt.Sprintf("%s/%s", ts.Tablet.Keyspace, ts.Tablet.Shard)
					if !ksFilterRegex.MatchString(keyspaceShardStr) {
						continue
					}
				}

				tabletHostPort := ts.GetTabletHostPort()
				throttlerStatus, err := getTabletThrottlerStatus(tabletHostPort)
				if err != nil {
					log.Warningf("Could not get throttler status from %s: %v", tabletHostPort, err)
				}

				replSourceHost := ""
				replSourcePort := int64(0)
				replIOThreadHealth := ""
				replSQLThreadHealth := ""
				replLastError := ""
				replLag := int64(-1)
				sql := "show slave status"
				results, err := e.txConn.gateway.Execute(ctx, ts.Target, sql, nil, 0, 0, nil)
				if err != nil || results == nil {
					log.Warningf("Could not get replication status from %s: %v", tabletHostPort, err)
				} else if row := results.Named().Row(); row != nil {
					replSourceHost = row["Master_Host"].ToString()
					replSourcePort, _ = row["Master_Port"].ToInt64()
					replIOThreadHealth = row["Slave_IO_Running"].ToString()
					replSQLThreadHealth = row["Slave_SQL_Running"].ToString()
					replLastError = row["Last_Error"].ToString()
					if ts.Stats != nil {
						replLag = int64(ts.Stats.ReplicationLagSeconds)
					}
				}
				replicationHealth := fmt.Sprintf("{\"EventStreamRunning\":\"%s\",\"EventApplierRunning\":\"%s\",\"LastError\":\"%s\"}", replIOThreadHealth, replSQLThreadHealth, replLastError)

				rows = append(rows, buildVarCharRow(
					s.Target.Keyspace,
					s.Target.Shard,
					ts.Target.TabletType.String(),
					topoproto.TabletAliasString(ts.Tablet.Alias),
					ts.Tablet.Hostname,
					fmt.Sprintf("%s:%d", replSourceHost, replSourcePort),
					replicationHealth,
					fmt.Sprintf("%d", replLag),
					throttlerStatus,
				))
			}
		}
	}
	return &sqltypes.Result{
		Fields: buildVarCharFields("Keyspace", "Shard", "TabletType", "Alias", "Hostname", "ReplicationSource", "ReplicationHealth", "ReplicationLag", "ThrottlerStatus"),
		Rows:   rows,
	}, nil
}

func (e *Executor) handleOther(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats, ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	if destKeyspace == "" {
		return nil, errNoKeyspace
	}
	if dest == nil {
		// shardExec will re-resolve this a bit later.
		rss, err := e.resolver.resolver.ResolveDestination(ctx, destKeyspace, destTabletType, key.DestinationAnyShard{})
		if err != nil {
			return nil, err
		}
		if len(rss) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "keyspace %s has no shards", destKeyspace)
		}
		destKeyspace, dest = rss[0].Target.Keyspace, key.DestinationShard(rss[0].Target.Shard)
	}

	switch dest.(type) {
	case key.DestinationKeyspaceID:
		rss, err := e.resolver.resolver.ResolveDestination(ctx, destKeyspace, destTabletType, dest)
		if err != nil {
			return nil, err
		}
		if len(rss) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %s, got: %v", sql, dest)
		}
		destKeyspace, dest = rss[0].Target.Keyspace, key.DestinationShard(rss[0].Target.Shard)
	case key.DestinationShard:
	// noop
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Destination can only be a single shard for statement: %s, got: %v", sql, dest)
	}

	execStart := time.Now()
	result, err := e.destinationExec(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats, ignoreMaxMemoryRows)

	e.updateQueryCounts("Other", "", "", int64(logStats.ShardQueries))

	logStats.ExecuteTime = time.Since(execStart)
	return result, err
}

// MessageStream is part of the vtgate service API. This is a V2 level API that's sent
// to the Resolver.
func (e *Executor) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	err := e.resolver.MessageStream(
		ctx,
		keyspace,
		shard,
		keyRange,
		name,
		callback,
	)
	return formatError(err)
}

// VSchema returns the VSchema.
func (e *Executor) VSchema() *vindexes.VSchema {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.vschema
}

// SaveVSchema updates the vschema and stats
func (e *Executor) SaveVSchema(vschema *vindexes.VSchema, stats *VSchemaStats) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if vschema != nil {
		e.vschema = vschema
	}
	e.vschemaStats = stats
	e.plans.Clear()

	if vschemaCounters != nil {
		vschemaCounters.Add("Reload", 1)
	}

}

// ParseDestinationTarget parses destination target string and sets default keyspace if possible.
func (e *Executor) ParseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, dest, err := topoproto.ParseDestination(targetString, defaultTabletType)
	// Set default keyspace
	if destKeyspace == "" && len(e.VSchema().Keyspaces) == 1 {
		for k := range e.VSchema().Keyspaces {
			destKeyspace = k
		}
	}
	return destKeyspace, destTabletType, dest, err
}

type iQueryOption interface {
	cachePlan() bool
	getSelectLimit() int
}

// getPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (e *Executor) getPlan(vcursor *vcursorImpl, sql string, comments sqlparser.MarginComments, bindVars map[string]*querypb.BindVariable, qo iQueryOption, logStats *LogStats) (*engine.Plan, error) {
	if logStats != nil {
		logStats.SQL = comments.Leading + sql + comments.Trailing
		logStats.BindVariables = bindVars
	}

	if e.VSchema() == nil {
		return nil, errors.New("vschema not initialized")
	}

	stmt, reserved, err := sqlparser.Parse2(sql)
	if err != nil {
		return nil, err
	}
	query := sql
	statement := stmt
	reservedVars := sqlparser.NewReservedVars("vtg", reserved)
	bindVarNeeds := &sqlparser.BindVarNeeds{}
	if !sqlparser.IgnoreMaxPayloadSizeDirective(statement) && !isValidPayloadSize(query) {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.NetPacketTooLarge, "query payload size above threshold")
	}
	ignoreMaxMemoryRows := sqlparser.IgnoreMaxMaxMemoryRowsDirective(stmt)
	vcursor.SetIgnoreMaxMemoryRows(ignoreMaxMemoryRows)

	setVarComment, err := prepareSetVarComment(vcursor, stmt)
	if err != nil {
		return nil, err
	}
	// Normalize if possible and retry.
	if e.canNormalizeStatement(stmt, bindVars, qo, setVarComment) {
		parameterize := e.normalize // the public flag is called normalize
		result, err := sqlparser.PrepareAST(
			stmt,
			reservedVars,
			bindVars,
			parameterize,
			vcursor.keyspace,
			qo.getSelectLimit(),
			setVarComment,
			vcursor.safeSession.SystemVariables,
		)
		if err != nil {
			return nil, err
		}
		statement = result.AST
		bindVarNeeds = result.BindVarNeeds
		query = sqlparser.String(statement)
	}

	if logStats != nil {
		logStats.SQL = comments.Leading + query + comments.Trailing
		logStats.BindVariables = bindVars
	}

	planHash := sha256.New()
	_, _ = planHash.Write([]byte(vcursor.planPrefixKey()))
	_, _ = planHash.Write([]byte{':'})
	_, _ = planHash.Write(hack.StringBytes(query))
	planKey := hex.EncodeToString(planHash.Sum(nil))

	if plan, ok := e.plans.Get(planKey); ok {
		return plan.(*engine.Plan), nil
	}

	plan, err := planbuilder.BuildFromStmt(query, statement, reservedVars, vcursor, bindVarNeeds, *enableOnlineDDL, *enableDirectDDL)
	if err != nil {
		return nil, err
	}

	plan.Warnings = vcursor.warnings
	vcursor.warnings = nil

	if qo.cachePlan() && sqlparser.CachePlan(statement) {
		e.plans.Set(planKey, plan)
	}

	return e.checkThatPlanIsValid(stmt, plan)
}

func (e *Executor) canNormalizeStatement(stmt sqlparser.Statement, bindVars map[string]*querypb.BindVariable, qo iQueryOption, setVarComment string) bool {
	// We do not normalize already prepared statements, by default

	if e.normalize && (*normalizePrepStmts || len(bindVars) == 0) && sqlparser.CanNormalize(stmt) {
		return true
	}

	if sqlparser.MustRewriteAST(stmt, qo.getSelectLimit() > 0) {
		return true
	}

	if setVarComment != "" {
		return true
	}

	return false
}

func prepareSetVarComment(vcursor *vcursorImpl, stmt sqlparser.Statement) (string, error) {
	if vcursor == nil || vcursor.Session().InReservedConn() {
		return "", nil
	}

	if !vcursor.Session().HasSystemVariables() {
		return "", nil
	}

	switch stmt.(type) {
	// If the statement is a transaction statement or a set no reserved connection / SET_VAR is needed
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint,
		*sqlparser.SRollback, *sqlparser.Release, *sqlparser.Set:
		return "", nil
	case sqlparser.SupportOptimizerHint:
		break
	default:
		vcursor.NeedsReservedConn()
		return "", nil
	}

	var res strings.Builder
	vcursor.Session().GetSystemVariables(func(k, v string) {
		res.WriteString(fmt.Sprintf("SET_VAR(%s = %s) ", k, v))
	})
	return strings.TrimSpace(res.String()), nil
}

func (e *Executor) debugGetPlan(planKey string) (*engine.Plan, bool) {
	planHash := sha256.Sum256([]byte(planKey))
	planHex := hex.EncodeToString(planHash[:])
	if plan, ok := e.plans.Get(planHex); ok {
		return plan.(*engine.Plan), true
	}
	return nil, false
}

type cacheItem struct {
	Key   string
	Value *engine.Plan
}

func (e *Executor) debugCacheEntries() (items []cacheItem) {
	e.plans.ForEach(func(value interface{}) bool {
		plan := value.(*engine.Plan)
		items = append(items, cacheItem{
			Key:   plan.Original,
			Value: plan,
		})
		return true
	})
	return
}

// ServeHTTP shows the current plans in the query cache.
func (e *Executor) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}

	switch request.URL.Path {
	case pathQueryPlans:
		returnAsJSON(response, e.debugCacheEntries())
	case pathVSchema:
		returnAsJSON(response, e.VSchema())
	case pathScatterStats:
		e.WriteScatterStats(response)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func returnAsJSON(response http.ResponseWriter, stuff interface{}) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	buf, err := json.MarshalIndent(stuff, "", " ")
	if err != nil {
		_, _ = response.Write([]byte(err.Error()))
		return
	}
	ebuf := bytes.NewBuffer(nil)
	json.HTMLEscape(ebuf, buf)
	_, _ = response.Write(ebuf.Bytes())
}

// Plans returns the LRU plan cache
func (e *Executor) Plans() cache.Cache {
	return e.plans
}

func (e *Executor) updateQueryCounts(planType, keyspace, tableName string, shardQueries int64) {
	queriesProcessed.Add(planType, 1)
	queriesRouted.Add(planType, shardQueries)
	if tableName != "" {
		queriesProcessedByTable.Add([]string{planType, keyspace, tableName}, 1)
		queriesRoutedByTable.Add([]string{planType, keyspace, tableName}, shardQueries)
	}
}

// VSchemaStats returns the loaded vschema stats.
func (e *Executor) VSchemaStats() *VSchemaStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.vschemaStats == nil {
		return &VSchemaStats{
			Error: "No VSchema loaded yet.",
		}
	}
	return e.vschemaStats
}

func buildVarCharFields(names ...string) []*querypb.Field {
	fields := make([]*querypb.Field, len(names))
	for i, v := range names {
		fields[i] = &querypb.Field{
			Name:    v,
			Type:    sqltypes.VarChar,
			Charset: collations.CollationUtf8ID,
			Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
		}
	}
	return fields
}

func buildVarCharRow(values ...string) []sqltypes.Value {
	row := make([]sqltypes.Value, len(values))
	for i, v := range values {
		row[i] = sqltypes.NewVarChar(v)
	}
	return row
}

// isValidPayloadSize validates whether a query payload is above the
// configured MaxPayloadSize threshold. The WarnPayloadSizeExceeded will increment
// if the payload size exceeds the warnPayloadSize.

func isValidPayloadSize(query string) bool {
	payloadSize := len(query)
	if *maxPayloadSize > 0 && payloadSize > *maxPayloadSize {
		return false
	}
	if *warnPayloadSize > 0 && payloadSize > *warnPayloadSize {
		warnings.Add("WarnPayloadSizeExceeded", 1)
	}
	return true
}

// Prepare executes a prepare statements.
func (e *Executor) Prepare(ctx context.Context, method string, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable) (fld []*querypb.Field, err error) {
	logStats := NewLogStats(ctx, method, sql, bindVars)
	fld, err = e.prepare(ctx, safeSession, sql, bindVars, logStats)
	logStats.Error = err

	// The mysql plugin runs an implicit rollback whenever a connection closes.
	// To avoid spamming the log with no-op rollback records, ignore it if
	// it was a no-op record (i.e. didn't issue any queries)
	if !(logStats.StmtType == "ROLLBACK" && logStats.ShardQueries == 0) {
		logStats.Send()
	}
	return fld, err
}

func (e *Executor) prepare(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) ([]*querypb.Field, error) {
	// Start an implicit transaction if necessary.
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession); err != nil {
			return nil, err
		}
	}

	destKeyspace, destTabletType, dest, err := e.ParseDestinationTarget(safeSession.TargetString)
	if err != nil {
		return nil, err
	}

	if UsingLegacyGateway() && safeSession.InTransaction() && destTabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "transaction is supported only for primary tablet type, current type: %v", destTabletType)
	}
	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	stmtType := sqlparser.Preview(sql)
	logStats.StmtType = stmtType.String()

	// Mysql warnings are scoped to the current session, but are
	// cleared when a "non-diagnostic statement" is executed:
	// https://dev.mysql.com/doc/refman/8.0/en/show-warnings.html
	//
	// To emulate this behavior, clear warnings from the session
	// for all statements _except_ SHOW, so that SHOW WARNINGS
	// can actually return them.
	if stmtType != sqlparser.StmtShow {
		safeSession.ClearWarnings()
	}

	switch stmtType {
	case sqlparser.StmtSelect:
		return e.handlePrepare(ctx, safeSession, sql, bindVars, logStats)
	case sqlparser.StmtShow:
		qr, err := e.handlePrepare(ctx, safeSession, sql, bindVars, logStats)
		if err == nil {
			return qr, nil
		}
		if err == planbuilder.ErrPlanNotSupported {
			res, err := e.handleShow(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
			if err == nil {
				return res.Fields, nil
			}
		}
		return nil, err
	case sqlparser.StmtDDL, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtSet, sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete,
		sqlparser.StmtUse, sqlparser.StmtOther, sqlparser.StmtComment, sqlparser.StmtExplain, sqlparser.StmtFlush:
		return nil, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unrecognized prepare statement: %s", sql)
}

func (e *Executor) handlePrepare(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) ([]*querypb.Field, error) {
	// V3 mode.
	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, _ := newVCursorImpl(ctx, safeSession, comments, e, logStats, e.vm, e.VSchema(), e.resolver.resolver, e.serv, e.warnShardedOnly)
	plan, err := e.getPlan(
		vcursor,
		query,
		comments,
		bindVars,
		safeSession,
		logStats,
	)
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	if err != nil {
		logStats.Error = err
		return nil, err
	}

	err = e.addNeededBindVars(plan.BindVarNeeds, bindVars, safeSession)
	if err != nil {
		logStats.Error = err
		return nil, err
	}

	qr, err := plan.Instructions.GetFields(vcursor, bindVars)
	logStats.ExecuteTime = time.Since(execStart)
	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1 // nolint
		return nil, err
	}
	logStats.RowsAffected = qr.RowsAffected

	plan.AddStats(1, time.Since(logStats.StartTime), logStats.ShardQueries, qr.RowsAffected, uint64(len(qr.Rows)), errCount)

	return qr.Fields, err
}

// ExecuteMultiShard implements the IExecutor interface
func (e *Executor) ExecuteMultiShard(ctx context.Context, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *SafeSession, autocommit bool, ignoreMaxMemoryRows bool) (qr *sqltypes.Result, errs []error) {
	return e.scatterConn.ExecuteMultiShard(ctx, rss, queries, session, autocommit, ignoreMaxMemoryRows)
}

// StreamExecuteMulti implements the IExecutor interface
func (e *Executor) StreamExecuteMulti(ctx context.Context, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, session *SafeSession, autocommit bool, callback func(reply *sqltypes.Result) error) []error {
	return e.scatterConn.StreamExecuteMulti(ctx, query, rss, vars, session, autocommit, callback)
}

// ExecuteLock implements the IExecutor interface
func (e *Executor) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *SafeSession) (*sqltypes.Result, error) {
	return e.scatterConn.ExecuteLock(ctx, rs, query, session)
}

// ExecuteMessageStream implements the IExecutor interface
func (e *Executor) ExecuteMessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, tableName string, callback func(reply *sqltypes.Result) error) error {
	return e.scatterConn.MessageStream(ctx, rss, tableName, callback)
}

// ExecuteVStream implements the IExecutor interface
func (e *Executor) ExecuteVStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	return e.startVStream(ctx, rss, filter, gtid, callback)
}

func (e *Executor) startVStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	var shardGtids []*binlogdatapb.ShardGtid
	for _, rs := range rss {
		shardGtid := &binlogdatapb.ShardGtid{
			Keyspace: rs.Target.Keyspace,
			Shard:    rs.Target.Shard,
			Gtid:     gtid,
		}
		shardGtids = append(shardGtids, shardGtid)
	}
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: shardGtids,
	}
	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return err
	}

	vsm := newVStreamManager(e.resolver.resolver, e.serv, e.cell)
	vs := &vstream{
		vgtid:              vgtid,
		tabletType:         topodatapb.TabletType_PRIMARY,
		filter:             filter,
		send:               callback,
		resolver:           e.resolver.resolver,
		journaler:          make(map[int64]*journalEvent),
		skewTimeoutSeconds: maxSkewTimeoutSeconds,
		timestamps:         make(map[string]int64),
		vsm:                vsm,
		eventCh:            make(chan []*binlogdatapb.VEvent),
		ts:                 ts,
	}
	_ = vs.stream(ctx)
	return nil
}

func (e *Executor) checkThatPlanIsValid(stmt sqlparser.Statement, plan *engine.Plan) (*engine.Plan, error) {
	if e.allowScatter || sqlparser.AllowScatterDirective(stmt) {
		return plan, nil
	}
	// we go over all the primitives in the plan, searching for a route that is of SelectScatter opcode
	badPrimitive := engine.Find(func(node engine.Primitive) bool {
		router, ok := node.(*engine.Route)
		if !ok {
			return false
		}
		return router.Opcode == engine.Scatter
	}, plan.Instructions)

	if badPrimitive == nil {
		return plan, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "plan includes scatter, which is disallowed using the `no_scatter` command line argument")
}

func getTabletThrottlerStatus(tabletHostPort string) (string, error) {
	client := http.Client{
		Timeout: 100 * time.Millisecond,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s/throttler/check?app=vtgate", tabletHostPort))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var elements struct {
		StatusCode int
		Value      float64
		Threshold  float64
		Message    string
	}
	err = json.Unmarshal(body, &elements)
	if err != nil {
		return "", err
	}

	httpStatusStr := http.StatusText(elements.StatusCode)

	load := float64(0)
	if elements.Threshold > 0 {
		load = float64((elements.Value / elements.Threshold) * 100)
	}

	status := fmt.Sprintf("{\"state\":\"%s\",\"load\":%.2f,\"message\":\"%s\"}", httpStatusStr, load, elements.Message)
	return status, nil
}
