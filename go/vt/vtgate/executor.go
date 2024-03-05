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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
	"vitess.io/vitess/go/vt/vthash"
)

var (
	errNoKeyspace     = vterrors.VT09005()
	defaultTabletType = topodatapb.TabletType_PRIMARY

	// TODO: @rafael - These two counters should be deprecated in favor of the ByTable ones in v17+. They are kept for now for backwards compatibility.
	queriesProcessed = stats.NewCountersWithSingleLabel("QueriesProcessed", "Queries processed at vtgate by plan type", "Plan")
	queriesRouted    = stats.NewCountersWithSingleLabel("QueriesRouted", "Queries routed from vtgate to vttablet by plan type", "Plan")

	queriesProcessedByTable = stats.NewCountersWithMultiLabels("QueriesProcessedByTable", "Queries processed at vtgate by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})
	queriesRoutedByTable    = stats.NewCountersWithMultiLabels("QueriesRoutedByTable", "Queries routed from vtgate to vttablet by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})
)

const (
	bindVarPrefix = "__vt"
)

func init() {
	registerTabletTypeFlag := func(fs *pflag.FlagSet) {
		fs.Var((*topoproto.TabletTypeFlag)(&defaultTabletType), "default_tablet_type", "The default tablet type to set for queries, when one is not explicitly selected.")
	}

	servenv.OnParseFor("vtgate", registerTabletTypeFlag)
	servenv.OnParseFor("vtgateclienttest", registerTabletTypeFlag)
	servenv.OnParseFor("vtcombo", registerTabletTypeFlag)
	servenv.OnParseFor("vtexplain", registerTabletTypeFlag)
}

// Executor is the engine that executes queries by utilizing
// the abilities of the underlying vttablets.
type Executor struct {
	env         *vtenv.Environment
	serv        srvtopo.Server
	cell        string
	resolver    *Resolver
	scatterConn *ScatterConn
	txConn      *TxConn
	pv          plancontext.PlannerVersion

	mu           sync.Mutex
	vschema      *vindexes.VSchema
	streamSize   int
	vschemaStats *VSchemaStats

	plans *PlanCache
	epoch atomic.Uint32

	normalize       bool
	warnShardedOnly bool

	vm            *VSchemaManager
	schemaTracker SchemaInfo

	// allowScatter will fail planning if set to false and a plan contains any scatter queries
	allowScatter bool

	// queryLogger is passed in for logging from this vtgate executor.
	queryLogger *streamlog.StreamLogger[*logstats.LogStats]

	warmingReadsPercent int
	warmingReadsChannel chan bool
}

var executorOnce sync.Once

const pathQueryPlans = "/debug/query_plans"
const pathScatterStats = "/debug/scatter_stats"
const pathVSchema = "/debug/vschema"

type PlanCacheKey = theine.HashKey256
type PlanCache = theine.Store[PlanCacheKey, *engine.Plan]

func DefaultPlanCache() *PlanCache {
	// when being endtoend tested, disable the doorkeeper to ensure reproducible results
	doorkeeper := !servenv.TestingEndtoend
	return theine.NewStore[PlanCacheKey, *engine.Plan](queryPlanCacheMemory, doorkeeper)
}

// NewExecutor creates a new Executor.
func NewExecutor(
	ctx context.Context,
	env *vtenv.Environment,
	serv srvtopo.Server,
	cell string,
	resolver *Resolver,
	normalize, warnOnShardedOnly bool,
	streamSize int,
	plans *PlanCache,
	schemaTracker SchemaInfo,
	noScatter bool,
	pv plancontext.PlannerVersion,
	warmingReadsPercent int,
) *Executor {
	e := &Executor{
		env:                 env,
		serv:                serv,
		cell:                cell,
		resolver:            resolver,
		scatterConn:         resolver.scatterConn,
		txConn:              resolver.scatterConn.txConn,
		normalize:           normalize,
		warnShardedOnly:     warnOnShardedOnly,
		streamSize:          streamSize,
		schemaTracker:       schemaTracker,
		allowScatter:        !noScatter,
		pv:                  pv,
		plans:               plans,
		warmingReadsPercent: warmingReadsPercent,
		warmingReadsChannel: make(chan bool, warmingReadsConcurrency),
	}

	vschemaacl.Init()
	// we subscribe to update from the VSchemaManager
	e.vm = &VSchemaManager{
		subscriber: e.SaveVSchema,
		serv:       serv,
		cell:       cell,
		schema:     e.schemaTracker,
		parser:     env.Parser(),
	}
	serv.WatchSrvVSchema(ctx, cell, e.vm.VSchemaUpdate)

	executorOnce.Do(func() {
		stats.NewGaugeFunc("QueryPlanCacheLength", "Query plan cache length", func() int64 {
			return int64(e.plans.Len())
		})
		stats.NewGaugeFunc("QueryPlanCacheSize", "Query plan cache size", func() int64 {
			return int64(e.plans.UsedCapacity())
		})
		stats.NewGaugeFunc("QueryPlanCacheCapacity", "Query plan cache capacity", func() int64 {
			return int64(e.plans.MaxCapacity())
		})
		stats.NewCounterFunc("QueryPlanCacheEvictions", "Query plan cache evictions", func() int64 {
			return e.plans.Metrics.Evicted()
		})
		stats.NewCounterFunc("QueryPlanCacheHits", "Query plan cache hits", func() int64 {
			return e.plans.Metrics.Hits()
		})
		stats.NewCounterFunc("QueryPlanCacheMisses", "Query plan cache misses", func() int64 {
			return e.plans.Metrics.Hits()
		})
		servenv.HTTPHandle(pathQueryPlans, e)
		servenv.HTTPHandle(pathScatterStats, e)
		servenv.HTTPHandle(pathVSchema, e)
	})
	return e
}

// Execute executes a non-streaming query.
func (e *Executor) Execute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, method string, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable) (result *sqltypes.Result, err error) {
	span, ctx := trace.NewSpan(ctx, "executor.Execute")
	span.Annotate("method", method)
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars)
	stmtType, result, err := e.execute(ctx, mysqlCtx, safeSession, sql, bindVars, logStats)
	logStats.Error = err
	if result == nil {
		saveSessionStats(safeSession, stmtType, 0, 0, 0, err)
	} else {
		saveSessionStats(safeSession, stmtType, result.RowsAffected, result.InsertID, len(result.Rows), err)
	}
	if result != nil && len(result.Rows) > warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := e.env.Parser().RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		log.Warningf("%q exceeds warning threshold of max memory rows: %v. Actual memory rows: %v", piiSafeSQL, warnMemoryRows, len(result.Rows))
	}

	logStats.SaveEndTime()
	e.queryLogger.Send(logStats)
	err = vterrors.TruncateError(err, truncateErrorLen)
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
	mysqlCtx vtgateservice.MySQLConnection,
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

	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars)
	srr := &streaminResultReceiver{callback: callback}
	var err error

	resultHandler := func(ctx context.Context, plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, execStart time.Time) error {
		var seenResults atomic.Bool
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
					if err := callback(qr.Metadata()); err != nil {
						return err
					}
					seenResults.Store(true)
				}

				for _, row := range qr.Rows {
					result.Rows = append(result.Rows, row)

					for _, col := range row {
						byteCount += col.Len()
					}

					if byteCount >= e.streamSize {
						err := callback(result)
						seenResults.Store(true)
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
		err := vc.StreamExecutePrimitive(ctx, plan.Instructions, bindVars, true, func(qr *sqltypes.Result) error {
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
		if len(result.Rows) > 0 || !seenResults.Load() {
			if err := callback(result); err != nil {
				return err
			}
		}

		// 5: Log and add statistics
		logStats.TablesUsed = plan.TablesUsed
		logStats.TabletType = vc.TabletType().String()
		logStats.ExecuteTime = time.Since(execStart)
		logStats.ActiveKeyspace = vc.keyspace

		e.updateQueryCounts(plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

		return err
	}

	err = e.newExecute(ctx, mysqlCtx, safeSession, sql, bindVars, logStats, resultHandler, srr.storeResultStats)

	logStats.Error = err
	saveSessionStats(safeSession, srr.stmtType, srr.rowsAffected, srr.insertID, srr.rowsReturned, err)
	if srr.rowsReturned > warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := e.env.Parser().RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		log.Warningf("%q exceeds warning threshold of max memory rows: %v. Actual memory rows: %v", piiSafeSQL, warnMemoryRows, srr.rowsReturned)
	}

	logStats.SaveEndTime()
	e.queryLogger.Send(logStats)
	return vterrors.TruncateError(err, truncateErrorLen)

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

func (e *Executor) execute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) (sqlparser.StatementType, *sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	var stmtType sqlparser.StatementType
	err = e.newExecute(ctx, mysqlCtx, safeSession, sql, bindVars, logStats, func(ctx context.Context, plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, time time.Time) error {
		stmtType = plan.Type
		qr, err = e.executePlan(ctx, safeSession, plan, vc, bindVars, logStats, time)
		return err
	}, func(typ sqlparser.StatementType, result *sqltypes.Result) error {
		stmtType = typ
		qr = result
		return nil
	})

	return stmtType, qr, err
}

// addNeededBindVars adds bind vars that are needed by the plan
func (e *Executor) addNeededBindVars(vcursor *vcursorImpl, bindVarNeeds *sqlparser.BindVarNeeds, bindVars map[string]*querypb.BindVariable, session *SafeSession) error {
	for _, funcName := range bindVarNeeds.NeedFunctionResult {
		switch funcName {
		case sqlparser.DBVarName:
			bindVars[sqlparser.DBVarName] = sqltypes.StringBindVariable(session.TargetString)
		case sqlparser.LastInsertIDName:
			bindVars[sqlparser.LastInsertIDName] = sqltypes.Uint64BindVariable(session.GetLastInsertId())
		case sqlparser.FoundRowsName:
			bindVars[sqlparser.FoundRowsName] = sqltypes.Int64BindVariable(int64(session.FoundRows))
		case sqlparser.RowCountName:
			bindVars[sqlparser.RowCountName] = sqltypes.Int64BindVariable(session.RowCount)
		}
	}

	for _, sysVar := range bindVarNeeds.NeedSystemVariable {
		key := bindVarPrefix + sysVar
		switch sysVar {
		case sysvars.Autocommit.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.Autocommit)
		case sysvars.QueryTimeout.Name:
			bindVars[key] = sqltypes.Int64BindVariable(session.GetQueryTimeout())
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
			txMode := session.TransactionMode
			if txMode == vtgatepb.TransactionMode_UNSPECIFIED {
				txMode = getTxMode()
			}
			bindVars[key] = sqltypes.StringBindVariable(txMode.String())
		case sysvars.Workload.Name:
			var v string
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.GetWorkload().String()
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.DDLStrategy.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.DDLStrategy)
		case sysvars.MigrationContext.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.MigrationContext)
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
				expr, err := e.env.Parser().ParseExpr(value)
				if err != nil {
					return err
				}

				evalExpr, err := evalengine.Translate(expr, &evalengine.Config{
					Collation:   vcursor.collation,
					Environment: e.env,
					SQLMode:     evalengine.ParseSQLMode(vcursor.SQLMode()),
				})
				if err != nil {
					return err
				}
				evaluated, err := evalengine.NewExpressionEnv(context.Background(), nil, vcursor).Evaluate(evalExpr)
				if err != nil {
					return err
				}
				bindVars[key] = sqltypes.ValueBindVariable(evaluated.Value(vcursor.collation))
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

func (e *Executor) handleBegin(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats, stmt sqlparser.Statement) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	begin := stmt.(*sqlparser.Begin)
	err := e.txConn.Begin(ctx, safeSession, begin.TxAccessModes)
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts("Begin", "", "", 0)

	return &sqltypes.Result{}, err
}

func (e *Executor) handleCommit(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats) (*sqltypes.Result, error) {
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

func (e *Executor) handleRollback(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts("Rollback", "", "", int64(logStats.ShardQueries))
	err := e.txConn.Rollback(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleSavepoint(ctx context.Context, safeSession *SafeSession, sql string, planType string, logStats *logstats.LogStats, nonTxResponse func(query string) (*sqltypes.Result, error), ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
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
		qr, errs = e.ExecuteMultiShard(ctx, nil, rss, queries, safeSession, false /*autocommit*/, ignoreMaxMemoryRows)
		err := vterrors.Aggregate(errs)
		if err != nil {
			return nil, err
		}
	}
	return qr, nil
}

// handleKill executed the kill statement.
func (e *Executor) handleKill(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, stmt sqlparser.Statement, logStats *logstats.LogStats) (result *sqltypes.Result, err error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	e.updateQueryCounts("Kill", "", "", 0)
	defer func() {
		logStats.ExecuteTime = time.Since(execStart)
	}()

	if !allowKillStmt {
		return nil, vterrors.VT07001("kill statement execution not permitted.")
	}

	if mysqlCtx == nil {
		return nil, vterrors.VT12001("kill statement works with access through mysql protocol")
	}

	killStmt := stmt.(*sqlparser.Kill)
	switch killStmt.Type {
	case sqlparser.QueryType:
		err = mysqlCtx.KillQuery(uint32(killStmt.ProcesslistID))
	default:
		err = mysqlCtx.KillConnection(ctx, uint32(killStmt.ProcesslistID))
	}
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

// CloseSession releases the current connection, which rollbacks open transactions and closes reserved connections.
// It is called then the MySQL servers closes the connection to its client.
func (e *Executor) CloseSession(ctx context.Context, safeSession *SafeSession) error {
	return e.txConn.ReleaseAll(ctx, safeSession)
}

func (e *Executor) setVitessMetadata(ctx context.Context, name, value string) error {
	// TODO(kalfonso): move to its own acl check and consolidate into an acl component that can handle multiple operations (vschema, metadata)
	user := callerid.ImmediateCallerIDFromContext(ctx)
	allowed := vschemaacl.Authorized(user)
	if !allowed {
		return vterrors.NewErrorf(vtrpcpb.Code_PERMISSION_DENIED, vterrors.AccessDeniedError, "User '%s' not authorized to perform vitess metadata operations", user.GetUsername())
	}

	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return err
	}

	if value == "" {
		return ts.DeleteMetadata(ctx, name)
	}
	return ts.UpsertMetadata(ctx, name, value)
}

func (e *Executor) showVitessMetadata(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return nil, err
	}

	var metadata map[string]string
	if filter == nil {
		metadata, err = ts.GetMetadata(ctx, "")
		if err != nil {
			return nil, err
		}
	} else {
		metadata, err = ts.GetMetadata(ctx, filter.Like)
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

type tabletFilter func(tablet *topodatapb.Tablet, servingState string, primaryTermStartTime int64) bool

func (e *Executor) showShards(ctx context.Context, filter *sqlparser.ShowFilter, destTabletType topodatapb.TabletType) (*sqltypes.Result, error) {
	showVitessShardsFilters := func(filter *sqlparser.ShowFilter) ([]func(string) bool, []func(string, *topodatapb.ShardReference) bool) {
		keyspaceFilters := []func(string) bool{}
		shardFilters := []func(string, *topodatapb.ShardReference) bool{}

		if filter == nil {
			return keyspaceFilters, shardFilters
		}

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

	keyspaceFilters, shardFilters := showVitessShardsFilters(filter)

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
}

func (e *Executor) showTablets(filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	getTabletFilters := func(filter *sqlparser.ShowFilter) []tabletFilter {
		var filters []tabletFilter

		if filter == nil {
			return filters
		}

		if filter.Like != "" {
			tabletRegexp := sqlparser.LikeToRegexp(filter.Like)

			f := func(tablet *topodatapb.Tablet, servingState string, primaryTermStartTime int64) bool {
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

	tabletFilters := getTabletFilters(filter)

	rows := [][]sqltypes.Value{}
	status := e.scatterConn.GetHealthCheckCacheStatus()
	for _, s := range status {
		for _, ts := range s.TabletsStats {
			state := "SERVING"
			if !ts.Serving {
				state = "NOT_SERVING"
			}
			ptst := ts.PrimaryTermStartTime
			ptstStr := ""
			if ptst > 0 {
				// this code depends on the fact that PrimaryTermStartTime is the seconds since epoch start
				ptstStr = time.Unix(ptst, 0).UTC().Format(time.RFC3339)
			}

			skipTablet := false
			for _, filter := range tabletFilters {
				if !filter(ts.Tablet, state, ptst) {
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
				ptstStr,
			))
		}
	}
	return &sqltypes.Result{
		Fields: buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "PrimaryTermStartTime"),
		Rows:   rows,
	}, nil
}

func (e *Executor) showVitessReplicationStatus(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()
	rows := [][]sqltypes.Value{}

	status := e.scatterConn.GetHealthCheckCacheStatus()

	for _, s := range status {
		for _, ts := range s.TabletsStats {
			// We only want to show REPLICA and RDONLY tablets
			if ts.Target.TabletType != topodatapb.TabletType_REPLICA && ts.Target.TabletType != topodatapb.TabletType_RDONLY {
				continue
			}

			// Allow people to filter by Keyspace and Shard using a LIKE clause
			if filter != nil {
				ksFilterRegex := sqlparser.LikeToRegexp(filter.Like)
				keyspaceShardStr := fmt.Sprintf("%s/%s", ts.Target.Keyspace, ts.Target.Shard)
				if !ksFilterRegex.MatchString(keyspaceShardStr) {
					continue
				}
			}

			tabletHostPort := ts.GetTabletHostPort()
			throttlerStatus, err := getTabletThrottlerStatus(tabletHostPort)
			if err != nil {
				log.Warningf("Could not get throttler status from %s: %v", topoproto.TabletAliasString(ts.Tablet.Alias), err)
			}

			replSourceHost := ""
			replSourcePort := int64(0)
			replIOThreadHealth := ""
			replSQLThreadHealth := ""
			replLastError := ""
			replLag := "-1" // A string to support NULL as a value
			sql := "show slave status"
			results, err := e.txConn.tabletGateway.Execute(ctx, ts.Target, sql, nil, 0, 0, nil)
			if err != nil || results == nil {
				log.Warningf("Could not get replication status from %s: %v", tabletHostPort, err)
			} else if row := results.Named().Row(); row != nil {
				replSourceHost = row["Master_Host"].ToString()
				replSourcePort, _ = row["Master_Port"].ToInt64()
				replIOThreadHealth = row["Slave_IO_Running"].ToString()
				replSQLThreadHealth = row["Slave_SQL_Running"].ToString()
				replLastError = row["Last_Error"].ToString()
				// We cannot check the tablet's tabletenv config from here so
				// we only use the tablet's stat -- which is managed by the
				// ReplicationTracker -- if we can tell that it's enabled,
				// meaning that it has a non-zero value. If it's actually
				// enabled AND zero (rather than the zeroval), then mysqld
				// should also return 0 so in this case the value is correct
				// and equivalent either way. The only reason that we would
				// want to use the ReplicationTracker based value, when we
				// can, is because the polling method allows us to get the
				// estimated lag value when replication is not running (based
				// on how long we've seen that it's not been running).
				if ts.Stats != nil && ts.Stats.ReplicationLagSeconds > 0 { // Use the value we get from the ReplicationTracker
					replLag = fmt.Sprintf("%d", ts.Stats.ReplicationLagSeconds)
				} else { // Use the value from mysqld
					if row["Seconds_Behind_Master"].IsNull() {
						replLag = strings.ToUpper(sqltypes.NullStr) // Uppercase to match mysqld's output in SHOW REPLICA STATUS
					} else {
						replLag = row["Seconds_Behind_Master"].ToString()
					}
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
				replLag,
				throttlerStatus,
			))
		}
	}
	return &sqltypes.Result{
		Fields: buildVarCharFields("Keyspace", "Shard", "TabletType", "Alias", "Hostname", "ReplicationSource", "ReplicationHealth", "ReplicationLag", "ThrottlerStatus"),
		Rows:   rows,
	}, nil
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
	e.ClearPlans()

	if vschemaCounters != nil {
		vschemaCounters.Add("Reload", 1)
	}

	if vindexUnknownParams != nil {
		var unknownParams int
		for _, ks := range stats.Keyspaces {
			unknownParams += ks.VindexUnknownParamsCount
		}
		vindexUnknownParams.Set(int64(unknownParams))
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
func (e *Executor) getPlan(
	ctx context.Context,
	vcursor *vcursorImpl,
	query string,
	stmt sqlparser.Statement,
	comments sqlparser.MarginComments,
	bindVars map[string]*querypb.BindVariable,
	reservedVars *sqlparser.ReservedVars,
	allowParameterization bool,
	logStats *logstats.LogStats,
) (*engine.Plan, error) {
	if e.VSchema() == nil {
		return nil, vterrors.VT13001("vschema not initialized")
	}

	vcursor.SetIgnoreMaxMemoryRows(sqlparser.IgnoreMaxMaxMemoryRowsDirective(stmt))
	vcursor.SetConsolidator(sqlparser.Consolidator(stmt))
	vcursor.SetWorkloadName(sqlparser.GetWorkloadNameFromStatement(stmt))
	vcursor.UpdateForeignKeyChecksState(sqlparser.ForeignKeyChecksState(stmt))
	priority, err := sqlparser.GetPriorityFromStatement(stmt)
	if err != nil {
		return nil, err
	}
	vcursor.SetPriority(priority)

	setVarComment, err := prepareSetVarComment(vcursor, stmt)
	if err != nil {
		return nil, err
	}

	// Normalize if possible
	shouldNormalize := e.canNormalizeStatement(stmt, setVarComment)
	parameterize := allowParameterization && shouldNormalize

	rewriteASTResult, err := sqlparser.PrepareAST(
		stmt,
		reservedVars,
		bindVars,
		parameterize,
		vcursor.keyspace,
		vcursor.safeSession.getSelectLimit(),
		setVarComment,
		vcursor.safeSession.SystemVariables,
		vcursor.GetForeignKeyChecksState(),
		vcursor,
	)
	if err != nil {
		return nil, err
	}
	stmt = rewriteASTResult.AST
	bindVarNeeds := rewriteASTResult.BindVarNeeds
	if shouldNormalize {
		query = sqlparser.String(stmt)
	}

	logStats.SQL = comments.Leading + query + comments.Trailing
	logStats.BindVariables = sqltypes.CopyBindVariables(bindVars)

	return e.cacheAndBuildStatement(ctx, vcursor, query, stmt, reservedVars, bindVarNeeds, logStats)
}

func (e *Executor) hashPlan(ctx context.Context, vcursor *vcursorImpl, query string) PlanCacheKey {
	hasher := vthash.New256()
	vcursor.keyForPlan(ctx, query, hasher)

	var planKey PlanCacheKey
	hasher.Sum(planKey[:0])
	return planKey
}

func (e *Executor) buildStatement(
	ctx context.Context,
	vcursor *vcursorImpl,
	query string,
	stmt sqlparser.Statement,
	reservedVars *sqlparser.ReservedVars,
	bindVarNeeds *sqlparser.BindVarNeeds,
) (*engine.Plan, error) {
	plan, err := planbuilder.BuildFromStmt(ctx, query, stmt, reservedVars, vcursor, bindVarNeeds, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}

	plan.Warnings = vcursor.warnings
	vcursor.warnings = nil

	err = e.checkThatPlanIsValid(stmt, plan)
	return plan, err
}

func (e *Executor) cacheAndBuildStatement(
	ctx context.Context,
	vcursor *vcursorImpl,
	query string,
	stmt sqlparser.Statement,
	reservedVars *sqlparser.ReservedVars,
	bindVarNeeds *sqlparser.BindVarNeeds,
	logStats *logstats.LogStats,
) (*engine.Plan, error) {
	planCachable := sqlparser.CachePlan(stmt) && vcursor.safeSession.cachePlan()
	if planCachable {
		planKey := e.hashPlan(ctx, vcursor, query)

		var plan *engine.Plan
		var err error
		plan, logStats.CachedPlan, err = e.plans.GetOrLoad(planKey, e.epoch.Load(), func() (*engine.Plan, error) {
			return e.buildStatement(ctx, vcursor, query, stmt, reservedVars, bindVarNeeds)
		})
		return plan, err
	}
	return e.buildStatement(ctx, vcursor, query, stmt, reservedVars, bindVarNeeds)
}

func (e *Executor) canNormalizeStatement(stmt sqlparser.Statement, setVarComment string) bool {
	return sqlparser.CanNormalize(stmt) || setVarComment != ""
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
		*sqlparser.SRollback, *sqlparser.Release, *sqlparser.Set, *sqlparser.Show:
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

func (e *Executor) debugCacheEntries() (items map[string]*engine.Plan) {
	items = make(map[string]*engine.Plan)
	e.ForEachPlan(func(plan *engine.Plan) bool {
		items[plan.Original] = plan
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

func returnAsJSON(response http.ResponseWriter, stuff any) {
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
func (e *Executor) Plans() *PlanCache {
	return e.plans
}

func (e *Executor) ForEachPlan(each func(plan *engine.Plan) bool) {
	e.plans.Range(e.epoch.Load(), func(_ PlanCacheKey, value *engine.Plan) bool {
		return each(value)
	})
}

func (e *Executor) ClearPlans() {
	e.epoch.Add(1)
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
			Charset: uint32(collations.SystemCollation.Collation),
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
	if maxPayloadSize > 0 && payloadSize > maxPayloadSize {
		return false
	}
	if warnPayloadSize > 0 && payloadSize > warnPayloadSize {
		warnings.Add("WarnPayloadSizeExceeded", 1)
	}
	return true
}

// Prepare executes a prepare statements.
func (e *Executor) Prepare(ctx context.Context, method string, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable) (fld []*querypb.Field, err error) {
	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars)
	fld, err = e.prepare(ctx, safeSession, sql, bindVars, logStats)
	logStats.Error = err

	// The mysql plugin runs an implicit rollback whenever a connection closes.
	// To avoid spamming the log with no-op rollback records, ignore it if
	// it was a no-op record (i.e. didn't issue any queries)
	if !(logStats.StmtType == "ROLLBACK" && logStats.ShardQueries == 0) {
		logStats.SaveEndTime()
		e.queryLogger.Send(logStats)
	}
	return fld, vterrors.TruncateError(err, truncateErrorLen)
}

func (e *Executor) prepare(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) ([]*querypb.Field, error) {
	// Start an implicit transaction if necessary.
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession, nil); err != nil {
			return nil, err
		}
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
	case sqlparser.StmtSelect, sqlparser.StmtShow:
		return e.handlePrepare(ctx, safeSession, sql, bindVars, logStats)
	case sqlparser.StmtDDL, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtSet, sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete,
		sqlparser.StmtUse, sqlparser.StmtOther, sqlparser.StmtAnalyze, sqlparser.StmtComment, sqlparser.StmtExplain, sqlparser.StmtFlush, sqlparser.StmtKill:
		return nil, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unrecognized prepare statement: %s", sql)
}

func (e *Executor) handlePrepare(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) ([]*querypb.Field, error) {
	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, _ := newVCursorImpl(safeSession, comments, e, logStats, e.vm, e.VSchema(), e.resolver.resolver, e.serv, e.warnShardedOnly, e.pv)

	stmt, reservedVars, err := parseAndValidateQuery(query, e.env.Parser())
	if err != nil {
		return nil, err
	}

	plan, err := e.getPlan(ctx, vcursor, sql, stmt, comments, bindVars, reservedVars /* parameterize */, false, logStats)
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	if err != nil {
		logStats.Error = err
		return nil, err
	}

	err = e.addNeededBindVars(vcursor, plan.BindVarNeeds, bindVars, safeSession)
	if err != nil {
		logStats.Error = err
		return nil, err
	}

	qr, err := plan.Instructions.GetFields(ctx, vcursor, bindVars)
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

func parseAndValidateQuery(query string, parser *sqlparser.Parser) (sqlparser.Statement, *sqlparser.ReservedVars, error) {
	stmt, reserved, err := parser.Parse2(query)
	if err != nil {
		return nil, nil, err
	}
	if !sqlparser.IgnoreMaxPayloadSizeDirective(stmt) && !isValidPayloadSize(query) {
		return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.NetPacketTooLarge, "query payload size above threshold")
	}
	return stmt, sqlparser.NewReservedVars("vtg", reserved), nil
}

// ExecuteMultiShard implements the IExecutor interface
func (e *Executor) ExecuteMultiShard(ctx context.Context, primitive engine.Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *SafeSession, autocommit bool, ignoreMaxMemoryRows bool) (qr *sqltypes.Result, errs []error) {
	return e.scatterConn.ExecuteMultiShard(ctx, primitive, rss, queries, session, autocommit, ignoreMaxMemoryRows)
}

// StreamExecuteMulti implements the IExecutor interface
func (e *Executor) StreamExecuteMulti(ctx context.Context, primitive engine.Primitive, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, session *SafeSession, autocommit bool, callback func(reply *sqltypes.Result) error) []error {
	return e.scatterConn.StreamExecuteMulti(ctx, primitive, query, rss, vars, session, autocommit, callback)
}

// ExecuteLock implements the IExecutor interface
func (e *Executor) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *SafeSession, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
	return e.scatterConn.ExecuteLock(ctx, rs, query, session, lockFuncType)
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
		copyCompletedShard: make(map[string]struct{}),
	}
	_ = vs.stream(ctx)
	return nil
}

func (e *Executor) checkThatPlanIsValid(stmt sqlparser.Statement, plan *engine.Plan) error {
	if e.allowScatter || plan.Instructions == nil || sqlparser.AllowScatterDirective(stmt) {
		return nil
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
		return nil
	}

	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "plan includes scatter, which is disallowed using the `no_scatter` command line argument")
}

// getTabletThrottlerStatus uses HTTP to get the throttler status
// on a tablet. It uses HTTP because the CheckThrottler RPC is a
// tmclient RPC and you cannot use tmclient outside of a tablet.
func getTabletThrottlerStatus(tabletHostPort string) (string, error) {
	client := http.Client{
		Timeout: 100 * time.Millisecond,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s/throttler/check-self", tabletHostPort))
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

// ReleaseLock implements the IExecutor interface
func (e *Executor) ReleaseLock(ctx context.Context, session *SafeSession) error {
	return e.txConn.ReleaseLock(ctx, session)
}

// planPrepareStmt implements the IExecutor interface
func (e *Executor) planPrepareStmt(ctx context.Context, vcursor *vcursorImpl, query string) (*engine.Plan, sqlparser.Statement, error) {
	stmt, reservedVars, err := parseAndValidateQuery(query, e.env.Parser())
	if err != nil {
		return nil, nil, err
	}

	// creating this log stats to not interfere with the original log stats.
	lStats := logstats.NewLogStats(ctx, "prepare", query, vcursor.safeSession.SessionUUID, nil)
	plan, err := e.getPlan(
		ctx,
		vcursor,
		query,
		sqlparser.CloneStatement(stmt),
		vcursor.marginComments,
		map[string]*querypb.BindVariable{},
		reservedVars, /* normalize */
		false,
		lStats,
	)
	if err != nil {
		return nil, nil, err
	}
	return plan, stmt, nil
}

func (e *Executor) Close() {
	e.scatterConn.Close()
	topo, err := e.serv.GetTopoServer()
	if err != nil {
		panic(err)
	}
	topo.Close()
	e.plans.Close()
}

func (e *Executor) environment() *vtenv.Environment {
	return e.env
}
