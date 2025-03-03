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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/dynamicconfig"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

var (
	defaultTabletType = topodatapb.TabletType_PRIMARY

	// TODO: @harshit/@systay - Remove these deprecated stats once we have a replacement in a released version.
	queriesProcessed        = stats.NewCountersWithSingleLabel("QueriesProcessed", "Deprecated: Queries processed at vtgate by plan type", "Plan")
	queriesRouted           = stats.NewCountersWithSingleLabel("QueriesRouted", "Deprecated: Queries routed from vtgate to vttablet by plan type", "Plan")
	queriesProcessedByTable = stats.NewCountersWithMultiLabels("QueriesProcessedByTable", "Deprecated: Queries processed at vtgate by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})
	queriesRoutedByTable    = stats.NewCountersWithMultiLabels("QueriesRoutedByTable", "Deprecated: Queries routed from vtgate to vttablet by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})

	queryExecutions        = stats.NewCountersWithMultiLabels("QueryExecutions", "Counts queries executed at VTGate by query type, plan type, and tablet type.", []string{"Query", "Plan", "Tablet"})
	queryRoutes            = stats.NewCountersWithMultiLabels("QueryRoutes", "Counts queries routed from VTGate to VTTablet by query type, plan type, and tablet type.", []string{"Query", "Plan", "Tablet"})
	queryExecutionsByTable = stats.NewCountersWithMultiLabels("QueryExecutionsByTable", "Counts queries executed at VTGate per table by query type and table.", []string{"Query", "Table"})

	// commitMode records the timing of the commit phase of a transaction.
	// It also tracks between different transaction mode i.e. Single, Multi and TwoPC
	commitMode       = stats.NewTimings("CommitModeTimings", "Commit Mode Time", "mode")
	commitUnresolved = stats.NewCounter("CommitUnresolved", "Atomic Commit failed to conclude after commit decision is made")

	exceedMemoryRowsLogger = logutil.NewThrottledLogger("ExceedMemoryRows", 1*time.Minute)

	errorTransform errorTransformer = nullErrorTransformer{}
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
type (
	ExecutorConfig struct {
		Normalize  bool
		StreamSize int
		// AllowScatter will fail planning if set to false and a plan contains any scatter queries
		AllowScatter        bool
		WarmingReadsPercent int
		QueryLogToFile      string
	}

	Executor struct {
		config ExecutorConfig

		env         *vtenv.Environment
		serv        srvtopo.Server
		cell        string
		resolver    *Resolver
		scatterConn *ScatterConn
		txConn      *TxConn

		mu           sync.Mutex
		vschema      *vindexes.VSchema
		vschemaStats *VSchemaStats

		plans *PlanCache
		epoch atomic.Uint32

		vm            *VSchemaManager
		schemaTracker SchemaInfo

		// queryLogger is passed in for logging from this vtgate executor.
		queryLogger *streamlog.StreamLogger[*logstats.LogStats]

		warmingReadsChannel chan bool

		vConfig   econtext.VCursorConfig
		ddlConfig dynamicconfig.DDL
	}
)

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
	eConfig ExecutorConfig,
	warnOnShardedOnly bool,
	plans *PlanCache,
	schemaTracker SchemaInfo,
	pv plancontext.PlannerVersion,
	ddlConfig dynamicconfig.DDL,
) *Executor {
	e := &Executor{
		config:      eConfig,
		env:         env,
		serv:        serv,
		cell:        cell,
		resolver:    resolver,
		scatterConn: resolver.scatterConn,
		txConn:      resolver.scatterConn.txConn,

		schemaTracker:       schemaTracker,
		plans:               plans,
		warmingReadsChannel: make(chan bool, warmingReadsConcurrency),
		ddlConfig:           ddlConfig,
	}
	// setting the vcursor config.
	e.initVConfig(warnOnShardedOnly, pv)

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
			return e.plans.Metrics.Misses()
		})
		servenv.HTTPHandle(pathQueryPlans, e)
		servenv.HTTPHandle(pathScatterStats, e)
		servenv.HTTPHandle(pathVSchema, e)
	})
	return e
}

// Execute executes a non-streaming query.
func (e *Executor) Execute(
	ctx context.Context,
	mysqlCtx vtgateservice.MySQLConnection,
	method string,
	safeSession *econtext.SafeSession,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	prepared bool,
) (result *sqltypes.Result, err error) {
	span, ctx := trace.NewSpan(ctx, "executor.Execute")
	span.Annotate("method", method)
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars, streamlog.GetQueryLogConfig())
	stmtType, result, err := e.execute(ctx, mysqlCtx, safeSession, sql, bindVars, prepared, logStats)
	logStats.Error = err
	if result == nil {
		saveSessionStats(safeSession, stmtType, 0, 0, err)
	} else {
		saveSessionStats(safeSession, stmtType, result.RowsAffected, len(result.Rows), err)
	}
	if result != nil && len(result.Rows) > warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := e.env.Parser().RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		warningMsg := fmt.Sprintf("%q exceeds warning threshold of max memory rows: %v. Actual memory rows: %v", piiSafeSQL, warnMemoryRows, len(result.Rows))
		exceedMemoryRowsLogger.Warningf(warningMsg)
		safeSession.RecordWarning(&querypb.QueryWarning{
			Code:    uint32(sqlerror.EROutOfMemory),
			Message: warningMsg,
		})
	}

	logStats.SaveEndTime()
	e.queryLogger.Send(logStats)

	err = errorTransform.TransformError(err)
	err = vterrors.TruncateError(err, truncateErrorLen)

	return result, err
}

type streaminResultReceiver struct {
	mu           sync.Mutex
	stmtType     sqlparser.StatementType
	rowsAffected uint64
	rowsReturned int
	callback     func(*sqltypes.Result) error
}

func (s *streaminResultReceiver) storeResultStats(typ sqlparser.StatementType, qr *sqltypes.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rowsAffected += qr.RowsAffected
	s.rowsReturned += len(qr.Rows)
	s.stmtType = typ
	return s.callback(qr)
}

// StreamExecute executes a streaming query.
func (e *Executor) StreamExecute(
	ctx context.Context,
	mysqlCtx vtgateservice.MySQLConnection,
	method string,
	safeSession *econtext.SafeSession,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result) error,
) error {
	span, ctx := trace.NewSpan(ctx, "executor.StreamExecute")
	span.Annotate("method", method)
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars, streamlog.GetQueryLogConfig())
	srr := &streaminResultReceiver{callback: callback}
	var err error

	resultHandler := func(ctx context.Context, plan *engine.Plan, vc *econtext.VCursorImpl, bindVars map[string]*querypb.BindVariable, execStart time.Time) error {
		var seenResults atomic.Bool
		var resultMu sync.Mutex
		result := &sqltypes.Result{}
		if canReturnRows(plan.QueryType) {
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

					if byteCount >= e.config.StreamSize {
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
			return srr.storeResultStats(plan.QueryType, qr)
		})

		// Check if there was partial DML execution. If so, rollback the effect of the partially executed query.
		if err != nil {
			if safeSession.InTransaction() && e.rollbackOnFatalTxError(ctx, safeSession, err) {
				return err
			}
			if !canReturnRows(plan.QueryType) {
				return e.rollbackExecIfNeeded(ctx, safeSession, bindVars, logStats, err)
			}
			return err
		}

		if !canReturnRows(plan.QueryType) {
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
		logStats.ActiveKeyspace = vc.GetKeyspace()

		e.updateQueryCounts(plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))
		e.updateQueryStats(plan.QueryType.String(), plan.Type.String(), vc.TabletType().String(), int64(logStats.ShardQueries), plan.TablesUsed)

		return err
	}

	err = e.newExecute(ctx, mysqlCtx, safeSession, sql, bindVars, false, logStats, resultHandler, srr.storeResultStats)

	logStats.Error = err
	saveSessionStats(safeSession, srr.stmtType, srr.rowsAffected, srr.rowsReturned, err)
	if srr.rowsReturned > warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := e.env.Parser().RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		warningMsg := fmt.Sprintf("%q exceeds warning threshold of max memory rows: %v. Actual memory rows: %v", piiSafeSQL, warnMemoryRows, srr.rowsReturned)
		exceedMemoryRowsLogger.Warningf(warningMsg)
		safeSession.RecordWarning(&querypb.QueryWarning{
			Code:    uint32(sqlerror.EROutOfMemory),
			Message: warningMsg,
		})
	}

	logStats.SaveEndTime()
	e.queryLogger.Send(logStats)

	err = errorTransform.TransformError(err)
	err = vterrors.TruncateError(err, truncateErrorLen)

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

func saveSessionStats(safeSession *econtext.SafeSession, stmtType sqlparser.StatementType, rowsAffected uint64, rowsReturned int, err error) {
	safeSession.RowCount = -1
	if err != nil {
		return
	}
	if !safeSession.IsFoundRowsHandled() {
		safeSession.FoundRows = uint64(rowsReturned)
	}
	switch stmtType {
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		safeSession.RowCount = int64(rowsAffected)
	case sqlparser.StmtDDL, sqlparser.StmtSet, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtFlush:
		safeSession.RowCount = 0
	}
}

func (e *Executor) execute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, safeSession *econtext.SafeSession, sql string, bindVars map[string]*querypb.BindVariable, prepared bool, logStats *logstats.LogStats) (sqlparser.StatementType, *sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	var stmtType sqlparser.StatementType
	err = e.newExecute(ctx, mysqlCtx, safeSession, sql, bindVars, prepared, logStats, func(ctx context.Context, plan *engine.Plan, vc *econtext.VCursorImpl, bindVars map[string]*querypb.BindVariable, time time.Time) error {
		stmtType = plan.QueryType
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
func (e *Executor) addNeededBindVars(vcursor *econtext.VCursorImpl, bindVarNeeds *sqlparser.BindVarNeeds, bindVars map[string]*querypb.BindVariable, session *econtext.SafeSession) error {
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
				txMode = transactionMode.Get()
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
					Collation:   vcursor.ConnCollation(),
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
				bindVars[key] = sqltypes.ValueBindVariable(evaluated.Value(vcursor.ConnCollation()))
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

func ifOptionsExist(session *econtext.SafeSession, f func(*querypb.ExecuteOptions)) {
	options := session.GetOptions()
	if options != nil {
		f(options)
	}
}

func ifReadAfterWriteExist(session *econtext.SafeSession, f func(*vtgatepb.ReadAfterWrite)) {
	raw := session.ReadAfterWrite
	if raw != nil {
		f(raw)
	}
}

func (e *Executor) handleBegin(ctx context.Context, vcursor *econtext.VCursorImpl, safeSession *econtext.SafeSession, logStats *logstats.LogStats, stmt sqlparser.Statement) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	e.updateQueryCounts(sqlparser.StmtBegin.String(), "", "", 0)
	e.updateQueryStats(sqlparser.StmtBegin.String(), engine.PlanTransaction.String(), vcursor.TabletType().String(), 0, nil)

	begin := stmt.(*sqlparser.Begin)
	err := e.txConn.Begin(ctx, safeSession, begin.TxAccessModes)
	logStats.ExecuteTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleCommit(ctx context.Context, vcursor *econtext.VCursorImpl, safeSession *econtext.SafeSession, logStats *logstats.LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts(sqlparser.StmtCommit.String(), "", "", int64(logStats.ShardQueries))
	e.updateQueryStats(sqlparser.StmtCommit.String(), engine.PlanTransaction.String(), vcursor.TabletType().String(), int64(logStats.ShardQueries), nil)

	err := e.txConn.Commit(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

// Commit commits the existing transactions
func (e *Executor) Commit(ctx context.Context, safeSession *econtext.SafeSession) error {
	return e.txConn.Commit(ctx, safeSession)
}

func (e *Executor) handleRollback(ctx context.Context, vcursor *econtext.VCursorImpl, safeSession *econtext.SafeSession, logStats *logstats.LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts(sqlparser.StmtRollback.String(), "", "", int64(logStats.ShardQueries))
	e.updateQueryStats(sqlparser.StmtRollback.String(), engine.PlanTransaction.String(), vcursor.TabletType().String(), int64(logStats.ShardQueries), nil)

	err := e.txConn.Rollback(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleSavepoint(ctx context.Context, vcursor *econtext.VCursorImpl, safeSession *econtext.SafeSession, sql string, queryType string, logStats *logstats.LogStats, nonTxResponse func(query string) (*sqltypes.Result, error), ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts(queryType, "", "", int64(logStats.ShardQueries))
	e.updateQueryStats(queryType, engine.PlanTransaction.String(), vcursor.TabletType().String(), int64(logStats.ShardQueries), nil)

	defer func() {
		logStats.ExecuteTime = time.Since(execStart)
	}()

	// If no transaction exists on any of the shard sessions,
	// then savepoint does not need to be executed, it will be only stored in the session
	// and later will be executed when a transaction is started.
	if !safeSession.IsTxOpen() {
		if safeSession.InTransaction() {
			// Storing, as this needs to be executed just after starting transaction on the shard.
			safeSession.StoreSavepoint(sql)
			return &sqltypes.Result{}, nil
		}
		return nonTxResponse(sql)
	}
	orig := safeSession.GetCommitOrder()
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
func (e *Executor) executeSPInAllSessions(ctx context.Context, safeSession *econtext.SafeSession, sql string, ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	var errs []error
	for _, co := range []vtgatepb.CommitOrder{vtgatepb.CommitOrder_PRE, vtgatepb.CommitOrder_NORMAL, vtgatepb.CommitOrder_POST} {
		safeSession.SetCommitOrder(co)

		var rss []*srvtopo.ResolvedShard
		var queries []*querypb.BoundQuery
		for _, shardSession := range safeSession.GetSessions() {
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
		qr, errs = e.ExecuteMultiShard(ctx, nil, rss, queries, safeSession, false /*autocommit*/, ignoreMaxMemoryRows, nullResultsObserver{}, false)
		err := vterrors.Aggregate(errs)
		if err != nil {
			return nil, err
		}
	}
	return qr, nil
}

// handleKill executed the kill statement.
func (e *Executor) handleKill(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, vcursor *econtext.VCursorImpl, stmt sqlparser.Statement, logStats *logstats.LogStats) (result *sqltypes.Result, err error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	e.updateQueryCounts("Kill", "", "", 0)
	e.updateQueryStats("Kill", engine.PlanLocal.String(), vcursor.TabletType().String(), 0, nil)

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
func (e *Executor) CloseSession(ctx context.Context, safeSession *econtext.SafeSession) error {
	return e.txConn.ReleaseAll(ctx, safeSession)
}

func (e *Executor) SetVitessMetadata(ctx context.Context, name, value string) error {
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

func (e *Executor) ShowVitessMetadata(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
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

func (e *Executor) ShowShards(ctx context.Context, filter *sqlparser.ShowFilter, destTabletType topodatapb.TabletType) (*sqltypes.Result, error) {
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

func (e *Executor) ShowTablets(filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
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

func (e *Executor) ShowVitessReplicationStatus(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
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
			replicaQueries, _ := capabilities.MySQLVersionHasCapability(e.env.MySQLVersion(), capabilities.ReplicaTerminologyCapability)
			sql := "show replica status"
			sourceHostField := "Source_Host"
			sourcePortField := "Source_Port"
			replicaIORunningField := "Replica_IO_Running"
			replicaSQLRunningField := "Replica_SQL_Running"
			secondsBehindSourceField := "Seconds_Behind_Source"
			if !replicaQueries {
				sql = "show slave status"
				sourceHostField = "Master_Host"
				sourcePortField = "Master_Port"
				replicaIORunningField = "Slave_IO_Running"
				replicaSQLRunningField = "Slave_SQL_Running"
				secondsBehindSourceField = "Seconds_Behind_Master"
			}
			results, err := e.txConn.tabletGateway.Execute(ctx, ts.Target, sql, nil, 0, 0, nil)
			if err != nil || results == nil {
				log.Warningf("Could not get replication status from %s: %v", tabletHostPort, err)
			} else if row := results.Named().Row(); row != nil {
				replSourceHost = row[sourceHostField].ToString()
				replSourcePort, _ = row[sourcePortField].ToInt64()
				replIOThreadHealth = row[replicaIORunningField].ToString()
				replSQLThreadHealth = row[replicaSQLRunningField].ToString()
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
					if row[secondsBehindSourceField].IsNull() {
						replLag = strings.ToUpper(sqltypes.NullStr) // Uppercase to match mysqld's output in SHOW REPLICA STATUS
					} else {
						replLag = row[secondsBehindSourceField].ToString()
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
func (e *Executor) ParseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.ShardDestination, error) {
	return econtext.ParseDestinationTarget(targetString, defaultTabletType, e.VSchema())
}

func (e *Executor) fetchOrCreatePlan(
	ctx context.Context,
	safeSession *econtext.SafeSession,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	parameterize bool,
	preparedPlan bool,
	logStats *logstats.LogStats,
) (
	plan *engine.Plan, vcursor *econtext.VCursorImpl, stmt sqlparser.Statement, err error) {
	if e.VSchema() == nil {
		return nil, nil, nil, vterrors.VT13001("vschema not initialized")
	}

	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, _ = econtext.NewVCursorImpl(safeSession, comments, e, logStats, e.vm, e.VSchema(), e.resolver.resolver, e.serv, nullResultsObserver{}, e.vConfig)

	var setVarComment string
	if e.vConfig.SetVarEnabled {
		setVarComment = vcursor.PrepareSetVarComment()
	}

	var planKey engine.PlanKey
	if preparedPlan {
		planKey = buildPlanKey(ctx, vcursor, query, setVarComment)
		plan, logStats.CachedPlan = e.plans.Get(planKey.Hash(), e.epoch.Load())
	}

	if plan == nil {
		plan, logStats.CachedPlan, stmt, err = e.getCachedOrBuildPlan(ctx, vcursor, query, bindVars, setVarComment, parameterize, planKey)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	qh := plan.QueryHints
	vcursor.SetIgnoreMaxMemoryRows(qh.IgnoreMaxMemoryRows)
	vcursor.SetConsolidator(qh.Consolidator)
	vcursor.SetWorkloadName(qh.Workload)
	vcursor.SetPriority(qh.Priority)
	vcursor.SetExecQueryTimeout(qh.Timeout)

	logStats.SQL = comments.Leading + plan.Original + comments.Trailing
	logStats.BindVariables = sqltypes.CopyBindVariables(bindVars)

	return plan, vcursor, stmt, nil
}

func (e *Executor) getCachedOrBuildPlan(
	ctx context.Context,
	vcursor *econtext.VCursorImpl,
	query string,
	bindVars map[string]*querypb.BindVariable,
	setVarComment string,
	parameterize bool,
	planKey engine.PlanKey,
) (plan *engine.Plan, cached bool, stmt sqlparser.Statement, err error) {
	stmt, reservedVars, err := parseAndValidateQuery(query, e.env.Parser())
	if err != nil {
		return nil, false, nil, err
	}

	defer func() {
		if err == nil {
			vcursor.CheckForReservedConnection(setVarComment, stmt)
		}
	}()

	qh, err := sqlparser.BuildQueryHints(stmt)
	if err != nil {
		return nil, false, nil, err
	}

	if qh.ForeignKeyChecks == nil {
		qh.ForeignKeyChecks = vcursor.SafeSession.ForeignKeyChecks()
	}
	vcursor.SetForeignKeyCheckState(qh.ForeignKeyChecks)

	paramsCount := uint16(0)
	preparedPlan := planKey.Query != ""
	if preparedPlan {
		// We need to count the number of arguments in the statement before we plan the query.
		// Planning could add additional arguments to the statement.
		paramsCount = countArguments(stmt)
		if bindVars == nil {
			bindVars = make(map[string]*querypb.BindVariable)
		}
	}

	rewriteASTResult, err := sqlparser.Normalize(
		stmt,
		reservedVars,
		bindVars,
		parameterize,
		vcursor.GetKeyspace(),
		vcursor.SafeSession.GetSelectLimit(),
		setVarComment,
		vcursor.GetSystemVariablesCopy(),
		qh.ForeignKeyChecks,
		vcursor,
	)
	if err != nil {
		return nil, false, nil, err
	}
	stmt = rewriteASTResult.AST
	bindVarNeeds := rewriteASTResult.BindVarNeeds
	if rewriteASTResult.UpdateQueryFromAST && !preparedPlan {
		query = sqlparser.String(stmt)
	}

	planCachable := sqlparser.CachePlan(stmt) && vcursor.CachePlan()
	if planCachable {
		if !preparedPlan {
			// build Plan key
			planKey = buildPlanKey(ctx, vcursor, query, setVarComment)
		}
		plan, cached, err = e.plans.GetOrLoad(planKey.Hash(), e.epoch.Load(), func() (*engine.Plan, error) {
			return e.buildStatement(ctx, vcursor, query, stmt, reservedVars, bindVarNeeds, qh, paramsCount)
		})
		return plan, cached, stmt, err
	}
	plan, err = e.buildStatement(ctx, vcursor, query, stmt, reservedVars, bindVarNeeds, qh, paramsCount)
	return plan, false, stmt, err
}

func buildPlanKey(ctx context.Context, vcursor *econtext.VCursorImpl, query string, setVarComment string) engine.PlanKey {
	allDest := getDestinations(ctx, vcursor)

	return engine.PlanKey{
		CurrentKeyspace: vcursor.GetKeyspace(),
		Destination:     strings.Join(allDest, ","),
		Query:           query,
		SetVarComment:   setVarComment,
		Collation:       vcursor.ConnCollation(),
	}
}

func getDestinations(ctx context.Context, vcursor *econtext.VCursorImpl) []string {
	currDest := vcursor.ShardDestination()
	if currDest == nil {
		return nil
	}

	switch currDest.(type) {
	case key.DestinationKeyspaceID, key.DestinationKeyspaceIDs:
		// these need to be resolved to shards
	default:
		return []string{currDest.String()}
	}

	resolved, _, err := vcursor.ResolveDestinations(ctx, vcursor.GetKeyspace(), nil, []key.ShardDestination{currDest})
	if err != nil || len(resolved) <= 0 {
		return nil
	}

	shards := make([]string, len(resolved))
	for i := 0; i < len(shards); i++ {
		shards[i] = resolved[i].Target.GetShard()
	}
	sort.Strings(shards)

	return shards
}

func (e *Executor) buildStatement(
	ctx context.Context,
	vcursor *econtext.VCursorImpl,
	query string,
	stmt sqlparser.Statement,
	reservedVars *sqlparser.ReservedVars,
	bindVarNeeds *sqlparser.BindVarNeeds,
	qh sqlparser.QueryHints,
	paramsCount uint16,
) (*engine.Plan, error) {
	plan, err := planbuilder.BuildFromStmt(ctx, query, stmt, reservedVars, vcursor, bindVarNeeds, e.ddlConfig)
	if err != nil {
		return nil, err
	}

	plan.ParamsCount = paramsCount
	plan.Warnings = vcursor.GetAndEmptyWarnings()
	plan.QueryHints = qh

	err = e.checkThatPlanIsValid(stmt, plan)
	return plan, err
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

func (e *Executor) updateQueryStats(queryType, planType, tabletType string, shards int64, tables []string) {
	queryExecutions.Add([]string{queryType, planType, tabletType}, 1)
	queryRoutes.Add([]string{queryType, planType, tabletType}, shards)
	for _, table := range tables {
		queryExecutionsByTable.Add([]string{queryType, table}, 1)
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
func (e *Executor) Prepare(ctx context.Context, method string, safeSession *econtext.SafeSession, sql string) (fld []*querypb.Field, paramsCount uint16, err error) {
	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), nil, streamlog.GetQueryLogConfig())
	fld, paramsCount, err = e.prepare(ctx, safeSession, sql, logStats)
	logStats.Error = err

	// The mysql plugin runs an implicit rollback whenever a connection closes.
	// To avoid spamming the log with no-op rollback records, ignore it if
	// it was a no-op record (i.e. didn't issue any queries)
	if !(logStats.StmtType == "ROLLBACK" && logStats.ShardQueries == 0) {
		logStats.SaveEndTime()
		e.queryLogger.Send(logStats)
	}

	err = errorTransform.TransformError(err)
	err = vterrors.TruncateError(err, truncateErrorLen)

	return fld, paramsCount, err
}

func (e *Executor) prepare(ctx context.Context, safeSession *econtext.SafeSession, sql string, logStats *logstats.LogStats) ([]*querypb.Field, uint16, error) {
	// Start an implicit transaction if necessary.
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession, nil); err != nil {
			return nil, 0, err
		}
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
	case sqlparser.StmtSelect, sqlparser.StmtShow,
		sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		return e.handlePrepare(ctx, safeSession, sql, logStats)
	case sqlparser.StmtDDL, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtSet,
		sqlparser.StmtUse, sqlparser.StmtOther, sqlparser.StmtAnalyze, sqlparser.StmtComment, sqlparser.StmtExplain, sqlparser.StmtFlush, sqlparser.StmtKill:
		return nil, 0, nil
	}
	return nil, 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unrecognized prepare statement: %s", sql)
}

func (e *Executor) initVConfig(warnOnShardedOnly bool, pv plancontext.PlannerVersion) {
	connCollation := collations.Unknown
	if gw, isTabletGw := e.resolver.resolver.GetGateway().(*TabletGateway); isTabletGw {
		connCollation = gw.DefaultConnCollation()
	}
	if connCollation == collations.Unknown {
		connCollation = e.env.CollationEnv().DefaultConnectionCharset()
	}

	e.vConfig = econtext.VCursorConfig{
		Collation:         connCollation,
		DefaultTabletType: defaultTabletType,
		PlannerVersion:    pv,

		QueryTimeout:  queryTimeout,
		MaxMemoryRows: maxMemoryRows,

		SetVarEnabled:      sysVarSetEnabled,
		EnableViews:        enableViews,
		ForeignKeyMode:     fkMode(foreignKeyMode),
		EnableShardRouting: enableShardRouting,
		WarnShardedOnly:    warnOnShardedOnly,

		DBDDLPlugin: dbDDLPlugin,

		WarmingReadsPercent: e.config.WarmingReadsPercent,
		WarmingReadsTimeout: warmingReadsQueryTimeout,
		WarmingReadsChannel: e.warmingReadsChannel,
	}
}

func countArguments(statement sqlparser.Statement) (paramsCount uint16) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Argument:
			if strings.HasPrefix(node.Name, "v") {
				paramsCount++
			}
		}
		return true, nil
	}, statement)
	return
}

func prepareBindVars(paramsCount uint16) map[string]*querypb.BindVariable {
	bindVars := make(map[string]*querypb.BindVariable, paramsCount)
	for i := range paramsCount {
		parameterID := fmt.Sprintf("v%d", i+1)
		bindVars[parameterID] = &querypb.BindVariable{}
	}
	return bindVars
}

func (e *Executor) handlePrepare(ctx context.Context, safeSession *econtext.SafeSession, sql string, logStats *logstats.LogStats) ([]*querypb.Field, uint16, error) {
	plan, vcursor, _, err := e.fetchOrCreatePlan(ctx, safeSession, sql, nil, false, true, logStats)
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	if err != nil {
		logStats.Error = err
		return nil, 0, err
	}

	bindVars := prepareBindVars(plan.ParamsCount)
	err = e.addNeededBindVars(vcursor, plan.BindVarNeeds, bindVars, safeSession)
	if err != nil {
		logStats.Error = err
		return nil, 0, err
	}

	qr, err := plan.Instructions.GetFields(ctx, vcursor, bindVars)
	logStats.ExecuteTime = time.Since(execStart)
	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1 // nolint
		return nil, 0, err
	}
	logStats.RowsAffected = qr.RowsAffected

	plan.AddStats(1, time.Since(logStats.StartTime), logStats.ShardQueries, qr.RowsAffected, uint64(len(qr.Rows)), errCount)

	return qr.Fields, plan.ParamsCount, err
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
func (e *Executor) ExecuteMultiShard(ctx context.Context, primitive engine.Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *econtext.SafeSession, autocommit bool, ignoreMaxMemoryRows bool, resultsObserver econtext.ResultsObserver, fetchLastInsertID bool) (qr *sqltypes.Result, errs []error) {
	return e.scatterConn.ExecuteMultiShard(ctx, primitive, rss, queries, session, autocommit, ignoreMaxMemoryRows, resultsObserver, fetchLastInsertID)
}

// StreamExecuteMulti implements the IExecutor interface
func (e *Executor) StreamExecuteMulti(ctx context.Context, primitive engine.Primitive, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, session *econtext.SafeSession, autocommit bool, callback func(reply *sqltypes.Result) error, resultsObserver econtext.ResultsObserver, fetchLastInsertID bool) []error {
	return e.scatterConn.StreamExecuteMulti(ctx, primitive, query, rss, vars, session, autocommit, callback, resultsObserver, fetchLastInsertID)
}

// ExecuteLock implements the IExecutor interface
func (e *Executor) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *econtext.SafeSession, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
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
	if e.config.AllowScatter || plan.Instructions == nil || sqlparser.AllowScatterDirective(stmt) {
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
func (e *Executor) ReleaseLock(ctx context.Context, session *econtext.SafeSession) error {
	return e.txConn.ReleaseLock(ctx, session)
}

// PlanPrepareStmt implements the IExecutor interface
func (e *Executor) PlanPrepareStmt(ctx context.Context, safeSession *econtext.SafeSession, query string) (*engine.Plan, error) {
	// creating this log stats to not interfere with the original log stats.
	lStats := logstats.NewLogStats(ctx, "prepare", query, safeSession.GetSessionUUID(), nil, streamlog.GetQueryLogConfig())
	plan, _, _, err := e.fetchOrCreatePlan(ctx, safeSession, query, nil, false, true, lStats)
	return plan, err
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

func (e *Executor) Environment() *vtenv.Environment {
	return e.env
}

func (e *Executor) ReadTransaction(ctx context.Context, transactionID string) (*querypb.TransactionMetadata, error) {
	return e.txConn.ReadTransaction(ctx, transactionID)
}

func (e *Executor) UnresolvedTransactions(ctx context.Context, targets []*querypb.Target) ([]*querypb.TransactionMetadata, error) {
	return e.txConn.UnresolvedTransactions(ctx, targets)
}

func (e *Executor) AddWarningCount(name string, count int64) {
	warnings.Add(name, count)
}

type (
	errorTransformer interface {
		TransformError(err error) error
	}
	nullErrorTransformer struct{}
)

func (nullErrorTransformer) TransformError(err error) error {
	return err
}

func fkMode(foreignkey string) vschemapb.Keyspace_ForeignKeyMode {
	switch foreignkey {
	case "disallow":
		return vschemapb.Keyspace_disallow
	case "managed":
		return vschemapb.Keyspace_managed
	case "unmanaged":
		return vschemapb.Keyspace_unmanaged

	}
	return vschemapb.Keyspace_unspecified
}
