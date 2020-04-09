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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	errNoKeyspace     = vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no keyspace in database name specified. Supported database name format (items in <> are optional): keyspace<:shard><@type> or keyspace<[range]><@type>")
	defaultTabletType topodatapb.TabletType

	// TODO: @rafael - These two counters should be deprecated in favor of the ByTable ones. They are kept for now for backwards compatibility.
	queriesProcessed = stats.NewCountersWithSingleLabel("QueriesProcessed", "Queries processed at vtgate by plan type", "Plan")
	queriesRouted    = stats.NewCountersWithSingleLabel("QueriesRouted", "Queries routed from vtgate to vttablet by plan type", "Plan")

	queriesProcessedByTable = stats.NewCountersWithMultiLabels("QueriesProcessedByTable", "Queries processed at vtgate by operation, plan type, keyspace and table", []string{"Operation", "Plan", "Keyspace", "Table"})
	queriesRoutedByTable    = stats.NewCountersWithMultiLabels("QueriesRoutedByTable", "Queries routed from vtgate to vttablet by operation, plan type, keyspace and table", []string{"Operation", "Plan", "Keyspace", "Table"})
)

const (
	utf8    = "utf8"
	utf8mb4 = "utf8mb4"
	both    = "both"
	charset = "charset"
)

func init() {
	topoproto.TabletTypeVar(&defaultTabletType, "default_tablet_type", topodatapb.TabletType_MASTER, "The default tablet type to set for queries, when one is not explicitly selected")
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
	normalize    bool
	streamSize   int
	plans        *cache.LRUCache
	vschemaStats *VSchemaStats

	// this is a way for us to be able to write tests with one method,
	// and run in production with an entierly different one
	exec executeMethod

	vm *VSchemaManager
}

// this interface is here to allow us to have two different implementations of the same method,
// and use the legacy one in production until we are comfortable with the new code.
// it's temporary and should be removed once we can do everything using the new planning strategy
type executeMethod interface {
	execute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error)
}

var executorOnce sync.Once

const pathQueryPlans = "/debug/query_plans"
const pathScatterStats = "/debug/scatter_stats"
const pathVSchema = "/debug/vschema"

// NewExecutor creates a new Executor.
func NewExecutor(ctx context.Context, serv srvtopo.Server, cell string, resolver *Resolver, normalize bool, streamSize int, queryPlanCacheSize int64) *Executor {
	e := &Executor{
		serv:        serv,
		cell:        cell,
		resolver:    resolver,
		scatterConn: resolver.scatterConn,
		txConn:      resolver.scatterConn.txConn,
		plans:       cache.NewLRUCache(queryPlanCacheSize),
		normalize:   normalize,
		streamSize:  streamSize,
	}
	e.exec = &fallbackExecutor{
		exA: &planExecute{e: e},
		exB: e,
	}

	vschemaacl.Init()
	e.vm = &VSchemaManager{e: e}
	e.vm.watchSrvVSchema(ctx, cell)

	executorOnce.Do(func() {
		stats.NewGaugeFunc("QueryPlanCacheLength", "Query plan cache length", e.plans.Length)
		stats.NewGaugeFunc("QueryPlanCacheSize", "Query plan cache size", e.plans.Size)
		stats.NewGaugeFunc("QueryPlanCacheCapacity", "Query plan cache capacity", e.plans.Capacity)
		stats.NewCounterFunc("QueryPlanCacheEvictions", "Query plan cache evictions", e.plans.Evictions)
		stats.Publish("QueryPlanCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", e.plans.Oldest())
		}))
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
	trace.AnnotateSQL(span, sql)
	defer span.Finish()

	logStats := NewLogStats(ctx, method, sql, bindVars)
	result, err = e.exec.execute(ctx, safeSession, sql, bindVars, logStats)
	if err == nil {
		safeSession.FoundRows = result.RowsAffected
	}
	logStats.Error = err
	if result != nil && len(result.Rows) > *warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
	}

	// The mysql plugin runs an implicit rollback whenever a connection closes.
	// To avoid spamming the log with no-op rollback records, ignore it if
	// it was a no-op record (i.e. didn't issue any queries)
	if !(logStats.StmtType == "ROLLBACK" && logStats.ShardQueries == 0) {
		logStats.Send()
	}
	return result, err
}

func (e *Executor) execute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error) {
	//Start an implicit transaction if necessary.
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession); err != nil {
			return nil, err
		}
	}

	destKeyspace, destTabletType, dest, err := e.ParseDestinationTarget(safeSession.TargetString)
	if err != nil {
		return nil, err
	}

	if safeSession.InTransaction() && destTabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "transactions are supported only for master tablet types, current type: %v", destTabletType)
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
		return e.handleExec(ctx, safeSession, sql, bindVars, logStats, stmtType)
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		safeSession := safeSession

		mustCommit := false
		if safeSession.Autocommit && !safeSession.InTransaction() {
			mustCommit = true
			if err := e.txConn.Begin(ctx, safeSession); err != nil {
				return nil, err
			}
			// The defer acts as a failsafe. If commit was successful,
			// the rollback will be a no-op.
			defer e.txConn.Rollback(ctx, safeSession)
		}

		// The SetAutocommitable flag should be same as mustCommit.
		// If we started a transaction because of autocommit, then mustCommit
		// will be true, which means that we can autocommit. If we were already
		// in a transaction, it means that the app started it, or we are being
		// called recursively. If so, we cannot autocommit because whatever we
		// do is likely not final.
		// The control flow is such that autocommitable can only be turned on
		// at the beginning, but never after.
		safeSession.SetAutocommittable(mustCommit)

		qr, err := e.handleExec(ctx, safeSession, sql, bindVars, logStats, stmtType)
		if err != nil {
			return nil, err
		}

		if mustCommit {
			commitStart := time.Now()
			if err = e.txConn.Commit(ctx, safeSession); err != nil {
				return nil, err
			}
			logStats.CommitTime = time.Since(commitStart)
		}
		return qr, nil
	case sqlparser.StmtDDL:
		return e.handleDDL(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
	case sqlparser.StmtSet:
		return e.handleSet(ctx, safeSession, sql, logStats)
	case sqlparser.StmtShow:
		return e.handleShow(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
	case sqlparser.StmtUse:
		return e.handleUse(safeSession, sql)
	case sqlparser.StmtOther:
		return e.handleOther(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
	case sqlparser.StmtComment:
		return e.handleComment(sql)
	case sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback:
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "should be handled by plan_execute")
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unrecognized statement: %s", sql)
}

func (e *Executor) handleExec(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats, stmtType sqlparser.StatementType) (*sqltypes.Result, error) {
	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, _ := newVCursorImpl(ctx, safeSession, comments, e, logStats, e.vm, e.resolver.resolver)
	plan, err := e.getPlan(
		vcursor,
		query,
		comments,
		bindVars,
		skipQueryPlanCache(safeSession),
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

	qr, err := plan.Instructions.Execute(vcursor, bindVars, true)
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts("Execute", plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1
	} else {
		logStats.RowsAffected = qr.RowsAffected
		if qr != nil && stmtType == sqlparser.StmtInsert {
			safeSession.LastInsertId = qr.InsertID
		}
	}

	// Check if there was partial DML execution. If so, rollback the transaction.
	if err != nil && safeSession.InTransaction() && vcursor.rollbackOnPartialExec {
		_ = e.txConn.Rollback(ctx, safeSession)
		err = vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction rolled back due to partial DML execution: %v", err)
	}

	plan.AddStats(1, time.Since(logStats.StartTime), uint64(logStats.ShardQueries), logStats.RowsAffected, errCount)

	return qr, err
}

// addNeededBindVars adds bind vars that are needed by the plan
func (e *Executor) addNeededBindVars(bindVarNeeds sqlparser.BindVarNeeds, bindVars map[string]*querypb.BindVariable, session *SafeSession) error {
	if bindVarNeeds.NeedDatabase {
		keyspace, _, _, _ := e.ParseDestinationTarget(session.TargetString)
		if keyspace == "" {
			bindVars[sqlparser.DBVarName] = sqltypes.NullBindVariable
		} else {
			bindVars[sqlparser.DBVarName] = sqltypes.StringBindVariable(keyspace)
		}
	}

	if bindVarNeeds.NeedLastInsertID {
		bindVars[sqlparser.LastInsertIDName] = sqltypes.Uint64BindVariable(session.GetLastInsertId())
	}

	// todo: do we need to check this map for nil?
	if bindVarNeeds.NeedUserDefinedVariables && session.UserDefinedVariables != nil {
		for k, v := range session.UserDefinedVariables {
			bindVars[sqlparser.UserDefinedVariableName+k] = v
		}
	}

	if bindVarNeeds.NeedFoundRows {
		bindVars[sqlparser.FoundRowsName] = sqltypes.Uint64BindVariable(session.FoundRows)
	}

	return nil
}

func (e *Executor) destinationExec(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	return e.resolver.Execute(ctx, sql, bindVars, destKeyspace, destTabletType, dest, safeSession, false /* notInTransaction */, safeSession.Options, logStats, false /* canAutocommit */)
}

func (e *Executor) handleDDL(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	// Parse the statement to handle vindex operations
	// If the statement failed to be properly parsed, fall through anyway
	// to broadcast the ddl to all shards.
	query, _ := sqlparser.SplitMarginComments(sql)
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	ddl, ok := stmt.(*sqlparser.DDL)
	if ok {
		execStart := time.Now()
		logStats.PlanTime = execStart.Sub(logStats.StartTime)
		switch ddl.Action {
		case sqlparser.CreateVindexStr,
			sqlparser.AddVschemaTableStr,
			sqlparser.DropVschemaTableStr,
			sqlparser.AddColVindexStr,
			sqlparser.DropColVindexStr,
			sqlparser.AddSequenceStr,
			sqlparser.AddAutoIncStr:

			err := e.handleVSchemaDDL(ctx, destKeyspace, ddl)
			logStats.ExecuteTime = time.Since(execStart)
			return &sqltypes.Result{}, err
		default:
			// fallthrough to broadcast the ddl to all shards
		}
	}

	if destKeyspace == "" {
		return nil, errNoKeyspace
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	result, err := e.destinationExec(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts("Execute", "DDL", "", "", int64(logStats.ShardQueries))

	return result, err
}

func (e *Executor) handleVSchemaDDL(ctx context.Context, destKeyspace string, ddl *sqlparser.DDL) error {
	vschema := e.vm.GetCurrentSrvVschema()
	if vschema == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
	}

	allowed := vschemaacl.Authorized(callerid.ImmediateCallerIDFromContext(ctx))
	if !allowed {
		return vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "not authorized to perform vschema operations")

	}

	// Resolve the keyspace either from the table qualifier or the target keyspace
	var ksName string
	if !ddl.Table.IsEmpty() {
		ksName = ddl.Table.Qualifier.String()
	}
	if ksName == "" {
		ksName = destKeyspace
	}
	if ksName == "" {
		return errNoKeyspace
	}

	ks := vschema.Keyspaces[ksName]
	ks, err := topotools.ApplyVSchemaDDL(ksName, ks, ddl)

	if err != nil {
		return err
	}

	vschema.Keyspaces[ksName] = ks

	return e.vm.UpdateVSchema(ctx, ksName, vschema)
}

func (e *Executor) handleBegin(ctx context.Context, safeSession *SafeSession, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	if destTabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "transactions are supported only for master tablet types, current type: %v", destTabletType)
	}
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	err := e.txConn.Begin(ctx, safeSession)
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts("Execute", "Begin", "", "", 0)

	return &sqltypes.Result{}, err
}

func (e *Executor) handleCommit(ctx context.Context, safeSession *SafeSession, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint32(len(safeSession.ShardSessions))
	e.updateQueryCounts("Execute", "Commit", "", "", int64(logStats.ShardQueries))

	err := e.txConn.Commit(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleRollback(ctx context.Context, safeSession *SafeSession, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint32(len(safeSession.ShardSessions))
	e.updateQueryCounts("Execute", "Rollback", "", "", int64(logStats.ShardQueries))
	err := e.txConn.Rollback(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleSet(ctx context.Context, safeSession *SafeSession, sql string, logStats *LogStats) (*sqltypes.Result, error) {
	vals, scope, err := sqlparser.ExtractSetValues(sql)
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	defer func() {
		logStats.ExecuteTime = time.Since(execStart)
	}()

	if err != nil {
		return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}

	if scope == sqlparser.GlobalStr {
		return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
	}

	for k, v := range vals {
		switch k.Scope {
		case sqlparser.GlobalStr:
			return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
		case sqlparser.VitessMetadataStr:
			return e.handleSetVitessMetadata(ctx, k, v)
		case sqlparser.VariableStr:
			err := handleSetUserDefinedVariables(safeSession, k, v)
			if err != nil {
				return nil, err
			}
		case sqlparser.ImplicitStr:
			switch k.Key {
			case "autocommit":
				val, err := validateSetOnOff(v, k.Key)
				if err != nil {
					return nil, err
				}

				switch val {
				case 0:
					safeSession.Autocommit = false
				case 1:
					if safeSession.InTransaction() {
						if err := e.txConn.Commit(ctx, safeSession); err != nil {
							return nil, err
						}
					}
					safeSession.Autocommit = true
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for autocommit: %d", val)
				}
			case "client_found_rows":
				val, ok := v.(int64)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for client_found_rows: %T", v)
				}
				if safeSession.Options == nil {
					safeSession.Options = &querypb.ExecuteOptions{}
				}
				switch val {
				case 0:
					safeSession.Options.ClientFoundRows = false
				case 1:
					safeSession.Options.ClientFoundRows = true
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for client_found_rows: %d", val)
				}
			case "skip_query_plan_cache":
				val, ok := v.(int64)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for skip_query_plan_cache: %T", v)
				}
				if safeSession.Options == nil {
					safeSession.Options = &querypb.ExecuteOptions{}
				}
				switch val {
				case 0:
					safeSession.Options.SkipQueryPlanCache = false
				case 1:
					safeSession.Options.SkipQueryPlanCache = true
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for skip_query_plan_cache: %d", val)
				}
			case "sql_safe_updates":
				val, err := validateSetOnOff(v, k.Key)
				if err != nil {
					return nil, err
				}

				switch val {
				case 0, 1:
					// no op
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for sql_safe_updates: %d", val)
				}
			case "transaction_mode":
				val, ok := v.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for transaction_mode: %T", v)
				}
				out, ok := vtgatepb.TransactionMode_value[strings.ToUpper(val)]
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid transaction_mode: %s", val)
				}
				safeSession.TransactionMode = vtgatepb.TransactionMode(out)
			case sqlparser.TransactionStr:
				// Parser ensures it's well-formed.

				// TODO: This is a NOP, modeled off of tx_isolation and tx_read_only.  It's incredibly
				// dangerous that it's a NOP, but fixing that is left to. Note that vtqueryservice needs
				// to be updated as well:
				// https://github.com/vitessio/vitess/issues/4127
			case "tx_isolation":
				val, ok := v.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for tx_isolation: %T", v)
				}
				switch val {
				case "repeatable read", "read committed", "read uncommitted", "serializable":
					// TODO (4127): This is a dangerous NOP.
				default:
					return nil, fmt.Errorf("unexpected value for tx_isolation: %v", val)
				}
			case "tx_read_only", "transaction_read_only":
				val, err := validateSetOnOff(v, k.Key)
				if err != nil {
					return nil, err
				}
				switch val {
				case 0, 1:
					// TODO (4127): This is a dangerous NOP.
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for %v: %d", k.Key, val)
				}
			case "workload":
				val, ok := v.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for workload: %T", v)
				}
				out, ok := querypb.ExecuteOptions_Workload_value[strings.ToUpper(val)]
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid workload: %s", val)
				}
				if safeSession.Options == nil {
					safeSession.Options = &querypb.ExecuteOptions{}
				}
				safeSession.Options.Workload = querypb.ExecuteOptions_Workload(out)
			case "sql_select_limit":
				var val int64

				switch cast := v.(type) {
				case int64:
					val = cast
				case string:
					if !strings.EqualFold(cast, "default") {
						return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected string value for sql_select_limit: %v", v)
					}
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for sql_select_limit: %T", v)
				}

				if safeSession.Options == nil {
					safeSession.Options = &querypb.ExecuteOptions{}
				}
				safeSession.Options.SqlSelectLimit = val
			case "sql_auto_is_null":
				val, ok := v.(int64)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for sql_auto_is_null: %T", v)
				}
				switch val {
				case 0:
					// This is the default setting for MySQL. Do nothing.
				case 1:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "sql_auto_is_null is not currently supported")
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for sql_auto_is_null: %d", val)
				}
			case "character_set_results":
				// This is a statement that mysql-connector-j sends at the beginning. We return a canned response for it.
				switch v {
				case nil, "utf8", "utf8mb4", "latin1":
				default:
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed value for character_set_results: %v", v)
				}
			case "wait_timeout":
				_, ok := v.(int64)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for wait_timeout: %T", v)
				}
			case "sql_mode", "net_write_timeout", "net_read_timeout", "lc_messages", "collation_connection", "foreign_key_checks", "sql_quote_show_create", "unique_checks":
				log.Warningf("Ignored inapplicable SET %v = %v", k, v)
				warnings.Add("IgnoredSet", 1)
			case "charset", "names":
				val, ok := v.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for charset/names: %T", v)
				}
				switch val {
				case "", "utf8", "utf8mb4", "latin1", "default":
					break
				default:
					return nil, fmt.Errorf("unexpected value for charset/names: %v", val)
				}
			default:
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported construct: %s", sql)
			}

		}

	}
	return &sqltypes.Result{}, nil
}

func handleSetUserDefinedVariables(session *SafeSession, k sqlparser.SetKey, v interface{}) error {
	variable, err := sqltypes.BuildBindVariable(v)
	if err != nil {
		return err
	}
	session.SetUserDefinedVariable(k.Key, variable)
	return nil
}

func (e *Executor) handleSetVitessMetadata(ctx context.Context, k sqlparser.SetKey, v interface{}) (*sqltypes.Result, error) {
	//TODO(kalfonso): move to its own acl check and consolidate into an acl component that can handle multiple operations (vschema, metadata)
	allowed := vschemaacl.Authorized(callerid.ImmediateCallerIDFromContext(ctx))
	if !allowed {
		return nil, vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "not authorized to perform vitess metadata operations")

	}

	val, ok := v.(string)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for charset: %T", v)
	}

	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return nil, err
	}

	if val == "" {
		err = ts.DeleteMetadata(ctx, k.Key)
	} else {
		err = ts.UpsertMetadata(ctx, k.Key, val)
	}

	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{RowsAffected: 1}, nil
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
		Fields:       buildVarCharFields("Key", "Value"),
		Rows:         rows,
		RowsAffected: uint64(len(rows)),
	}, nil
}

func validateSetOnOff(v interface{}, typ string) (int64, error) {
	var val int64
	switch v := v.(type) {
	case int64:
		val = v
	case string:
		lcaseV := strings.ToLower(v)
		if lcaseV == "on" {
			val = 1
		} else if lcaseV == "off" {
			val = 0
		} else {
			return -1, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for %s: %s", typ, v)
		}
	default:
		return -1, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for %s: %T", typ, v)
	}
	return val, nil
}

func (e *Executor) handleShow(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	show, ok := stmt.(*sqlparser.Show)
	if !ok {
		// This code is unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unrecognized SHOW statement: %v", sql)
	}
	execStart := time.Now()
	defer func() { logStats.ExecuteTime = time.Since(execStart) }()
	switch strings.ToLower(show.Type) {
	case sqlparser.KeywordString(sqlparser.COLLATION), sqlparser.KeywordString(sqlparser.VARIABLES):
		if show.Scope == sqlparser.VitessMetadataStr {
			return e.handleShowVitessMetadata(ctx, show.ShowTablesOpt)
		}

		if destKeyspace == "" {
			keyspaces, err := e.resolver.resolver.GetAllKeyspaces(ctx)
			if err != nil {
				return nil, err
			}
			if len(keyspaces) == 0 {
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no keyspaces available")
			}
			return e.handleOther(ctx, safeSession, sql, bindVars, dest, keyspaces[0], destTabletType, logStats)
		}
	// for STATUS, return empty result set
	case sqlparser.KeywordString(sqlparser.STATUS):
		return &sqltypes.Result{
			Fields:       buildVarCharFields("Variable_name", "Value"),
			Rows:         make([][]sqltypes.Value, 0, 2),
			RowsAffected: 0,
		}, nil
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
			Fields:       buildVarCharFields("Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"),
			Rows:         rows,
			RowsAffected: 1,
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
			Fields:       buildVarCharFields("Name", "Status", "Type", "Library", "License"),
			Rows:         rows,
			RowsAffected: 1,
		}, nil
	// CHARSET & CHARACTER SET return utf8mb4 & utf8
	case sqlparser.KeywordString(sqlparser.CHARSET):
		fields := buildVarCharFields("Charset", "Description", "Default collation")
		maxLenField := &querypb.Field{Name: "Maxlen", Type: sqltypes.Int32}
		fields = append(fields, maxLenField)

		charsets := []string{utf8, utf8mb4}
		filter := show.ShowTablesOpt.Filter
		rows, err := generateCharsetRows(filter, charsets)
		if err != nil {
			return nil, err
		}
		rowsAffected := uint64(len(rows))

		return &sqltypes.Result{
			Fields:       fields,
			Rows:         rows,
			RowsAffected: rowsAffected,
		}, err
	case "create table":
		if !show.Table.Qualifier.IsEmpty() {
			// Explicit keyspace was passed. Use that for targeting but remove from the query itself.
			destKeyspace = show.Table.Qualifier.String()
			show.Table.Qualifier = sqlparser.NewTableIdent("")
		} else {
			// No keyspace was indicated. Try to find one using the vschema.
			tbl, err := e.VSchema().FindTable(destKeyspace, show.Table.Name.String())
			if err != nil {
				return nil, err
			}
			destKeyspace = tbl.Keyspace.Name
		}
		sql = sqlparser.String(show)
	case sqlparser.KeywordString(sqlparser.COLUMNS):
		if !show.OnTable.Qualifier.IsEmpty() {
			destKeyspace = show.OnTable.Qualifier.String()
			show.OnTable.Qualifier = sqlparser.NewTableIdent("")
		} else if show.ShowTablesOpt != nil {
			destKeyspace = show.ShowTablesOpt.DbName
			show.ShowTablesOpt.DbName = ""
		} else {
			break
		}
		sql = sqlparser.String(show)
	case sqlparser.KeywordString(sqlparser.INDEX), sqlparser.KeywordString(sqlparser.KEYS), sqlparser.KeywordString(sqlparser.INDEXES):
		if !show.OnTable.Qualifier.IsEmpty() {
			destKeyspace = show.OnTable.Qualifier.String()
			show.OnTable.Qualifier = sqlparser.NewTableIdent("")
		} else if show.ShowTablesOpt != nil {
			if show.ShowTablesOpt.DbName != "" {
				destKeyspace = show.ShowTablesOpt.DbName
				show.ShowTablesOpt.DbName = ""
			}
		} else {
			break
		}
		sql = sqlparser.String(show)
	case sqlparser.KeywordString(sqlparser.TABLES):
		if show.ShowTablesOpt != nil && show.ShowTablesOpt.DbName != "" {
			if destKeyspace == "" {
				// Change "show tables from <keyspace>" to "show tables" directed to that keyspace.
				destKeyspace = show.ShowTablesOpt.DbName
			}
			show.ShowTablesOpt.DbName = ""
		}
		sql = sqlparser.String(show)
	case sqlparser.KeywordString(sqlparser.DATABASES), "vitess_keyspaces", "keyspaces":
		keyspaces, err := e.resolver.resolver.GetAllKeyspaces(ctx)
		if err != nil {
			return nil, err
		}

		rows := make([][]sqltypes.Value, len(keyspaces))
		for i, v := range keyspaces {
			rows[i] = buildVarCharRow(v)
		}

		return &sqltypes.Result{
			Fields:       buildVarCharFields("Databases"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	case "vitess_shards":
		keyspaces, err := e.resolver.resolver.GetAllKeyspaces(ctx)
		if err != nil {
			return nil, err
		}

		var rows [][]sqltypes.Value
		for _, keyspace := range keyspaces {
			_, _, shards, err := e.resolver.resolver.GetKeyspaceShards(ctx, keyspace, destTabletType)
			if err != nil {
				// There might be a misconfigured keyspace or no shards in the keyspace.
				// Skip any errors and move on.
				continue
			}

			for _, shard := range shards {
				rows = append(rows, buildVarCharRow(topoproto.KeyspaceShardString(keyspace, shard.Name)))
			}
		}

		return &sqltypes.Result{
			Fields:       buildVarCharFields("Shards"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	case "vitess_tablets":
		var rows [][]sqltypes.Value
		stats := e.scatterConn.healthCheck.CacheStatus()
		for _, s := range stats {
			for _, ts := range s.TabletsStats {
				state := "SERVING"
				if !ts.Serving {
					state = "NOT_SERVING"
				}
				rows = append(rows, buildVarCharRow(
					s.Cell,
					s.Target.Keyspace,
					s.Target.Shard,
					ts.Target.TabletType.String(),
					state,
					topoproto.TabletAliasString(ts.Tablet.Alias),
					ts.Tablet.Hostname,
				))
			}
		}
		return &sqltypes.Result{
			Fields:       buildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	case "vitess_target":
		var rows [][]sqltypes.Value
		rows = append(rows, buildVarCharRow(safeSession.TargetString))
		return &sqltypes.Result{
			Fields:       buildVarCharFields("Target"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	case "vschema tables":
		if destKeyspace == "" {
			return nil, errNoKeyspace
		}
		ks, ok := e.VSchema().Keyspaces[destKeyspace]
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace %s not found in vschema", destKeyspace)
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
			Fields:       buildVarCharFields("Tables"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
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
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table `%s` does not exist in keyspace `%s`", tableName, ksName)
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
				Fields:       buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
				Rows:         rows,
				RowsAffected: uint64(len(rows)),
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
			Fields:       buildVarCharFields("Keyspace", "Name", "Type", "Params", "Owner"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	case sqlparser.KeywordString(sqlparser.WARNINGS):
		fields := []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar},
			{Name: "Type", Type: sqltypes.Uint16},
			{Name: "Message", Type: sqltypes.VarChar},
		}
		rows := make([][]sqltypes.Value, 0)

		if safeSession.Warnings != nil {
			for _, warning := range safeSession.Warnings {
				rows = append(rows, []sqltypes.Value{
					sqltypes.NewVarChar("Warning"),
					sqltypes.NewUint32(warning.Code),
					sqltypes.NewVarChar(warning.Message),
				})
			}
		}

		return &sqltypes.Result{
			Fields: fields,
			Rows:   rows,
		}, nil
	}

	// Any other show statement is passed through
	return e.handleOther(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
}

func (e *Executor) handleUse(safeSession *SafeSession, sql string) (*sqltypes.Result, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	use, ok := stmt.(*sqlparser.Use)
	if !ok {
		// This code is unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unrecognized USE statement: %v", sql)
	}
	destKeyspace, destTabletType, _, err := e.ParseDestinationTarget(use.DBName.String())
	if err != nil {
		return nil, err
	}
	if _, ok := e.VSchema().Keyspaces[destKeyspace]; destKeyspace != "" && !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace provided: %s", destKeyspace)
	}

	if safeSession.InTransaction() && destTabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot change to a non-master type in the middle of a transaction: %v", destTabletType)
	}
	safeSession.TargetString = use.DBName.String()
	return &sqltypes.Result{}, nil
}

func (e *Executor) handleOther(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
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
	case key.DestinationShard:
	// noop
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Destination can only be a single shard for statement: %s, got: %v", sql, dest)
	}
	execStart := time.Now()
	result, err := e.destinationExec(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)

	e.updateQueryCounts("Execute", "Other", "", "", int64(logStats.ShardQueries))

	logStats.ExecuteTime = time.Since(execStart)
	return result, err
}

func (e *Executor) handleComment(sql string) (*sqltypes.Result, error) {
	_, _ = sqlparser.ExtractMysqlComment(sql)
	// Not sure if this is a good idea.
	return &sqltypes.Result{}, nil
}

// StreamExecute executes a streaming query.
func (e *Executor) StreamExecute(ctx context.Context, method string, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, callback func(*sqltypes.Result) error) (err error) {
	logStats := NewLogStats(ctx, method, sql, bindVars)
	logStats.StmtType = sqlparser.Preview(sql).String()
	defer logStats.Send()

	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}
	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, _ := newVCursorImpl(ctx, safeSession, comments, e, logStats, e.vm, e.resolver.resolver)

	// check if this is a stream statement for messaging
	// TODO: support keyRange syntax
	if logStats.StmtType == sqlparser.StmtStream.String() {
		return e.handleMessageStream(ctx, sql, target, callback, vcursor, logStats)
	}

	plan, err := e.getPlan(
		vcursor,
		query,
		comments,
		bindVars,
		skipQueryPlanCache(safeSession),
		logStats,
	)
	if err != nil {
		logStats.Error = err
		return err
	}

	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	// Some of the underlying primitives may send results one row at a time.
	// So, we need the ability to consolidate those into reasonable chunks.
	// The callback wrapper below accumulates rows and sends them as chunks
	// dictated by stream_buffer_size.
	result := &sqltypes.Result{}
	byteCount := 0
	err = plan.Instructions.StreamExecute(vcursor, bindVars, true, func(qr *sqltypes.Result) error {
		// If the row has field info, send it separately.
		// TODO(sougou): this behavior is for handling tests because
		// the framework currently sends all results as one packet.
		if len(qr.Fields) > 0 {
			qrfield := &sqltypes.Result{Fields: qr.Fields}
			if err := callback(qrfield); err != nil {
				return err
			}
		}

		for _, row := range qr.Rows {
			result.Rows = append(result.Rows, row)
			for _, col := range row {
				byteCount += col.Len()
			}

			if byteCount >= e.streamSize {
				err := callback(result)
				result = &sqltypes.Result{}
				byteCount = 0
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	// Send left-over rows.
	if len(result.Rows) > 0 {
		if err := callback(result); err != nil {
			return err
		}
	}

	logStats.ExecuteTime = time.Since(execStart)
	e.updateQueryCounts("StreamExecute", plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

	return err
}

// handleMessageStream executes queries of the form 'stream * from t'
func (e *Executor) handleMessageStream(ctx context.Context, sql string, target querypb.Target, callback func(*sqltypes.Result) error, vcursor *vcursorImpl, logStats *LogStats) error {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		logStats.Error = err
		return err
	}

	streamStmt, ok := stmt.(*sqlparser.Stream)
	if !ok {
		logStats.Error = err
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unrecognized STREAM statement: %v", sql)
	}

	// TODO: Add support for destination target in streamed queries
	table, _, _, _, err := vcursor.FindTable(streamStmt.Table)
	if err != nil {
		logStats.Error = err
		return err
	}

	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	err = e.MessageStream(ctx, table.Keyspace.Name, target.Shard, nil, table.Name.CompliantName(), callback)
	logStats.Error = err
	logStats.ExecuteTime = time.Since(execStart)
	return err
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
	e.vschema = vschema
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

// getPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (e *Executor) getPlan(vcursor *vcursorImpl, sql string, comments sqlparser.MarginComments, bindVars map[string]*querypb.BindVariable, skipQueryPlanCache bool, logStats *LogStats) (*engine.Plan, error) {
	if logStats != nil {
		logStats.SQL = comments.Leading + sql + comments.Trailing
		logStats.BindVariables = bindVars
	}

	if e.VSchema() == nil {
		return nil, errors.New("vschema not initialized")
	}
	planKey := vcursor.planPrefixKey() + ":" + sql
	if plan, ok := e.plans.Get(planKey); ok {
		return plan.(*engine.Plan), nil
	}
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	if !e.normalize || !sqlparser.CanNormalize(stmt) {
		plan, err := planbuilder.BuildFromStmt(sql, stmt, vcursor, sqlparser.BindVarNeeds{})
		if err != nil {
			return nil, err
		}
		if !skipQueryPlanCache && !sqlparser.SkipQueryPlanCacheDirective(stmt) {
			e.plans.Set(planKey, plan)
		}
		return plan, nil
	}

	// Normalize and retry.
	result, err := sqlparser.PrepareAST(stmt, bindVars, "vtg")
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to rewrite ast before planning")
	}
	rewrittenStatement := result.AST
	normalized := sqlparser.String(rewrittenStatement)

	if logStats != nil {
		logStats.SQL = comments.Leading + normalized + comments.Trailing
		logStats.BindVariables = bindVars
	}

	planKey = vcursor.planPrefixKey() + ":" + normalized
	if plan, ok := e.plans.Get(planKey); ok {
		return plan.(*engine.Plan), nil
	}
	plan, err := planbuilder.BuildFromStmt(normalized, rewrittenStatement, vcursor, result.BindVarNeeds)
	if err != nil {
		return nil, err
	}
	if !skipQueryPlanCache && !sqlparser.SkipQueryPlanCacheDirective(rewrittenStatement) {
		e.plans.Set(planKey, plan)
	}
	return plan, nil
}

// skipQueryPlanCache extracts SkipQueryPlanCache from session
func skipQueryPlanCache(safeSession *SafeSession) bool {
	if safeSession == nil || safeSession.Options == nil {
		return false
	}
	return safeSession.Options.SkipQueryPlanCache
}

// ServeHTTP shows the current plans in the query cache.
func (e *Executor) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}

	switch request.URL.Path {
	case pathQueryPlans:
		returnAsJSON(response, e.plans.Items())
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
func (e *Executor) Plans() *cache.LRUCache {
	return e.plans
}

func (e *Executor) updateQueryCounts(operation, planType, keyspace, tableName string, shardQueries int64) {
	queriesProcessed.Add(planType, 1)
	queriesRouted.Add(planType, shardQueries)
	if tableName != "" {
		queriesProcessedByTable.Add([]string{operation, planType, keyspace, tableName}, 1)
		queriesRoutedByTable.Add([]string{operation, planType, keyspace, tableName}, shardQueries)
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
			Charset: mysql.CharacterSetUtf8,
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

func generateCharsetRows(showFilter *sqlparser.ShowFilter, colNames []string) ([][]sqltypes.Value, error) {
	if showFilter == nil {
		return buildCharsetRows(both), nil
	}

	var filteredColName string
	var err error

	if showFilter.Like != "" {
		filteredColName, err = checkLikeOpt(showFilter.Like, colNames)
		if err != nil {
			return nil, err
		}

	} else {
		cmpExp, ok := showFilter.Filter.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expect a 'LIKE' or '=' expression")
		}

		left, ok := cmpExp.Left.(*sqlparser.ColName)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expect left side to be 'charset'")
		}
		leftOk := left.Name.EqualString(charset)

		if leftOk {
			sqlVal, ok := cmpExp.Right.(*sqlparser.SQLVal)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "we expect the right side to be a string")
			}
			rightString := string(sqlVal.Val)

			switch cmpExp.Operator {
			case sqlparser.EqualStr:
				for _, colName := range colNames {
					if rightString == colName {
						filteredColName = colName
					}
				}
			case sqlparser.LikeStr:
				filteredColName, err = checkLikeOpt(rightString, colNames)
				if err != nil {
					return nil, err
				}
			}
		}

	}

	return buildCharsetRows(filteredColName), nil
}

func buildCharsetRows(colName string) [][]sqltypes.Value {
	row0 := buildVarCharRow(
		"utf8",
		"UTF-8 Unicode",
		"utf8_general_ci")
	row0 = append(row0, sqltypes.NewInt32(3))
	row1 := buildVarCharRow(
		"utf8mb4",
		"UTF-8 Unicode",
		"utf8mb4_general_ci")
	row1 = append(row1, sqltypes.NewInt32(4))

	switch colName {
	case utf8:
		return [][]sqltypes.Value{row0}
	case utf8mb4:
		return [][]sqltypes.Value{row1}
	case both:
		return [][]sqltypes.Value{row0, row1}
	}

	return [][]sqltypes.Value{}
}

func checkLikeOpt(likeOpt string, colNames []string) (string, error) {
	likeRegexp := strings.ReplaceAll(likeOpt, "%", ".*")
	for _, v := range colNames {
		match, err := regexp.MatchString(likeRegexp, v)
		if err != nil {
			return "", err
		}
		if match {
			return v, nil
		}
	}

	return "", nil
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

	if safeSession.InTransaction() && destTabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "transactions are supported only for master tablet types, current type: %v", destTabletType)
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
		return e.handlePrepare(ctx, safeSession, sql, bindVars, destKeyspace, destTabletType, logStats)
	case sqlparser.StmtDDL, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtSet, sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete,
		sqlparser.StmtUse, sqlparser.StmtOther, sqlparser.StmtComment:
		return nil, nil
	case sqlparser.StmtShow:
		res, err := e.handleShow(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
		if err != nil {
			return nil, err
		}
		return res.Fields, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unrecognized statement: %s", sql)
}

func (e *Executor) handlePrepare(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) ([]*querypb.Field, error) {
	// V3 mode.
	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, _ := newVCursorImpl(ctx, safeSession, comments, e, logStats, e.vm, e.resolver.resolver)
	plan, err := e.getPlan(
		vcursor,
		query,
		comments,
		bindVars,
		skipQueryPlanCache(safeSession),
		logStats,
	)
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	if err != nil {
		logStats.Error = err
		return nil, err
	}

	qr, err := plan.Instructions.GetFields(vcursor, bindVars)
	logStats.ExecuteTime = time.Since(execStart)
	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1
		return nil, err
	}
	logStats.RowsAffected = qr.RowsAffected

	plan.AddStats(1, time.Since(logStats.StartTime), uint64(logStats.ShardQueries), logStats.RowsAffected, errCount)

	return qr.Fields, err
}

// ExecuteMultiShard implements the IExecutor interface
func (e *Executor) ExecuteMultiShard(ctx context.Context, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, tabletType topodatapb.TabletType, session *SafeSession, notInTransaction bool, autocommit bool) (qr *sqltypes.Result, errs []error) {
	return e.scatterConn.ExecuteMultiShard(ctx, rss, queries, tabletType, session, notInTransaction, autocommit)
}

// StreamExecuteMulti implements the IExecutor interface
func (e *Executor) StreamExecuteMulti(ctx context.Context, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(reply *sqltypes.Result) error) error {
	return e.scatterConn.StreamExecuteMulti(ctx, query, rss, vars, tabletType, options, callback)
}
