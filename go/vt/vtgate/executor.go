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

package vtgate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	"vitess.io/vitess/go/vt/sqlannotation"
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

	queriesProcessed = stats.NewCountersWithSingleLabel("QueriesProcessed", "Queries processed at vtgate by plan type", "Plan")
	queriesRouted    = stats.NewCountersWithSingleLabel("QueriesRouted", "Queries routed from vtgate to vttablet by plan type", "Plan")
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

	vm VSchemaManager
}

var executorOnce sync.Once

// NewExecutor creates a new Executor.
func NewExecutor(ctx context.Context, serv srvtopo.Server, cell, statsName string, resolver *Resolver, normalize bool, streamSize int, queryPlanCacheSize int64) *Executor {
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

	vschemaacl.Init()
	e.vm = VSchemaManager{e: e}
	e.vm.watchSrvVSchema(ctx, cell)

	executorOnce.Do(func() {
		stats.NewGaugeFunc("QueryPlanCacheLength", "Query plan cache length", e.plans.Length)
		stats.NewGaugeFunc("QueryPlanCacheSize", "Query plan cache size", e.plans.Size)
		stats.NewGaugeFunc("QueryPlanCacheCapacity", "Query plan cache capacity", e.plans.Capacity)
		stats.NewCounterFunc("QueryPlanCacheEvictions", "Query plan cache evictions", e.plans.Evictions)
		stats.Publish("QueryPlanCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", e.plans.Oldest())
		}))
		http.Handle("/debug/query_plans", e)
		http.Handle("/debug/vschema", e)
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
	result, err = e.execute(ctx, safeSession, sql, bindVars, logStats)
	logStats.Error = err

	// The mysql plugin runs an implicit rollback whenever a connection closes.
	// To avoid spamming the log with no-op rollback records, ignore it if
	// it was a no-op record (i.e. didn't issue any queries)
	if !(logStats.StmtType == "ROLLBACK" && logStats.ShardQueries == 0) {
		logStats.Send()
	}
	return result, err
}

func (e *Executor) execute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error) {
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
	logStats.StmtType = sqlparser.StmtType(stmtType)

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
		return e.handleExec(ctx, safeSession, sql, bindVars, destKeyspace, destTabletType, dest, logStats)
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

		qr, err := e.handleExec(ctx, safeSession, sql, bindVars, destKeyspace, destTabletType, dest, logStats)
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
	case sqlparser.StmtBegin:
		return e.handleBegin(ctx, safeSession, sql, bindVars, destTabletType, logStats)
	case sqlparser.StmtCommit:
		return e.handleCommit(ctx, safeSession, sql, bindVars, logStats)
	case sqlparser.StmtRollback:
		return e.handleRollback(ctx, safeSession, sql, bindVars, logStats)
	case sqlparser.StmtSet:
		return e.handleSet(ctx, safeSession, sql, bindVars, logStats)
	case sqlparser.StmtShow:
		return e.handleShow(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
	case sqlparser.StmtUse:
		return e.handleUse(ctx, safeSession, sql, bindVars)
	case sqlparser.StmtOther:
		return e.handleOther(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
	case sqlparser.StmtComment:
		return e.handleComment(sql)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unrecognized statement: %s", sql)
}

func (e *Executor) handleExec(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, destKeyspace string, destTabletType topodatapb.TabletType, dest key.Destination, logStats *LogStats) (*sqltypes.Result, error) {
	if dest != nil {
		// V1 mode or V3 mode with a forced shard or range target
		// TODO(sougou): change this flow to go through V3 functions
		// which will allow us to benefit from the autocommitable flag.

		queriesProcessed.Add("ShardDirect", 1)

		if destKeyspace == "" {
			return nil, errNoKeyspace
		}

		switch dest.(type) {
		case key.DestinationExactKeyRange:
			stmtType := sqlparser.Preview(sql)
			if stmtType == sqlparser.StmtInsert {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "range queries not supported for inserts: %s", safeSession.TargetString)
			}

		}

		execStart := time.Now()
		sql = sqlannotation.AnnotateIfDML(sql, nil)
		if e.normalize {
			query, comments := sqlparser.SplitMarginComments(sql)
			stmt, err := sqlparser.Parse(query)
			if err != nil {
				return nil, err
			}
			sqlparser.Normalize(stmt, bindVars, "vtg")
			normalized := sqlparser.String(stmt)
			sql = comments.Leading + normalized + comments.Trailing
		}
		logStats.PlanTime = execStart.Sub(logStats.StartTime)
		logStats.SQL = sql
		logStats.BindVariables = bindVars
		result, err := e.destinationExec(ctx, safeSession, sql, bindVars, dest, destKeyspace, destTabletType, logStats)
		logStats.ExecuteTime = time.Since(execStart)
		queriesRouted.Add("ShardDirect", int64(logStats.ShardQueries))
		return result, err
	}

	// V3 mode.
	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor := newVCursorImpl(ctx, safeSession, destKeyspace, destTabletType, comments, e, logStats)
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

	qr, err := plan.Instructions.Execute(vcursor, bindVars, true)

	logStats.ExecuteTime = time.Since(execStart)
	queriesProcessed.Add(plan.Instructions.RouteType(), 1)
	queriesRouted.Add(plan.Instructions.RouteType(), int64(logStats.ShardQueries))

	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1
	} else {
		logStats.RowsAffected = qr.RowsAffected
	}

	// Check if there was partial DML execution. If so, rollback the transaction.
	if err != nil && safeSession.InTransaction() && vcursor.hasPartialDML {
		_ = e.txConn.Rollback(ctx, safeSession)
		err = vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction rolled back due to partial DML execution: %v", err)
	}

	plan.AddStats(1, time.Since(logStats.StartTime), uint64(logStats.ShardQueries), logStats.RowsAffected, errCount)

	return qr, err
}

func (e *Executor) destinationExec(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	return e.resolver.Execute(ctx, sql, bindVars, destKeyspace, destTabletType, dest, safeSession.Session, false /* notInTransaction */, safeSession.Options, logStats)
}

func (e *Executor) handleDDL(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	// Parse the statement to handle vindex operations
	// If the statement failed to be properly parsed, fall through anyway
	// to broadcast the ddl to all shards.
	stmt, _ := sqlparser.Parse(sql)
	ddl, ok := stmt.(*sqlparser.DDL)
	if ok {
		execStart := time.Now()
		logStats.PlanTime = execStart.Sub(logStats.StartTime)
		switch ddl.Action {
		case sqlparser.CreateVindexStr,
			sqlparser.AddVschemaTableStr,
			sqlparser.DropVschemaTableStr,
			sqlparser.AddColVindexStr,
			sqlparser.DropColVindexStr:

			err := e.handleVSchemaDDL(ctx, safeSession, dest, destKeyspace, destTabletType, ddl, logStats)
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

	queriesProcessed.Add("DDL", 1)
	queriesRouted.Add("DDL", int64(logStats.ShardQueries))

	return result, err
}

func (e *Executor) handleVSchemaDDL(ctx context.Context, safeSession *SafeSession, dest key.Destination, destKeyspace string, destTabletType topodatapb.TabletType, ddl *sqlparser.DDL, logStats *LogStats) error {
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

func (e *Executor) handleBegin(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, destTabletType topodatapb.TabletType, logStats *LogStats) (*sqltypes.Result, error) {
	if destTabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "transactions are supported only for master tablet types, current type: %v", destTabletType)
	}
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	err := e.txConn.Begin(ctx, safeSession)
	logStats.ExecuteTime = time.Since(execStart)

	queriesProcessed.Add("Begin", 1)

	return &sqltypes.Result{}, err
}

func (e *Executor) handleCommit(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint32(len(safeSession.ShardSessions))
	queriesProcessed.Add("Commit", 1)
	queriesRouted.Add("Commit", int64(logStats.ShardQueries))
	err := e.txConn.Commit(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleRollback(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint32(len(safeSession.ShardSessions))
	queriesProcessed.Add("Rollback", 1)
	queriesRouted.Add("Rollback", int64(logStats.ShardQueries))
	err := e.txConn.Rollback(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleSet(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error) {
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
		if k.Scope == sqlparser.GlobalStr {
			return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
		}
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
		case "tx_read_only":
			val, err := validateSetOnOff(v, k.Key)
			if err != nil {
				return nil, err
			}
			switch val {
			case 0, 1:
				// TODO (4127): This is a dangerous NOP.
			default:
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for tx_read_only: %d", val)
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
		case "sql_mode", "net_write_timeout", "net_read_timeout", "lc_messages", "collation_connection":
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
	return &sqltypes.Result{}, nil
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

	switch show.Type {
	case sqlparser.KeywordString(sqlparser.COLLATION), sqlparser.KeywordString(sqlparser.VARIABLES):
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
		rows := make([][]sqltypes.Value, 0, 4)
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
		rows = append(rows, row0, row1)
		return &sqltypes.Result{
			Fields:       fields,
			Rows:         rows,
			RowsAffected: 2,
		}, nil
	case "create table":
		if destKeyspace == "" && show.HasTable() {
			// For "show create table", if there isn't a targeted keyspace already
			// we can either get a keyspace from the statement or potentially from
			// the vschema.

			if !show.Table.Qualifier.IsEmpty() {
				// Explicit keyspace was passed. Use that for targeting but remove from the query itself.
				destKeyspace = show.Table.Qualifier.String()
				show.Table.Qualifier = sqlparser.NewTableIdent("")
				sql = sqlparser.String(show)
			} else {
				// No keyspace was indicated. Try to find one using the vschema.
				tbl, err := e.VSchema().FindTable("", show.Table.Name.String())
				if err == nil {
					destKeyspace = tbl.Keyspace.Name
				}
			}
		}
	case sqlparser.KeywordString(sqlparser.TABLES):
		if show.ShowTablesOpt != nil && show.ShowTablesOpt.DbName != "" {
			if destKeyspace == "" {
				// Change "show tables from <keyspace>" to "show tables" directed to that keyspace.
				destKeyspace = show.ShowTablesOpt.DbName
			}
			show.ShowTablesOpt.DbName = ""
		}
		sql = sqlparser.String(show)
	case sqlparser.KeywordString(sqlparser.DATABASES), sqlparser.KeywordString(sqlparser.SCHEMAS), sqlparser.KeywordString(sqlparser.VITESS_KEYSPACES):
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
	case sqlparser.KeywordString(sqlparser.VITESS_SHARDS):
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
	case sqlparser.KeywordString(sqlparser.VITESS_TABLETS):
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
	case sqlparser.KeywordString(sqlparser.VITESS_TARGET):
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

func (e *Executor) handleUse(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
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

	queriesProcessed.Add("Other", 1)
	queriesRouted.Add("Other", int64(logStats.ShardQueries))

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
	logStats.StmtType = sqlparser.StmtType(sqlparser.Preview(sql))
	defer logStats.Send()

	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}
	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor := newVCursorImpl(ctx, safeSession, target.Keyspace, target.TabletType, comments, e, logStats)

	// check if this is a stream statement for messaging
	// TODO: support keyRange syntax
	if logStats.StmtType == sqlparser.StmtType(sqlparser.StmtStream) {
		return e.handleMessageStream(ctx, safeSession, sql, target, callback, vcursor, logStats)
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

	return err
}

// handleMessageStream executes queries of the form 'stream * from t'
func (e *Executor) handleMessageStream(ctx context.Context, safeSession *SafeSession, sql string, target querypb.Target, callback func(*sqltypes.Result) error, vcursor *vcursorImpl, logStats *LogStats) error {
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

// MessageAck acks messages.
// FIXME(alainjobart) the keyspace field here is not used for routing,
// but just for finding the table in the VSchema. If we don't find the
// table in the VSchema, we could just assume it's sharded (which would work
// for unsharded as well) and route it to the provided keyspace.
func (e *Executor) MessageAck(ctx context.Context, keyspace, name string, ids []*querypb.Value) (int64, error) {
	table, err := e.VSchema().FindTable(keyspace, name)
	if err != nil {
		return 0, err
	}

	var rss []*srvtopo.ResolvedShard
	var rssValues [][]*querypb.Value
	if table.Keyspace.Sharded {
		// TODO(sougou): Change this to use Session.
		vcursor := newVCursorImpl(
			ctx,
			NewSafeSession(&vtgatepb.Session{}),
			table.Keyspace.Name,
			topodatapb.TabletType_MASTER,
			sqlparser.MarginComments{},
			e,
			nil,
		)

		// convert []*querypb.Value to []sqltypes.Value for calling Map.
		values := make([]sqltypes.Value, 0, len(ids))
		for _, id := range ids {
			values = append(values, sqltypes.ProtoToValue(id))
		}
		// We always use the (unique) primary vindex. The ID must be the
		// primary vindex for message tables.
		destinations, err := table.ColumnVindexes[0].Vindex.Map(vcursor, values)
		if err != nil {
			return 0, err
		}
		rss, rssValues, err = e.resolver.resolver.ResolveDestinations(ctx, table.Keyspace.Name, topodatapb.TabletType_MASTER, ids, destinations)
		if err != nil {
			return 0, err
		}
	} else {
		// All ids go into the first shard, so we only resolve
		// one destination, and put all IDs in there.
		rss, err = e.resolver.resolver.ResolveDestination(ctx, table.Keyspace.Name, topodatapb.TabletType_MASTER, key.DestinationAnyShard{})
		if err != nil {
			return 0, err
		}
		rssValues = [][]*querypb.Value{ids}
	}
	return e.scatterConn.MessageAck(ctx, rss, rssValues, name)
}

// IsKeyspaceRangeBasedSharded returns true if the keyspace in the vschema is
// marked as sharded.
func (e *Executor) IsKeyspaceRangeBasedSharded(keyspace string) bool {
	vschema := e.VSchema()
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return false
	}
	if ks.Keyspace == nil {
		return false
	}
	return ks.Keyspace.Sharded
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
	keyspace := vcursor.keyspace
	planKey := keyspace + vindexes.TabletTypeSuffix[vcursor.tabletType] + ":" + sql
	if result, ok := e.plans.Get(planKey); ok {
		return result.(*engine.Plan), nil
	}
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	if !e.normalize {
		plan, err := planbuilder.BuildFromStmt(sql, stmt, vcursor)
		if err != nil {
			return nil, err
		}
		if !skipQueryPlanCache && !sqlparser.SkipQueryPlanCacheDirective(stmt) {
			e.plans.Set(planKey, plan)
		}
		return plan, nil
	}

	// Normalize and retry.
	sqlparser.Normalize(stmt, bindVars, "vtg")
	normalized := sqlparser.String(stmt)

	if logStats != nil {
		logStats.SQL = comments.Leading + normalized + comments.Trailing
		logStats.BindVariables = bindVars
	}

	planKey = keyspace + vindexes.TabletTypeSuffix[vcursor.tabletType] + ":" + normalized
	if result, ok := e.plans.Get(planKey); ok {
		return result.(*engine.Plan), nil
	}
	plan, err := planbuilder.BuildFromStmt(normalized, stmt, vcursor)
	if err != nil {
		return nil, err
	}
	if !skipQueryPlanCache && !sqlparser.SkipQueryPlanCacheDirective(stmt) {
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
	if request.URL.Path == "/debug/query_plans" {
		keys := e.plans.Keys()
		response.Header().Set("Content-Type", "text/plain")
		response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
		for _, v := range keys {
			response.Write([]byte(fmt.Sprintf("%#v\n", sqlparser.TruncateForUI(v))))
			if plan, ok := e.plans.Peek(v); ok {
				if b, err := json.MarshalIndent(plan, "", "  "); err != nil {
					response.Write([]byte(err.Error()))
				} else {
					response.Write(b)
				}
				response.Write(([]byte)("\n\n"))
			}
		}
	} else if request.URL.Path == "/debug/vschema" {
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		b, err := json.MarshalIndent(e.VSchema().Keyspaces, "", " ")
		if err != nil {
			response.Write([]byte(err.Error()))
			return
		}
		buf := bytes.NewBuffer(nil)
		json.HTMLEscape(buf, b)
		response.Write(buf.Bytes())
	} else {
		response.WriteHeader(http.StatusNotFound)
	}
}

// Plans returns the LRU plan cache
func (e *Executor) Plans() *cache.LRUCache {
	return e.plans
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
