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
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlannotation"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	errNoKeyspace     = vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no keyspace in database name specified. Supported database name format (items in <> are optional): keyspace<:shard><@type> or keyspace<[range]><@type>")
	defaultTabletType topodatapb.TabletType
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

	mu               sync.Mutex
	vschema          *vindexes.VSchema
	normalize        bool
	streamSize       int
	legacyAutocommit bool
	plans            *cache.LRUCache
	vschemaStats     *VSchemaStats

	vm VSchemaManager
}

var executorOnce sync.Once

// NewExecutor creates a new Executor.
func NewExecutor(ctx context.Context, serv srvtopo.Server, cell, statsName string, resolver *Resolver, normalize bool, streamSize int, queryPlanCacheSize int64, legacyAutocommit bool) *Executor {
	e := &Executor{
		serv:             serv,
		cell:             cell,
		resolver:         resolver,
		scatterConn:      resolver.scatterConn,
		txConn:           resolver.scatterConn.txConn,
		plans:            cache.NewLRUCache(queryPlanCacheSize),
		normalize:        normalize,
		streamSize:       streamSize,
		legacyAutocommit: legacyAutocommit,
	}

	e.vm = VSchemaManager{e: e}
	e.vm.watchSrvVSchema(ctx, cell)

	executorOnce.Do(func() {
		stats.Publish("QueryPlanCacheLength", stats.IntFunc(e.plans.Length))
		stats.Publish("QueryPlanCacheSize", stats.IntFunc(e.plans.Size))
		stats.Publish("QueryPlanCacheCapacity", stats.IntFunc(e.plans.Capacity))
		stats.Publish("QueryPlanCacheEvictions", stats.IntFunc(e.plans.Evictions))
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
	// TODO(sougou): deprecate legacyMode after all users are migrated out.
	if !e.legacyAutocommit && !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession); err != nil {
			return nil, err
		}
	}

	target := e.ParseTarget(safeSession.TargetString)
	if safeSession.InTransaction() && target.TabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "transactions are supported only for master tablet types, current type: %v", target.TabletType)
	}
	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	stmtType := sqlparser.Preview(sql)
	logStats.StmtType = sqlparser.StmtType(stmtType)

	switch stmtType {
	case sqlparser.StmtSelect:
		return e.handleExec(ctx, safeSession, sql, bindVars, target, logStats)
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		safeSession := safeSession

		// In legacy mode, we ignore autocommit settings.
		if e.legacyAutocommit {
			return e.handleExec(ctx, safeSession, sql, bindVars, target, logStats)
		}

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
		safeSession.SetAutocommitable(mustCommit)

		qr, err := e.handleExec(ctx, safeSession, sql, bindVars, target, logStats)
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
		return e.handleDDL(ctx, safeSession, sql, bindVars, target, logStats)
	case sqlparser.StmtBegin:
		return e.handleBegin(ctx, safeSession, sql, bindVars, target, logStats)
	case sqlparser.StmtCommit:
		return e.handleCommit(ctx, safeSession, sql, bindVars, target, logStats)
	case sqlparser.StmtRollback:
		return e.handleRollback(ctx, safeSession, sql, bindVars, target, logStats)
	case sqlparser.StmtSet:
		return e.handleSet(ctx, safeSession, sql, bindVars, logStats)
	case sqlparser.StmtShow:
		return e.handleShow(ctx, safeSession, sql, bindVars, target, logStats)
	case sqlparser.StmtUse:
		return e.handleUse(ctx, safeSession, sql, bindVars)
	case sqlparser.StmtOther:
		return e.handleOther(ctx, safeSession, sql, bindVars, target, logStats)
	case sqlparser.StmtComment:
		return e.handleComment(ctx, safeSession, sql, bindVars, target, logStats)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unrecognized statement: %s", sql)
}

func (e *Executor) handleExec(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
	keyRange, err := parseRange(safeSession.TargetString)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse target %s (%s)", safeSession.TargetString, err.Error())
	}

	if keyRange != nil || target.Shard != "" {
		// V1 mode or V3 mode with a forced shard or range target
		// TODO(sougou): change this flow to go through V3 functions
		// which will allow us to benefit from the autocommitable flag.
		if target.Keyspace == "" {
			return nil, errNoKeyspace
		}

		var destination key.Destination
		if keyRange != nil {
			stmtType := sqlparser.Preview(sql)
			if stmtType == sqlparser.StmtInsert {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "range queries not supported for inserts: %s", safeSession.TargetString)
			}
			destination = key.DestinationExactKeyRange{KeyRange: keyRange}
		} else {
			destination = key.DestinationShard(target.Shard)
		}

		execStart := time.Now()
		sql = sqlannotation.AnnotateIfDML(sql, nil)
		if e.normalize {
			query, comments := sqlparser.SplitTrailingComments(sql)
			stmt, err := sqlparser.Parse(query)
			if err != nil {
				return nil, err
			}
			sqlparser.Normalize(stmt, bindVars, "vtg")
			normalized := sqlparser.String(stmt)
			sql = normalized + comments
		}
		logStats.PlanTime = execStart.Sub(logStats.StartTime)
		logStats.SQL = sql
		logStats.BindVariables = bindVars
		result, err := e.destinationExec(ctx, safeSession, sql, bindVars, target, destination, logStats)
		logStats.ExecuteTime = time.Now().Sub(execStart)
		return result, err
	}

	// V3 mode.
	query, comments := sqlparser.SplitTrailingComments(sql)
	vcursor := newVCursorImpl(ctx, safeSession, target, comments, e, logStats)
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

func (e *Executor) destinationExec(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, destination key.Destination, logStats *LogStats) (*sqltypes.Result, error) {
	return e.resolver.Execute(ctx, sql, bindVars, target.Keyspace, target.TabletType, destination, safeSession.Session, false /* notInTransaction */, safeSession.Options, logStats)
}

func (e *Executor) handleDDL(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
	if target.Keyspace == "" {
		return nil, errNoKeyspace
	}

	// Parse the statement to handle vindex operations
	// If the statement failed to be properly parsed, fall through anyway
	// to broadcast the ddl to all shards.
	stmt, _ := sqlparser.Parse(sql)
	ddl, ok := stmt.(*sqlparser.DDL)
	if ok {
		execStart := time.Now()
		logStats.PlanTime = execStart.Sub(logStats.StartTime)
		switch ddl.Action {
		case sqlparser.CreateVindexStr, sqlparser.AddColVindexStr, sqlparser.DropColVindexStr:
			err := e.handleVindexDDL(ctx, safeSession, target, ddl, logStats)
			logStats.ExecuteTime = time.Since(execStart)
			return &sqltypes.Result{}, err
		default:
			// fallthrough to broadcast the ddl to all shards
		}
	}

	keyRange, err := parseRange(safeSession.TargetString)
	if err != nil {
		return nil, errNoKeyspace
	}

	var destination key.Destination
	if keyRange != nil {
		destination = key.DestinationExactKeyRange{KeyRange: keyRange}
	} else if target.Shard != "" {
		destination = key.DestinationShard(target.Shard)
	} else {
		destination = key.DestinationAllShards{}
	}

	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	result, err := e.destinationExec(ctx, safeSession, sql, bindVars, target, destination, logStats)
	logStats.ExecuteTime = time.Since(execStart)
	return result, err
}

func (e *Executor) handleVindexDDL(ctx context.Context, safeSession *SafeSession, target querypb.Target, ddl *sqlparser.DDL, logStats *LogStats) error {
	vschema := e.vm.GetCurrentSrvVschema()
	if vschema == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
	}

	ks, ok := vschema.Keyspaces[target.Keyspace]
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema does not contain keyspace %s", target.Keyspace)
	}

	switch ddl.Action {
	case sqlparser.CreateVindexStr:
		name := ddl.VindexSpec.Name.String()
		if _, ok := ks.Vindexes[name]; ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s already exists in keyspace %s", name, target.Keyspace)
		}
		params := map[string]string{}
		for _, p := range ddl.VindexSpec.Params {
			params[p.Key.String()] = p.Val
		}
		ks.Vindexes[name] = &vschemapb.Vindex{
			Type:   ddl.VindexSpec.Type.String(),
			Params: params,
		}

		e.vm.UpdateVSchema(ctx, target.Keyspace, vschema)
	case sqlparser.AddColVindexStr:
		// Support two cases:
		//
		// 1. The vindex type / params / owner are specified. If the
		//    named vindex doesn't exist, create it. If it does exist,
		//    require the parameters to match.
		//
		// 2. The vindex type is not specified. Make sure the vindex
		//    already exists.
		spec := ddl.VindexSpec
		name := spec.Name.String()
		if !spec.Type.IsEmpty() {
			params := map[string]string{}
			for _, p := range ddl.VindexSpec.Params {
				params[p.Key.String()] = p.Val
			}

			if vindex, ok := ks.Vindexes[name]; ok {
				if vindex.Type != spec.Type.String() {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s defined with type %s not %s", name, vindex.Type, spec.Type.String())
				}
				if vindex.Owner != spec.Owner {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s defined with owner %s not %s", name, vindex.Owner, spec.Owner)
				}
				if (len(vindex.Params) != 0 || len(params) != 0) && !reflect.DeepEqual(vindex.Params, params) {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s defined with different parameters", name)
				}
			} else {
				ks.Vindexes[name] = &vschemapb.Vindex{
					Type:   spec.Type.String(),
					Params: params,
					Owner:  spec.Owner,
				}
			}
		} else {
			if _, ok := ks.Vindexes[name]; !ok {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s does not exist in keyspace %s", name, target.Keyspace)
			}
		}

		ksName := ddl.Table.Qualifier.String()
		if ksName == "" {
			ksName = target.Keyspace
		}

		ks, ok := vschema.Keyspaces[ksName]
		if !ok {
			return errNoKeyspace
		}

		tableName := ddl.Table.Name.String()
		table, ok := ks.Tables[tableName]
		if !ok {
			table = &vschemapb.Table{
				ColumnVindexes: make([]*vschemapb.ColumnVindex, 0, 4),
			}
		}

		columns := make([]string, len(ddl.VindexCols), len(ddl.VindexCols))
		for i, col := range ddl.VindexCols {
			columns[i] = col.String()
		}
		table.ColumnVindexes = append(table.ColumnVindexes, &vschemapb.ColumnVindex{
			Name:    name,
			Columns: columns,
		})
		ks.Tables[tableName] = table

		e.vm.UpdateVSchema(ctx, target.Keyspace, vschema)

	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected vindex ddl operation %s", ddl.Action)
	}

	return nil
}

func (e *Executor) handleBegin(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
	if target.TabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "transactions are supported only for master tablet types, current type: %v", target.TabletType)
	}
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	err := e.txConn.Begin(ctx, safeSession)
	logStats.ExecuteTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleCommit(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint32(len(safeSession.ShardSessions))
	err := e.txConn.Commit(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleRollback(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint32(len(safeSession.ShardSessions))
	err := e.txConn.Rollback(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleSet(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error) {
	vals, charset, scope, err := sqlparser.ExtractSetValues(sql)
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	defer func() {
		logStats.ExecuteTime = time.Since(execStart)
	}()

	if err != nil {
		return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}
	if len(vals) > 0 && charset != "" {
		return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected key values and charset, must specify one")
	}

	if scope == "global" {
		return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
	}

	switch charset {
	case "", "utf8", "utf8mb4", "latin1", "default":
		break
	default:
		return &sqltypes.Result{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for charset: %v", charset)
	}

	for k, v := range vals {
		switch k {
		case "autocommit":
			val, ok := v.(int64)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for autocommit: %T", v)
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
		case "net_write_timeout", "net_read_timeout", "lc_messages", "collation_connection":
			log.Warningf("Ignored inapplicable SET %v = %v", k, v)
			warnings.Add("IgnoredSet", 1)
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported construct: %s", sql)
		}
	}
	return &sqltypes.Result{}, nil
}

func (e *Executor) handleShow(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
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
	case sqlparser.KeywordString(sqlparser.DATABASES), sqlparser.KeywordString(sqlparser.VITESS_KEYSPACES):
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
			_, _, shards, err := e.resolver.resolver.GetKeyspaceShards(ctx, keyspace, target.TabletType)
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
	case sqlparser.KeywordString(sqlparser.VSCHEMA_TABLES):
		if target.Keyspace == "" {
			return nil, errNoKeyspace
		}
		ks, ok := e.VSchema().Keyspaces[target.Keyspace]
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace %s not found in vschema", target.Keyspace)
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
	case sqlparser.KeywordString(sqlparser.VINDEXES):
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
				ksName = target.Keyspace
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
			ks, _ := vschema.Keyspaces[ksName]

			vindexNames := make([]string, 0, len(ks.Vindexes))
			for name := range ks.Vindexes {
				vindexNames = append(vindexNames, name)
			}
			sort.Strings(vindexNames)
			for _, vindexName := range vindexNames {
				vindex, _ := ks.Vindexes[vindexName]

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

	}

	// Any other show statement is passed through
	return e.handleOther(ctx, safeSession, sql, bindVars, target, logStats)
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
	target := e.ParseTarget(use.DBName.String())

	if _, ok := e.VSchema().Keyspaces[target.Keyspace]; target.Keyspace != "" && !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace provided: %s", target.Keyspace)
	}

	if safeSession.InTransaction() && target.TabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot change to a non-master type in the middle of a transaction: %v", target.TabletType)
	}
	safeSession.TargetString = use.DBName.String()
	return &sqltypes.Result{}, nil
}

func (e *Executor) handleOther(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
	if target.Keyspace == "" {
		return nil, errNoKeyspace
	}
	if target.Shard == "" {
		// shardExec will re-resolve this a bit later.
		rss, err := e.resolver.resolver.ResolveDestination(ctx, target.Keyspace, target.TabletType, key.DestinationAnyShard{})
		if err != nil {
			return nil, err
		}
		if len(rss) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "keyspace %s has no shards", target.Keyspace)
		}
		target.Keyspace, target.Shard = rss[0].Target.Keyspace, rss[0].Target.Shard
	}
	execStart := time.Now()
	result, err := e.destinationExec(ctx, safeSession, sql, bindVars, target, key.DestinationShard(target.Shard), logStats)
	logStats.ExecuteTime = time.Since(execStart)
	return result, err
}

func (e *Executor) handleComment(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, target querypb.Target, logStats *LogStats) (*sqltypes.Result, error) {
	_, sql = sqlparser.ExtractMysqlComment(sql)

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
	query, comments := sqlparser.SplitTrailingComments(sql)
	vcursor := newVCursorImpl(ctx, safeSession, target, comments, e, logStats)

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

	table, err := vcursor.FindTable(streamStmt.Table)
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
			querypb.Target{
				Keyspace:   table.Keyspace.Name,
				TabletType: topodatapb.TabletType_MASTER,
			},
			"",
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

// ParseTarget parses the string representation of a Target
// of the form keyspace:shard@tablet_type. You can use a / instead of a :.
// If the keyspace was not specified in the target, but the VSchema has
// only one keyspace, then that name is assigned as the keyspace.
// This is similar to how Find works for VSchema: if there's only
// one keyspace, it's used as the default qualifier.
func (e *Executor) ParseTarget(targetString string) querypb.Target {
	// Default tablet type is master.
	target := querypb.Target{
		TabletType: defaultTabletType,
	}
	last := strings.LastIndexAny(targetString, "@")
	if last != -1 {
		// No need to check the error. UNKNOWN will be returned on
		// error and it will fail downstream.
		target.TabletType, _ = topoproto.ParseTabletType(targetString[last+1:])
		targetString = targetString[:last]
	}
	last = strings.LastIndexAny(targetString, "/:")
	if last != -1 {
		target.Shard = targetString[last+1:]
		targetString = targetString[:last]
	}
	// Remove range query from string if present
	last = strings.LastIndexAny(targetString, "[")
	if last != -1 {
		targetString = targetString[:last]
	}
	if targetString == "" && len(e.VSchema().Keyspaces) == 1 {
		// Loop to extract the only keyspace name.
		for k := range e.VSchema().Keyspaces {
			targetString = k
		}
	}
	target.Keyspace = targetString
	return target
}

// parseRange parses range from target string.
func parseRange(targetString string) (*topodatapb.KeyRange, error) {
	last := strings.LastIndexAny(targetString, "[")
	if last != -1 {
		rangeEnd := strings.LastIndexAny(targetString, "]")
		if rangeEnd == -1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid key range provided. Couldn't find range end ']'")

		}
		rangeString := targetString[last+1 : rangeEnd]
		keyRange, err := key.ParseShardingSpec(rangeString)
		if err != nil {
			return nil, err
		}
		if len(keyRange) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "single keyrange expected in %s", rangeString)
		}
		return keyRange[0], nil
	}
	return nil, nil
}

// getPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (e *Executor) getPlan(vcursor *vcursorImpl, sql string, comments string, bindVars map[string]*querypb.BindVariable, skipQueryPlanCache bool, logStats *LogStats) (*engine.Plan, error) {
	if logStats != nil {
		logStats.SQL = sql + comments
		logStats.BindVariables = bindVars
	}

	if e.VSchema() == nil {
		return nil, errors.New("vschema not initialized")
	}
	keyspace := vcursor.target.Keyspace
	key := sql
	if keyspace != "" {
		key = keyspace + ":" + sql
	}
	if result, ok := e.plans.Get(key); ok {
		return result.(*engine.Plan), nil
	}
	if !e.normalize {
		plan, err := planbuilder.Build(sql, vcursor)
		if err != nil {
			return nil, err
		}
		if !skipQueryPlanCache {
			e.plans.Set(key, plan)
		}
		return plan, nil
	}
	// Normalize and retry.
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	sqlparser.Normalize(stmt, bindVars, "vtg")
	normalized := sqlparser.String(stmt)

	if logStats != nil {
		logStats.SQL = normalized + comments
		logStats.BindVariables = bindVars
	}

	normkey := normalized
	if keyspace != "" {
		normkey = keyspace + ":" + normalized
	}
	if result, ok := e.plans.Get(normkey); ok {
		return result.(*engine.Plan), nil
	}
	plan, err := planbuilder.BuildFromStmt(normalized, stmt, vcursor)
	if err != nil {
		return nil, err
	}
	if !skipQueryPlanCache {
		e.plans.Set(normkey, plan)
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
		fields[i] = &querypb.Field{Name: v, Type: sqltypes.VarChar}
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
