/*
Copyright 2022 The Vitess Authors.

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

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	ExecuteEntry struct {
		ID        int
		Target    *querypb.Target
		Gateway   srvtopo.Gateway
		Query     string
		FiredFrom Primitive
	}

	VExplain struct {
		Input Primitive
		Type  sqlparser.VExplainType
	}

	ShardsQueried int
	RowsReceived  []int

	Stats struct {
		InterOpStats map[Primitive]RowsReceived
		ShardsStats  map[Primitive]ShardsQueried
	}

	// mysqlExplainTask is the set of EXPLAIN FORMAT=JSON queries to run against the
	// resolved shards of a single primitive (a Route or read Send). rss and queries
	// are aligned by index, one entry per targeted shard.
	mysqlExplainTask struct {
		primitive Primitive
		rss       []*srvtopo.ResolvedShard
		queries   []*querypb.BoundQuery
	}
)

// vexplainMySQLReservedConnError is returned when VEXPLAIN MYSQLPLAN runs in a
// session that holds a reserved connection (for example, one that has created a
// temporary table). Each EXPLAIN is issued on a fresh standalone connection that
// cannot see that session-local state, so the captured plan would not match the
// one the real query would use. VEXPLAIN ALL shares the same standalone-EXPLAIN
// path, so we do not point the user at it.
const vexplainMySQLReservedConnError = "VEXPLAIN MYSQLPLAN is not supported in a session that holds a reserved connection " +
	"(for example, one that has created a temporary table), because EXPLAIN runs on a separate connection that cannot see the session's temporary tables"

var _ Primitive = (*VExplain)(nil)

// GetFields implements the Primitive interface
func (v *VExplain) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	var fields []*querypb.Field
	switch v.Type {
	case sqlparser.QueriesVExplainType:
		fields = getVExplainQueriesFields()
	case sqlparser.AllVExplainType, sqlparser.MySQLVExplainType:
		fields = getVExplainAllFields()
	case sqlparser.TraceVExplainType:
		fields = getVExplainTraceFields()
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown type of VExplain plan")
	}
	return &sqltypes.Result{Fields: fields}, nil
}

func getVExplainTraceFields() []*querypb.Field {
	return []*querypb.Field{{
		Name:    "Trace",
		Type:    sqltypes.VarChar,
		Charset: uint32(collations.SystemCollation.Collation),
		Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
	}}
}

func getVExplainQueriesFields() []*querypb.Field {
	return []*querypb.Field{
		{Name: "#", Type: sqltypes.Int32},
		{Name: "keyspace", Type: sqltypes.VarChar},
		{Name: "shard", Type: sqltypes.VarChar},
		{Name: "query", Type: sqltypes.VarChar},
	}
}

func getVExplainAllFields() []*querypb.Field {
	return []*querypb.Field{{
		Name: "VExplain", Type: sqltypes.VarChar,
	}}
}

// NeedsTransaction implements the Primitive interface
func (v *VExplain) NeedsTransaction() bool {
	return v.Input.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (v *VExplain) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// MySQLPLAN resolves target shards and runs EXPLAIN against them without
	// executing the wrapped query, so we neither trace nor log nor run the input.
	if v.Type == sqlparser.MySQLVExplainType {
		return v.convertToVExplainMySQLResult(ctx, vcursor, bindVars)
	}
	var stats func() Stats
	if v.Type == sqlparser.TraceVExplainType {
		stats = vcursor.StartPrimitiveTrace()
	} else {
		vcursor.Session().VExplainLogging()
	}
	_, err := vcursor.ExecutePrimitive(ctx, v.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return v.convertToResult(ctx, vcursor, stats)
}

func noOpCallback(*sqltypes.Result) error {
	return nil
}

// TryStreamExecute implements the Primitive interface
func (v *VExplain) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// MySQLPLAN resolves target shards and runs EXPLAIN against them without
	// executing the wrapped query, so we neither trace nor log nor run the input.
	if v.Type == sqlparser.MySQLVExplainType {
		result, err := v.convertToVExplainMySQLResult(ctx, vcursor, bindVars)
		if err != nil {
			return err
		}
		return callback(result)
	}
	var stats func() Stats
	if v.Type == sqlparser.TraceVExplainType {
		stats = vcursor.StartPrimitiveTrace()
	} else {
		vcursor.Session().VExplainLogging()
	}

	err := vcursor.StreamExecutePrimitive(ctx, v.Input, bindVars, wantfields, noOpCallback)
	if err != nil {
		return err
	}
	result, err := v.convertToResult(ctx, vcursor, stats)
	if err != nil {
		return err
	}
	return callback(result)
}

func (v *VExplain) convertToResult(ctx context.Context, vcursor VCursor, stats func() Stats) (*sqltypes.Result, error) {
	switch v.Type {
	case sqlparser.QueriesVExplainType:
		result := convertToVExplainQueriesResult(vcursor.Session().GetVExplainLogs())
		return result, nil
	case sqlparser.AllVExplainType:
		return v.convertToVExplainAllResult(ctx, vcursor)
	case sqlparser.TraceVExplainType:
		return v.getExplainTraceOutput(stats)

	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown type of VExplain plan")
	}
}

func (v *VExplain) getExplainTraceOutput(getOpStats func() Stats) (*sqltypes.Result, error) {
	stats := getOpStats()
	description := PrimitiveToPlanDescription(v.Input, &stats)

	output, err := json.MarshalIndent(description, "", "\t")
	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{
		Fields: getVExplainTraceFields(),
		Rows: []sqltypes.Row{{
			sqltypes.NewVarChar(string(output)),
		}},
	}, nil
}

func (v *VExplain) convertToVExplainAllResult(ctx context.Context, vcursor VCursor) (*sqltypes.Result, error) {
	logEntries := vcursor.Session().GetVExplainLogs()
	explainResults := make(map[Primitive]string)
	for _, entry := range logEntries {
		if entry.Target == nil || entry.Gateway == nil || entry.FiredFrom == nil {
			continue
		}
		if explainResults[entry.FiredFrom] != "" {
			continue
		}
		explainQuery := fmt.Sprintf("explain format = json %v", entry.Query)
		// We rely on the parser to see if the query we have is explainable or not
		// If we get an error in parsing then we can't execute explain on the given query, and we skip it
		_, err := vcursor.Environment().Parser().Parse(explainQuery)
		if err != nil {
			continue
		}
		// Explain statement should now succeed
		res, err := vcursor.ExecuteStandalone(ctx, nil, explainQuery, nil, &srvtopo.ResolvedShard{
			Target:  entry.Target,
			Gateway: entry.Gateway,
		}, false)
		if err != nil {
			return nil, err
		}
		explainResults[entry.FiredFrom] = res.Rows[0][0].ToString()
	}

	planDescription := primitiveToPlanDescriptionWithSQLResults(v.Input, explainResults)
	resultBytes, err := json.MarshalIndent(planDescription, "", "\t")
	if err != nil {
		return nil, err
	}

	result := string(resultBytes)

	rows := []sqltypes.Row{
		{
			sqltypes.NewVarChar(result),
		},
	}
	qr := &sqltypes.Result{
		Fields: getVExplainAllFields(),
		Rows:   rows,
	}
	return qr, nil
}

// convertToVExplainMySQLResult resolves the target shards of each Route in the
// plan and runs EXPLAIN FORMAT=JSON against every resolved shard, without
// executing the wrapped query. The MySQL EXPLAIN output is attached to each Route
// node keyed by shard, so per-shard plan and cost differences are visible.
func (v *VExplain) convertToVExplainMySQLResult(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if vcursor.Session().InReservedConn() {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLReservedConnError)
	}

	var tasks []mysqlExplainTask
	if err := v.collectMySQLExplainTasks(ctx, vcursor, v.Input, bindVars, &tasks); err != nil {
		return nil, err
	}

	explainResults, err := runMySQLExplainTasks(ctx, vcursor, tasks)
	if err != nil {
		return nil, err
	}

	planDescription := primitiveToPlanDescriptionWithShardedSQLResults(v.Input, explainResults)
	resultBytes, err := json.MarshalIndent(planDescription, "", "\t")
	if err != nil {
		return nil, err
	}

	rows := []sqltypes.Row{{sqltypes.NewVarChar(string(resultBytes))}}
	return &sqltypes.Result{
		Fields: getVExplainAllFields(),
		Rows:   rows,
	}, nil
}

// collectMySQLExplainTasks walks the primitive tree and, for each Route or read
// Send, resolves its target shards and appends one EXPLAIN task covering all of
// that primitive's shards. Shard resolution runs serially here (it is cheap and,
// for a pushed-down Limit, must happen before its child Route is visited); the
// EXPLAIN queries themselves are run concurrently by runMySQLExplainTasks.
func (v *VExplain) collectMySQLExplainTasks(ctx context.Context, vcursor VCursor, primitive Primitive, bindVars map[string]*querypb.BindVariable, tasks *[]mysqlExplainTask) error {
	switch prim := primitive.(type) {
	case *Route:
		rss, bvs, err := prim.findRoute(ctx, vcursor, bindVars)
		if err != nil {
			return err
		}
		// Mirror executeShards: when routing resolves no shard but the Route is
		// marked for no-routes special handling (e.g. an aggregate SELECT whose
		// predicate maps to no shard), the real query would still be sent to an
		// arbitrary shard. Fall back to anyShard so EXPLAIN reflects that.
		if len(rss) == 0 && prim.NoRoutesSpecialHandling {
			rss, bvs, err = prim.anyShard(ctx, vcursor, bindVars)
			if err != nil {
				return err
			}
		}
		appendMySQLExplainTasks(tasks, prim, rss, getQueries(prim.Query, bvs))
	case *Send:
		// A read Send targets an explicit shard/keyrange destination, so its shards
		// resolve without executing the query. DML/DDL sends are rejected at plan
		// time, so anything reaching here is a read.
		rss, _, err := vcursor.ResolveDestinations(ctx, prim.Keyspace.Name, nil, []key.ShardDestination{prim.TargetDestination})
		if err != nil {
			return err
		}
		queries := make([]*querypb.BoundQuery, len(rss))
		for i := range rss {
			queries[i] = &querypb.BoundQuery{Sql: prim.Query, BindVariables: bindVars}
		}
		appendMySQLExplainTasks(tasks, prim, rss, queries)
	case *Limit:
		// A pushed-down scatter limit rewrites its child Route's query to use
		// :__upper_limit, which Limit.TryExecute computes before executing its
		// input. Compute it here too so the child Route's EXPLAIN can bind it.
		count, offset, err := prim.getCountAndOffset(ctx, vcursor, bindVars)
		if err != nil {
			return err
		}
		bindVars = copyBindVars(bindVars)
		bindVars[UpperLimitStr] = sqltypes.Int64BindVariable(int64(count + offset))
	}

	inputs, _ := primitive.Inputs()
	for _, input := range inputs {
		// Each input gets its own copy: a Route's findRoute mutates the map in
		// place (e.g. DBA/information_schema routes populate schema/table
		// replacement bind vars), and the resulting task keeps that map pointer.
		// Sharing one map across sibling inputs (e.g. the arms of a Concatenate)
		// would let a later sibling overwrite an earlier task's bindings before
		// the EXPLAINs run. This mirrors Concatenate's per-source copyBindVars.
		if err := v.collectMySQLExplainTasks(ctx, vcursor, input, copyBindVars(bindVars), tasks); err != nil {
			return err
		}
	}
	return nil
}

// appendMySQLExplainTasks records one EXPLAIN task for the given primitive,
// wrapping each shard's query in EXPLAIN FORMAT=JSON. rss and queries are
// aligned by index.
func appendMySQLExplainTasks(tasks *[]mysqlExplainTask, primitive Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery) {
	if len(rss) == 0 {
		return
	}
	explainQueries := make([]*querypb.BoundQuery, len(queries))
	for i, q := range queries {
		explainQueries[i] = &querypb.BoundQuery{
			Sql:           "explain format = json " + q.Sql,
			BindVariables: q.BindVariables,
		}
	}
	*tasks = append(*tasks, mysqlExplainTask{primitive: primitive, rss: rss, queries: explainQueries})
}

// runMySQLExplainTasks runs each task's EXPLAIN FORMAT=JSON queries against its
// shards and returns the per-shard results keyed by shard against the primitive
// that owns each. Each task fans out through the VCursor's per-shard executor,
// which reuses the normal scatter machinery: the shard queries run concurrently,
// every targeted shard is counted in ShardQueries regardless of per-shard
// outcome, and a failure against any shard fails the whole command. The per-shard
// executor is an optional VCursor capability; a VCursor that does not implement it
// yields no MySQL EXPLAIN output.
func runMySQLExplainTasks(ctx context.Context, vcursor VCursor, tasks []mysqlExplainTask) (map[Primitive]map[string]json.RawMessage, error) {
	explainResults := make(map[Primitive]map[string]json.RawMessage)
	if len(tasks) == 0 {
		return explainResults, nil
	}
	executor, ok := vcursor.(MultiShardPerShardExecutor)
	if !ok {
		return explainResults, nil
	}

	for _, task := range tasks {
		results, errs := executor.ExecuteMultiShardPerShard(ctx, task.primitive, task.rss, task.queries)
		if err := vterrors.Aggregate(errs); err != nil {
			return nil, err
		}
		for i, res := range results {
			if res == nil || len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
				// EXPLAIN FORMAT=JSON always returns one row of one column, and a
				// per-shard failure would already have aborted above, so an empty
				// result here is anomalous. Warn rather than silently omitting the
				// shard from the output map.
				log.Warn("VEXPLAIN MYSQLPLAN got an empty EXPLAIN result; omitting shard from output",
					slog.String("keyspace", task.rss[i].Target.Keyspace),
					slog.String("shard", task.rss[i].Target.Shard))
				continue
			}
			perShard := explainResults[task.primitive]
			if perShard == nil {
				perShard = make(map[string]json.RawMessage)
				explainResults[task.primitive] = perShard
			}
			perShard[task.rss[i].Target.Shard] = json.RawMessage(res.Rows[0][0].ToString())
		}
	}
	return explainResults, nil
}

// primitiveToPlanDescriptionWithShardedSQLResults transforms a primitive tree
// into a corresponding PlanDescription tree, attaching the per-shard MySQL
// EXPLAIN output (keyed by shard) to the matching Route nodes.
func primitiveToPlanDescriptionWithShardedSQLResults(in Primitive, res map[Primitive]map[string]json.RawMessage) PrimitiveDescription {
	return primitiveToPlanDescriptionWith(in, func(prim Primitive, pd *PrimitiveDescription) {
		if perShard, found := res[prim]; found {
			pd.Other["mysql_explain_json_by_shard"] = perShard
		}
	})
}

// primitiveToPlanDescriptionWithSQLResults transforms a primitive tree into a
// corresponding PlanDescription tree, attaching the given per-primitive MySQL
// EXPLAIN output to the matching nodes.
func primitiveToPlanDescriptionWithSQLResults(in Primitive, res map[Primitive]string) PrimitiveDescription {
	return primitiveToPlanDescriptionWith(in, func(prim Primitive, pd *PrimitiveDescription) {
		if v, found := res[prim]; found {
			pd.Other["mysql_explain_json"] = json.RawMessage(v)
		}
	})
}

// primitiveToPlanDescriptionWith walks the primitive tree into a PlanDescription
// tree, calling attach on each node so the caller can decorate it with its own
// EXPLAIN output (which differs in shape between VEXPLAIN ALL and MYSQLPLAN).
func primitiveToPlanDescriptionWith(in Primitive, attach func(Primitive, *PrimitiveDescription)) PrimitiveDescription {
	this := in.description()

	attach(in, &this)

	inputs, infos := in.Inputs()
	for idx, input := range inputs {
		pd := primitiveToPlanDescriptionWith(input, attach)
		if infos != nil {
			for k, v := range infos[idx] {
				if k == inputName {
					pd.InputName = v.(string)
					continue
				}
				pd.Other[k] = v
			}
		}
		this.Inputs = append(this.Inputs, pd)
	}

	if len(inputs) == 0 {
		this.Inputs = []PrimitiveDescription{}
	}

	return this
}

func convertToVExplainQueriesResult(logs []ExecuteEntry) *sqltypes.Result {
	qr := &sqltypes.Result{
		Fields: getVExplainQueriesFields(),
	}
	for _, line := range logs {
		qr.Rows = append(qr.Rows, sqltypes.Row{
			sqltypes.NewInt32(int32(line.ID)),
			sqltypes.NewVarChar(line.Target.Keyspace),
			sqltypes.NewVarChar(line.Target.Shard),
			sqltypes.NewVarChar(line.Query),
		})
	}
	return qr
}

// Inputs implements the Primitive interface
func (v *VExplain) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{v.Input}, nil
}

func (v *VExplain) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "VEXPLAIN",
		Other:        map[string]any{"Type": v.Type.ToString()},
	}
}
