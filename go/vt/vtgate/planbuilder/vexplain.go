/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"context"
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/dynamicconfig"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func buildVExplainPlan(
	ctx context.Context,
	vexplainStmt *sqlparser.VExplainStmt,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
	cfg dynamicconfig.DDL,
) (*planResult, error) {
	switch vexplainStmt.Type {
	case sqlparser.QueriesVExplainType, sqlparser.AllVExplainType:
		return buildVExplainLoggingPlan(ctx, vexplainStmt, reservedVars, vschema, cfg)
	case sqlparser.PlanVExplainType:
		return buildVExplainVtgatePlan(ctx, vexplainStmt.Statement, reservedVars, vschema, cfg)
	case sqlparser.TraceVExplainType:
		return buildVExplainTracePlan(ctx, vexplainStmt.Statement, reservedVars, vschema, cfg)
	case sqlparser.KeysVExplainType:
		return buildVExplainKeysPlan(vexplainStmt.Statement, vschema)
	case sqlparser.MySQLVExplainType:
		return buildVExplainMySQLPlan(ctx, vexplainStmt.Statement, reservedVars, vschema, cfg)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected vtexplain type: %s", vexplainStmt.Type.ToString())
}

// buildVExplainMySQLPlan builds the plan for VEXPLAIN MYSQLPLAN, which runs
// EXPLAIN FORMAT=JSON against the shards a query would target, without executing
// the query itself. It only supports read plans whose target shards can be resolved
// from a vindex without reading cluster data; any other plan is rejected here with a
// message pointing the user to VEXPLAIN ALL.
func buildVExplainMySQLPlan(ctx context.Context, explainStatement sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, cfg dynamicconfig.DDL) (*planResult, error) {
	// A DML statement can never plan to a Route or read Send whose shards resolve
	// from a vindex, so reject it up front with a SELECT-only message rather than
	// after planning. Checking the statement type here avoids a hand-maintained list
	// of DML engine primitives (Insert, Update, Upsert, FkCascade, ...) that would
	// drift as new ones are added.
	if sqlparser.IsDMLStatement(explainStatement) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLDMLError)
	}

	if err := checkVExplainMySQLAST(explainStatement); err != nil {
		return nil, err
	}

	if err := checkVExplainMySQLNoCalcFoundRows(explainStatement); err != nil {
		return nil, err
	}

	innerInstruction, err := createInstructionFor(ctx, sqlparser.String(explainStatement), explainStatement, reservedVars, vschema, cfg)
	if err != nil {
		return nil, err
	}

	if err := checkVExplainMySQLSupported(innerInstruction.primitive); err != nil {
		return nil, err
	}

	innerInstruction.primitive = &engine.VExplain{
		Input: innerInstruction.primitive,
		Type:  sqlparser.MySQLVExplainType,
	}
	return innerInstruction, nil
}

const (
	vexplainMySQLDMLError = "VEXPLAIN MYSQLPLAN only supports SELECT statements; use VEXPLAIN ALL instead"

	vexplainMySQLUnresolvableError = "VEXPLAIN MYSQLPLAN cannot resolve the target shards without executing the query " +
		"(the query uses a cross-shard join, subquery, or lookup vindex); use VEXPLAIN ALL instead"

	vexplainMySQLDerivedTableError = "VEXPLAIN MYSQLPLAN does not support derived tables or views, " +
		"because EXPLAIN FORMAT=JSON can materialize a derived table during optimization - running any stored function " +
		"inside it once per shard - which would violate MYSQLPLAN's promise never to run the wrapped query; use VEXPLAIN ALL instead"

	vexplainMySQLSequenceError = "VEXPLAIN MYSQLPLAN does not support sequence next value queries, " +
		"because the 'select next ... values' syntax is Vitess-specific and cannot be sent to MySQL as EXPLAIN"

	vexplainMySQLLockError = "VEXPLAIN MYSQLPLAN does not support advisory lock functions " +
		"(get_lock, release_lock, release_all_locks, is_free_lock, is_used_lock)"

	vexplainMySQLCalcFoundRowsError = "VEXPLAIN MYSQLPLAN does not support SELECT SQL_CALC_FOUND_ROWS with GROUP BY or HAVING, " +
		"because the planner rewrites the row count into a derived table that EXPLAIN could materialize; use VEXPLAIN ALL instead"
)

// checkVExplainMySQLAST rejects, in a single AST walk, the statement shapes that
// MYSQLPLAN cannot safely EXPLAIN. All must be caught on the original AST before
// planning, because each can otherwise reach a Route or Send that the primitive
// allowlist would accept.
//
// Two of these rejections - advisory lock functions and sequence next-value
// queries - must never point the user at VEXPLAIN ALL, because running the query
// there would acquire or release a lock, or consume a sequence value. The other
// two - derived tables/views and subqueries/CTEs - do recommend VEXPLAIN ALL. So
// when a query carries both kinds (e.g. a lock function beside or nested inside a
// subquery), the lock/sequence rejection must win, or the user would be steered to
// a VEXPLAIN ALL that runs the very construct MYSQLPLAN refused. The walk therefore
// aborts immediately on a lock or sequence node by returning its error, and only
// records the first derived-table/subquery rejection while continuing to descend,
// so a lock or sequence node anywhere in the tree still overrides it.
//
//   - Nested query blocks (subquery, derived table, CTE): EXPLAIN FORMAT=JSON
//     materializes a derived table during optimization - executing any stored
//     function inside it once per shard - which would violate MYSQLPLAN's promise
//     never to run the wrapped query. A CTE is inlined as a derived table during
//     planning, so it carries the same risk. A view reference is already rewritten
//     into a *sqlparser.DerivedTable by the normalizer before this walk runs, so it
//     is caught by the DerivedTable case; that case gets its own message that names
//     derived tables and views, because the generic unresolvable-shards message
//     (cross-shard join, subquery, lookup vindex) does not describe why an otherwise
//     -routable derived table or view is rejected. UNION is not a nested query block
//     (it is a *sqlparser.Union, not a Subquery/DerivedTable/With), so it is allowed.
//   - Sequence next-value queries: the Vitess-specific 'select next ... values'
//     syntax cannot be parsed by MySQL as EXPLAIN. An untargeted session plans this
//     as a Route with Opcode == Next, but a session with an explicit shard/keyrange
//     target routes through bypass planning as a read Send that the allowlist would
//     otherwise accept. The rejection does not point at VEXPLAIN ALL, which would
//     execute the query and consume sequence values.
//   - Advisory lock functions: such a SELECT plans as an engine.Lock primitive
//     rather than a Route or Send. The rejection does not point at VEXPLAIN ALL:
//     running the query would acquire or release advisory locks as a side effect.
//     The read-only variants (is_free_lock, is_used_lock) are rejected the same way
//     for a consistent message; they are equally unexplainable through MYSQLPLAN.
func checkVExplainMySQLAST(statement sqlparser.Statement) error {
	var recommendAllErr error
	unsafeErr := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node.(type) {
		case *sqlparser.Nextval:
			return false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLSequenceError)
		case *sqlparser.LockingFunc:
			return false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLLockError)
		case *sqlparser.DerivedTable:
			if recommendAllErr == nil {
				recommendAllErr = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLDerivedTableError)
			}
		case *sqlparser.Subquery, *sqlparser.With:
			if recommendAllErr == nil {
				recommendAllErr = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLUnresolvableError)
			}
		}
		return true, nil
	}, statement)
	if unsafeErr != nil {
		return unsafeErr
	}
	return recommendAllErr
}

// checkVExplainMySQLNoCalcFoundRows rejects a SELECT SQL_CALC_FOUND_ROWS with a
// LIMIT that also carries GROUP BY or HAVING. The nested-query walk above runs on
// the original AST, where no derived table is present, but for exactly this shape
// the planner rewrites the row-count half into `select count(*) from (select ...) as t`
// (see buildSQLCalcFoundRowsPlan). That derived-table query would ship to every shard
// inside EXPLAIN FORMAT=JSON, which can materialize the derived table during
// optimization - running any stored function inside it once per shard - violating
// MYSQLPLAN's promise never to run the wrapped query. Without a LIMIT the directive
// is ignored, and without GROUP BY/HAVING the count query reuses the original SELECT
// with a single count(*), so neither introduces a derived table. It must be caught on
// the AST before planning, since SQLCalcFoundRows is on the primitive allowlist.
func checkVExplainMySQLNoCalcFoundRows(statement sqlparser.Statement) error {
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil
	}
	if sel.SQLCalcFoundRows && sel.Limit != nil && (sel.GroupBy != nil || sel.Having != nil) {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLCalcFoundRowsError)
	}
	return nil
}

// checkVExplainMySQLSupported returns an error if any primitive in the tree cannot
// be handled by VEXPLAIN MYSQLPLAN. It is an allowlist: only primitives whose target
// shards can be resolved from a vindex without reading cluster data are permitted -
// a Route (with a resolvable vindex), a read Send (whose shards come from an explicit
// target destination) and the shard-independent container primitives that pass their
// bind variables through to their inputs unchanged. DML statements are already
// rejected up front by buildVExplainMySQLPlan, so only a DML/DDL bypass Send is
// rejected here with a SELECT-only message. Everything else - cross-shard joins,
// subqueries, recursive CTEs (whose child Routes are parameterized by rows produced
// at runtime) and lookup vindexes (which resolve shards by querying a lookup table) -
// is rejected and directed to VEXPLAIN ALL instead. Defaulting to reject keeps the
// check fail-closed as new primitive types are added.
func checkVExplainMySQLSupported(primitive engine.Primitive) error {
	switch prim := primitive.(type) {
	case *engine.Route:
		if prim.Vindex != nil && prim.Vindex.NeedsVCursor() {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLUnresolvableError)
		}
	case *engine.Send:
		// A read with an explicit shard/keyrange target (bypass planning) resolves
		// its shards from the target destination alone, so it is explainable. DML
		// and DDL sends are not SELECTs and are rejected.
		if prim.IsDML || prim.IsDDL {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLDMLError)
		}
	case *engine.Concatenate,
		*engine.Distinct,
		*engine.Filter,
		*engine.Limit,
		*engine.MemorySort,
		*engine.OrderedAggregate,
		*engine.Projection,
		*engine.RenameFields,
		*engine.Rows,
		*engine.ScalarAggregate,
		*engine.SimpleProjection,
		*engine.SingleRow,
		*engine.SQLCalcFoundRows:
		// Shard-independent container primitives: they forward bind variables to
		// their inputs unchanged, so shard resolution is unaffected. Fall through
		// to recurse into their inputs.
	default:
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLUnresolvableError)
	}

	inputs, _ := primitive.Inputs()
	for _, input := range inputs {
		if err := checkVExplainMySQLSupported(input); err != nil {
			return err
		}
	}
	return nil
}

func explainTabPlan(explain *sqlparser.ExplainTab, vschema plancontext.VSchema) (*planResult, error) {
	var keyspace *vindexes.Keyspace
	var dest key.ShardDestination

	if sqlparser.SystemSchema(explain.Table.Qualifier.String()) {
		var err error
		keyspace, err = vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
	} else {
		var tbl *vindexes.BaseTable
		var err error
		tbl, _, _, _, dest, err = vschema.FindTableOrVindex(explain.Table)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, vterrors.VT05004(explain.Table.Name.String())
		}
		keyspace = tbl.Keyspace
		explain.Table = sqlparser.NewTableName(tbl.Name.String())
	}

	if dest == nil {
		dest = key.DestinationAnyShard{}
	}

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: dest,
		Query:             sqlparser.String(explain),
		SingleShardOnly:   true,
	}, singleTable(keyspace.Name, explain.Table.Name.String())), nil
}

func buildVExplainVtgatePlan(ctx context.Context, explainStatement sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, cfg dynamicconfig.DDL) (*planResult, error) {
	innerInstruction, err := createInstructionFor(ctx, sqlparser.String(explainStatement), explainStatement, reservedVars, vschema, cfg)
	if err != nil {
		return nil, err
	}

	return getJsonResultPlan(
		engine.PrimitiveToPlanDescription(innerInstruction.primitive, nil),
		"JSON",
	)
}

// getJsonResultPlan marshals the given struct into a JSON string and returns it as a planResult.
func getJsonResultPlan(v any, colName string) (*planResult, error) {
	output, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil, err
	}
	fields := []*querypb.Field{{Name: colName, Type: querypb.Type_VARCHAR}}
	rows := []sqltypes.Row{{sqltypes.NewVarChar(string(output))}}
	return newPlanResult(engine.NewRowsPrimitive(rows, fields)), nil
}

func buildVExplainKeysPlan(statement sqlparser.Statement, vschema plancontext.VSchema) (*planResult, error) {
	ctx, err := plancontext.CreatePlanningContext(statement, sqlparser.NewReservedVars("", sqlparser.BindVars{}), vschema, querypb.ExecuteOptions_Gen4)
	if err != nil {
		return nil, err
	}
	result := operators.GetVExplainKeys(ctx, statement)
	return getJsonResultPlan(result, "ColumnUsage")
}

func buildVExplainLoggingPlan(ctx context.Context, explain *sqlparser.VExplainStmt, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, cfg dynamicconfig.DDL) (*planResult, error) {
	input, err := createInstructionFor(ctx, sqlparser.String(explain.Statement), explain.Statement, reservedVars, vschema, cfg)
	if err != nil {
		return nil, err
	}
	switch input.primitive.(type) {
	case *engine.Insert, *engine.Delete, *engine.Update:
		directives := explain.GetParsedComments().Directives()
		if !directives.IsSet(sqlparser.DirectiveVExplainRunDMLQueries) {
			return nil, vterrors.VT09008()
		}
	}

	return &planResult{primitive: &engine.VExplain{Input: input.primitive, Type: explain.Type}, tables: input.tables}, nil
}

// buildExplainStmtPlan takes an EXPLAIN query and if possible sends the whole query to a single shard
func buildExplainStmtPlan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	explain := stmt.(*sqlparser.ExplainStmt)
	switch explain.Statement.(type) {
	case sqlparser.SelectStatement, *sqlparser.Update, *sqlparser.Delete, *sqlparser.Insert:
		return explainPlan(explain, reservedVars, vschema)
	default:
		return buildOtherReadAndAdmin(sqlparser.String(explain), vschema)
	}
}

func explainPlan(explain *sqlparser.ExplainStmt, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	ctx, err := plancontext.CreatePlanningContext(explain.Statement, reservedVars, vschema, Gen4)
	if err != nil {
		return nil, err
	}

	ks := ctx.SemTable.SingleKeyspace()
	if ks == nil {
		return nil, vterrors.VT03031()
	}

	if err = queryRewrite(ctx, explain.Statement); err != nil {
		return nil, err
	}

	// Remove keyspace qualifier from columns and tables.
	sqlparser.RemoveKeyspace(explain.Statement)

	var tables []string
	for _, table := range ctx.SemTable.Tables {
		name, err := table.Name()
		if err != nil {
			// this is just for reporting which tables we are touching
			// it's OK to ignore errors here
			continue
		}
		tables = append(tables, operators.QualifiedString(ks, name.Name.String()))
	}

	return newPlanResult(&engine.Send{
		Keyspace:          ks,
		TargetDestination: key.DestinationAnyShard{},
		Query:             sqlparser.String(explain),
		SingleShardOnly:   true,
	}, tables...), nil
}

func buildVExplainTracePlan(ctx context.Context, explainStatement sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, cfg dynamicconfig.DDL) (*planResult, error) {
	innerInstruction, err := createInstructionFor(ctx, sqlparser.String(explainStatement), explainStatement, reservedVars, vschema, cfg)
	if err != nil {
		return nil, err
	}

	// We'll set the trace engine as the root primitive
	innerInstruction.primitive = &engine.VExplain{
		Input: innerInstruction.primitive,
		Type:  sqlparser.TraceVExplainType,
	}
	return innerInstruction, nil
}
