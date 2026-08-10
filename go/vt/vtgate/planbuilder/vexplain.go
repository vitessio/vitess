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
)

// checkVExplainMySQLSupported returns an error if any primitive in the tree cannot
// be handled by VEXPLAIN MYSQLPLAN. It is an allowlist: only primitives whose target
// shards can be resolved from a vindex without reading cluster data are permitted -
// a Route (with a resolvable vindex) and the shard-independent container primitives
// that pass their bind variables through to their inputs unchanged. DML is rejected
// with a SELECT-only message. Everything else - cross-shard joins, subqueries,
// recursive CTEs (whose child Routes are parameterized by rows produced at runtime)
// and lookup vindexes (which resolve shards by querying a lookup table) - is rejected
// and directed to VEXPLAIN ALL instead. Defaulting to reject keeps the check fail-closed
// as new primitive types are added.
func checkVExplainMySQLSupported(primitive engine.Primitive) error {
	switch prim := primitive.(type) {
	case *engine.Route:
		if prim.Vindex != nil && prim.Vindex.NeedsVCursor() {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLUnresolvableError)
		}
	case *engine.Concatenate,
		*engine.Distinct,
		*engine.Filter,
		*engine.Limit,
		*engine.MemorySort,
		*engine.MergeSort,
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
	case *engine.Insert, *engine.InsertSelect, *engine.Update, *engine.Delete, *engine.Upsert, *engine.DMLWithInput, *engine.FkCascade, *engine.FkVerify:
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vexplainMySQLDMLError)
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
