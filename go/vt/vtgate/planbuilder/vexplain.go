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
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// Builds an explain-plan for the given Primitive
func buildExplainPlan(ctx context.Context, stmt sqlparser.Explain, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	switch explain := stmt.(type) {
	case *sqlparser.ExplainTab:
		return explainTabPlan(explain, vschema)
	case *sqlparser.ExplainStmt:
		switch explain.Type {
		case sqlparser.VitessType:
			vschema.PlannerWarning("EXPLAIN FORMAT = VITESS is deprecated, please use VEXPLAIN PLAN instead.")
			return buildVExplainVtgatePlan(ctx, explain.Statement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
		case sqlparser.VTExplainType:
			vschema.PlannerWarning("EXPLAIN FORMAT = VTEXPLAIN is deprecated, please use VEXPLAIN QUERIES instead.")
			return buildVExplainLoggingPlan(ctx, &sqlparser.VExplainStmt{Type: sqlparser.QueriesVExplainType, Statement: explain.Statement, Comments: explain.Comments}, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
		default:
			return buildOtherReadAndAdmin(sqlparser.String(explain), vschema)
		}
	}
	return nil, vterrors.VT13001(fmt.Sprintf("unexpected explain type: %T", stmt))
}

func buildVExplainPlan(ctx context.Context, vexplainStmt *sqlparser.VExplainStmt, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	switch vexplainStmt.Type {
	case sqlparser.QueriesVExplainType, sqlparser.AllVExplainType:
		return buildVExplainLoggingPlan(ctx, vexplainStmt, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	case sqlparser.PlanVExplainType:
		return buildVExplainVtgatePlan(ctx, vexplainStmt.Statement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected vtexplain type: %s", vexplainStmt.Type.ToString())
}

func explainTabPlan(explain *sqlparser.ExplainTab, vschema plancontext.VSchema) (*planResult, error) {
	_, _, ks, _, destination, err := vschema.FindTableOrVindex(explain.Table)
	if err != nil {
		return nil, err
	}
	explain.Table.Qualifier = sqlparser.NewIdentifierCS("")

	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	keyspace, err := vschema.FindKeyspace(ks)
	if err != nil {
		return nil, err
	}
	if keyspace == nil {
		return nil, vterrors.VT14004(ks)
	}

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(explain),
		SingleShardOnly:   true,
	}, singleTable(keyspace.Name, explain.Table.Name.String())), nil
}

func buildVExplainVtgatePlan(ctx context.Context, explainStatement sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	innerInstruction, err := createInstructionFor(ctx, sqlparser.String(explainStatement), explainStatement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}
	description := engine.PrimitiveToPlanDescription(innerInstruction.primitive)
	output, err := json.MarshalIndent(description, "", "\t")
	if err != nil {
		return nil, err
	}
	fields := []*querypb.Field{
		{Name: "JSON", Type: querypb.Type_VARCHAR},
	}
	rows := []sqltypes.Row{
		{
			sqltypes.NewVarChar(string(output)),
		},
	}
	return newPlanResult(engine.NewRowsPrimitive(rows, fields)), nil
}

func buildVExplainLoggingPlan(ctx context.Context, explain *sqlparser.VExplainStmt, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	input, err := createInstructionFor(ctx, sqlparser.String(explain.Statement), explain.Statement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}
	switch input.primitive.(type) {
	case *engine.Insert, *engine.Delete, *engine.Update:
		directives := explain.GetParsedComments().Directives()
		if directives.IsSet(sqlparser.DirectiveVExplainRunDMLQueries) {
			break
		}
		return nil, vterrors.VT09008()
	}

	return &planResult{primitive: &engine.VExplain{Input: input.primitive, Type: explain.Type}, tables: input.tables}, nil
}
