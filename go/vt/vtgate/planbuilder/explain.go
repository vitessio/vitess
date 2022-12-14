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
	"encoding/json"
	"strings"

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
func buildExplainPlan(stmt sqlparser.Explain, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	switch explain := stmt.(type) {
	case *sqlparser.ExplainTab:
		return explainTabPlan(explain, vschema)
	case *sqlparser.ExplainStmt:
		switch explain.Type {
		case sqlparser.VitessType:
			vschema.PlannerWarning("EXPLAIN FORMAT = VITESS is deprecated, please use VEXPLAIN PLAN instead.")
			return buildVExplainVtgatePlan(explain.Statement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
		case sqlparser.VTExplainType:
			vschema.PlannerWarning("EXPLAIN FORMAT = VTEXPLAIN is deprecated, please use VEXPLAIN QUERIES instead.")
			return buildVExplainQueriesPlan(&sqlparser.VExplainStmt{Type: sqlparser.QueriesVExplainType, Statement: explain.Statement, Comments: explain.Comments}, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
		default:
			return buildOtherReadAndAdmin(sqlparser.String(explain), vschema)
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected explain type: %T", stmt)
}

func buildVExplainPlan(vexplainStmt *sqlparser.VExplainStmt, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	switch vexplainStmt.Type {
	case sqlparser.QueriesVExplainType:
		return buildVExplainQueriesPlan(vexplainStmt, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	case sqlparser.PlanVExplainType:
		return buildVExplainVtgatePlan(vexplainStmt.Statement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	case sqlparser.AllVExplainType:
		// TODO
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
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "Cannot find keyspace for: %s", ks)
	}

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(explain),
		SingleShardOnly:   true,
	}, singleTable(keyspace.Name, explain.Table.Name.String())), nil
}

func buildVExplainVtgatePlan(explainStatement sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	innerInstruction, err := createInstructionFor(sqlparser.String(explainStatement), explainStatement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
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

func buildVExplainQueriesPlan(explain *sqlparser.VExplainStmt, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	input, err := createInstructionFor(sqlparser.String(explain.Statement), explain.Statement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}
	switch input.primitive.(type) {
	case *engine.Insert, *engine.Delete, *engine.Update:
		directives := explain.GetParsedComments().Directives()
		if directives.IsSet(sqlparser.DirectiveVtexplainRunDMLQueries) {
			break
		}
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vexplain queries will actually run queries. `/*vt+ %s */` must be set to run DML queries in vtexplain. Example: `vexplain /*vt+ %s */ queries delete from t1`", sqlparser.DirectiveVtexplainRunDMLQueries, sqlparser.DirectiveVtexplainRunDMLQueries)
	}

	return &planResult{primitive: &engine.VTExplain{Input: input.primitive, Type: explain.Type}, tables: input.tables}, nil
}

func extractQuery(m map[string]any) string {
	queryObj, ok := m["Query"]
	if !ok {
		return ""
	}
	query, ok := queryObj.(string)
	if !ok {
		return ""
	}

	return query
}

type description struct {
	header string
	descr  engine.PrimitiveDescription
}

func treeLines(root engine.PrimitiveDescription) []description {
	l := len(root.Inputs) - 1
	output := []description{{
		header: "",
		descr:  root,
	}}
	for i, child := range root.Inputs {
		childLines := treeLines(child)
		var header string
		var lastHdr string
		if i == l {
			header = "└─" + " "
			lastHdr = strings.Repeat(" ", 3)
		} else {
			header = "├─" + " "
			lastHdr = "│" + strings.Repeat(" ", 2)
		}

		for x, childLine := range childLines {
			if x == 0 {
				childLine.header = header + childLine.header
			} else {
				childLine.header = lastHdr + childLine.header
			}

			output = append(output, childLine)
		}
	}
	return output
}
