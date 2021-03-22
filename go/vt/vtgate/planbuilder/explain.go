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
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// Builds an explain-plan for the given Primitive
func buildExplainPlan(stmt sqlparser.Explain, reservedVars sqlparser.BindVars, vschema ContextVSchema) (engine.Primitive, error) {
	switch explain := stmt.(type) {
	case *sqlparser.ExplainTab:
		return explainTabPlan(explain, vschema)
	case *sqlparser.ExplainStmt:
		if explain.Type == sqlparser.VitessType {
			return buildVitessTypePlan(explain, reservedVars, vschema)
		}
		return buildOtherReadAndAdmin(sqlparser.String(explain), vschema)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected explain type: %T", stmt)
}

func explainTabPlan(explain *sqlparser.ExplainTab, vschema ContextVSchema) (engine.Primitive, error) {
	table, _, _, _, destination, err := vschema.FindTableOrVindex(explain.Table)
	if err != nil {
		return nil, err
	}
	explain.Table.Qualifier = sqlparser.NewTableIdent("")

	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	return &engine.Send{
		Keyspace:          table.Keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(explain),
		SingleShardOnly:   true,
	}, nil
}

func buildVitessTypePlan(explain *sqlparser.ExplainStmt, reservedVars sqlparser.BindVars, vschema ContextVSchema) (engine.Primitive, error) {
	innerInstruction, err := createInstructionFor(sqlparser.String(explain.Statement), explain.Statement, reservedVars, vschema)
	if err != nil {
		return nil, err
	}
	descriptions := treeLines(engine.PrimitiveToPlanDescription(innerInstruction))

	var rows [][]sqltypes.Value
	for _, line := range descriptions {
		var targetDest string
		if line.descr.TargetDestination != nil {
			targetDest = line.descr.TargetDestination.String()
		}
		keyspaceName := ""
		if line.descr.Keyspace != nil {
			keyspaceName = line.descr.Keyspace.Name
		}

		rows = append(rows, []sqltypes.Value{
			sqltypes.NewVarChar(line.header + line.descr.OperatorType), // operator
			sqltypes.NewVarChar(line.descr.Variant),                    // variant
			sqltypes.NewVarChar(keyspaceName),                          // keyspace
			sqltypes.NewVarChar(targetDest),                            // destination
			sqltypes.NewVarChar(line.descr.TargetTabletType.String()),  // tabletType
			sqltypes.NewVarChar(extractQuery(line.descr.Other)),        // query
		})
	}

	fields := []*querypb.Field{
		{Name: "operator", Type: querypb.Type_VARCHAR},
		{Name: "variant", Type: querypb.Type_VARCHAR},
		{Name: "keyspace", Type: querypb.Type_VARCHAR},
		{Name: "destination", Type: querypb.Type_VARCHAR},
		{Name: "tabletType", Type: querypb.Type_VARCHAR},
		{Name: "query", Type: querypb.Type_VARCHAR},
	}

	return engine.NewRowsPrimitive(rows, fields), nil
}

func extractQuery(m map[string]interface{}) string {
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
