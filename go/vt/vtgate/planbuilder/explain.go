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
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

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

// Builds an explain-plan for the given Primitive
func buildExplainPlan(input engine.Primitive) (engine.Primitive, error) {
	descriptions := treeLines(engine.PrimitiveToPlanDescription(input))

	var rows [][]sqltypes.Value
	for _, line := range descriptions {
		var targetDest string
		if line.descr.TargetDestination != nil {
			targetDest = line.descr.TargetDestination.String()
		}
		rows = append(rows, []sqltypes.Value{
			sqltypes.NewVarChar(line.header + line.descr.OperatorType),
			sqltypes.NewVarChar(line.descr.Variant),
			sqltypes.NewVarChar(line.descr.Keyspace.Name),
			sqltypes.NewVarChar(targetDest),
			sqltypes.NewVarChar(line.descr.TargetTabletType.String()),
			sqltypes.NewVarChar(extractQuery(line.descr.Other)),
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
