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
	"fmt"
	"sort"
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

	var rows [][]string
	columns := []string{"operator", "variant", "keyspace", "destination", "query", "other"}
	
	// this variable is used to filter out columns that have no content
	hasContent := make([]bool, len(columns))
	
	for _, line := range descriptions {
		var targetDest string
		if line.descr.TargetDestination != nil {
			targetDest = line.descr.TargetDestination.String()
		}
		keyspaceName := ""
		if line.descr.Keyspace != nil {
			keyspaceName = line.descr.Keyspace.Name
		}

		other, err := commaSeparatedString(line.descr.Other)
		if err != nil {
			return nil, err
		}

		row := []string{
			line.header + line.descr.OperatorType, // operator
			line.descr.Variant,                    // variant
			keyspaceName,                          // keyspace
			targetDest,                            // destination
			extractQuery(line.descr.Other),        // query
			other,                                 // other
		}

		for i, s := range row {
			if s != "" {
				hasContent[i] = true
			}
		}

		rows = append(rows, row)
	}

	return engine.NewRowsPrimitive(filterOutColumns(rows, hasContent), filterOutFields(columns, hasContent)), nil
}

func filterOutFields(columns []string, hasContent []bool) []*querypb.Field {
	var fields []*querypb.Field
	for i, c := range columns {
		if hasContent[i] {
			fields = append(fields, &querypb.Field{Name: c, Type: querypb.Type_VARCHAR})
		}
	}
	return fields
}

func filterOutColumns(rowsIn [][]string, hasContent []bool) [][]sqltypes.Value {
	var outputRows [][]sqltypes.Value
	for _, currentRow := range rowsIn {
		var currentOut []sqltypes.Value
		for i, value := range currentRow {
			if hasContent[i] {
				currentOut = append(currentOut, sqltypes.NewVarChar(value))
			}
		}
		outputRows = append(outputRows, currentOut)
	}
	return outputRows
}

func commaSeparatedString(other map[string]interface{}) (string, error) {
	var vals []string
	for k, v := range other {
		value, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		vals = append(vals, fmt.Sprintf("%s=%s", k, string(value)))
	}
	sort.Strings(vals)
	return strings.Join(vals, ","), nil
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
