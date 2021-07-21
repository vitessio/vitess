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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func planGroupBy(pb *primitiveBuilder, input logicalPlan, groupBy sqlparser.GroupBy) (logicalPlan, error) {
	if len(groupBy) == 0 {
		// if we have no grouping declared, we only want to visit orderedAggregate
		_, isOrdered := input.(*orderedAggregate)
		if !isOrdered {
			return input, nil
		}
	}

	switch node := input.(type) {
	case *mergeSort, *pulloutSubquery, *distinct:
		inputs := node.Inputs()
		input := inputs[0]

		newInput, err := planGroupBy(pb, input, groupBy)
		if err != nil {
			return nil, err
		}
		inputs[0] = newInput
		err = node.Rewrite(inputs...)
		if err != nil {
			return nil, err
		}
		return node, nil
	case *route:
		node.Select.(*sqlparser.Select).GroupBy = groupBy
		return node, nil
	case *orderedAggregate:
		for _, expr := range groupBy {
			colNumber := -1
			switch e := expr.(type) {
			case *sqlparser.ColName:
				c := e.Metadata.(*column)
				if c.Origin() == node {
					return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongGroupField, "group by expression cannot reference an aggregate function: %v", sqlparser.String(e))
				}
				for i, rc := range node.resultColumns {
					if rc.column == c {
						colNumber = i
						break
					}
				}
				if colNumber == -1 {
					return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: group by column must reference column in SELECT list")
				}
			case *sqlparser.Literal:
				num, err := ResultFromNumber(node.resultColumns, e)
				if err != nil {
					return nil, err
				}
				colNumber = num
			default:
				return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: only simple references allowed")
			}
			node.eaggr.Keys = append(node.eaggr.Keys, colNumber)
		}
		// Append the distinct aggregate if any.
		if node.extraDistinct != nil {
			groupBy = append(groupBy, node.extraDistinct)
		}

		newInput, err := planGroupBy(pb, node.input, groupBy)
		if err != nil {
			return nil, err
		}
		node.input = newInput

		return node, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable %T.groupBy: ", input)
}

// planDistinct makes the output distinct
func planDistinct(input logicalPlan) (logicalPlan, error) {
	switch node := input.(type) {
	case *route:
		node.Select.MakeDistinct()
		return node, nil
	case *orderedAggregate:
		for i, rc := range node.resultColumns {
			// If the column origin is oa (and not the underlying route),
			// it means that it's an aggregate function supplied by oa.
			// So, the distinct 'operator' cannot be pushed down into the
			// route.
			if rc.column.Origin() == node {
				return newDistinct(node), nil
			}
			node.eaggr.Keys = append(node.eaggr.Keys, i)
		}
		newInput, err := planDistinct(node.input)
		if err != nil {
			return nil, err
		}
		node.input = newInput
		return node, nil

	case *distinct:
		return input, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable %T.distinct", input)
}
