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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// logicalPlan defines the interface that a primitive must
// satisfy.
type logicalPlan interface {
	// Order is the execution order of the primitive. If there are subprimitives,
	// the order is one above the order of the subprimitives.
	// This is because the primitive executes its subprimitives first and
	// processes their results to generate its own values.
	// Please copy code from an existing primitive to define this function.
	Order() int

	// ResultColumns returns the list of result columns the
	// primitive returns.
	// Please copy code from an existing primitive to define this function.
	ResultColumns() []*resultColumn

	// Reorder reassigns order for the primitive and its sub-primitives.
	// The input is the order of the previous primitive that should
	// execute before this one.
	Reorder(int)

	// Wireup performs the wire-up work. Nodes should be traversed
	// from right to left because the rhs nodes can request vars from
	// the lhs nodes.
	Wireup(bldr logicalPlan, jt *jointab) error

	// SupplyVar finds the common root between from and to. If it's
	// the common root, it supplies the requested var to the rhs tree.
	// If the primitive already has the column in its list, it should
	// just supply it to the 'to' node. Otherwise, it should request
	// for it by calling SupplyCol on the 'from' sub-tree to request the
	// column, and then supply it to the 'to' node.
	SupplyVar(from, to int, col *sqlparser.ColName, varname string)

	// SupplyCol is meant to be used for the wire-up process. This function
	// changes the primitive to supply the requested column and returns
	// the resultColumn and column number of the result. SupplyCol
	// is different from PushSelect because it may reuse an existing
	// resultColumn, whereas PushSelect guarantees the addition of a new
	// result column and returns a distinct symbol for it.
	SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int)

	// SupplyWeightString must supply a weight_string expression of the
	// specified column.
	SupplyWeightString(colNumber int) (weightcolNumber int, err error)

	// Primitive returns the underlying primitive.
	// This function should only be called after Wireup is finished.
	Primitive() engine.Primitive

	// Rewrite replaces the inputs on the buider with new ones
	Rewrite(inputs ...logicalPlan) error

	Inputs() []logicalPlan
}

type builderVisitor func(logicalPlan) (bool, logicalPlan, error)

func visit(node logicalPlan, visitor builderVisitor) (logicalPlan, error) {
	if visitor != nil {
		kontinue, newNode, err := visitor(node)
		if err != nil {
			return nil, err
		}
		if !kontinue {
			return newNode, nil
		}
		node = newNode
	}
	inputs := node.Inputs()
	for i, input := range inputs {
		newInput, err := visit(input, visitor)
		if err != nil {
			return nil, err
		}
		inputs[i] = newInput
	}
	err := node.Rewrite(inputs...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// First returns the first logical plan of the tree,
// which is usually the left most leaf.
func First(input logicalPlan) logicalPlan {
	inputs := input.Inputs()
	if len(inputs) == 0 {
		return input
	}
	return First(inputs[0])
}
