/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
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
	Wireup(lp logicalPlan, jt *jointab) error

	// WireupV4 does the wire up work for the V4 planner
	WireupV4(semTable *semantics.SemTable) error

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
	// specified column. It returns an error if we cannot supply a weight column for it.
	SupplyWeightString(colNumber int) (weightcolNumber int, err error)

	// Primitive returns the underlying primitive.
	// This function should only be called after Wireup is finished.
	Primitive() engine.Primitive

	// Inputs are the children of this plan
	Inputs() []logicalPlan

	// Rewrite replaces the inputs of this plan with the ones provided
	Rewrite(inputs ...logicalPlan) error

	// ContainsTables keeps track which query tables are being solved by this logical plan
	// This is only applicable for plans that have been built with the V4 planner
	ContainsTables() semantics.TableSet
}

//-------------------------------------------------------------------------

type planVisitor func(logicalPlan) (bool, logicalPlan, error)

func visit(node logicalPlan, visitor planVisitor) (logicalPlan, error) {
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

//-------------------------------------------------------------------------

// logicalPlanCommon implements some common functionality of builders.
// Make sure to override in case behavior needs to be changed.
type logicalPlanCommon struct {
	order int
	input logicalPlan
}

func newBuilderCommon(input logicalPlan) logicalPlanCommon {
	return logicalPlanCommon{input: input}
}

func (bc *logicalPlanCommon) Order() int {
	return bc.order
}

func (bc *logicalPlanCommon) Reorder(order int) {
	bc.input.Reorder(order)
	bc.order = bc.input.Order() + 1
}

func (bc *logicalPlanCommon) ResultColumns() []*resultColumn {
	return bc.input.ResultColumns()
}

func (bc *logicalPlanCommon) Wireup(plan logicalPlan, jt *jointab) error {
	return bc.input.Wireup(plan, jt)
}

func (bc *logicalPlanCommon) WireupV4(semTable *semantics.SemTable) error {
	return bc.input.WireupV4(semTable)
}

func (bc *logicalPlanCommon) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	bc.input.SupplyVar(from, to, col, varname)
}

func (bc *logicalPlanCommon) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	return bc.input.SupplyCol(col)
}

func (bc *logicalPlanCommon) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	return bc.input.SupplyWeightString(colNumber)
}

// Rewrite implements the logicalPlan interface
func (bc *logicalPlanCommon) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "builderCommon: wrong number of inputs")
	}
	bc.input = inputs[0]
	return nil
}

// Inputs implements the logicalPlan interface
func (bc *logicalPlanCommon) Inputs() []logicalPlan {
	return []logicalPlan{bc.input}
}

// Solves implements the logicalPlan interface
func (bc *logicalPlanCommon) ContainsTables() semantics.TableSet {
	return bc.input.ContainsTables()
}

//-------------------------------------------------------------------------

// resultsBuilder is a superset of logicalPlanCommon. It also handles
// resultsColumn functionality.
type resultsBuilder struct {
	logicalPlanCommon
	resultColumns []*resultColumn
	weightStrings map[*resultColumn]int
	truncater     truncater
}

func newResultsBuilder(input logicalPlan, truncater truncater) resultsBuilder {
	return resultsBuilder{
		logicalPlanCommon: newBuilderCommon(input),
		resultColumns:     input.ResultColumns(),
		weightStrings:     make(map[*resultColumn]int),
		truncater:         truncater,
	}
}

func (rsb *resultsBuilder) ResultColumns() []*resultColumn {
	return rsb.resultColumns
}

// SupplyCol is currently unreachable because the builders using resultsBuilder
// are currently above a join, which is the only logicalPlan that uses it for now.
// This can change if we start supporting correlated subqueries.
func (rsb *resultsBuilder) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	c := col.Metadata.(*column)
	for i, rc := range rsb.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}
	rc, colNumber = rsb.input.SupplyCol(col)
	if colNumber < len(rsb.resultColumns) {
		return rc, colNumber
	}
	// Add result columns from input until colNumber is reached.
	for colNumber >= len(rsb.resultColumns) {
		rsb.resultColumns = append(rsb.resultColumns, rsb.input.ResultColumns()[len(rsb.resultColumns)])
	}
	rsb.truncater.SetTruncateColumnCount(len(rsb.resultColumns))
	return rc, colNumber
}

func (rsb *resultsBuilder) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	rc := rsb.resultColumns[colNumber]
	if weightcolNumber, ok := rsb.weightStrings[rc]; ok {
		return weightcolNumber, nil
	}
	weightcolNumber, err = rsb.input.SupplyWeightString(colNumber)
	if err != nil {
		return 0, nil
	}
	rsb.weightStrings[rc] = weightcolNumber
	if weightcolNumber < len(rsb.resultColumns) {
		return weightcolNumber, nil
	}
	// Add result columns from input until weightcolNumber is reached.
	for weightcolNumber >= len(rsb.resultColumns) {
		rsb.resultColumns = append(rsb.resultColumns, rsb.input.ResultColumns()[len(rsb.resultColumns)])
	}
	rsb.truncater.SetTruncateColumnCount(len(rsb.resultColumns))
	return weightcolNumber, nil
}

//-------------------------------------------------------------------------
