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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// logicalPlan defines the interface that a primitive must
// satisfy.
type logicalPlan interface {

	// Wireup does the wire up of primitive with the source.
	Wireup(*plancontext.PlanningContext) error

	// Primitive returns the underlying primitive.
	// This function should only be called after Wireup is finished.
	Primitive() engine.Primitive

	// Inputs are the children of this plan
	Inputs() []logicalPlan

	// Rewrite replaces the inputs of this plan with the ones provided
	Rewrite(inputs ...logicalPlan) error

	// ContainsTables keeps track which query tables are being solved by this logical plan
	// This is only applicable for plans that have been built with the Gen4 planner
	ContainsTables() semantics.TableSet

	// OutputColumns shows the columns that this plan will produce
	OutputColumns() []sqlparser.SelectExpr
}

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
	rewrite := false
	for i, input := range inputs {
		newInput, err := visit(input, visitor)
		if err != nil {
			return nil, err
		}
		if newInput != input {
			rewrite = true
		}
		inputs[i] = newInput
	}
	if rewrite {
		err := node.Rewrite(inputs...)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// -------------------------------------------------------------------------

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

func (bc *logicalPlanCommon) Wireup(ctx *plancontext.PlanningContext) error {
	return bc.input.Wireup(ctx)
}

// Rewrite implements the logicalPlan interface
func (bc *logicalPlanCommon) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.VT13001(fmt.Sprintf("builderCommon: wrong number of inputs, got: %d, expect: 1", len(inputs)))
	}
	bc.input = inputs[0]
	return nil
}

// Inputs implements the logicalPlan interface
func (bc *logicalPlanCommon) Inputs() []logicalPlan {
	return []logicalPlan{bc.input}
}

// ContainsTables implements the logicalPlan interface
func (bc *logicalPlanCommon) ContainsTables() semantics.TableSet {
	return bc.input.ContainsTables()
}

// OutputColumns implements the logicalPlan interface
func (bc *logicalPlanCommon) OutputColumns() []sqlparser.SelectExpr {
	return bc.input.OutputColumns()
}

// -------------------------------------------------------------------------

// resultsBuilder is a superset of logicalPlanCommon. It also handles
// resultsColumn functionality.
type resultsBuilder struct {
	logicalPlanCommon
	truncater truncater
}

func newResultsBuilder(input logicalPlan, truncater truncater) resultsBuilder {
	return resultsBuilder{
		logicalPlanCommon: newBuilderCommon(input),
		truncater:         truncater,
	}
}
