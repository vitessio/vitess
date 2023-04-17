/*
Copyright 2021 The Vitess Authors.

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

// Package operators contains the operators used to plan queries.
/*
The operators go through a few phases while planning:
1. Logical
   In this first pass, we build an operator tree from the incoming parsed query.
   It will contain logical joins - we still haven't decided on the join algorithm to use yet.
   At the leaves, it will contain QueryGraphs - these are the tables in the FROM clause
   that we can easily do join ordering on. The logical tree will represent the full query,
   including projections, grouping, ordering and so on.
2. Physical
   Once the logical plan has been fully built, we go bottom up and plan which routes that will be used.
   During this phase, we will also decide which join algorithms should be used on the vtgate level
3. Columns & Aggregation
   Once we know which queries will be sent to the tablets, we go over the tree and decide which
   columns each operator should output. At this point, we also do offset lookups,
   so we know at runtime from which columns in the input table we need to read.
*/
package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// helper type that implements Inputs() returning nil
	noInputs struct{}

	// helper type that implements AddColumn() returning an error
	noColumns struct{}

	// helper type that implements AddPredicate() returning an error
	noPredicates struct{}
)

// PlanQuery creates a query plan for a given SQL statement
func PlanQuery(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (ops.Operator, error) {
	op, err := createLogicalOperatorFromAST(ctx, selStmt)
	if err != nil {
		return nil, err
	}

	if err = checkValid(op); err != nil {
		return nil, err
	}

	if op, err = transformToPhysical(ctx, op); err != nil {
		return nil, err
	}

	if op, err = tryHorizonPlanning(ctx, op); err != nil {
		return nil, err
	}

	if op, err = compact(ctx, op); err != nil {
		return nil, err
	}

	_, isRoute := op.(*Route)
	if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
		// If we got here, we don't have a single shard plan
		return nil, ctx.SemTable.NotSingleRouteErr
	}

	return op, err
}

func tryHorizonPlanning(ctx *plancontext.PlanningContext, op ops.Operator) (output ops.Operator, err error) {
	backup := Clone(op)
	defer func() {
		if err == errHorizonNotPlanned {
			err = planOffsetsOnJoins(ctx, backup)
			if err == nil {
				output = backup
			}
		}
	}()

	_, ok := op.(*Horizon)

	if !ok || len(ctx.SemTable.SubqueryMap) > 0 || len(ctx.SemTable.SubqueryRef) > 0 {
		// we are not ready to deal with subqueries yet
		return op, errHorizonNotPlanned
	}

	output, err = planHorizons(ctx, op)
	if err != nil {
		return nil, err
	}

	output, err = planOffsets(ctx, output)
	if err != nil {
		return nil, err
	}

	err = makeSureOutputIsCorrect(ctx, op, output)
	if err != nil {
		return nil, err
	}

	return
}

func makeSureOutputIsCorrect(ctx *plancontext.PlanningContext, op ops.Operator, output ops.Operator) error {
	// next we use the original Horizon to make sure that the output columns line up with what the user asked for
	// in the future, we'll tidy up the results. for now, we are just failing these queries and going back to the
	// old horizon planning instead
	cols, err := output.GetColumns()
	if err != nil {
		return err
	}

	horizon := op.(*Horizon)

	sel := sqlparser.GetFirstSelect(horizon.Select)
	if len(sel.SelectExprs) != len(cols) {
		return errHorizonNotPlanned
	}
	for i, expr := range sel.SelectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok || !ctx.SemTable.EqualsExpr(ae.Expr, cols[i]) {
			return errHorizonNotPlanned
		}
	}
	return nil
}

// Inputs implements the Operator interface
func (noInputs) Inputs() []ops.Operator {
	return nil
}

// SetInputs implements the Operator interface
func (noInputs) SetInputs(ops []ops.Operator) {
	if len(ops) > 0 {
		panic("the noInputs operator does not have inputs")
	}
}

// AddColumn implements the Operator interface
func (noColumns) AddColumn(*plancontext.PlanningContext, *sqlparser.AliasedExpr) (ops.Operator, int, error) {
	return nil, 0, vterrors.VT13001("the noColumns operator cannot accept columns")
}

func (noColumns) GetColumns() ([]sqlparser.Expr, error) {
	return nil, vterrors.VT13001("the noColumns operator cannot accept columns")
}

// AddPredicate implements the Operator interface
func (noPredicates) AddPredicate(*plancontext.PlanningContext, sqlparser.Expr) (ops.Operator, error) {
	return nil, vterrors.VT13001("the noColumns operator cannot accept predicates")
}
