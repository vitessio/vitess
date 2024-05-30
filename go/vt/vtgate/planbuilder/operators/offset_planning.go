/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// planOffsets will walk the tree top down, adding offset information to columns in the tree for use in further optimization,
func planOffsets(ctx *plancontext.PlanningContext, root Operator) Operator {
	type offsettable interface {
		Operator
		planOffsets(ctx *plancontext.PlanningContext) Operator
	}

	visitor := func(in Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		switch op := in.(type) {
		case *Horizon:
			panic(vterrors.VT13001(fmt.Sprintf("should not see %T here", in)))
		case offsettable:
			newOp := op.planOffsets(ctx)

			if newOp == nil {
				newOp = op
			}

			if DebugOperatorTree {
				fmt.Println("Planned offsets for:")
				fmt.Println(ToTree(newOp))
			}
			return newOp, nil
		}
		return in, NoRewrite
	}

	return TopDown(root, TableID, visitor, stopAtRoute)
}

// mustFetchFromInput returns true for expressions that have to be fetched from the input and cannot be evaluated
func mustFetchFromInput(ctx *plancontext.PlanningContext, e sqlparser.SQLNode) bool {
	switch fun := e.(type) {
	case *sqlparser.ColName, sqlparser.AggrFunc:
		return true
	case *sqlparser.FuncExpr:
		return fun.Name.EqualsAnyString(ctx.VSchema.GetAggregateUDFs())
	default:
		return false
	}
}

// useOffsets rewrites an expression to use values from the input
func useOffsets(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op Operator) sqlparser.Expr {
	var exprOffset *sqlparser.Offset

	in := op.Inputs()[0]
	found := func(e sqlparser.Expr, offset int) { exprOffset = sqlparser.NewOffset(offset, e) }

	notFound := func(e sqlparser.Expr) {
		_, addToGroupBy := e.(*sqlparser.ColName)
		offset := in.AddColumn(ctx, true, addToGroupBy, aeWrap(e))
		exprOffset = sqlparser.NewOffset(offset, e)
	}

	visitor := getOffsetRewritingVisitor(ctx, in.FindCol, found, notFound)

	// The cursor replace is not available while walking `down`, so `up` is used to do the replacement.
	up := func(cursor *sqlparser.CopyOnWriteCursor) {
		if exprOffset != nil {
			cursor.Replace(exprOffset)
			exprOffset = nil
		}
	}

	rewritten := sqlparser.CopyOnRewrite(expr, visitor, up, ctx.SemTable.CopySemanticInfo)

	return rewritten.(sqlparser.Expr)
}

func addColumnsToInput(ctx *plancontext.PlanningContext, root Operator) Operator {
	// addColumnsToInput adds columns needed by an operator to its input.
	// This happens only when the filter expression can be retrieved as an offset from the underlying mysql.
	addColumnsNeededByFilter := func(in Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		filter, ok := in.(*Filter)
		if !ok {
			return in, NoRewrite
		}

		proj, areOnTopOfProj := filter.Source.(selectExpressions)
		if !areOnTopOfProj {
			// not much we can do here
			return in, NoRewrite
		}
		addedColumns := false
		found := func(expr sqlparser.Expr, i int) {}
		notFound := func(e sqlparser.Expr) {
			_, addToGroupBy := e.(*sqlparser.ColName)
			proj.addColumnWithoutPushing(ctx, aeWrap(e), addToGroupBy)
			addedColumns = true
		}
		visitor := getOffsetRewritingVisitor(ctx, proj.FindCol, found, notFound)

		for _, expr := range filter.Predicates {
			_ = sqlparser.CopyOnRewrite(expr, visitor, nil, ctx.SemTable.CopySemanticInfo)
		}
		if addedColumns {
			return in, Rewrote("added columns because filter needs it")
		}

		return in, NoRewrite
	}

	// while we are out here walking the operator tree, if we find a UDF in an aggregation, we should fail
	failUDFAggregation := func(in Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		aggrOp, ok := in.(*Aggregator)
		if !ok {
			return in, NoRewrite
		}
		for _, aggr := range aggrOp.Aggregations {
			if aggr.OpCode == opcode.AggregateUDF {
				// we don't support UDFs in aggregation if it's still above a route
				message := fmt.Sprintf("Aggregate UDF '%s' must be pushed down to MySQL", sqlparser.String(aggr.Original.Expr))
				panic(vterrors.VT12001(message))
			}
		}
		return in, NoRewrite
	}

	visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		out, res := addColumnsNeededByFilter(in, semantics.EmptyTableSet(), isRoot)
		failUDFAggregation(in, semantics.EmptyTableSet(), isRoot)
		return out, res
	}

	return TopDown(root, TableID, visitor, stopAtRoute)
}

// isolateDistinctFromUnion will pull out the distinct from a union operator
func isolateDistinctFromUnion(_ *plancontext.PlanningContext, root Operator) Operator {
	visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		union, ok := in.(*Union)
		if !ok || !union.distinct {
			return in, NoRewrite
		}

		union.distinct = false

		distinct := &Distinct{
			Required: true,
			Source:   union,
		}
		return distinct, Rewrote("pulled out DISTINCT from union")
	}

	return TopDown(root, TableID, visitor, stopAtRoute)
}

func getOffsetRewritingVisitor(
	ctx *plancontext.PlanningContext,
	// this is the function that will be called to try to find the offset for an expression
	findCol func(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int,
	// this function will be called when an expression has been found on the input
	found func(sqlparser.Expr, int),
	// if we have an expression that mush be fetched, this method will be called
	notFound func(sqlparser.Expr),
) func(node, parent sqlparser.SQLNode) bool {
	return func(node, parent sqlparser.SQLNode) bool {
		e, ok := node.(sqlparser.Expr)
		if !ok {
			return true
		}
		offset := findCol(ctx, e, false)
		if offset >= 0 {
			found(e, offset)
			return false
		}

		if mustFetchFromInput(ctx, e) {
			notFound(e)
			return false
		}

		return true
	}
}
