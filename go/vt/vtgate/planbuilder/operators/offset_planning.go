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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// planOffsets will walk the tree top down, adding offset information to columns in the tree for use in further optimization,
func planOffsets(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	type offsettable interface {
		planOffsets(ctx *plancontext.PlanningContext) error
	}

	visitor := func(in ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		var err error
		switch op := in.(type) {
		case *Horizon:
			return nil, nil, vterrors.VT13001(fmt.Sprintf("should not see %T here", in))
		case offsettable:
			err = op.planOffsets(ctx)
		}
		if err != nil {
			return nil, nil, err
		}
		return in, rewrite.SameTree, nil
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtRoute)
}

func fetchByOffset(e sqlparser.SQLNode) bool {
	switch e.(type) {
	case *sqlparser.ColName, sqlparser.AggrFunc:
		return true
	default:
		return false
	}
}

// useOffsets rewrites an expression to use values from the input
func useOffsets(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op ops.Operator) (sqlparser.Expr, error) {
	var exprOffset *sqlparser.Offset

	in := op.Inputs()[0]
	found := func(e sqlparser.Expr, offset int) { exprOffset = sqlparser.NewOffset(offset, e) }

	notFound := func(e sqlparser.Expr) error {
		_, addToGroupBy := e.(*sqlparser.ColName)
		offset, err := in.AddColumn(ctx, true, addToGroupBy, aeWrap(e))
		if err != nil {
			return err
		}
		exprOffset = sqlparser.NewOffset(offset, e)
		return nil
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

	return rewritten.(sqlparser.Expr), nil
}

// addColumnsToInput adds columns needed by an operator to its input.
// This happens only when the filter expression can be retrieved as an offset from the underlying mysql.
func addColumnsToInput(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		filter, ok := in.(*Filter)
		if !ok {
			return in, rewrite.SameTree, nil
		}

		proj, areOnTopOfProj := filter.Source.(selectExpressions)
		if !areOnTopOfProj {
			// not much we can do here
			return in, rewrite.SameTree, nil
		}
		addedColumns := false
		found := func(expr sqlparser.Expr, i int) {}
		notFound := func(e sqlparser.Expr) error {
			_, addToGroupBy := e.(*sqlparser.ColName)
			_, err := proj.addColumnWithoutPushing(ctx, aeWrap(e), addToGroupBy)
			if err != nil {
				return err
			}
			addedColumns = true
			return nil
		}
		visitor := getOffsetRewritingVisitor(ctx, proj.FindCol, found, notFound)

		for _, expr := range filter.Predicates {
			_ = sqlparser.CopyOnRewrite(expr, visitor, nil, ctx.SemTable.CopySemanticInfo)
		}
		if addedColumns {
			return in, rewrite.NewTree("added columns because filter needs it", in), nil
		}

		return in, rewrite.SameTree, nil
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtRoute)
}

// addColumnsToInput adds columns needed by an operator to its input.
// This happens only when the filter expression can be retrieved as an offset from the underlying mysql.
func pullDistinctFromUNION(_ *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		union, ok := in.(*Union)
		if !ok || !union.distinct {
			return in, rewrite.SameTree, nil
		}

		union.distinct = false

		distinct := &Distinct{
			Required: true,
			Source:   union,
		}
		return distinct, rewrite.NewTree("pulled out DISTINCT from union", union), nil
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtRoute)
}

func getOffsetRewritingVisitor(
	ctx *plancontext.PlanningContext,
	// this is the function that will be called to try to find the offset for an expression
	findCol func(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error),
	// this function will be called when an expression has been found on the input
	found func(sqlparser.Expr, int),
	// if we have an expression that mush be fetched, this method will be called
	notFound func(sqlparser.Expr) error,
) func(node, parent sqlparser.SQLNode) bool {
	var err error
	return func(node, parent sqlparser.SQLNode) bool {
		if err != nil {
			return false
		}
		e, ok := node.(sqlparser.Expr)
		if !ok {
			return true
		}
		var offset int
		offset, err = findCol(ctx, e, false)
		if err != nil {
			return false
		}
		if offset >= 0 {
			found(e, offset)
			return false
		}

		if fetchByOffset(e) {
			err = notFound(e)
			return false
		}

		return true
	}
}
