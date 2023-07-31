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

	"golang.org/x/exp/slices"

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

func planOffsetsOnJoins(ctx *plancontext.PlanningContext, op ops.Operator) error {
	err := rewrite.Visit(op, func(current ops.Operator) error {
		join, ok := current.(*ApplyJoin)
		if !ok {
			return nil
		}
		return join.planOffsets(ctx)
	})
	return err
}

// useOffsets rewrites an expression to use values from the input
func useOffsets(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op ops.Operator) (sqlparser.Expr, error) {
	in := op.Inputs()[0]
	columns, err := in.GetColumns()
	if err != nil {
		return nil, err
	}

	var exprOffset *sqlparser.Offset

	found := func(e sqlparser.Expr, offset int) { exprOffset = sqlparser.NewOffset(offset, e) }

	notFound := func(e sqlparser.Expr) error {
		_, addToGroupBy := e.(*sqlparser.ColName)
		var offset int
		in, offset, err = in.AddColumn(ctx, aeWrap(e), true, addToGroupBy)
		if err != nil {
			return err
		}
		op.SetInputs([]ops.Operator{in})
		columns, err = in.GetColumns()
		if err != nil {
			return err
		}
		exprOffset = sqlparser.NewOffset(offset, e)
		return nil
	}

	getColumns := func() []*sqlparser.AliasedExpr { return columns }
	findCol := func(ctx *plancontext.PlanningContext, e sqlparser.Expr) (int, error) {
		return slices.IndexFunc(getColumns(), func(expr *sqlparser.AliasedExpr) bool {
			return ctx.SemTable.EqualsExprWithDeps(expr.Expr, e)
		}), nil
	}
	visitor := getVisitor(ctx, findCol, found, notFound)

	// The cursor replace is not available while walking `down`, so `up` is used to do the replacement.
	up := func(cursor *sqlparser.CopyOnWriteCursor) {
		if exprOffset != nil {
			cursor.Replace(exprOffset)
			exprOffset = nil
		}
	}

	rewritten := sqlparser.CopyOnRewrite(expr, visitor, up, ctx.SemTable.CopyDependenciesOnSQLNodes)
	if err != nil {
		return nil, err
	}

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
			proj.addColumnWithoutPushing(aeWrap(e), addToGroupBy)
			addedColumns = true
			return nil
		}
		visitor := getVisitor(ctx, proj.findCol, found, notFound)

		for _, expr := range filter.Predicates {
			_ = sqlparser.CopyOnRewrite(expr, visitor, nil, ctx.SemTable.CopyDependenciesOnSQLNodes)
		}
		if addedColumns {
			return in, rewrite.NewTree("added columns because filter needs it", in), nil
		}

		return in, rewrite.SameTree, nil
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtRoute)
}

func getVisitor(
	ctx *plancontext.PlanningContext,
	findCol func(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error),
	found func(sqlparser.Expr, int),
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
		offset, err = findCol(ctx, e)
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
