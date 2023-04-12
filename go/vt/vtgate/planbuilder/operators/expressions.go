/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// BreakExpressionInLHSandRHS takes an expression and
// extracts the parts that are coming from one of the sides into `ColName`s that are needed
func BreakExpressionInLHSandRHS(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	lhs semantics.TableSet,
) (col JoinColumn, err error) {
	rewrittenExpr := sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		node, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return
		}
		deps := ctx.SemTable.RecursiveDeps(node)
		if deps.IsEmpty() {
			err = vterrors.VT13001("unknown column. has the AST been copied?")
			cursor.StopTreeWalk()
			return
		}
		if !deps.IsSolvedBy(lhs) {
			return
		}

		node.Qualifier.Qualifier = sqlparser.NewIdentifierCS("")
		col.LHSExprs = append(col.LHSExprs, node)
		bvName := node.CompliantName()
		col.BvNames = append(col.BvNames, bvName)
		arg := sqlparser.NewArgument(bvName)
		// we are replacing one of the sides of the comparison with an argument,
		// but we don't want to lose the type information we have, so we copy it over
		ctx.SemTable.CopyExprInfo(node, arg)
		cursor.Replace(arg)
	}, nil).(sqlparser.Expr)

	if err != nil {
		return JoinColumn{}, err
	}
	ctx.JoinPredicates[expr] = append(ctx.JoinPredicates[expr], rewrittenExpr)
	col.RHSExpr = rewrittenExpr
	return
}

// BreakExpressionInLHSandRHSOld takes an expression and
// extracts the parts that are coming from one of the sides into `ColName`s that are needed
func BreakExpressionInLHSandRHSOld(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	lhs semantics.TableSet,
) (bvNames []string, columns []sqlparser.Expr, rewrittenExpr sqlparser.Expr, err error) {
	col, err := BreakExpressionInLHSandRHS(ctx, expr, lhs)
	if err != nil {
		return nil, nil, nil, err
	}

	return col.BvNames, col.LHSExprs, col.RHSExpr, nil
}
