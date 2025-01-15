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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// breakExpressionInLHSandRHS takes an expression and
// extracts the parts that are coming from one of the sides into `ColName`s that are needed
func breakExpressionInLHSandRHS(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	lhs semantics.TableSet,
) (col applyJoinColumn) {
	rewrittenExpr := sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		nodeExpr, ok := cursor.Node().(sqlparser.Expr)
		if !ok || !mustFetchFromInput(ctx, nodeExpr) {
			return
		}
		deps := ctx.SemTable.RecursiveDeps(nodeExpr)
		if !deps.IsSolvedBy(lhs) {
			return
		}

		bvName := ctx.GetReservedArgumentFor(nodeExpr)
		col.LHSExprs = append(col.LHSExprs, BindVarExpr{
			Name: bvName,
			Expr: nodeExpr,
		})
		typeForExpr, _ := ctx.TypeForExpr(nodeExpr)
		arg := sqlparser.NewTypedArgument(bvName, typeForExpr.Type())
		arg.Scale = typeForExpr.Scale()
		arg.Size = typeForExpr.Size()

		// we are replacing one of the sides of the comparison with an argument,
		// but we don't want to lose the type information we have, so we copy it over
		ctx.SemTable.CopyExprInfo(nodeExpr, arg)
		cursor.Replace(arg)
	}, nil).(sqlparser.Expr)

	col.RHSExpr = rewrittenExpr
	col.Original = expr
	return
}

// nothingNeedsFetching will return true if all the nodes in the expression are constant
func nothingNeedsFetching(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (constant bool) {
	constant = true
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if mustFetchFromInput(ctx, node) {
			constant = false
		}
		return true, nil
	}, expr)
	return
}

func simplifyPredicates(ctx *plancontext.PlanningContext, in sqlparser.Expr) sqlparser.Expr {
	var replace sqlparser.Expr

	// if expr is constant true, replace with trueReplacement, if constant false, replace with falseReplacement
	handleExpr := func(expr, trueReplacement, falseReplacement sqlparser.Expr) bool {
		b := ctx.IsConstantBool(expr)
		if b != nil {
			if *b {
				replace = trueReplacement
			} else {
				replace = falseReplacement
			}
			return true
		}
		return false
	}

	pre := func(node, _ sqlparser.SQLNode) bool {
		switch node := node.(type) {
		case *sqlparser.OrExpr:
			if handleExpr(node.Left, sqlparser.NewIntLiteral("1"), node.Right) {
				return false
			}
			if handleExpr(node.Right, sqlparser.NewIntLiteral("1"), node.Left) {
				return false
			}
		case *sqlparser.AndExpr:
			if handleExpr(node.Left, node.Right, sqlparser.NewIntLiteral("0")) {
				return false
			}
			if handleExpr(node.Right, node.Left, sqlparser.NewIntLiteral("0")) {
				return false
			}
		}
		return true
	}
	post := func(cursor *sqlparser.CopyOnWriteCursor) {
		if replace != nil {
			cursor.Replace(replace)
			replace = nil
		}
	}
	output := sqlparser.CopyOnRewrite(in, pre, post, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)
	if in != output {
		// we need to do this, since one simplification might lead to another
		return simplifyPredicates(ctx, output)
	}
	return output
}

func getFirstSelect(selStmt sqlparser.TableStatement) *sqlparser.Select {
	firstSelect, err := sqlparser.GetFirstSelect(selStmt)
	if err != nil {
		panic(err)
	}
	return firstSelect
}
