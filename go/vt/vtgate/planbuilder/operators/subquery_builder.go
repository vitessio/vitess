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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type SubQueryBuilder struct {
	Inner []*SubQuery

	totalID,
	subqID,
	outerID semantics.TableSet
}

func (sqb *SubQueryBuilder) getRootOperator(op Operator, decorator func(operator Operator) Operator) Operator {
	if len(sqb.Inner) == 0 {
		return op
	}

	if decorator != nil {
		for _, sq := range sqb.Inner {
			sq.Subquery = decorator(sq.Subquery)
		}
	}

	return &SubQueryContainer{
		Outer: op,
		Inner: sqb.Inner,
	}
}

func (sqb *SubQueryBuilder) handleSubquery(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	outerID semantics.TableSet,
) *SubQuery {
	subq, parentExpr, path := getSubQuery(expr)
	if subq == nil {
		return nil
	}
	argName := ctx.ReservedVars.ReserveSubQuery()
	sqInner := createSubqueryOp(ctx, parentExpr, expr, subq, outerID, argName, path)
	sqb.Inner = append(sqb.Inner, sqInner)

	return sqInner
}

func getSubQuery(expr sqlparser.Expr) (subqueryExprExists *sqlparser.Subquery, parentExpr sqlparser.Expr, path sqlparser.ASTPath) {
	flipped := false
	_ = sqlparser.RewriteWithPath(expr, func(cursor *sqlparser.Cursor) bool {
		if subq, ok := cursor.Node().(*sqlparser.Subquery); ok {
			subqueryExprExists = subq
			path = cursor.Path()
			parentExpr = subq
			if expr, ok := cursor.Parent().(sqlparser.Expr); ok {
				parentExpr = expr
			}
			flipped = true
			return false
		}
		return true
	}, func(cursor *sqlparser.Cursor) bool {
		if !flipped {
			return true
		}
		if not, isNot := cursor.Parent().(*sqlparser.NotExpr); isNot {
			parentExpr = not
		}
		return false
	})
	return
}

func createSubqueryOp(
	ctx *plancontext.PlanningContext,
	parent, original sqlparser.Expr,
	subq *sqlparser.Subquery,
	outerID semantics.TableSet,
	name string,
	path sqlparser.ASTPath,
) *SubQuery {
	switch parent := parent.(type) {
	case *sqlparser.NotExpr:
		switch parent.Expr.(type) {
		case *sqlparser.ExistsExpr:
			return createSubqueryFromPath(ctx, original, subq, path, outerID, parent, name, opcode.PulloutNotExists, false)
		case *sqlparser.ComparisonExpr:
			panic("should have been rewritten")
		}
	case *sqlparser.ExistsExpr:
		return createSubqueryFromPath(ctx, original, subq, path, outerID, parent, name, opcode.PulloutExists, false)
	case *sqlparser.ComparisonExpr:
		return createComparisonSubQuery(ctx, parent, original, subq, path, outerID, name)
	}
	return createSubqueryFromPath(ctx, original, subq, path, outerID, parent, name, opcode.PulloutValue, false)
}

// inspectStatement goes through all the predicates contained in the AST
// and extracts subqueries into operators
func (sqb *SubQueryBuilder) inspectStatement(ctx *plancontext.PlanningContext,
	stmt sqlparser.TableStatement,
) ([]sqlparser.Expr, []applyJoinColumn) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return sqb.inspectSelect(ctx, stmt)
	case *sqlparser.Union:
		exprs1, cols1 := sqb.inspectStatement(ctx, stmt.Left)
		exprs2, cols2 := sqb.inspectStatement(ctx, stmt.Right)
		return append(exprs1, exprs2...), append(cols1, cols2...)
	}
	panic("unknown type")
}

// inspectSelect goes through all the predicates contained in the SELECT query
// and extracts subqueries into operators, and rewrites the original query to use
// arguments instead of subqueries.
func (sqb *SubQueryBuilder) inspectSelect(
	ctx *plancontext.PlanningContext,
	sel *sqlparser.Select,
) ([]sqlparser.Expr, []applyJoinColumn) {
	// first we need to go through all the places where one can find predicates
	// and search for subqueries
	newWhere, wherePreds, whereJoinCols := sqb.inspectWhere(ctx, sel.Where)
	newHaving, havingPreds, havingJoinCols := sqb.inspectWhere(ctx, sel.Having)
	newFrom, onPreds, onJoinCols := sqb.inspectOnExpr(ctx, sel.From)

	// then we use the updated AST structs to build the operator
	// these AST elements have any subqueries replace by arguments
	sel.Where = newWhere
	sel.Having = newHaving
	sel.From = newFrom

	return append(append(wherePreds, havingPreds...), onPreds...),
		append(append(whereJoinCols, havingJoinCols...), onJoinCols...)
}

func createSubquery(
	ctx *plancontext.PlanningContext,
	original sqlparser.Expr,
	subq *sqlparser.Subquery,
	outerID semantics.TableSet,
	parent sqlparser.Expr,
	argName string,
	filterType opcode.PulloutOpcode,
	isArg bool,
) *SubQuery {
	topLevel := ctx.SemTable.EqualsExpr(original, parent)
	original = cloneASTAndSemState(ctx, original)
	originalSq := cloneASTAndSemState(ctx, subq)
	subqID := findTablesContained(ctx, subq.Select)
	totalID := subqID.Merge(outerID)
	sqc := &SubQueryBuilder{totalID: totalID, subqID: subqID, outerID: outerID}

	predicates, joinCols := sqc.inspectStatement(ctx, subq.Select)
	correlated := !ctx.SemTable.RecursiveDeps(subq).IsEmpty()

	opInner := translateQueryToOp(ctx, subq.Select)

	opInner = sqc.getRootOperator(opInner, nil)
	return &SubQuery{
		FilterType:       filterType,
		Subquery:         opInner,
		Predicates:       predicates,
		Original:         original,
		ArgName:          argName,
		originalSubquery: originalSq,
		IsArgument:       isArg,
		TopLevel:         topLevel,
		JoinColumns:      joinCols,
		correlated:       correlated,
	}
}

func createSubqueryFromPath(
	ctx *plancontext.PlanningContext,
	original sqlparser.Expr,
	subq *sqlparser.Subquery,
	path sqlparser.ASTPath,
	outerID semantics.TableSet,
	parent sqlparser.Expr,
	argName string,
	filterType opcode.PulloutOpcode,
	isArg bool,
) *SubQuery {
	topLevel := ctx.SemTable.EqualsExpr(original, parent)
	original = cloneASTAndSemState(ctx, original)
	originalSq := sqlparser.GetNodeFromPath(original, path).(*sqlparser.Subquery)
	subqID := findTablesContained(ctx, originalSq.Select)
	totalID := subqID.Merge(outerID)
	sqc := &SubQueryBuilder{totalID: totalID, subqID: subqID, outerID: outerID}

	predicates, joinCols := sqc.inspectStatement(ctx, subq.Select)
	correlated := !ctx.SemTable.RecursiveDeps(subq).IsEmpty()

	opInner := translateQueryToOp(ctx, subq.Select)

	opInner = sqc.getRootOperator(opInner, nil)
	return &SubQuery{
		FilterType:       filterType,
		Subquery:         opInner,
		Predicates:       predicates,
		Original:         original,
		ArgName:          argName,
		originalSubquery: originalSq,
		IsArgument:       isArg,
		TopLevel:         topLevel,
		JoinColumns:      joinCols,
		correlated:       correlated,
	}
}

func (sqb *SubQueryBuilder) inspectWhere(
	ctx *plancontext.PlanningContext,
	in *sqlparser.Where,
) (*sqlparser.Where, []sqlparser.Expr, []applyJoinColumn) {
	if in == nil {
		return nil, nil, nil
	}
	jpc := &joinPredicateCollector{
		totalID: sqb.totalID,
		subqID:  sqb.subqID,
		outerID: sqb.outerID,
	}
	for _, predicate := range sqlparser.SplitAndExpression(nil, in.Expr) {
		sqlparser.RemoveKeyspaceInCol(predicate)
		subq := sqb.handleSubquery(ctx, predicate, sqb.totalID)
		if subq != nil {
			continue
		}
		jpc.inspectPredicate(ctx, predicate)
	}

	if len(jpc.remainingPredicates) == 0 {
		in = nil
	} else {
		in.Expr = sqlparser.AndExpressions(jpc.remainingPredicates...)
	}

	return in, jpc.predicates, jpc.joinColumns
}

func (sqb *SubQueryBuilder) inspectOnExpr(
	ctx *plancontext.PlanningContext,
	from []sqlparser.TableExpr,
) (newFrom []sqlparser.TableExpr, onPreds []sqlparser.Expr, onJoinCols []applyJoinColumn) {
	for _, tbl := range from {
		tbl := sqlparser.CopyOnRewrite(tbl, dontEnterSubqueries, func(cursor *sqlparser.CopyOnWriteCursor) {
			cond, ok := cursor.Node().(*sqlparser.JoinCondition)
			if !ok || cond.On == nil {
				return
			}

			jpc := &joinPredicateCollector{
				totalID: sqb.totalID,
				subqID:  sqb.subqID,
				outerID: sqb.outerID,
			}

			for _, pred := range sqlparser.SplitAndExpression(nil, cond.On) {
				subq := sqb.handleSubquery(ctx, pred, sqb.totalID)
				if subq != nil {
					continue
				}
				jpc.inspectPredicate(ctx, pred)
			}
			if len(jpc.remainingPredicates) == 0 {
				cond.On = nil
			} else {
				cond.On = sqlparser.AndExpressions(jpc.remainingPredicates...)
			}
			onPreds = append(onPreds, jpc.predicates...)
			onJoinCols = append(onJoinCols, jpc.joinColumns...)
		}, ctx.SemTable.CopySemanticInfo)
		newFrom = append(newFrom, tbl.(sqlparser.TableExpr))
	}
	return
}

func createComparisonSubQuery(
	ctx *plancontext.PlanningContext,
	parent *sqlparser.ComparisonExpr,
	original sqlparser.Expr,
	subFromOutside *sqlparser.Subquery,
	path sqlparser.ASTPath,
	outerID semantics.TableSet,
	name string,
) *SubQuery {
	subq, outside := semantics.GetSubqueryAndOtherSide(parent)
	if outside == nil || subq != subFromOutside {
		panic("uh oh")
	}

	filterType := opcode.PulloutValue
	switch parent.Operator {
	case sqlparser.InOp:
		filterType = opcode.PulloutIn
	case sqlparser.NotInOp:
		filterType = opcode.PulloutNotIn
	}

	subquery := createSubqueryFromPath(ctx, original, subq, path, outerID, parent, name, filterType, false)

	// if we are comparing with a column from the inner subquery,
	// we add this extra predicate to check if the two sides are mergable or not
	if ae, ok := subq.Select.GetColumns()[0].(*sqlparser.AliasedExpr); ok {
		subquery.OuterPredicate = &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualOp,
			Left:     outside,
			Right:    ae.Expr,
		}
	}

	return subquery
}

// pullOutValueSubqueries extracts all subqueries from an expression and replaces them with arguments.
// Used for expressions in SELECT lists, ORDER BY, and UPDATE SET clauses where subqueries must be pulled out.
// Returns the rewritten expression and extracted SubQuery operators.
//
// Each subquery occurrence gets its own operator and bind var name, even if
// the same subquery text appears multiple times. MySQL evaluates each
// occurrence independently (observable with volatile functions like UUID(),
// RAND(), or locking reads), so coalescing would change semantics.
func (sqb *SubQueryBuilder) pullOutValueSubqueries(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	outerID semantics.TableSet,
	isDML bool,
) (sqlparser.Expr, []*SubQuery) {
	original := sqlparser.Clone(expr)
	var allSubqs []*SubQuery

	replaceWithArg := func(cursor *sqlparser.Cursor, sq *sqlparser.Subquery, filterType opcode.PulloutOpcode) {
		argName := ctx.ReservedVars.ReserveSubQuery()
		sqInner := createSubquery(ctx, original, sq, outerID, original, argName, filterType, true)
		allSubqs = append(allSubqs, sqInner)
		sqb.Inner = append(sqb.Inner, sqInner)
		sqb.replaceSubqueryNode(cursor, argName, filterType, isDML)
	}

	expr = sqlparser.Rewrite(expr, nil, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Subquery:
			t := getOpCodeFromParent(cursor.Parent())
			if t == nil {
				return true
			}
			replaceWithArg(cursor, node, *t)
		case *sqlparser.ExistsExpr:
			replaceWithArg(cursor, node.Subquery, opcode.PulloutExists)
		}
		return true
	}).(sqlparser.Expr)

	if len(allSubqs) == 0 {
		return nil, nil
	}
	return expr, allSubqs
}

// replaceSubqueryNode replaces the current cursor node with the appropriate
// argument placeholder for the given bind var name and opcode.
func (sqb *SubQueryBuilder) replaceSubqueryNode(cursor *sqlparser.Cursor, argName string, filterType opcode.PulloutOpcode, isDML bool) {
	if isDML {
		if filterType.NeedsListArg() {
			cursor.Replace(sqlparser.NewListArg(argName))
		} else {
			cursor.Replace(sqlparser.NewArgument(argName))
		}
	} else {
		cursor.Replace(sqlparser.NewColName(argName))
	}
}

func getOpCodeFromParent(parent sqlparser.SQLNode) *opcode.PulloutOpcode {
	code := opcode.PulloutValue
	switch parent := parent.(type) {
	case *sqlparser.ExistsExpr:
		return nil
	case *sqlparser.ComparisonExpr:
		switch parent.Operator {
		case sqlparser.InOp:
			code = opcode.PulloutIn
		case sqlparser.NotInOp:
			code = opcode.PulloutNotIn
		}
	}
	return &code
}
