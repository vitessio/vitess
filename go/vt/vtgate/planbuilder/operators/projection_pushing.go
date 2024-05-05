/*
Copyright 2024 The Vitess Authors.

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
	"slices"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	projector struct {
		columns               []*ProjExpr
		columnAliases         []string
		explicitColumnAliases bool
		tableName             sqlparser.TableName
	}
)

// add introduces a new projection with the specified alias to the projector.
func (p *projector) add(pe *ProjExpr, alias string) {
	p.columns = append(p.columns, pe)
	if alias != "" && slices.Index(p.columnAliases, alias) > -1 {
		panic("alias already used")
	}
	p.columnAliases = append(p.columnAliases, alias)
}

// get finds or adds an expression in the projector, returning its SQL representation with the appropriate alias
func (p *projector) get(ctx *plancontext.PlanningContext, expr sqlparser.Expr) sqlparser.Expr {
	for _, column := range p.columns {
		if ctx.SemTable.EqualsExprWithDeps(expr, column.ColExpr) {
			out := sqlparser.NewColName(column.Original.ColumnName())
			out.Qualifier = p.tableName

			ctx.SemTable.CopySemanticInfo(expr, out)
			return out
		}
	}

	// we could not find the expression, so we add it
	alias := sqlparser.UnescapedString(expr)
	pe := newProjExpr(sqlparser.NewAliasedExpr(expr, alias))
	p.columns = append(p.columns, pe)
	p.columnAliases = append(p.columnAliases, alias)

	out := sqlparser.NewColName(alias)
	out.Qualifier = p.tableName

	ctx.SemTable.CopySemanticInfo(expr, out)

	return out
}

// tryPushProjection attempts to optimize a projection by pushing it down in the query plan
func tryPushProjection(
	ctx *plancontext.PlanningContext,
	p *Projection,
) (Operator, *ApplyResult) {
	switch src := p.Source.(type) {
	case *Route:
		return Swap(p, src, "push projection under route")
	case *Limit:
		return Swap(p, src, "push projection under limit")
	case *ApplyJoin:
		if p.FromAggr || !p.canPush(ctx) {
			return p, NoRewrite
		}
		return pushProjectionInApplyJoin(ctx, p, src)
	case *HashJoin:
		if !p.canPush(ctx) {
			return p, NoRewrite
		}
		return pushProjectionThroughHashJoin(ctx, p, src)
	case *Vindex:
		if !p.canPush(ctx) {
			return p, NoRewrite
		}
		return pushProjectionInVindex(ctx, p, src)
	case *SubQueryContainer:
		if !p.canPush(ctx) {
			return p, NoRewrite
		}
		return pushProjectionToOuterContainer(ctx, p, src)
	case *SubQuery:
		return pushProjectionToOuter(ctx, p, src)
	default:
		return p, NoRewrite
	}
}

// pushProjectionThroughHashJoin optimizes projection operations within a hash join
func pushProjectionThroughHashJoin(ctx *plancontext.PlanningContext, p *Projection, hj *HashJoin) (Operator, *ApplyResult) {
	cols := p.Columns.(AliasedProjections)
	for _, col := range cols {
		if !col.isSameInAndOut(ctx) {
			return p, NoRewrite
		}
		hj.columns.add(col.ColExpr)
	}
	return hj, Rewrote("merged projection into hash join")
}

func pushProjectionToOuter(ctx *plancontext.PlanningContext, p *Projection, sq *SubQuery) (Operator, *ApplyResult) {
	ap, err := p.GetAliasedProjections()
	if err != nil {
		return p, NoRewrite
	}

	if !reachedPhase(ctx, subquerySettling) {
		return p, NoRewrite
	}

	outer := TableID(sq.Outer)
	for _, pe := range ap {
		_, isOffset := pe.Info.(Offset)
		if isOffset {
			continue
		}

		if !ctx.SemTable.RecursiveDeps(pe.EvalExpr).IsSolvedBy(outer) {
			return p, NoRewrite
		}

		se, ok := pe.Info.(SubQueryExpression)
		if ok {
			pe.EvalExpr = rewriteColNameToArgument(ctx, pe.EvalExpr, se, sq)
		}
	}
	// all projections can be pushed to the outer
	sq.Outer, p.Source = p, sq.Outer
	return sq, Rewrote("push projection into outer side of subquery")
}

func pushProjectionInVindex(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *Vindex,
) (Operator, *ApplyResult) {
	ap, err := p.GetAliasedProjections()
	if err != nil {
		panic(err)
	}
	for _, pe := range ap {
		src.AddColumn(ctx, true, false, aeWrap(pe.EvalExpr))
	}
	return src, Rewrote("push projection into vindex")
}

func pushProjectionToOuterContainer(ctx *plancontext.PlanningContext, p *Projection, src *SubQueryContainer) (Operator, *ApplyResult) {
	ap, err := p.GetAliasedProjections()
	if err != nil {
		return p, NoRewrite
	}

	outer := TableID(src.Outer)
	for _, pe := range ap {
		_, isOffset := pe.Info.(Offset)
		if isOffset {
			continue
		}

		if !ctx.SemTable.RecursiveDeps(pe.EvalExpr).IsSolvedBy(outer) {
			return p, NoRewrite
		}

		if se, ok := pe.Info.(SubQueryExpression); ok {
			pe.EvalExpr = rewriteColNameToArgument(ctx, pe.EvalExpr, se, src.Inner...)
		}
	}
	// all projections can be pushed to the outer
	src.Outer, p.Source = p, src.Outer
	return src, Rewrote("push projection into outer side of subquery container")
}

// nullInNullOutExpr returns true if the expression will return NULL if any of its inputs are NULL
// When we are evaluating an ApplyJoin, the expressions that have any dependency on the outer side of the join
// will be sent to the outer side of the join. If the expression is null intolerant, then we can push it down,
// and the result would be NULL for missing matches from the outer side. If the expression is something that can
// return values other than NULL, like `COALESCE(tbl.foo, 'bar')`, then we can't push it down, because we would
// get a different result if the outer side is missing.
func nullInNullOutExpr(expr sqlparser.Expr) bool {
	// TODO: This is a very basic implementation. We should expand this to handle more cases.
	switch expr.(type) {
	case *sqlparser.ColName:
		return true
	default:
		return false
	}
}

// pushProjectionInApplyJoin optimizes the ApplyJoin operation by pushing down the projection operation into it. This function works as follows:
//
// 1. It traverses each input column of the projection operation.
// 2. For each column, it generates new JoinPredicates for the ApplyJoin operation. These predicates are derived from the column's expression.
/*
Here's an ASCII representation of the transformation:
  Before:
   Projection[L.colX, R.colY]
 	    |
 	ApplyJoin
 	 /   \
 	LHS  RHS
  After:
 	             ApplyJoin
 	            /         \
  Projection[L.colX] Projection[R.colY]
 	     |                   |
 	     LHS                 RHS
*/
// In the transformed state, if necessary, new Projection operators are created for the left and right children of the ApplyJoin operation.
// These Projections can then hopefully be pushed down under a Route or Limit operation.
func pushProjectionInApplyJoin(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *ApplyJoin,
) (Operator, *ApplyResult) {
	ap, err := p.GetAliasedProjections()
	if err != nil {
		// we can't push down expression evaluation to the rhs if we are not sure if it will even be executed
		return p, NoRewrite
	}
	if IsOuter(src) {
		// for outer joins, we have to check that we can send down the projection to the rhs
		for _, expr := range ap.GetColumns() {
			if !nullInNullOutExpr(expr.Expr) {
				return p, NoRewrite
			}
		}
	}

	lhs, rhs := &projector{}, &projector{}
	if p.DT != nil && len(p.DT.Columns) > 0 {
		lhs.explicitColumnAliases = true
		rhs.explicitColumnAliases = true
	}

	// We store the original join columns to reuse them.
	originalJoinColumns := src.JoinColumns
	src.JoinColumns = &applyJoinColumns{}
	for idx, pe := range ap {
		// First we check if we have already done the work to find how to push this expression.
		// If we find it then we can directly use it. This is not just a performance improvement, but
		// is also required for pushing a projection that is just an alias.
		foundIdx := slices.IndexFunc(originalJoinColumns.columns, applyJoinCompare(ctx, pe.ColExpr))
		if foundIdx != -1 {
			src.JoinColumns.add(originalJoinColumns.columns[foundIdx])
			continue
		}
		var alias string
		if p.DT != nil && len(p.DT.Columns) > 0 {
			if len(p.DT.Columns) <= idx {
				panic(vterrors.VT13001("no such alias found for derived table"))
			}
			alias = p.DT.Columns[idx].String()
		}
		splitProjectionAcrossJoin(ctx, src, lhs, rhs, pe, alias, p.DT)
	}

	if p.isDerived() {
		exposeColumnsThroughDerivedTable(ctx, p, src, lhs, rhs)
	}

	// Create and update the Projection operators for the left and right children, if needed.
	src.LHS = createProjectionWithTheseColumns(ctx, src.LHS, lhs, p.DT)
	src.RHS = createProjectionWithTheseColumns(ctx, src.RHS, rhs, p.DT)

	return src, Rewrote("split projection to either side of join")
}

// splitProjectionAcrossJoin creates JoinPredicates for all projections,
// and pushes down columns as needed between the LHS and RHS of a join
func splitProjectionAcrossJoin(
	ctx *plancontext.PlanningContext,
	join *ApplyJoin,
	lhs, rhs *projector,
	pe *ProjExpr,
	colAlias string,
	dt *DerivedTable,
) {
	switch pe.Info.(type) {
	case Offset, nil:
		// for offsets, we'll just treat the expression as unexplored, and later stages will handle the new offset
		join.JoinColumns.add(splitUnexploredExpression(ctx, join, lhs, rhs, pe, colAlias, dt))
	case SubQueryExpression:
		join.JoinColumns.add(splitSubqueryExpression(ctx, join, lhs, rhs, pe, colAlias))
	default:
		panic(vterrors.VT13001(fmt.Sprintf("unknown projection type %T", pe.Info)))
	}
}

func splitSubqueryExpression(
	ctx *plancontext.PlanningContext,
	join *ApplyJoin,
	lhs, rhs *projector,
	pe *ProjExpr,
	alias string,
) applyJoinColumn {
	col := join.getJoinColumnFor(ctx, pe.Original, pe.ColExpr, false)
	return pushDownSplitJoinCol(col, lhs, pe, alias, rhs)
}

func splitUnexploredExpression(
	ctx *plancontext.PlanningContext,
	join *ApplyJoin,
	lhs, rhs *projector,
	pe *ProjExpr,
	alias string,
	dt *DerivedTable,
) applyJoinColumn {
	original := sqlparser.CloneRefOfAliasedExpr(pe.Original)
	expr := pe.ColExpr

	var colName *sqlparser.ColName
	if dt != nil {
		if !pe.isSameInAndOut(ctx) {
			panic(vterrors.VT13001("derived table columns must be the same in and out"))
		}
		colName = sqlparser.NewColNameWithQualifier(pe.Original.ColumnName(), sqlparser.NewTableName(dt.Alias))
		ctx.SemTable.CopySemanticInfo(expr, colName)
	}

	// Get a applyJoinColumn for the current expression.
	col := join.getJoinColumnFor(ctx, original, expr, false)
	col.DTColName = colName

	return pushDownSplitJoinCol(col, lhs, pe, alias, rhs)
}

func pushDownSplitJoinCol(col applyJoinColumn, lhs *projector, pe *ProjExpr, alias string, rhs *projector) applyJoinColumn {
	// Update the left and right child columns and names based on the applyJoinColumn type.
	switch {
	case col.IsPureLeft():
		lhs.add(pe, alias)
	case col.IsPureRight():
		rhs.add(pe, alias)
	case col.IsMixedLeftAndRight():
		for _, lhsExpr := range col.LHSExprs {
			var lhsAlias string
			if alias != "" {
				// we need to add an explicit column alias here. let's try just the ColName as is first
				lhsAlias = sqlparser.String(lhsExpr.Expr)
			}
			lhs.add(newProjExpr(aeWrap(lhsExpr.Expr)), lhsAlias)
		}
		innerPE := newProjExprWithInner(pe.Original, col.RHSExpr)
		innerPE.ColExpr = col.RHSExpr
		innerPE.Info = pe.Info
		rhs.add(innerPE, alias)
	}
	return col
}

// exposeColumnsThroughDerivedTable rewrites expressions within a join that is inside a derived table
// in order to make them accessible outside the derived table. This is necessary when swapping the
// positions of the derived table and join operation.
//
// For example, consider the input query:
// select ... from (select T1.foo from T1 join T2 on T1.id = T2.id) as t
// If we push the derived table under the join, with T1 on the LHS of the join, we need to expose
// the values of T1.id through the derived table, or they will not be accessible on the RHS.
//
// The function iterates through each join predicate, rewriting the expressions in the predicate's
// LHS expressions to include the derived table. This allows the expressions to be accessed outside
// the derived table.
func exposeColumnsThroughDerivedTable(ctx *plancontext.PlanningContext, p *Projection, src *ApplyJoin, lhs, rhs *projector) {
	derivedTbl, err := ctx.SemTable.TableInfoFor(p.DT.TableID)
	if err != nil {
		panic(err)
	}
	derivedTblName, err := derivedTbl.Name()
	if err != nil {
		panic(err)
	}
	lhs.tableName = derivedTblName
	rhs.tableName = derivedTblName

	lhsIDs := TableID(src.LHS)
	rhsIDs := TableID(src.RHS)
	rewriteColumnsForJoin(ctx, src.JoinPredicates.columns, lhsIDs, rhsIDs, lhs, rhs)
}

func rewriteColumnsForJoin(
	ctx *plancontext.PlanningContext,
	columns []applyJoinColumn,
	lhsIDs, rhsIDs semantics.TableSet,
	lhs, rhs *projector,
) {
	for colIdx, column := range columns {
		for lhsIdx, bve := range column.LHSExprs {
			// since this is on the LHSExprs, we know that dependencies are from that side of the join
			column.LHSExprs[lhsIdx].Expr = lhs.get(ctx, bve.Expr)
		}
		if column.IsPureLeft() {
			continue
		}

		// The RHSExprs are the expressions on the RHS of the join, and these have already been pushed down on the RHS
		// of the ApplyJoin. These expressions don't need to be exposed through the derived table, they are just
		// receiving the expressions from the LHS of the join using parameters.

		var rewriteTo sqlparser.Expr

		pre := func(node, _ sqlparser.SQLNode) bool {
			// We are looking for ColNames that belong to either the RHS or LHS of the join
			// We'll replace these with columns being passed through the derived table
			var col *sqlparser.ColName
			switch node := node.(type) {
			case *sqlparser.ColName:
				col = node
			case *sqlparser.Subquery:
				return false
			default:
				return true
			}

			deps := ctx.SemTable.RecursiveDeps(col)

			switch {
			case deps.IsSolvedBy(lhsIDs):
				rewriteTo = lhs.get(ctx, col)
				return false
			case deps.IsSolvedBy(rhsIDs):
				return false
			default:
				return true
			}
		}

		post := func(cursor *sqlparser.CopyOnWriteCursor) {
			if rewriteTo != nil {
				cursor.Replace(rewriteTo)
				rewriteTo = nil
				return
			}
		}
		newOriginal := sqlparser.CopyOnRewrite(column.Original, pre, post, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)
		column.Original = newOriginal

		columns[colIdx] = column
	}
}

// prefixColNames adds qualifier prefixes to all ColName:s.
// We want to be more explicit than the user was to make sure we never produce invalid SQL
func prefixColNames(ctx *plancontext.PlanningContext, tblName sqlparser.TableName, e sqlparser.Expr) sqlparser.Expr {
	return sqlparser.CopyOnRewrite(e, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return
		}
		cursor.Replace(sqlparser.NewColNameWithQualifier(col.Name.String(), tblName))
	}, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)
}

func createProjectionWithTheseColumns(
	ctx *plancontext.PlanningContext,
	src Operator,
	p *projector,
	dt *DerivedTable,
) Operator {
	if len(p.columns) == 0 {
		return src
	}
	proj := createProjection(ctx, src, "")
	proj.Columns = AliasedProjections(p.columns)
	if dt != nil {
		kopy := *dt
		if p.explicitColumnAliases {
			kopy.Columns = slice.Map(p.columnAliases, func(s string) sqlparser.IdentifierCI {
				return sqlparser.NewIdentifierCI(s)
			})
		}
		proj.DT = &kopy
	}

	return proj
}
