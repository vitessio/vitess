/*
Copyright 2020 The Vitess Authors.

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
	"io"
	"sort"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type planningContext struct {
	reservedVars *sqlparser.ReservedVars
	semTable     *semantics.SemTable
	vschema      ContextVSchema
}

func (c planningContext) isSubQueryToReplace(e sqlparser.Expr) bool {
	ext, ok := e.(*sqlparser.Subquery)
	if !ok {
		return false
	}
	for _, extractedSubq := range c.semTable.GetSubqueryNeedingRewrite() {
		if extractedSubq.NeedsRewrite && sqlparser.EqualsRefOfSubquery(extractedSubq.Subquery, ext) {
			return true
		}
	}
	return false
}

func optimizeQuery(ctx *planningContext, opTree abstract.Operator) (queryTree, error) {
	switch op := opTree.(type) {
	case *abstract.QueryGraph:
		switch {
		case ctx.vschema.Planner() == Gen4Left2Right:
			return leftToRightSolve(ctx, op)
		default:
			return greedySolve(ctx, op)
		}
	case *abstract.Join:
		treeInner, err := optimizeQuery(ctx, op.LHS)
		if err != nil {
			return nil, err
		}
		treeOuter, err := optimizeQuery(ctx, op.RHS)
		if err != nil {
			return nil, err
		}
		return mergeOrJoin(ctx, treeInner, treeOuter, sqlparser.SplitAndExpression(nil, op.Predicate), !op.LeftJoin)
	case *abstract.Derived:
		treeInner, err := optimizeQuery(ctx, op.Inner)
		if err != nil {
			return nil, err
		}
		return &derivedTree{
			query:         op.Sel,
			inner:         treeInner,
			alias:         op.Alias,
			columnAliases: op.ColumnAliases,
		}, nil
	case *abstract.SubQuery:
		return optimizeSubQuery(ctx, op)
	case *abstract.Vindex:
		return createVindexTree(ctx, op)
	case *abstract.Concatenate:
		return optimizeUnion(ctx, op)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid operator tree: %T", op)
	}
}

func optimizeUnion(ctx *planningContext, op *abstract.Concatenate) (queryTree, error) {
	var sources []queryTree
	for _, source := range op.Sources {
		qt, err := optimizeQuery(ctx, source)
		if err != nil {
			return nil, err
		}

		sources = append(sources, qt)
	}

	tree := &concatenateTree{
		distinct:    op.Distinct,
		ordering:    op.OrderBy,
		limit:       op.Limit,
		selectStmts: op.SelectStmts,
		sources:     sources,
	}
	return tree, nil
}

func createVindexTree(ctx *planningContext, op *abstract.Vindex) (*vindexTree, error) {
	solves := ctx.semTable.TableSetFor(op.Table.Alias)
	plan := &vindexTree{
		opCode: op.OpCode,
		solved: solves,
		table:  vindexTable{table: op.Table},
		vindex: op.Vindex,
		value:  op.Value,
	}
	return plan, nil
}

func optimizeSubQuery(ctx *planningContext, op *abstract.SubQuery) (queryTree, error) {
	outerTree, err := optimizeQuery(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	var unmerged []*subqueryTree

	// first loop over the subqueries and try to merge them into the outer plan
	for _, inner := range op.Inner {
		treeInner, err := optimizeQuery(ctx, inner.Inner)
		if err != nil {
			return nil, err
		}

		preds := inner.Inner.UnsolvedPredicates(ctx.semTable)
		merger := func(a, b *routeTree) (*routeTree, error) {
			return mergeSubQuery(ctx, a, b, inner)
		}

		merged, err := tryMergeSubQuery(ctx, outerTree, treeInner, inner, preds, merger)
		if err != nil {
			return nil, err
		}

		if merged == nil {
			if len(preds) > 0 {
				return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard correlated subquery")
			}
			unmerged = append(unmerged, &subqueryTree{
				extracted: inner.ExtractedSubquery,
				inner:     treeInner,
			})
		} else {
			outerTree = merged
		}
	}

	/*
		build a tree of the unmerged subqueries
		rt: route, sqt: subqueryTree


		            sqt
		         sqt   rt
		        rt rt
	*/
	for _, tree := range unmerged {
		tree.outer = outerTree
		outerTree = tree
	}
	return outerTree, nil
}

func tryMergeSubQuery(ctx *planningContext, outer, subq queryTree, subQueryInner *abstract.SubQueryInner, joinPredicates []sqlparser.Expr, merger mergeFunc) (queryTree, error) {
	var merged queryTree
	var err error
	switch outerTree := outer.(type) {
	case *routeTree:
		merged, err = tryMerge(ctx, outerTree, subq, joinPredicates, merger)
		if err != nil {
			return nil, err
		}
		return merged, err
	case *joinTree:
		if outerTree.leftJoin {
			return nil, nil
		}
		newMergefunc := func(a, b *routeTree) (*routeTree, error) {
			rt, err := merger(a, b)
			if err != nil {
				return nil, err
			}
			return rt, rewriteColumnsInSubqueryForJoin(ctx, outerTree.rhs, outerTree, subQueryInner)
		}
		merged, err = tryMergeSubQuery(ctx, outerTree.lhs, subq, subQueryInner, joinPredicates, newMergefunc)
		if err != nil {
			return nil, err
		}
		if merged != nil {
			outerTree.lhs = merged
			return outerTree, nil
		}

		newMergefunc = func(a, b *routeTree) (*routeTree, error) {
			rt, err := merger(a, b)
			if err != nil {
				return nil, err
			}
			return rt, rewriteColumnsInSubqueryForJoin(ctx, outerTree.lhs, outerTree, subQueryInner)
		}
		merged, err = tryMergeSubQuery(ctx, outerTree.rhs, subq, subQueryInner, joinPredicates, newMergefunc)
		if err != nil {
			return nil, err
		}
		if merged != nil {
			outerTree.rhs = merged
			return outerTree, nil
		}
		return nil, nil
	default:
		return nil, nil
	}
}

// rewriteColumnsInSubqueryForJoin rewrites the columns that appear from the other side
// of the join. For example, let's say we merged a subquery on the right side of a join tree
// If it was using any columns from the left side then they need to be replaced by bind variables supplied
// from that side.
// outerTree is the joinTree within whose children the subquery lives in
// the child of joinTree which does not contain the subquery is the otherTree
func rewriteColumnsInSubqueryForJoin(ctx *planningContext, otherTree queryTree, outerTree *joinTree, subQueryInner *abstract.SubQueryInner) error {
	var rewriteError error
	// go over the entire expression in the subquery
	sqlparser.Rewrite(subQueryInner.ExtractedSubquery.Original, func(cursor *sqlparser.Cursor) bool {
		sqlNode := cursor.Node()
		switch node := sqlNode.(type) {
		case *sqlparser.ColName:
			// check whether the column name belongs to the other side of the join tree
			if ctx.semTable.DirectDeps(node).IsSolvedBy(otherTree.tableID()) {
				// get the bindVariable for that column name and replace it in the subquery
				bindVar := node.CompliantName()
				cursor.Replace(sqlparser.NewArgument(bindVar))
				// check whether the bindVariable already exists in the joinVars of the other tree
				_, alreadyExists := outerTree.vars[bindVar]
				if alreadyExists {
					return false
				}
				// if it does not exist, then push this as an output column there and add it to the joinVars
				columnIndexes, err := otherTree.pushOutputColumns([]*sqlparser.ColName{node}, ctx.semTable)
				if err != nil {
					rewriteError = err
					return false
				}
				columnIndex := columnIndexes[0]
				outerTree.vars[bindVar] = columnIndex
				return false
			}
		}
		return true
	}, nil)

	// update the dependencies for the subquery by removing the dependencies from the otherTree
	tableSet := ctx.semTable.Direct[subQueryInner.ExtractedSubquery.Subquery]
	tableSet.RemoveInPlace(otherTree.tableID())
	ctx.semTable.Direct[subQueryInner.ExtractedSubquery.Subquery] = tableSet
	tableSet = ctx.semTable.Recursive[subQueryInner.ExtractedSubquery.Subquery]
	tableSet.RemoveInPlace(otherTree.tableID())
	ctx.semTable.Recursive[subQueryInner.ExtractedSubquery.Subquery] = tableSet

	// return any error while rewriting
	return rewriteError
}

func mergeSubQuery(ctx *planningContext, outer *routeTree, inner *routeTree, subq *abstract.SubQueryInner) (*routeTree, error) {
	subq.ExtractedSubquery.NeedsRewrite = true

	// go over the subquery and add its tables to the one's solved by the route it is merged with
	// this is needed to so that later when we try to push projections, we get the correct
	// solved tableID from the route, since it also includes the tables from the subquery after merging
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch n := node.(type) {
		case *sqlparser.AliasedTableExpr:
			outer.solved.MergeInPlace(ctx.semTable.TableSetFor(n))
		}
		return true, nil
	}, subq.ExtractedSubquery.Subquery)
	if err != nil {
		return nil, err
	}
	outer.SysTableTableSchema = append(outer.SysTableTableSchema, inner.SysTableTableSchema...)
	for k, v := range inner.SysTableTableName {
		outer.SysTableTableName[k] = v
	}

	err = outer.resetRoutingSelections(ctx)
	if err != nil {
		return nil, err
	}
	return outer, nil
}

func exprHasUniqueVindex(vschema ContextVSchema, semTable *semantics.SemTable, expr sqlparser.Expr) bool {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		return false
	}
	ts := semTable.RecursiveDeps(expr)
	tableInfo, err := semTable.TableInfoFor(ts)
	if err != nil {
		return false
	}
	tableName, err := tableInfo.Name()
	if err != nil {
		return false
	}
	vschemaTable, _, _, _, _, err := vschema.FindTableOrVindex(tableName)
	if err != nil {
		return false
	}
	for _, vindex := range vschemaTable.ColumnVindexes {
		if len(vindex.Columns) > 1 || !vindex.Vindex.IsUnique() {
			return false
		}
		if col.Name.Equal(vindex.Columns[0]) {
			return true
		}
	}
	return false
}

func planSingleShardRoutePlan(sel sqlparser.SelectStatement, rb *route) error {
	err := stripDownQuery(sel, rb.Select)
	if err != nil {
		return err
	}
	sqlparser.Rewrite(rb.Select, func(cursor *sqlparser.Cursor) bool {
		if aliasedExpr, ok := cursor.Node().(*sqlparser.AliasedExpr); ok {
			cursor.Replace(removeKeyspaceFromColName(aliasedExpr))
		}
		return true
	}, nil)
	return nil
}

func removeKeyspaceFromSelectExpr(expr sqlparser.SelectExpr, ast *sqlparser.Select, i int) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		ast.SelectExprs[i] = removeKeyspaceFromColName(expr)
	case *sqlparser.StarExpr:
		expr.TableName.Qualifier = sqlparser.NewTableIdent("")
	}
}

func stripDownQuery(from, to sqlparser.SelectStatement) error {
	var err error

	switch node := from.(type) {
	case *sqlparser.Select:
		toNode, ok := to.(*sqlparser.Select)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
		}
		toNode.Distinct = node.Distinct
		toNode.GroupBy = node.GroupBy
		toNode.Having = node.Having
		toNode.OrderBy = node.OrderBy
		toNode.Comments = node.Comments
		toNode.SelectExprs = node.SelectExprs
		for i, expr := range toNode.SelectExprs {
			removeKeyspaceFromSelectExpr(expr, toNode, i)
		}
	case *sqlparser.Union:
		toNode, ok := to.(*sqlparser.Union)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
		}
		err = stripDownQuery(node.Left, toNode.Left)
		if err != nil {
			return err
		}
		err = stripDownQuery(node.Right, toNode.Right)
		if err != nil {
			return err
		}
		toNode.OrderBy = node.OrderBy
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: this should not happen - we have covered all implementations of SelectStatement %T", from)
	}
	return nil
}

func pushJoinPredicate(ctx *planningContext, exprs []sqlparser.Expr, tree queryTree) (queryTree, error) {
	if len(exprs) == 0 {
		return tree, nil
	}
	switch node := tree.(type) {
	case *routeTree:
		return pushJoinPredicateOnRoute(ctx, exprs, node)
	case *joinTree:
		return pushJoinPredicateOnJoin(ctx, exprs, node)
	case *derivedTree:
		return pushJoinPredicateOnDerived(ctx, exprs, node)
	case *vindexTree:
		// vindexFunc cannot accept predicates from the other side of a join
		return node, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unknown type %T", node)
	}
}

func pushJoinPredicateOnRoute(ctx *planningContext, exprs []sqlparser.Expr, node *routeTree) (queryTree, error) {
	plan := node.clone().(*routeTree)
	err := plan.addPredicate(ctx, exprs...)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func pushJoinPredicateOnDerived(ctx *planningContext, exprs []sqlparser.Expr, node *derivedTree) (queryTree, error) {
	plan := node.clone().(*derivedTree)

	newExpressions := make([]sqlparser.Expr, 0, len(exprs))
	for _, expr := range exprs {
		tblInfo, err := ctx.semTable.TableInfoForExpr(expr)
		if err != nil {
			return nil, err
		}
		rewritten, err := semantics.RewriteDerivedExpression(expr, tblInfo)
		if err != nil {
			return nil, err
		}
		newExpressions = append(newExpressions, rewritten)
	}

	newInner, err := pushJoinPredicate(ctx, newExpressions, plan.inner)
	if err != nil {
		return nil, err
	}

	plan.inner = newInner
	return plan, nil
}

func pushJoinPredicateOnJoin(ctx *planningContext, exprs []sqlparser.Expr, node *joinTree) (queryTree, error) {
	node = node.clone().(*joinTree)

	var rhsPreds []sqlparser.Expr
	var lhsPreds []sqlparser.Expr
	var lhsColumns []*sqlparser.ColName
	var lhsVarsName []string

	for _, expr := range exprs {
		// We find the dependencies for the given expression and if they are solved entirely by one
		// side of the join tree, then we push the predicate there and do not break it into parts.
		// In case a predicate has no dependencies, then it is pushed to both sides so that we can filter
		// rows as early as possible making join cheaper on the vtgate level.
		depsForExpr := ctx.semTable.RecursiveDeps(expr)
		singleSideDeps := false
		if depsForExpr.IsSolvedBy(node.lhs.tableID()) {
			lhsPreds = append(lhsPreds, expr)
			singleSideDeps = true
		}
		if depsForExpr.IsSolvedBy(node.rhs.tableID()) {
			rhsPreds = append(rhsPreds, expr)
			singleSideDeps = true
		}

		if singleSideDeps {
			continue
		}

		bvName, cols, predicate, err := breakPredicateInLHSandRHS(expr, ctx.semTable, node.lhs.tableID())
		if err != nil {
			return nil, err
		}
		lhsColumns = append(lhsColumns, cols...)
		lhsVarsName = append(lhsVarsName, bvName...)
		rhsPreds = append(rhsPreds, predicate)
	}

	if lhsColumns != nil && lhsVarsName != nil {
		idxs, err := node.pushOutputColumns(lhsColumns, ctx.semTable)
		if err != nil {
			return nil, err
		}
		for i, idx := range idxs {
			node.vars[lhsVarsName[i]] = idx
		}
	}
	lhsPlan, err := pushJoinPredicate(ctx, lhsPreds, node.lhs)
	if err != nil {
		return nil, err
	}

	rhsPlan, err := pushJoinPredicate(ctx, rhsPreds, node.rhs)
	if err != nil {
		return nil, err
	}
	return &joinTree{
		lhs:      lhsPlan,
		rhs:      rhsPlan,
		leftJoin: node.leftJoin,
		vars:     node.vars,
	}, nil
}

func breakPredicateInLHSandRHS(
	expr sqlparser.Expr,
	semTable *semantics.SemTable,
	lhs semantics.TableSet,
) (bvNames []string, columns []*sqlparser.ColName, predicate sqlparser.Expr, err error) {
	predicate = sqlparser.CloneExpr(expr)
	_ = sqlparser.Rewrite(predicate, nil, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			deps := semTable.RecursiveDeps(node)
			if deps.NumberOfTables() == 0 {
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown column. has the AST been copied?")
				return false
			}
			if deps.IsSolvedBy(lhs) {
				node.Qualifier.Qualifier = sqlparser.NewTableIdent("")
				columns = append(columns, node)
				bvName := node.CompliantName()
				bvNames = append(bvNames, bvName)
				arg := sqlparser.NewArgument(bvName)
				cursor.Replace(arg)
			}
		}
		return true
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return
}

func mergeOrJoin(ctx *planningContext, lhs, rhs queryTree, joinPredicates []sqlparser.Expr, inner bool) (queryTree, error) {
	newTabletSet := lhs.tableID().Merge(rhs.tableID())

	merger := func(a, b *routeTree) (*routeTree, error) {
		if inner {
			return createRoutePlanForInner(a, b, newTabletSet, joinPredicates), nil
		}
		return createRoutePlanForOuter(ctx, a, b, newTabletSet, joinPredicates), nil
	}

	newPlan, _ := tryMerge(ctx, lhs, rhs, joinPredicates, merger)
	if newPlan != nil {
		return newPlan, nil
	}

	tree := &joinTree{lhs: lhs.clone(), rhs: rhs.clone(), leftJoin: !inner, vars: map[string]int{}}
	return pushJoinPredicate(ctx, joinPredicates, tree)
}

type (
	tableSetPair struct {
		left, right semantics.TableSet
	}
	cacheMap map[tableSetPair]queryTree
)

/*
	The greedy planner will plan a query by finding first finding the best route plan for every table.
    Then, iteratively, it finds the cheapest join that can be produced between the remaining plans,
	and removes the two inputs to this cheapest plan and instead adds the join.
	As an optimization, it first only considers joining tables that have predicates defined between them
*/
func greedySolve(ctx *planningContext, qg *abstract.QueryGraph) (queryTree, error) {
	joinTrees, err := seedPlanList(ctx, qg)
	planCache := cacheMap{}
	if err != nil {
		return nil, err
	}

	tree, err := mergeJoinTrees(ctx, qg, joinTrees, planCache, false)
	if err != nil {
		return nil, err
	}
	return tree, nil
}

func mergeJoinTrees(ctx *planningContext, qg *abstract.QueryGraph, joinTrees []queryTree, planCache cacheMap, crossJoinsOK bool) (queryTree, error) {
	if len(joinTrees) == 0 {
		return nil, nil
	}
	for len(joinTrees) > 1 {
		bestTree, lIdx, rIdx, err := findBestJoinTree(ctx, qg, joinTrees, planCache, crossJoinsOK)
		if err != nil {
			return nil, err
		}
		// if we found a best plan, we'll replace the two plans that were joined with the join plan created
		if bestTree != nil {
			// we need to remove the larger of the two plans first
			if rIdx > lIdx {
				joinTrees = removeAt(joinTrees, rIdx)
				joinTrees = removeAt(joinTrees, lIdx)
			} else {
				joinTrees = removeAt(joinTrees, lIdx)
				joinTrees = removeAt(joinTrees, rIdx)
			}
			joinTrees = append(joinTrees, bestTree)
		} else {
			// we will only fail to find a join plan when there are only cross joins left
			// when that happens, we switch over to allow cross joins as well.
			// this way we prioritize joining joinTrees with predicates first
			crossJoinsOK = true
		}
	}
	return joinTrees[0], nil
}

func (cm cacheMap) getJoinTreeFor(ctx *planningContext, lhs, rhs queryTree, joinPredicates []sqlparser.Expr) (queryTree, error) {
	solves := tableSetPair{left: lhs.tableID(), right: rhs.tableID()}
	cachedPlan := cm[solves]
	if cachedPlan != nil {
		return cachedPlan, nil
	}

	join, err := mergeOrJoin(ctx, lhs, rhs, joinPredicates, true)
	if err != nil {
		return nil, err
	}
	cm[solves] = join
	return join, nil
}

func findBestJoinTree(
	ctx *planningContext,
	qg *abstract.QueryGraph,
	plans []queryTree,
	planCache cacheMap,
	crossJoinsOK bool,
) (bestPlan queryTree, lIdx int, rIdx int, err error) {
	for i, lhs := range plans {
		for j, rhs := range plans {
			if i == j {
				continue
			}
			joinPredicates := qg.GetPredicates(lhs.tableID(), rhs.tableID())
			if len(joinPredicates) == 0 && !crossJoinsOK {
				// if there are no predicates joining the two tables,
				// creating a join between them would produce a
				// cartesian product, which is almost always a bad idea
				continue
			}
			plan, err := planCache.getJoinTreeFor(ctx, lhs, rhs, joinPredicates)
			if err != nil {
				return nil, 0, 0, err
			}
			if bestPlan == nil || plan.cost() < bestPlan.cost() {
				bestPlan = plan
				// remember which plans we based on, so we can remove them later
				lIdx = i
				rIdx = j
			}
		}
	}
	return bestPlan, lIdx, rIdx, nil
}

func leftToRightSolve(ctx *planningContext, qg *abstract.QueryGraph) (queryTree, error) {
	plans, err := seedPlanList(ctx, qg)
	if err != nil {
		return nil, err
	}

	var acc queryTree
	for _, plan := range plans {
		if acc == nil {
			acc = plan
			continue
		}
		joinPredicates := qg.GetPredicates(acc.tableID(), plan.tableID())
		acc, err = mergeOrJoin(ctx, acc, plan, joinPredicates, true)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// seedPlanList returns a routeTree for each table in the qg
func seedPlanList(ctx *planningContext, qg *abstract.QueryGraph) ([]queryTree, error) {
	plans := make([]queryTree, len(qg.Tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.Tables {
		solves := ctx.semTable.TableSetFor(table.Alias)
		plan, err := createRoutePlan(ctx, table, solves)
		if err != nil {
			return nil, err
		}
		if qg.NoDeps != nil {
			plan.predicates = append(plan.predicates, sqlparser.SplitAndExpression(nil, qg.NoDeps)...)
		}
		plans[i] = plan
	}
	return plans, nil
}

func removeAt(plans []queryTree, idx int) []queryTree {
	return append(plans[:idx], plans[idx+1:]...)
}

func createRoutePlan(ctx *planningContext, table *abstract.QueryTable, solves semantics.TableSet) (*routeTree, error) {
	if table.IsInfSchema {
		ks, err := ctx.vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
		rp := &routeTree{
			routeOpCode: engine.SelectDBA,
			solved:      solves,
			keyspace:    ks,
			tables: []relation{&routeTable{
				qtable: table,
				vtable: &vindexes.Table{
					Name:     table.Table.Name,
					Keyspace: ks,
				},
			}},
			predicates: table.Predicates,
		}
		err = rp.findSysInfoRoutingPredicatesGen4(ctx.reservedVars)
		if err != nil {
			return nil, err
		}

		return rp, nil
	}
	vschemaTable, _, _, _, _, err := ctx.vschema.FindTableOrVindex(table.Table)
	if err != nil {
		return nil, err
	}
	if vschemaTable.Name.String() != table.Table.Name.String() {
		// we are dealing with a routed table
		name := table.Table.Name
		table.Table.Name = vschemaTable.Name
		astTable, ok := table.Alias.Expr.(sqlparser.TableName)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] a derived table should never be a routed table")
		}
		realTableName := sqlparser.NewTableIdent(vschemaTable.Name.String())
		astTable.Name = realTableName
		if table.Alias.As.IsEmpty() {
			// if the user hasn't specified an alias, we'll insert one here so the old table name still works
			table.Alias.As = sqlparser.NewTableIdent(name.String())
		}
	}
	plan := &routeTree{
		solved: solves,
		tables: []relation{&routeTable{
			qtable: table,
			vtable: vschemaTable,
		}},
		keyspace: vschemaTable.Keyspace,
	}

	for _, columnVindex := range vschemaTable.ColumnVindexes {
		plan.vindexPreds = append(plan.vindexPreds, &vindexPlusPredicates{colVindex: columnVindex, tableID: solves})
	}

	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		plan.routeOpCode = engine.SelectNext
	case vschemaTable.Type == vindexes.TypeReference:
		plan.routeOpCode = engine.SelectReference
	case !vschemaTable.Keyspace.Sharded:
		plan.routeOpCode = engine.SelectUnsharded
	case vschemaTable.Pinned != nil:
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		plan.routeOpCode = engine.SelectEqualUnique
		vindex, _ := vindexes.NewBinary("binary", nil)
		plan.selected = &vindexOption{
			ready:       true,
			values:      []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, vschemaTable.Pinned)}},
			valueExprs:  nil,
			predicates:  nil,
			opcode:      engine.SelectEqualUnique,
			foundVindex: vindex,
			cost: cost{
				opCode: engine.SelectEqualUnique,
			},
		}
	default:
		plan.routeOpCode = engine.SelectScatter
	}
	err = plan.addPredicate(ctx, table.Predicates...)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func findColumnVindex(ctx *planningContext, a *routeTree, exp sqlparser.Expr) vindexes.SingleColumn {
	_, isCol := exp.(*sqlparser.ColName)
	if !isCol {
		return nil
	}

	var singCol vindexes.SingleColumn

	// for each equality expression that exp has with other column name, we check if it
	// can be solved by any table in our routeTree a. If an equality expression can be solved,
	// we check if the equality expression and our table share the same vindex, if they do:
	// the method will return the associated vindexes.SingleColumn.
	for _, expr := range ctx.semTable.GetExprAndEqualities(exp) {
		col, isCol := expr.(*sqlparser.ColName)
		if !isCol {
			continue
		}
		leftDep := ctx.semTable.RecursiveDeps(expr)
		_ = visitRelations(a.tables, func(rel relation) (bool, error) {
			rb, isRoute := rel.(*routeTable)
			if !isRoute {
				return true, nil
			}
			if leftDep.IsSolvedBy(rb.qtable.TableID) {
				for _, vindex := range rb.vtable.ColumnVindexes {
					sC, isSingle := vindex.Vindex.(vindexes.SingleColumn)
					if isSingle && vindex.Columns[0].Equal(col.Name) {
						singCol = sC
						return false, io.EOF
					}
				}
			}
			return false, nil
		})
		if singCol != nil {
			return singCol
		}
	}

	return singCol
}

func canMergeOnFilter(ctx *planningContext, a, b *routeTree, predicate sqlparser.Expr) bool {
	comparison, ok := predicate.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualOp {
		return false
	}
	left := comparison.Left
	right := comparison.Right

	lVindex := findColumnVindex(ctx, a, left)
	if lVindex == nil {
		left, right = right, left
		lVindex = findColumnVindex(ctx, a, left)
	}
	if lVindex == nil || !lVindex.IsUnique() {
		return false
	}
	rVindex := findColumnVindex(ctx, b, right)
	if rVindex == nil {
		return false
	}
	return rVindex == lVindex
}

func canMergeOnFilters(ctx *planningContext, a, b *routeTree, joinPredicates []sqlparser.Expr) bool {
	for _, predicate := range joinPredicates {
		for _, expr := range sqlparser.SplitAndExpression(nil, predicate) {
			if canMergeOnFilter(ctx, a, b, expr) {
				return true
			}
		}
	}
	return false
}

type mergeFunc func(a, b *routeTree) (*routeTree, error)

func canMergeUnionPlans(ctx *planningContext, a, b *route) bool {
	// this method should be close to tryMerge below. it does the same thing, but on logicalPlans instead of queryTrees
	if a.eroute.Keyspace.Name != b.eroute.Keyspace.Name {
		return false
	}
	switch a.eroute.Opcode {
	case engine.SelectUnsharded, engine.SelectReference:
		return a.eroute.Opcode == b.eroute.Opcode
	case engine.SelectDBA:
		return b.eroute.Opcode == engine.SelectDBA &&
			len(a.eroute.SysTableTableSchema) == 0 &&
			len(a.eroute.SysTableTableName) == 0 &&
			len(b.eroute.SysTableTableSchema) == 0 &&
			len(b.eroute.SysTableTableName) == 0
	case engine.SelectEqualUnique:
		// Check if they target the same shard.
		if b.eroute.Opcode == engine.SelectEqualUnique &&
			a.eroute.Vindex == b.eroute.Vindex &&
			a.condition != nil &&
			b.condition != nil &&
			gen4ValuesEqual(ctx, []sqlparser.Expr{a.condition}, []sqlparser.Expr{b.condition}) {
			return true
		}
	case engine.SelectScatter:
		return b.eroute.Opcode == engine.SelectScatter
	case engine.SelectNext:
		return false
	}
	return false
}
func canMergeSubqueryPlans(ctx *planningContext, a, b *route) bool {
	// this method should be close to tryMerge below. it does the same thing, but on logicalPlans instead of queryTrees
	if a.eroute.Keyspace.Name != b.eroute.Keyspace.Name {
		return false
	}
	switch a.eroute.Opcode {
	case engine.SelectUnsharded, engine.SelectReference:
		return a.eroute.Opcode == b.eroute.Opcode
	case engine.SelectDBA:
		return b.eroute.Opcode == engine.SelectDBA &&
			len(a.eroute.SysTableTableSchema) == 0 &&
			len(a.eroute.SysTableTableName) == 0 &&
			len(b.eroute.SysTableTableSchema) == 0 &&
			len(b.eroute.SysTableTableName) == 0
	case engine.SelectEqualUnique:
		// Check if they target the same shard.
		if b.eroute.Opcode == engine.SelectEqualUnique &&
			a.eroute.Vindex == b.eroute.Vindex &&
			a.condition != nil &&
			b.condition != nil &&
			gen4ValuesEqual(ctx, []sqlparser.Expr{a.condition}, []sqlparser.Expr{b.condition}) {
			return true
		}
	}
	return false
}

func tryMerge(ctx *planningContext, a, b queryTree, joinPredicates []sqlparser.Expr, merger mergeFunc) (queryTree, error) {
	aRoute, bRoute := queryTreesToRoutes(a.clone(), b.clone())
	if aRoute == nil || bRoute == nil {
		return nil, nil
	}

	sameKeyspace := aRoute.keyspace == bRoute.keyspace

	if sameKeyspace || (isDualTable(aRoute) || isDualTable(bRoute)) {
		tree, err := tryMergeReferenceTable(aRoute, bRoute, merger)
		if tree != nil || err != nil {
			return tree, err
		}
	}

	switch aRoute.routeOpCode {
	case engine.SelectUnsharded, engine.SelectDBA:
		if aRoute.routeOpCode == bRoute.routeOpCode {
			return merger(aRoute, bRoute)
		}
	case engine.SelectEqualUnique:
		// if they are already both being sent to the same shard, we can merge
		if bRoute.routeOpCode == engine.SelectEqualUnique {
			if aRoute.selectedVindex() == bRoute.selectedVindex() &&
				gen4ValuesEqual(ctx, aRoute.vindexExpressions(), bRoute.vindexExpressions()) {
				return merger(aRoute, bRoute)
			}
			return nil, nil
		}
		fallthrough
	case engine.SelectScatter, engine.SelectIN:
		if len(joinPredicates) == 0 {
			// If we are doing two Scatters, we have to make sure that the
			// joins are on the correct vindex to allow them to be merged
			// no join predicates - no vindex
			return nil, nil
		}
		if !sameKeyspace {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard correlated subquery")
		}

		canMerge := canMergeOnFilters(ctx, aRoute, bRoute, joinPredicates)
		if !canMerge {
			return nil, nil
		}
		r, err := merger(aRoute, bRoute)
		r.pickBestAvailableVindex()
		return r, err
	}
	return nil, nil
}

func tryMergeReferenceTable(aRoute *routeTree, bRoute *routeTree, merger mergeFunc) (*routeTree, error) {
	// if either side is a reference table, we can just merge it and use the opcode of the other side
	var opCode engine.RouteOpcode
	var selected *vindexOption

	switch {
	case aRoute.routeOpCode == engine.SelectReference:
		selected = bRoute.selected
		opCode = bRoute.routeOpCode
	case bRoute.routeOpCode == engine.SelectReference:
		selected = aRoute.selected
		opCode = aRoute.routeOpCode
	default:
		return nil, nil
	}

	r, err := merger(aRoute, bRoute)
	if err != nil {
		return nil, err
	}
	r.routeOpCode = opCode
	r.selected = selected
	return r, nil
}

func isDualTable(route *routeTree) bool {
	if len(route.tables) != 1 || route.tables.tableNames()[0] != "dual" {
		return false
	}
	table := route.tables[0]
	routeTable, ok := table.(*routeTable)
	if !ok {
		return false
	}
	if routeTable.qtable.Table.Qualifier.IsEmpty() {
		return true
	}
	return false
}

func makeRoute(j queryTree) *routeTree {
	rb, ok := j.(*routeTree)
	if ok {
		return rb
	}

	x, ok := j.(*derivedTree)
	if !ok {
		return nil
	}
	dp := x.clone().(*derivedTree)

	inner := makeRoute(dp.inner)
	if inner == nil {
		return nil
	}

	dt := &derivedTable{
		tables:     inner.tables,
		query:      dp.query,
		predicates: inner.predicates,
		leftJoins:  inner.leftJoins,
		alias:      dp.alias,
	}

	inner.tables = parenTables{dt}
	inner.predicates = nil
	inner.leftJoins = nil
	return inner
}

func queryTreesToRoutes(a, b queryTree) (*routeTree, *routeTree) {
	aRoute := makeRoute(a)
	if aRoute == nil {
		return nil, nil
	}
	bRoute := makeRoute(b)
	if bRoute == nil {
		return nil, nil
	}
	return aRoute, bRoute
}

func createRoutePlanForInner(aRoute, bRoute *routeTree, newTabletSet semantics.TableSet, joinPredicates []sqlparser.Expr) *routeTree {
	var tables parenTables
	if !aRoute.hasOuterjoins() {
		tables = append(aRoute.tables, bRoute.tables...)
	} else {
		tables = append(parenTables{aRoute.tables}, bRoute.tables...)
	}

	// append system table names from both the routes.
	sysTableName := aRoute.SysTableTableName
	if sysTableName == nil {
		sysTableName = bRoute.SysTableTableName
	} else {
		for k, v := range bRoute.SysTableTableName {
			sysTableName[k] = v
		}
	}

	r := &routeTree{
		routeOpCode: aRoute.routeOpCode,
		solved:      newTabletSet,
		tables:      tables,
		predicates: append(
			append(aRoute.predicates, bRoute.predicates...),
			joinPredicates...),
		keyspace:            aRoute.keyspace,
		vindexPreds:         append(aRoute.vindexPreds, bRoute.vindexPreds...),
		leftJoins:           append(aRoute.leftJoins, bRoute.leftJoins...),
		SysTableTableSchema: append(aRoute.SysTableTableSchema, bRoute.SysTableTableSchema...),
		SysTableTableName:   sysTableName,
	}

	if aRoute.selectedVindex() == bRoute.selectedVindex() {
		r.selected = aRoute.selected
	}

	return r
}

func findTables(deps semantics.TableSet, tables parenTables) (relation, relation, parenTables) {
	foundTables := parenTables{}
	newTables := parenTables{}

	for i, t := range tables {
		if t.tableID().IsSolvedBy(deps) {
			foundTables = append(foundTables, t)
			if len(foundTables) == 2 {
				return foundTables[0], foundTables[1], append(newTables, tables[i:])
			}
		} else {
			newTables = append(newTables, t)
		}
	}
	return nil, nil, tables
}

func createRoutePlanForOuter(ctx *planningContext, aRoute, bRoute *routeTree, newTabletSet semantics.TableSet, joinPredicates []sqlparser.Expr) *routeTree {
	// create relation slice with all tables
	tables := bRoute.tables
	// we are doing an outer join where the outer part contains multiple tables - we have to turn the outer part into a join or two
	for _, predicate := range bRoute.predicates {
		deps := ctx.semTable.RecursiveDeps(predicate)
		aTbl, bTbl, newTables := findTables(deps, tables)
		tables = newTables
		if aTbl != nil && bTbl != nil {
			tables = append(tables, &joinTables{
				lhs:  aTbl,
				rhs:  bTbl,
				pred: predicate,
			})
		}
	}

	var outer relation
	if len(tables) == 1 {
		// if we have a single relation, no need to put it inside parens
		outer = tables[0]
	} else {
		outer = tables
	}

	return &routeTree{
		routeOpCode: aRoute.routeOpCode,
		solved:      newTabletSet,
		tables:      aRoute.tables,
		leftJoins: append(aRoute.leftJoins, &outerTable{
			right: outer,
			pred:  sqlparser.AndExpressions(joinPredicates...),
		}),
		keyspace:    aRoute.keyspace,
		vindexPreds: append(aRoute.vindexPreds, bRoute.vindexPreds...),
	}
}

func gen4ValuesEqual(ctx *planningContext, a, b []sqlparser.Expr) bool {
	if len(a) != len(b) {
		return false
	}

	// TODO: check semTable's columnEqualities for better plan

	for i, aExpr := range a {
		bExpr := b[i]
		if !gen4ValEqual(ctx, aExpr, bExpr) {
			return false
		}
	}
	return true
}

func gen4ValEqual(ctx *planningContext, a, b sqlparser.Expr) bool {
	switch a := a.(type) {
	case *sqlparser.ColName:
		if b, ok := b.(*sqlparser.ColName); ok {
			if !a.Name.Equal(b.Name) {
				return false
			}

			return ctx.semTable.DirectDeps(a) == ctx.semTable.DirectDeps(b)
		}
	case sqlparser.Argument:
		b, ok := b.(sqlparser.Argument)
		if !ok {
			return false
		}
		return a == b
	case *sqlparser.Literal:
		b, ok := b.(*sqlparser.Literal)
		if !ok {
			return false
		}
		switch a.Type {
		case sqlparser.StrVal:
			switch b.Type {
			case sqlparser.StrVal:
				return a.Val == b.Val
			case sqlparser.HexVal:
				return hexEqual(b, a)
			}
		case sqlparser.HexVal:
			return hexEqual(a, b)
		case sqlparser.IntVal:
			if b.Type == (sqlparser.IntVal) {
				return a.Val == b.Val
			}
		}
	}
	return false
}

var _ sort.Interface = (parenTables)(nil)

func (p parenTables) Len() int {
	return len(p)
}

func (p parenTables) Less(i, j int) bool {
	return p[i].tableID().TableOffset() < p[j].tableID().TableOffset()
}

func (p parenTables) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
