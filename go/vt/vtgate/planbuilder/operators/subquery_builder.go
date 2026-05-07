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

// SubQueryBuilder extracts subqueries from SQL expressions and builds SubQuery operators.
// ARCHITECTURE: This struct is used in two ways: (1) as a container when created empty with &SubQueryBuilder{} - the
// ID fields remain zero and unused, only Inner accumulates results; (2) as a processor when created with explicit ID
// fields - used internally to analyze a single subquery's predicates.
// When processing multiple subqueries, the outer container SQB delegates to nested processor SQBs (one per subquery).
type SubQueryBuilder struct {
	// Inner accumulates all extracted SubQuery operators.
	Inner []*SubQuery

	// totalID contains all tables from both the subquery being processed and its outer context (equals subqID ∪ outerID).
	// Used to identify predicates that span inner/outer boundaries. Only set when this SQB is a processor (not a container).
	totalID semantics.TableSet

	// subqID contains tables inside the subquery being processed.
	// Only set when this SQB is a processor (not a container).
	subqID semantics.TableSet

	// outerID contains tables available from the outer query context that the subquery can reference.
	// Only set when this SQB is a processor (not a container).
	outerID semantics.TableSet
}

// getRootOperator wraps the given operator with a SubQueryContainer if any subqueries were extracted.
// If decorator is provided, it's applied to each subquery's operator before wrapping. Returns the original operator if no subqueries were found.
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

// handleSubquery extracts every subquery within the given predicate and creates
// a SubQuery operator for each. When the predicate contains more than one
// subquery (e.g. `EXISTS(s1) OR EXISTS(s2)`), the operators are linked via a
// shared subqueryGroup so subsequent merge / settle stages can coordinate
// substitutions and emit the predicate exactly once.
//
// outerID specifies which tables are visible to the subqueries from their outer
// context. Returns true if at least one subquery was found and consumed.
func (sqb *SubQueryBuilder) handleSubquery(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	outerID semantics.TableSet,
) bool {
	found := getAllSubQueries(expr)
	if len(found) == 0 {
		return false
	}

	created := make([]*SubQuery, 0, len(found))
	for _, f := range found {
		argName := ctx.ReservedVars.ReserveSubQuery()
		sqInner := createSubqueryOp(ctx, f.parent, expr, f.subq, outerID, argName, f.path)
		created = append(created, sqInner)
	}

	if len(created) > 1 {
		// Multiple subqueries share one predicate. Tie them together with a
		// single subqueryGroup so they read/write a shared Original and so
		// settle / merge can identify which member owns which Subquery node
		// by path.
		group := &subqueryGroup{
			Original: created[0].Original,
			Members:  created,
			merged:   make(map[*SubQuery]bool, len(created)),
		}
		for _, sq := range created {
			sq.group = group
			sq.Original = group.Original
		}
	}

	sqb.Inner = append(sqb.Inner, created...)
	return true
}

// foundSubquery describes a *sqlparser.Subquery node located within a predicate
// during extraction. parent is the most-relevant enclosing expression (e.g. the
// wrapping ExistsExpr or NotExpr-wrapping-ExistsExpr) used to determine the
// pullout opcode; path locates the Subquery within the predicate tree.
type foundSubquery struct {
	subq   *sqlparser.Subquery
	parent sqlparser.Expr
	path   sqlparser.ASTPath
}

// getAllSubQueries walks the given expression in DFS order and returns every
// *sqlparser.Subquery node found, paired with the parent expression that
// determines its pullout opcode and the path locating it within the tree.
//
// For each found subquery, parent is the immediate parent expression in the
// AST (e.g. *sqlparser.ExistsExpr, *sqlparser.ComparisonExpr). If the parent
// is *sqlparser.ExistsExpr that is itself wrapped in *sqlparser.NotExpr, the
// parent is reported as the NotExpr so callers can identify NOT EXISTS.
//
// Predicates like `A OR EXISTS(s1) OR EXISTS(s2)` produce two entries.
func getAllSubQueries(expr sqlparser.Expr) []foundSubquery {
	var results []foundSubquery
	pendingIdx := -1

	_ = sqlparser.RewriteWithPath(expr, func(cursor *sqlparser.Cursor) bool {
		if subq, ok := cursor.Node().(*sqlparser.Subquery); ok {
			info := foundSubquery{subq: subq, path: cursor.Path(), parent: subq}
			if pe, ok := cursor.Parent().(sqlparser.Expr); ok {
				info.parent = pe
			}
			results = append(results, info)
			pendingIdx = len(results) - 1
			// Don't descend into the subquery body — nested subqueries belong
			// to a different inspection scope handled by the inner SQB.
			return false
		}
		return true
	}, func(cursor *sqlparser.Cursor) bool {
		if pendingIdx < 0 {
			return true
		}
		// We're exiting a node above the most-recently-found subquery. If
		// that node is wrapped in a NotExpr, promote the NotExpr to be the
		// reported parent so the opcode dispatcher can detect NOT EXISTS.
		if not, isNot := cursor.Parent().(*sqlparser.NotExpr); isNot {
			results[pendingIdx].parent = not
		}
		pendingIdx = -1
		return true
	})
	return results
}

// createSubqueryOp creates a SubQuery operator by dispatching to the appropriate creation function based on the parent expression type.
// The parent determines the pullout opcode (EXISTS, IN, NOT IN, etc.). Returns a fully constructed SubQuery operator.
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

// inspectStatement recursively processes a SELECT or UNION statement to find and extract subqueries.
// Returns predicates that connect inner/outer queries and join columns for correlation. Only called on processor SQBs (with initialized ID fields), not container SQBs.
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

// inspectSelect extracts subqueries from WHERE, HAVING, and ON clauses of a SELECT statement.
// Rewrites the AST to replace subqueries with argument placeholders. Returns predicates and join columns for merging inner/outer queries.
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

// createSubquery builds a SubQuery operator for a pulled-out subquery expression.
// Creates a nested processor SQB (with initialized ID fields) to analyze predicates within the subquery's SELECT statement. Returns a complete SubQuery operator with join predicates and correlation information.
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

	subqDependencies := ctx.SemTable.RecursiveDeps(subq)
	correlated := subqDependencies.KeepOnly(outerID).NotEmpty()

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
		// path intentionally unset: this constructor is used by
		// pullOutValueSubqueries (projection IsArgument), not by handleSubquery.
	}
}

// createSubqueryFromPath builds a SubQuery operator using an AST path to locate the subquery node.
// Uses the path to extract the correct subquery node after AST cloning. Otherwise identical to createSubquery.
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

	subqDependencies := ctx.SemTable.RecursiveDeps(subq)
	correlated := subqDependencies.KeepOnly(outerID).NotEmpty()

	opInner := translateQueryToOp(ctx, subq.Select)

	opInner = sqc.getRootOperator(opInner, nil)
	return &SubQuery{
		FilterType:       filterType,
		Subquery:         opInner,
		Predicates:       predicates,
		Original:         original,
		ArgName:          argName,
		originalSubquery: originalSq,
		path:             path,
		IsArgument:       isArg,
		TopLevel:         topLevel,
		JoinColumns:      joinCols,
		correlated:       correlated,
	}
}

// inspectWhere processes a WHERE or HAVING clause to extract subqueries and identify join predicates.
// Uses this SQB's ID fields (subqID, outerID, totalID) to classify predicates that connect inner/outer queries. Returns the rewritten WHERE clause, extracted predicates, and join columns.
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
		if sqb.handleSubquery(ctx, predicate, sqb.totalID) {
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

// inspectOnExpr processes JOIN ON conditions to extract subqueries and identify join predicates.
// Rewrites the FROM clause with subqueries replaced by arguments. Returns the rewritten FROM clause, extracted predicates, and join columns.
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
				if sqb.handleSubquery(ctx, pred, sqb.totalID) {
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

// createComparisonSubQuery handles subqueries within comparison expressions (IN, NOT IN).
// Extracts the comparison's outer side to create an OuterPredicate for potential merge optimization. Returns a SubQuery with the appropriate pullout opcode.
func createComparisonSubQuery(
	ctx *plancontext.PlanningContext,
	parent *sqlparser.ComparisonExpr,
	original sqlparser.Expr,
	subFromOutside *sqlparser.Subquery,
	path sqlparser.ASTPath,
	outerID semantics.TableSet,
	name string,
) *SubQuery {
	var outside sqlparser.Expr
	filterType := opcode.PulloutValue
	switch {
	case parent.Left == subFromOutside:
		outside = parent.Right
	case parent.Right == subFromOutside:
		outside = parent.Left
		switch parent.Operator {
		case sqlparser.InOp:
			filterType = opcode.PulloutIn
		case sqlparser.NotInOp:
			filterType = opcode.PulloutNotIn
		}
	default:
		panic("uh oh")
	}

	subquery := createSubqueryFromPath(ctx, original, subFromOutside, path, outerID, parent, name, filterType, false)

	// if we are comparing with a column from the inner subquery,
	// we add this extra predicate to check if the two sides are mergable or not
	if _, ok := outside.(*sqlparser.Subquery); ok {
		return subquery
	}
	if ae, ok := subFromOutside.Select.GetColumns()[0].(*sqlparser.AliasedExpr); ok {
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

// getOpCodeFromParent determines the pullout opcode for a subquery based on its parent expression type.
// Returns nil for EXISTS (handled separately) or the appropriate opcode for IN/NOT IN/value contexts.
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
