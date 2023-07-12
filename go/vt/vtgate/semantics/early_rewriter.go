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

package semantics

import (
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type earlyRewriter struct {
	binder          *binder
	scoper          *scoper
	clause          string
	warning         string
	expandedColumns map[sqlparser.TableName][]*sqlparser.ColName
}

func (r *earlyRewriter) down(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Where:
		handleWhereClause(node, cursor.Parent())
	case sqlparser.SelectExprs:
		return handleSelectExprs(r, cursor, node)
	case *sqlparser.JoinTableExpr:
		handleJoinTableExpr(r, node)
	case sqlparser.OrderBy:
		handleOrderBy(r, cursor, node)
	case *sqlparser.OrExpr:
		rewriteOrExpr(cursor, node)
	case sqlparser.GroupBy:
		r.clause = "group statement"
	case *sqlparser.Literal:
		return handleLiteral(r, cursor, node)
	case *sqlparser.CollateExpr:
		return handleCollateExpr(r, node)
	case *sqlparser.ComparisonExpr:
		return handleComparisonExpr(cursor, node)
	}
	return nil
}

// handleWhereClause processes WHERE clauses, specifically the HAVING clause.
func handleWhereClause(node *sqlparser.Where, parent sqlparser.SQLNode) {
	if node.Type != sqlparser.HavingClause {
		return
	}
	rewriteHavingAndOrderBy(node, parent)
}

// handleSelectExprs expands * in SELECT expressions.
func handleSelectExprs(r *earlyRewriter, cursor *sqlparser.Cursor, node sqlparser.SelectExprs) error {
	_, isSel := cursor.Parent().(*sqlparser.Select)
	if !isSel {
		return nil
	}
	return r.expandStar(cursor, node)
}

// handleJoinTableExpr processes JOIN table expressions and handles the Straight Join type.
func handleJoinTableExpr(r *earlyRewriter, node *sqlparser.JoinTableExpr) {
	if node.Join != sqlparser.StraightJoinType {
		return
	}
	node.Join = sqlparser.NormalJoinType
	r.warning = "straight join is converted to normal join"
}

// handleOrderBy processes the ORDER BY clause.
func handleOrderBy(r *earlyRewriter, cursor *sqlparser.Cursor, node sqlparser.OrderBy) {
	r.clause = "order clause"
	rewriteHavingAndOrderBy(node, cursor.Parent())
}

// rewriteOrExpr rewrites OR expressions when the right side is FALSE.
func rewriteOrExpr(cursor *sqlparser.Cursor, node *sqlparser.OrExpr) {
	newNode := rewriteOrFalse(*node)
	if newNode != nil {
		cursor.Replace(newNode)
	}
}

// handleLiteral processes literals within the context of ORDER BY expressions.
func handleLiteral(r *earlyRewriter, cursor *sqlparser.Cursor, node *sqlparser.Literal) error {
	newNode, err := r.rewriteOrderByExpr(node)
	if err != nil {
		return err
	}
	if newNode != nil {
		cursor.Replace(newNode)
	}
	return nil
}

// handleCollateExpr processes COLLATE expressions.
func handleCollateExpr(r *earlyRewriter, node *sqlparser.CollateExpr) error {
	lit, ok := node.Expr.(*sqlparser.Literal)
	if !ok {
		return nil
	}
	newNode, err := r.rewriteOrderByExpr(lit)
	if err != nil {
		return err
	}
	if newNode != nil {
		node.Expr = newNode
	}
	return nil
}

// handleComparisonExpr processes Comparison expressions, specifically for tuples with equal length and EqualOp operator.
func handleComparisonExpr(cursor *sqlparser.Cursor, node *sqlparser.ComparisonExpr) error {
	lft, lftOK := node.Left.(sqlparser.ValTuple)
	rgt, rgtOK := node.Right.(sqlparser.ValTuple)
	if !lftOK || !rgtOK || len(lft) != len(rgt) || node.Operator != sqlparser.EqualOp {
		return nil
	}
	var predicates []sqlparser.Expr
	for i, l := range lft {
		r := rgt[i]
		predicates = append(predicates, &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualOp,
			Left:     l,
			Right:    r,
			Escape:   node.Escape,
		})
	}
	cursor.Replace(sqlparser.AndExpressions(predicates...))
	return nil
}

func (r *earlyRewriter) expandStar(cursor *sqlparser.Cursor, node sqlparser.SelectExprs) error {
	currentScope := r.scoper.currentScope()
	var selExprs sqlparser.SelectExprs
	changed := false
	for _, selectExpr := range node {
		starExpr, isStarExpr := selectExpr.(*sqlparser.StarExpr)
		if !isStarExpr {
			selExprs = append(selExprs, selectExpr)
			continue
		}
		starExpanded, colNames, err := r.expandTableColumns(starExpr, currentScope.tables, r.binder.usingJoinInfo, r.scoper.org)
		if err != nil {
			return err
		}
		if !starExpanded || colNames == nil {
			selExprs = append(selExprs, selectExpr)
			continue
		}
		selExprs = append(selExprs, colNames...)
		changed = true
	}
	if changed {
		cursor.ReplaceAndRevisit(selExprs)
	}
	return nil
}

// rewriteHavingAndOrderBy rewrites columns in the ORDER BY and HAVING clauses to use aliases
// from the SELECT expressions when applicable, following MySQL scoping rules:
//   - A column identifier without a table qualifier that matches an alias introduced
//     in SELECT points to that expression, not any table column.
//   - However, if the aliased expression is an aggregation and the column identifier in
//     the HAVING/ORDER BY clause is inside an aggregation function, the rule does not apply.
func rewriteHavingAndOrderBy(node, parent sqlparser.SQLNode) {
	sel, isSel := parent.(*sqlparser.Select)
	if !isSel {
		return
	}

	sqlparser.SafeRewrite(node, avoidSubqueries,
		func(cursor *sqlparser.Cursor) bool {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok || !col.Qualifier.IsEmpty() {
				// we are only interested in columns not qualified by table names
				return true
			}

			_, parentIsAggr := cursor.Parent().(sqlparser.AggrFunc)

			// Iterate through SELECT expressions.
			for _, e := range sel.SelectExprs {
				ae, ok := e.(*sqlparser.AliasedExpr)
				if !ok || !ae.As.Equal(col.Name) {
					// we are searching for aliased expressions that match the column we have found
					continue
				}

				expr := ae.Expr
				if parentIsAggr {
					if _, aliasPointsToAggr := expr.(sqlparser.AggrFunc); aliasPointsToAggr {
						return false
					}
				}

				if isSafeToRewrite(expr) {
					cursor.Replace(expr)
				}
			}
			return true
		})
}

func avoidSubqueries(node, _ sqlparser.SQLNode) bool {
	_, isSubQ := node.(*sqlparser.Subquery)
	return !isSubQ
}

func isSafeToRewrite(e sqlparser.Expr) bool {
	safeToRewrite := true
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.ColName:
			safeToRewrite = false
			return false, nil
		case sqlparser.AggrFunc:
			return false, nil
		}
		return true, nil
	}, e)
	return safeToRewrite
}

func (r *earlyRewriter) rewriteOrderByExpr(node *sqlparser.Literal) (sqlparser.Expr, error) {
	currScope, found := r.scoper.specialExprScopes[node]
	if !found {
		return nil, nil
	}
	num, err := strconv.Atoi(node.Val)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error parsing column number: %s", node.Val)
	}
	stmt, isSel := currScope.stmt.(*sqlparser.Select)
	if !isSel {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error invalid statement type, expect Select, got: %T", currScope.stmt)
	}

	if num < 1 || num > len(stmt.SelectExprs) {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%d' in '%s'", num, r.clause)
	}

	for i := 0; i < num; i++ {
		expr := stmt.SelectExprs[i]
		_, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot use column offsets in %s when using `%s`", r.clause, sqlparser.String(expr))
		}
	}

	aliasedExpr, ok := stmt.SelectExprs[num-1].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to handle %s", sqlparser.String(node))
	}

	if !aliasedExpr.As.IsEmpty() {
		return sqlparser.NewColName(aliasedExpr.As.String()), nil
	}

	expr := realCloneOfColNames(aliasedExpr.Expr, currScope.isUnion)
	return expr, nil
}

// realCloneOfColNames clones all the expressions including ColName.
// Since sqlparser.CloneRefOfColName does not clone col names, this method is needed.
func realCloneOfColNames(expr sqlparser.Expr, union bool) sqlparser.Expr {
	return sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		exp, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return
		}

		newColName := *exp
		if union {
			newColName.Qualifier = sqlparser.TableName{}
		}
		cursor.Replace(&newColName)
	}, nil).(sqlparser.Expr)
}

func rewriteOrFalse(orExpr sqlparser.OrExpr) sqlparser.Expr {
	// we are looking for the pattern `WHERE c = 1 OR 1 = 0`
	isFalse := func(subExpr sqlparser.Expr) bool {
		evalEnginePred, err := evalengine.Translate(subExpr, nil)
		if err != nil {
			return false
		}

		env := evalengine.EmptyExpressionEnv()
		res, err := env.Evaluate(evalEnginePred)
		if err != nil {
			return false
		}

		boolValue, err := res.Value().ToBool()
		if err != nil {
			return false
		}

		return !boolValue
	}

	if isFalse(orExpr.Left) {
		return orExpr.Right
	} else if isFalse(orExpr.Right) {
		return orExpr.Left
	}

	return nil
}

// rewriteJoinUsing rewrites SQL JOINs that use the USING clause to their equivalent
// JOINs with the ON condition. This function finds all the tables that have the
// specified columns in the USING clause, constructs an equality predicate for
// each pair of tables, and adds the resulting predicates to the WHERE clause
// of the outermost SELECT statement.
//
// For example, given the query:
//
//	SELECT * FROM t1 JOIN t2 USING (col1, col2)
//
// The rewriteJoinUsing function will rewrite the query to:
//
//	SELECT * FROM t1 JOIN t2 ON (t1.col1 = t2.col1 AND t1.col2 = t2.col2)
//
// This function returns an error if it encounters a non-authoritative table or
// if it cannot find a SELECT statement to add the WHERE predicate to.
func rewriteJoinUsing(
	current *scope,
	using sqlparser.Columns,
	org originable,
) error {
	predicates, err := buildJoinPredicates(current, using, org)
	if err != nil {
		return err
	}
	// now, we go up the scope until we find a SELECT
	// with a where clause we can add this predicate to
	for current != nil {
		sel, found := current.stmt.(*sqlparser.Select)
		if !found {
			current = current.parent
			continue
		}
		if sel.Where != nil {
			predicates = append(predicates, sel.Where.Expr)
			sel.Where = nil
		}
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: sqlparser.AndExpressions(predicates...),
		}
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "did not find WHERE clause")
}

// buildJoinPredicates constructs the join predicates for a given set of USING columns.
// It returns a slice of sqlparser.Expr, each representing a join predicate for the given columns.
func buildJoinPredicates(current *scope, using sqlparser.Columns, org originable) ([]sqlparser.Expr, error) {
	joinUsing := current.prepareUsingMap()
	var predicates []sqlparser.Expr

	for _, column := range using {
		foundTables, err := findTablesWithColumn(current, joinUsing, org, column)
		if err != nil {
			return nil, err
		}

		predicates = append(predicates, createComparisonPredicates(column, foundTables)...)
	}

	return predicates, nil
}

// findTablesWithColumn finds the tables with the specified column in the current scope.
func findTablesWithColumn(current *scope, joinUsing map[TableSet]map[string]TableSet, org originable, column sqlparser.IdentifierCI) ([]sqlparser.TableName, error) {
	var foundTables []sqlparser.TableName

	for _, tbl := range current.tables {
		if !tbl.authoritative() {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't handle JOIN USING without authoritative tables")
		}

		currTable := tbl.getTableSet(org)
		usingCols := joinUsing[currTable]
		if usingCols == nil {
			usingCols = map[string]TableSet{}
		}

		if hasColumnInTable(tbl, usingCols) {
			tblName, err := tbl.Name()
			if err != nil {
				return nil, err
			}
			foundTables = append(foundTables, tblName)
		}
	}

	return foundTables, nil
}

// hasColumnInTable checks if the specified table has the given column.
func hasColumnInTable(tbl TableInfo, usingCols map[string]TableSet) bool {
	for _, col := range tbl.getColumns() {
		_, found := usingCols[strings.ToLower(col.Name)]
		if found {
			return true
		}
	}
	return false
}

// createComparisonPredicates creates a list of comparison predicates between the given column and foundTables.
func createComparisonPredicates(column sqlparser.IdentifierCI, foundTables []sqlparser.TableName) []sqlparser.Expr {
	var predicates []sqlparser.Expr
	for i, lft := range foundTables {
		for j := i + 1; j < len(foundTables); j++ {
			rgt := foundTables[j]
			predicates = append(predicates, createComparisonBetween(column, lft, rgt))
		}
	}
	return predicates
}

func createComparisonBetween(column sqlparser.IdentifierCI, lft, rgt sqlparser.TableName) *sqlparser.ComparisonExpr {
	return &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     sqlparser.NewColNameWithQualifier(column.String(), lft),
		Right:    sqlparser.NewColNameWithQualifier(column.String(), rgt),
	}
}

func (r *earlyRewriter) expandTableColumns(
	starExpr *sqlparser.StarExpr,
	tables []TableInfo,
	joinUsing map[TableSet]map[string]TableSet,
	org originable,
) (bool, sqlparser.SelectExprs, error) {
	unknownTbl := true
	starExpanded := true
	state := &expanderState{
		colNames:        []sqlparser.SelectExpr{},
		needsQualifier:  len(tables) > 1,
		joinUsing:       joinUsing,
		org:             org,
		expandedColumns: map[sqlparser.TableName][]*sqlparser.ColName{},
	}

	for _, tbl := range tables {
		if !starExpr.TableName.IsEmpty() && !tbl.matches(starExpr.TableName) {
			continue
		}
		unknownTbl = false
		if !tbl.authoritative() {
			starExpanded = false
			break
		}
		err := state.processColumnsFor(tbl)
		if err != nil {
			return false, nil, err
		}
	}

	if unknownTbl {
		// This will only happen for case when starExpr has qualifier.
		return false, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadDb, "Unknown table '%s'", sqlparser.String(starExpr.TableName))
	}

	if starExpanded {
		for k, v := range state.expandedColumns {
			r.expandedColumns[k] = v
		}
	}

	return starExpanded, state.colNames, nil
}

func (e *expanderState) processColumnsFor(tbl TableInfo) error {
	tblName, err := tbl.Name()
	if err != nil {
		return err
	}
	currTable := tbl.getTableSet(e.org)
	usingCols := e.joinUsing[currTable]
	if usingCols == nil {
		usingCols = map[string]TableSet{}
	}

	/*
		Redundant column elimination and column ordering occurs according to standard SQL, producing this display order:
		  *	First, coalesced common columns of the two joined tables, in the order in which they occur in the first table
		  *	Second, columns unique to the first table, in order in which they occur in that table
		  *	Third, columns unique to the second table, in order in which they occur in that table

		From: https://dev.mysql.com/doc/refman/8.0/en/join.html
	*/

outer:
	// in this first loop we just find columns used in any JOIN USING used on this table
	for _, col := range tbl.getColumns() {
		ts, found := usingCols[col.Name]
		if found {
			for i, ts := range ts.Constituents() {
				if ts == currTable {
					if i == 0 {
						e.addColumn(col, tbl, tblName)
					} else {
						continue outer
					}
				}
			}
		}
	}

	// and this time around we are printing any columns not involved in any JOIN USING
	for _, col := range tbl.getColumns() {
		if ts, found := usingCols[col.Name]; found && currTable.IsSolvedBy(ts) {
			continue
		}

		e.addColumn(col, tbl, tblName)
	}
	return nil
}

type expanderState struct {
	needsQualifier  bool
	colNames        sqlparser.SelectExprs
	joinUsing       map[TableSet]map[string]TableSet
	org             originable
	expandedColumns map[sqlparser.TableName][]*sqlparser.ColName
}

// addColumn adds columns to the expander state. If we have vschema info about the query,
// we also store which columns were expanded
func (e *expanderState) addColumn(col ColumnInfo, tbl TableInfo, tblName sqlparser.TableName) {
	tableAliased := !tbl.GetExpr().As.IsEmpty()
	withQualifier := e.needsQualifier || tableAliased
	var colName *sqlparser.ColName
	var alias sqlparser.IdentifierCI
	if withQualifier {
		colName = sqlparser.NewColNameWithQualifier(col.Name, tblName)
	} else {
		colName = sqlparser.NewColName(col.Name)
	}
	if e.needsQualifier {
		alias = sqlparser.NewIdentifierCI(col.Name)
	}
	e.colNames = append(e.colNames, &sqlparser.AliasedExpr{Expr: colName, As: alias})
	e.storeExpandInfo(tbl, tblName, colName)
}

func (e *expanderState) storeExpandInfo(tbl TableInfo, tblName sqlparser.TableName, colName *sqlparser.ColName) {
	vt := tbl.GetVindexTable()
	if vt == nil {
		return
	}
	keyspace := vt.Keyspace
	var ks sqlparser.IdentifierCS
	if keyspace != nil {
		ks = sqlparser.NewIdentifierCS(keyspace.Name)
	}
	tblName = sqlparser.TableName{
		Name:      tblName.Name,
		Qualifier: ks,
	}
	e.expandedColumns[tblName] = append(e.expandedColumns[tblName], colName)
}

// createAliasedExpr creates an AliasedExpr with a ColName and an optional alias based on the given ColumnInfo.
// We need table qualifiers if there are more than one table in the FROM clause.
// If we are adding qualifiers, we also add an alias so the qualifiers do not show
// up in the result. For example, SELECT * FROM t1, t2 -> SELECT t1.col AS col, t2.col AS col ...
func (e *expanderState) createAliasedExpr(
	col ColumnInfo,
	tbl TableInfo,
	tblName sqlparser.TableName,
) *sqlparser.AliasedExpr {
	tableAliased := !tbl.GetExpr().As.IsEmpty()
	withQualifier := e.needsQualifier || tableAliased
	var colName *sqlparser.ColName
	var alias sqlparser.IdentifierCI
	if withQualifier {
		colName = sqlparser.NewColNameWithQualifier(col.Name, tblName)
	} else {
		colName = sqlparser.NewColName(col.Name)
	}
	if e.needsQualifier {
		alias = sqlparser.NewIdentifierCI(col.Name)
	}
	return &sqlparser.AliasedExpr{Expr: colName, As: alias}
}
