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
	"fmt"
	"strconv"

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
		if node.Type != sqlparser.HavingClause {
			return nil
		}
		rewriteHavingAndOrderBy(node, cursor.Parent())
	case sqlparser.SelectExprs:
		_, isSel := cursor.Parent().(*sqlparser.Select)
		if !isSel {
			return nil
		}
		err := r.expandStar(cursor, node)
		if err != nil {
			return err
		}
	case *sqlparser.JoinTableExpr:
		if node.Join == sqlparser.StraightJoinType {
			node.Join = sqlparser.NormalJoinType
			r.warning = "straight join is converted to normal join"
		}
	case sqlparser.OrderBy:
		r.clause = "order clause"
		rewriteHavingAndOrderBy(node, cursor.Parent())
	case *sqlparser.OrExpr:
		newNode := rewriteOrFalse(*node)
		if newNode != nil {
			cursor.Replace(newNode)
		}
	case sqlparser.GroupBy:
		r.clause = "group statement"

	case *sqlparser.Literal:
		newNode, err := r.rewriteOrderByExpr(node)
		if err != nil {
			return err
		}
		if newNode != nil {
			cursor.Replace(newNode)
		}
	case *sqlparser.CollateExpr:
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
	case *sqlparser.ComparisonExpr:
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
	}
	return nil
}

func (r *earlyRewriter) up(cursor *sqlparser.Cursor) error {
	// this rewriting is done in the `up` phase, because we need the scope to have been
	// filled in with the available tables
	node, ok := cursor.Node().(*sqlparser.JoinTableExpr)
	if !ok || len(node.Condition.Using) == 0 {
		return nil
	}

	err := rewriteJoinUsing(r.binder, node)
	if err != nil {
		return err
	}

	// since the binder has already been over the join, we need to invoke it again so it
	// can bind columns to the right tables
	sqlparser.Rewrite(node.Condition.On, nil, func(cursor *sqlparser.Cursor) bool {
		innerErr := r.binder.up(cursor)
		if innerErr == nil {
			return true
		}

		err = innerErr
		return false
	})
	return err
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

// rewriteHavingAndOrderBy rewrites columns on the ORDER BY/HAVING
// clauses to use aliases from the SELECT expressions when available.
// The scoping rules are:
//   - A column identifier with no table qualifier that matches an alias introduced
//     in SELECT points to that expression, and not at any table column
//   - Except when expression aliased is an aggregation, and the column identifier in the
//     HAVING/ORDER BY clause is inside an aggregation function
//
// This is a fucking weird scoping rule, but it's what MySQL seems to do... ¯\_(ツ)_/¯
func rewriteHavingAndOrderBy(node, parent sqlparser.SQLNode) {
	// TODO - clean up and comment this mess
	sel, isSel := parent.(*sqlparser.Select)
	if !isSel {
		return
	}

	sqlparser.SafeRewrite(node, func(node, _ sqlparser.SQLNode) bool {
		_, isSubQ := node.(*sqlparser.Subquery)
		return !isSubQ
	}, func(cursor *sqlparser.Cursor) bool {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return true
		}
		if !col.Qualifier.IsEmpty() {
			return true
		}
		_, parentIsAggr := cursor.Parent().(sqlparser.AggrFunc)
		for _, e := range sel.SelectExprs {
			ae, ok := e.(*sqlparser.AliasedExpr)
			if !ok || !ae.As.Equal(col.Name) {
				continue
			}
			_, aliasPointsToAggr := ae.Expr.(sqlparser.AggrFunc)
			if parentIsAggr && aliasPointsToAggr {
				return false
			}

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
			}, ae.Expr)
			if safeToRewrite {
				cursor.Replace(ae.Expr)
			}
		}
		return true
	})
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
func rewriteJoinUsing(b *binder, join *sqlparser.JoinTableExpr) error {
	predicates, err := buildJoinPredicates(b, join)
	if err != nil {
		return err
	}
	if len(predicates) > 0 {
		join.Condition.On = sqlparser.AndExpressions(predicates...)
		join.Condition.Using = nil
	}
	return nil
}

// buildJoinPredicates constructs the join predicates for a given set of USING columns.
// It returns a slice of sqlparser.Expr, each representing a join predicate for the given columns.
func buildJoinPredicates(b *binder, join *sqlparser.JoinTableExpr) ([]sqlparser.Expr, error) {
	var predicates []sqlparser.Expr

	for _, column := range join.Condition.Using {
		foundTables, err := findTablesWithColumn(b, join, column)
		if err != nil {
			return nil, err
		}

		predicates = append(predicates, createComparisonPredicates(column, foundTables)...)
	}

	return predicates, nil
}

func findOnlyOneTableInfoThatHasColumn(b *binder, tbl sqlparser.TableExpr, column sqlparser.IdentifierCI) ([]TableInfo, error) {
	switch tbl := tbl.(type) {
	case *sqlparser.AliasedTableExpr:
		ts := b.tc.tableSetFor(tbl)
		tblInfo := b.tc.Tables[ts.TableOffset()]
		for _, info := range tblInfo.getColumns() {
			if column.EqualString(info.Name) {
				return []TableInfo{tblInfo}, nil
			}
		}
		return nil, nil
	case *sqlparser.JoinTableExpr:
		tblInfoR, err := findOnlyOneTableInfoThatHasColumn(b, tbl.RightExpr, column)
		if err != nil {
			return nil, err
		}
		tblInfoL, err := findOnlyOneTableInfoThatHasColumn(b, tbl.LeftExpr, column)
		if err != nil {
			return nil, err
		}

		return append(tblInfoL, tblInfoR...), nil
	case *sqlparser.ParenTableExpr:
		var tblInfo []TableInfo
		for _, parenTable := range tbl.Exprs {
			newTblInfo, err := findOnlyOneTableInfoThatHasColumn(b, parenTable, column)
			if err != nil {
				return nil, err
			}
			if tblInfo != nil && newTblInfo != nil {
				return nil, vterrors.VT03021(column.String())
			}
			if newTblInfo != nil {
				tblInfo = newTblInfo
			}
		}
		return tblInfo, nil
	default:
		panic(fmt.Sprintf("unsupported TableExpr type in JOIN: %T", tbl))
	}
}

// findTablesWithColumn finds the tables with the specified column in the current scope.
func findTablesWithColumn(b *binder, join *sqlparser.JoinTableExpr, column sqlparser.IdentifierCI) ([]sqlparser.TableName, error) {
	leftTableInfo, err := findOnlyOneTableInfoThatHasColumn(b, join.LeftExpr, column)
	if err != nil {
		return nil, err
	}

	rightTableInfo, err := findOnlyOneTableInfoThatHasColumn(b, join.RightExpr, column)
	if err != nil {
		return nil, err
	}

	if leftTableInfo == nil || rightTableInfo == nil {
		return nil, ShardedError{Inner: vterrors.VT09015()}
	}
	var tableNames []sqlparser.TableName
	for _, info := range leftTableInfo {
		nm, err := info.Name()
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, nm)
	}
	for _, info := range rightTableInfo {
		nm, err := info.Name()
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, nm)
	}
	return tableNames, nil
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
	var colNames sqlparser.SelectExprs
	starExpanded := true
	expandedColumns := map[sqlparser.TableName][]*sqlparser.ColName{}
	for _, tbl := range tables {
		if !starExpr.TableName.IsEmpty() && !tbl.matches(starExpr.TableName) {
			continue
		}
		unknownTbl = false
		if !tbl.authoritative() {
			starExpanded = false
			break
		}
		tblName, err := tbl.Name()
		if err != nil {
			return false, nil, err
		}

		needsQualifier := len(tables) > 1
		tableAliased := !tbl.getExpr().As.IsEmpty()
		withQualifier := needsQualifier || tableAliased
		currTable := tbl.getTableSet(org)
		usingCols := joinUsing[currTable]
		if usingCols == nil {
			usingCols = map[string]TableSet{}
		}

		addColName := func(col ColumnInfo) {
			var colName *sqlparser.ColName
			var alias sqlparser.IdentifierCI
			if withQualifier {
				colName = sqlparser.NewColNameWithQualifier(col.Name, tblName)
			} else {
				colName = sqlparser.NewColName(col.Name)
			}
			if needsQualifier {
				alias = sqlparser.NewIdentifierCI(col.Name)
			}
			colNames = append(colNames, &sqlparser.AliasedExpr{Expr: colName, As: alias})
			vt := tbl.GetVindexTable()
			if vt != nil {
				keyspace := vt.Keyspace
				var ks sqlparser.IdentifierCS
				if keyspace != nil {
					ks = sqlparser.NewIdentifierCS(keyspace.Name)
				}
				tblName := sqlparser.TableName{
					Name:      tblName.Name,
					Qualifier: ks,
				}
				expandedColumns[tblName] = append(expandedColumns[tblName], colName)
			}
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
							addColName(col)
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

			addColName(col)
		}
	}

	if unknownTbl {
		// This will only happen for case when starExpr has qualifier.
		return false, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadDb, "Unknown table '%s'", sqlparser.String(starExpr.TableName))
	}
	if starExpanded {
		r.expandedColumns = expandedColumns
	}
	return starExpanded, colNames, nil
}
