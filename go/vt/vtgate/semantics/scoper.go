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
	"reflect"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// scoper is responsible for figuring out the scoping for the query,
	// and keeps the current scope when walking the tree
	scoper struct {
		rScope map[*sqlparser.Select]*scope
		wScope map[*sqlparser.Select]*scope
		scopes []*scope
		org    originable
		binder *binder

		// These scopes are only used for rewriting ORDER BY 1 and GROUP BY 1
		specialExprScopes map[*sqlparser.Literal]*scope
		statementIDs      map[sqlparser.Statement]TableSet
	}

	scope struct {
		parent       *scope
		stmt         sqlparser.Statement
		tables       []TableInfo
		isUnion      bool
		joinUsing    map[string]TableSet
		stmtScope    bool
		ctes         map[string]*sqlparser.CommonTableExpr
		inGroupBy    bool
		inHaving     bool
		inHavingAggr bool
	}
)

func newScoper() *scoper {
	return &scoper{
		rScope:            map[*sqlparser.Select]*scope{},
		wScope:            map[*sqlparser.Select]*scope{},
		specialExprScopes: map[*sqlparser.Literal]*scope{},
		statementIDs:      map[sqlparser.Statement]TableSet{},
	}
}

func (s *scoper) down(cursor *sqlparser.Cursor) error {
	node := cursor.Node()
	switch node := node.(type) {
	case *sqlparser.Update, *sqlparser.Delete, *sqlparser.Insert:
		s.pushDMLScope(node)
	case *sqlparser.Select:
		s.pushSelectScope(node)
	case *sqlparser.Union:
		s.pushUnionScope(node)
	case sqlparser.TableExpr:
		s.enterJoinScope(cursor)
	case sqlparser.SelectExprs:
		s.copySelectExprs(cursor, node)
	case sqlparser.OrderBy:
		return s.addColumnInfoForOrderBy(cursor, node)
	case sqlparser.GroupBy:
		return s.addColumnInfoForGroupBy(cursor, node)
	case sqlparser.AggrFunc:
		if !s.currentScope().inHaving {
			break
		}
		s.currentScope().inHavingAggr = true
	case *sqlparser.Where:
		if node.Type == sqlparser.HavingClause {
			err := s.createSpecialScopePostProjection(cursor.Parent())
			if err != nil {
				return err
			}
			s.currentScope().inHaving = true
			return nil
		}
	}
	return nil
}

func (s *scoper) pushUnionScope(union *sqlparser.Union) {
	currentScope := s.currentScope()
	currScope := newScope(currentScope)
	currScope.stmtScope = true
	currScope.stmt = union
	s.push(currScope)
}

func (s *scoper) addColumnInfoForGroupBy(cursor *sqlparser.Cursor, node sqlparser.GroupBy) error {
	err := s.createSpecialScopePostProjection(cursor.Parent())
	if err != nil {
		return err
	}
	currentScope := s.currentScope()
	currentScope.inGroupBy = true
	for _, expr := range node {
		lit := keepIntLiteral(expr)
		if lit != nil {
			s.specialExprScopes[lit] = currentScope
		}
	}
	return nil
}

func (s *scoper) addColumnInfoForOrderBy(cursor *sqlparser.Cursor, node sqlparser.OrderBy) error {
	if isParentSelectStatement(cursor) {
		err := s.createSpecialScopePostProjection(cursor.Parent())
		if err != nil {
			return err
		}
		for _, order := range node {
			lit := keepIntLiteral(order.Expr)
			if lit != nil {
				s.specialExprScopes[lit] = s.currentScope()
			}
		}
	}
	return nil
}

func (s *scoper) copySelectExprs(cursor *sqlparser.Cursor, node sqlparser.SelectExprs) {
	sel, parentIsSelect := cursor.Parent().(*sqlparser.Select)
	if !parentIsSelect {
		return
	}

	// adding a vTableInfo for each SELECT, so it can be used by GROUP BY, HAVING, ORDER BY
	// the vTableInfo we are creating here should not be confused with derived tables' vTableInfo
	wScope, exists := s.wScope[sel]
	if !exists {
		return
	}
	wScope.tables = []TableInfo{createVTableInfoForExpressions(node, s.currentScope().tables, s.org)}
}

func (s *scoper) enterJoinScope(cursor *sqlparser.Cursor) {
	if isParentSelect(cursor) {
		// when checking the expressions used in JOIN conditions, special rules apply where the ON expression
		// can only see the two tables involved in the JOIN, and no other tables of that select statement.
		// They are allowed to see the tables of the outer select query.
		// To create this special context, we will find the parent scope of the select statement involved.
		currScope := s.currentScope()
		stmtScope := currScope.findParentScopeOfStatement()
		nScope := newScope(stmtScope)
		if stmtScope == nil {
			// TODO: this feels hacky. revisit with a better plan
			nScope.ctes = currScope.ctes
		}
		nScope.stmt = cursor.Parent().(*sqlparser.Select)
		s.push(nScope)
	}
}

func (s *scoper) pushSelectScope(node *sqlparser.Select) {
	currScope := newScope(s.currentScope())
	currScope.stmtScope = true
	s.push(currScope)

	// Needed for order by with Literal to find the Expression.
	currScope.stmt = node

	s.rScope[node] = currScope
	s.wScope[node] = newScope(nil)
}

func (s *scoper) pushDMLScope(node sqlparser.SQLNode) {
	currScope := newScope(s.currentScope())
	currScope.stmtScope = true
	s.push(currScope)

	currScope.stmt = node.(sqlparser.Statement)
}

func keepIntLiteral(e sqlparser.Expr) *sqlparser.Literal {
	coll, ok := e.(*sqlparser.CollateExpr)
	if ok {
		e = coll.Expr
	}
	l, ok := e.(*sqlparser.Literal)
	if !ok {
		return nil
	}
	if l.Type != sqlparser.IntVal {
		return nil
	}
	return l
}

func (s *scoper) up(cursor *sqlparser.Cursor) error {
	node := cursor.Node()
	switch node := node.(type) {
	case sqlparser.OrderBy:
		if isParentSelectStatement(cursor) {
			s.popScope()
		}
	case *sqlparser.Select, sqlparser.GroupBy, *sqlparser.Update, *sqlparser.Insert, *sqlparser.Union, *sqlparser.Delete:
		id := EmptyTableSet()
		for _, tableInfo := range s.currentScope().tables {
			set := tableInfo.getTableSet(s.org)
			id = id.Merge(set)
		}
		s.statementIDs[s.currentScope().stmt] = id
		s.popScope()
	case *sqlparser.Where:
		if node.Type != sqlparser.HavingClause {
			break
		}
		s.popScope()
	case sqlparser.AggrFunc:
		s.currentScope().inHavingAggr = false
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			curScope := s.currentScope()
			s.popScope()
			earlierScope := s.currentScope()
			// copy curScope into the earlierScope
			for _, table := range curScope.tables {
				err := earlierScope.addTable(table)
				if err != nil {
					return err
				}
			}
		}
		if isParentDeleteOrUpdate(cursor) {
			usingMap := s.currentScope().prepareUsingMap()
			for ts, m := range usingMap {
				s.binder.usingJoinInfo[ts] = m
			}
		}
	}
	return nil
}

func ValidAsMapKey(s sqlparser.SQLNode) bool {
	return reflect.TypeOf(s).Comparable()
}

// createSpecialScopePostProjection is used for the special projection in ORDER BY, GROUP BY and HAVING
func (s *scoper) createSpecialScopePostProjection(parent sqlparser.SQLNode) error {
	switch parent := parent.(type) {
	case *sqlparser.Select:
		// In ORDER BY, GROUP BY and HAVING, we can see both the scope in the FROM part of the query, and the SELECT columns created
		// so before walking the rest of the tree, we change the scope to match this behaviour
		incomingScope := s.currentScope()
		nScope := newScope(incomingScope)
		nScope.tables = s.wScope[parent].tables
		nScope.stmt = incomingScope.stmt
		s.push(nScope)

		if s.rScope[parent] != incomingScope {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: scope counts did not match")
		}
	case *sqlparser.Union:
		nScope := newScope(nil)
		nScope.isUnion = true
		var tableInfo *vTableInfo

		for i, sel := range sqlparser.GetAllSelects(parent) {
			if i == 0 {
				nScope.stmt = sel
				tableInfo = createVTableInfoForExpressions(sel.SelectExprs, nil /*needed for star expressions*/, s.org)
				nScope.tables = append(nScope.tables, tableInfo)
			}
			thisTableInfo := createVTableInfoForExpressions(sel.SelectExprs, nil /*needed for star expressions*/, s.org)
			if len(tableInfo.cols) != len(thisTableInfo.cols) {
				return vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.WrongNumberOfColumnsInSelect, "The used SELECT statements have a different number of columns")
			}
			for i, col := range tableInfo.cols {
				// at this stage, we don't store the actual dependencies, we only store the expressions.
				// only later will we walk the expression tree and figure out the deps. so, we need to create a
				// composite expression that contains all the expressions in the SELECTs that this UNION consists of
				tableInfo.cols[i] = sqlparser.AndExpressions(col, thisTableInfo.cols[i])
			}
		}

		s.push(nScope)
	}
	return nil
}

func (s *scoper) currentScope() *scope {
	size := len(s.scopes)
	if size == 0 {
		return nil
	}
	return s.scopes[size-1]
}

func (s *scoper) push(sc *scope) {
	s.scopes = append(s.scopes, sc)
}

func (s *scoper) popScope() {
	usingMap := s.currentScope().prepareUsingMap()
	for ts, m := range usingMap {
		s.binder.usingJoinInfo[ts] = m
	}
	l := len(s.scopes) - 1
	s.scopes = s.scopes[:l]
}

func newScope(parent *scope) *scope {
	return &scope{
		parent:    parent,
		joinUsing: map[string]TableSet{},
		ctes:      map[string]*sqlparser.CommonTableExpr{},
	}
}

func (s *scope) addCTE(cte *sqlparser.CommonTableExpr) error {
	name := cte.ID.String()
	_, exists := s.ctes[name]
	if exists {
		return vterrors.VT03013(name)
	}
	if err := checkForInvalidAliasUse(cte, name); err != nil {
		return err
	}
	s.ctes[name] = cte
	return nil
}

func checkForInvalidAliasUse(cte *sqlparser.CommonTableExpr, name string) (err error) {
	// TODO I'm sure there is a better. way, but we need to do this to stop infinite loops from occurring
	down := func(node sqlparser.SQLNode, parent sqlparser.SQLNode) bool {
		tbl, ok := node.(sqlparser.TableName)
		if !ok || tbl.Qualifier.NotEmpty() {
			return err == nil
		}
		if tbl.Name.String() == name {
			err = vterrors.VT12001("do not support CTE that use the CTE alias inside the CTE query")
		}
		return err == nil
	}
	_ = sqlparser.CopyOnRewrite(cte.Subquery.Select, down, nil, nil)
	return err
}

func (s *scope) addTable(info TableInfo) error {
	name, err := info.Name()
	if err != nil {
		return err
	}
	tblName := name.Name.String()
	for _, table := range s.tables {
		name, err := table.Name()
		if err != nil {
			return err
		}

		if tblName == name.Name.String() {
			return vterrors.VT03013(name.Name.String())
		}
	}
	s.tables = append(s.tables, info)
	return nil
}

func (s *scope) prepareUsingMap() (result map[TableSet]map[string]TableSet) {
	result = map[TableSet]map[string]TableSet{}
	for colName, tss := range s.joinUsing {
		for _, ts := range tss.Constituents() {
			m := result[ts]
			if m == nil {
				m = map[string]TableSet{}
			}
			m[colName] = tss
			result[ts] = m
		}
	}
	return
}

// findParentScopeOfStatement finds the scope that belongs to a statement.
func (s *scope) findParentScopeOfStatement() *scope {
	if s.stmtScope {
		return s.parent
	}
	if s.parent == nil {
		return nil
	}
	return s.parent.findParentScopeOfStatement()
}

// findCTE will search in this scope, and then recursively search the parents
func (s *scope) findCTE(name string) *sqlparser.CommonTableExpr {
	cte, found := s.ctes[name]
	if found || s.parent == nil {
		// if we don't have a parent, we'll return
		// whatever we have, even if it happens to be nil
		return cte
	}
	return s.parent.findCTE(name)
}
