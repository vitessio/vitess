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

package semantics

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// TableInfo contains information about tables
	TableInfo interface {
		// Name returns the table name
		Name() (sqlparser.TableName, error)

		// GetVindexTable returns the vschema version of this TableInfo
		GetVindexTable() *vindexes.Table

		// IsInfSchema returns true if this table is information_schema
		IsInfSchema() bool

		// matches returns true if the provided table name matches this TableInfo
		matches(name sqlparser.TableName) bool

		// authoritative is true if we have exhaustive column information
		authoritative() bool

		// getExpr returns the AST struct behind this table
		getExpr() *sqlparser.AliasedTableExpr

		// getColumns returns the known column information for this table
		getColumns() []ColumnInfo

		dependencies(colName string, org originable) (dependencies, error)
		getExprFor(s string) (sqlparser.Expr, error)
		getTableSet(org originable) TableSet
	}

	// ColumnInfo contains information about columns
	ColumnInfo struct {
		Name string
		Type Type
	}

	// ExprDependencies stores the tables that an expression depends on as a map
	ExprDependencies map[sqlparser.Expr]TableSet

	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		Tables []TableInfo
		// ProjectionErr stores the error that we got during the semantic analysis of the SelectExprs.
		// This is only a real error if we are unable to plan the query as a single route
		ProjectionErr error

		// Recursive contains the dependencies from the expression to the actual tables
		// in the query (i.e. not including derived tables). If an expression is a column on a derived table,
		// this map will contain the accumulated dependencies for the column expression inside the derived table
		Recursive ExprDependencies

		// Direct keeps information about the closest dependency for an expression.
		// It does not recurse inside derived tables and the like to find the original dependencies
		Direct ExprDependencies

		exprTypes   map[sqlparser.Expr]Type
		selectScope map[*sqlparser.Select]*scope
		Comments    sqlparser.Comments
		SubqueryMap map[*sqlparser.Select][]*sqlparser.ExtractedSubquery
		SubqueryRef map[*sqlparser.Subquery]*sqlparser.ExtractedSubquery

		// ColumnEqualities is used to enable transitive closures
		// if a == b and b == c then a == c
		ColumnEqualities map[columnName][]sqlparser.Expr

		Warning string
	}

	columnName struct {
		Table      TableSet
		ColumnName string
	}

	// SchemaInformation is used tp provide table information from Vschema.
	SchemaInformation interface {
		FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	}
)

var (
	// ErrMultipleTables refers to an error happening when something should be used only for single tables
	ErrMultipleTables = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should only be used for single tables")
)

// CopyDependencies copies the dependencies from one expression into the other
func (st *SemTable) CopyDependencies(from, to sqlparser.Expr) {
	st.Recursive[to] = st.RecursiveDeps(from)
	st.Direct[to] = st.DirectDeps(from)
}

// NewSemTable creates a new empty SemTable
func NewSemTable() *SemTable {
	return &SemTable{
		Recursive:        map[sqlparser.Expr]TableSet{},
		Direct:           map[sqlparser.Expr]TableSet{},
		ColumnEqualities: map[columnName][]sqlparser.Expr{},
	}
}

// TableSetFor returns the bitmask for this particular table
func (st *SemTable) TableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for idx, t2 := range st.Tables {
		if t == t2.getExpr() {
			return SingleTableSet(idx)
		}
	}
	return TableSet{}
}

// TableInfoFor returns the table info for the table set. It should contains only single table.
func (st *SemTable) TableInfoFor(id TableSet) (TableInfo, error) {
	offset := id.TableOffset()
	if offset < 0 {
		return nil, ErrMultipleTables
	}
	return st.Tables[offset], nil
}

// RecursiveDeps return the table dependencies of the expression.
func (st *SemTable) RecursiveDeps(expr sqlparser.Expr) TableSet {
	return st.Recursive.dependencies(expr)
}

// DirectDeps return the table dependencies of the expression.
func (st *SemTable) DirectDeps(expr sqlparser.Expr) TableSet {
	return st.Direct.dependencies(expr)
}

// AddColumnEquality adds a relation of the given colName to the ColumnEqualities map
func (st *SemTable) AddColumnEquality(colName *sqlparser.ColName, expr sqlparser.Expr) {
	ts := st.Direct.dependencies(colName)
	columnName := columnName{
		Table:      ts,
		ColumnName: colName.Name.String(),
	}
	elem := st.ColumnEqualities[columnName]
	elem = append(elem, expr)
	st.ColumnEqualities[columnName] = elem
}

// GetExprAndEqualities returns a slice containing the given expression, and it's known equalities if any
func (st *SemTable) GetExprAndEqualities(expr sqlparser.Expr) []sqlparser.Expr {
	result := []sqlparser.Expr{expr}
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		table := st.DirectDeps(expr)
		k := columnName{Table: table, ColumnName: expr.Name.String()}
		result = append(result, st.ColumnEqualities[k]...)
	}
	return result
}

// TableInfoForExpr returns the table info of the table that this expression depends on.
// Careful: this only works for expressions that have a single table dependency
func (st *SemTable) TableInfoForExpr(expr sqlparser.Expr) (TableInfo, error) {
	return st.TableInfoFor(st.Direct.dependencies(expr))
}

// GetSelectTables returns the table in the select.
func (st *SemTable) GetSelectTables(node *sqlparser.Select) []TableInfo {
	scope := st.selectScope[node]
	return scope.tables
}

// AddExprs adds new select exprs to the SemTable.
func (st *SemTable) AddExprs(tbl *sqlparser.AliasedTableExpr, cols sqlparser.SelectExprs) {
	tableSet := st.TableSetFor(tbl)
	for _, col := range cols {
		st.Recursive[col.(*sqlparser.AliasedExpr).Expr] = tableSet
	}
}

// TypeFor returns the type of expressions in the query
func (st *SemTable) TypeFor(e sqlparser.Expr) *querypb.Type {
	typ, found := st.exprTypes[e]
	if found {
		return &typ.Type
	}
	return nil
}

// CollationFor returns the collation name of expressions in the query
func (st *SemTable) CollationFor(e sqlparser.Expr) collations.ID {
	typ, found := st.exprTypes[e]
	if found {
		return typ.Collation
	}
	return collations.Unknown
}

// dependencies return the table dependencies of the expression. This method finds table dependencies recursively
func (d ExprDependencies) dependencies(expr sqlparser.Expr) (deps TableSet) {
	if ValidAsMapKey(expr) {
		// we have something that could live in the cache
		var found bool
		deps, found = d[expr]
		if found {
			return deps
		}
		defer func() {
			d[expr] = deps
		}()
	}

	// During the original semantic analysis, all ColNames were found and bound to the corresponding tables
	// Here, we'll walk the expression tree and look to see if we can find any sub-expressions
	// that have already set dependencies.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		expr, ok := node.(sqlparser.Expr)
		if !ok || !ValidAsMapKey(expr) {
			// if this is not an expression, or it is an expression we can't use as a map-key,
			// just carry on down the tree
			return true, nil
		}

		if extracted, ok := expr.(*sqlparser.ExtractedSubquery); ok {
			if extracted.OtherSide != nil {
				set := d.dependencies(extracted.OtherSide)
				deps.MergeInPlace(set)
			}
			return false, nil
		}
		set, found := d[expr]
		deps.MergeInPlace(set)

		// if we found a cached value, there is no need to continue down to visit children
		return !found, nil
	}, expr)

	return deps
}

// RewriteDerivedExpression rewrites all the ColName instances in the supplied expression with
// the expressions behind the column definition of the derived table
// SELECT foo FROM (SELECT id+42 as foo FROM user) as t
// We need `foo` to be translated to `id+42` on the inside of the derived table
func RewriteDerivedExpression(expr sqlparser.Expr, vt TableInfo) (sqlparser.Expr, error) {
	newExpr := sqlparser.CloneExpr(expr)
	sqlparser.Rewrite(newExpr, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			exp, err := vt.getExprFor(node.Name.String())
			if err == nil {
				cursor.Replace(exp)
			} else {
				// cloning the expression and removing the qualifier
				col := *node
				col.Qualifier = sqlparser.TableName{}
				cursor.Replace(&col)
			}
			return false
		}
		return true
	}, nil)
	return newExpr, nil
}

// FindSubqueryReference goes over the sub queries and searches for it by value equality instead of reference equality
func (st *SemTable) FindSubqueryReference(subquery *sqlparser.Subquery) *sqlparser.ExtractedSubquery {
	for foundSubq, extractedSubquery := range st.SubqueryRef {
		if sqlparser.EqualsRefOfSubquery(subquery, foundSubq) {
			return extractedSubquery
		}
	}
	return nil
}

// GetSubqueryNeedingRewrite returns a list of sub-queries that need to be rewritten
func (st *SemTable) GetSubqueryNeedingRewrite() []*sqlparser.ExtractedSubquery {
	var res []*sqlparser.ExtractedSubquery
	for _, extractedSubquery := range st.SubqueryRef {
		if extractedSubquery.NeedsRewrite {
			res = append(res, extractedSubquery)
		}
	}
	return res
}
