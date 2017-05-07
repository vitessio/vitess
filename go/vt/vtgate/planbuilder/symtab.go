/*
Copyright 2017 Google Inc.

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
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// column represents a unique symbol in the query that other
// parts can refer to. A column can be a table column or
// a result column (select expression). Every column
// contains the route it originates from.
// Columns for table columns are created or reused by
// symtab through the Find function.
//
// Columns for select expressions are created by the originating
// primitive, and referenced by other primitives that pass
// the value through. Right now, only a route can be the
// originating primitve for a column, and the data structures
// imply this restriction. In the future, other primitives
// may originate columns. For example, an Aggregator primitive
// could originate a `count(*)` by using an underlying column.
// If a select expression is a simple reference to
// a table column, that column object gets reused.
// If not, an anonymous reference is created.
//
// Anonymous columns can also be created by symtab to represent
// ambiguous column references, but whose route can still be
// identified. For exeample, in the case of 'select a from t1, t2',
// if t1 and t2 are from the same unsharded keyspace, 'a' will
// be created as an anonymous column because we don't know
// which table it's coming from. Consequently, anonymous columns
// can only be created for single-route plans, and they're
// used only for push-down decisions.
//
// For a column whose table is known, the column name and
// pointer to the table are stored. This information is
// used to construct a select expression if the column is
// requested during the wire-up phase.
// If the table column has a vindex, then that information
// is also stored and used to make routing decisions.
// Two columns are equal only if their pointer values match,
// and not their content.
type column struct {
	route  *route
	Vindex vindexes.Vindex
	name   sqlparser.ColIdent
	table  *table
}

// Route returns the route that originates the column.
func (c *column) Route() *route {
	return c.route.Resolve()
}

// SelectExpr returns a select expression that
// can be added to the AST.
func (c *column) SelectExpr() sqlparser.SelectExpr {
	return &sqlparser.NonStarExpr{
		Expr: &sqlparser.ColName{
			Metadata:  c,
			Qualifier: sqlparser.TableName{Name: c.table.alias.Name},
			Name:      c.name,
		},
	}
}

// symtab represents the symbol table for a SELECT statement
// or a subquery. The symtab evolves over time. It starts off
// with tables. It adds columns to table columns as they are
// referenced. After a select expression is analyzed, the
// ResultColumns field is set. In the case of a subquery, the
// Outer field points to the outer symtab. Any symbols that
// are not resolved locally are added to the Externs field,
// which is later used to figure out if the subquery can be
// merged with an outer route.
type symtab struct {
	tables        map[sqlparser.TableName]*table
	ResultColumns []*resultColumn
	Outer         *symtab
	Externs       []*sqlparser.ColName
	VSchema       VSchema
}

// newSymtab creates a new symtab initialized
// to contain the provided table alias.
func newSymtab(vschema VSchema) *symtab {
	return &symtab{
		tables:  make(map[sqlparser.TableName]*table),
		VSchema: vschema,
	}
}

// AddAlias adds a table alias to symtab. Currently, this function
// is only called to add the first table. Additional tables are
// added to the symtab through calls to Merge.
func (st *symtab) AddAlias(alias sqlparser.TableName, vindexTable *vindexes.Table, rb *route) {
	table := &table{
		alias:   alias,
		columns: make(map[string]*column),
		route:   rb,
	}

	// Pre-create vindex columns
	for _, cv := range vindexTable.ColumnVindexes {
		table.columns[cv.Column.Lowered()] = &column{
			route:  rb,
			Vindex: cv.Vindex,
			name:   cv.Column,
			table:  table,
		}
	}
	st.tables[alias] = table
}

// Merge merges the new symtab into the current one.
// Duplicate aliases return an error.
// The routes of the table aliases are not affected.
// Merge is only allowed in the early stages of symtab.
// The function panics if ResultColumns or Externs contain values.
// This is because symbol tables are allowed to merge only during
// analysis for the FROM clause. At that time, there should be
// no ResultColumns or Externs.
func (st *symtab) Merge(newsyms *symtab) error {
	if st.ResultColumns != nil || newsyms.ResultColumns != nil {
		panic("unexpected ResultColumns")
	}
	if st.Externs != nil || newsyms.Externs != nil {
		panic("unexpected Externs")
	}
	for k, t := range newsyms.tables {
		if _, ok := st.tables[k]; ok {
			return fmt.Errorf("duplicate symbol: %s", sqlparser.String(k))
		}
		st.tables[k] = t
	}
	return nil
}

// ClearVindexes removes the Column Vindexes from the aliases signifying
// that they cannot be used to make routing improvements. This is
// called if a primitive is in the RHS of a LEFT JOIN.
func (st *symtab) ClearVindexes() {
	for _, t := range st.tables {
		for _, c := range t.columns {
			c.Vindex = nil
		}
	}
}

// Find returns the route for the symbol referenced by col.
// If a reference is found, the column's Metadata is set to point
// to it. Subsequent searches will reuse this metadata.
// If autoResolve is true, and there is only one table present,
// then an unqualified reference is assumed to be implicitly against
// that table. The table info doesn't contain the full list of columns.
// So, any column reference is presumed valid. If a ResultColumns scope is
// present, then unqualifed column names are searched there first.
// If a symbol is found in the current symtab, then isLocal is
// set to true. Otherwise, the search is continued in the outer symtab.
// If found, isLocal will be set to false. If the symbol was not found,
// an error is returned. If isLocal is false, then it means the primitive
// comes from an outer route. So, you cannot push-down to such a route.
// If a symbol was found in an outer scope, then the column reference
// is added to the Externs field.
func (st *symtab) Find(col *sqlparser.ColName, autoResolve bool) (rb *route, isLocal bool, err error) {
	if column, ok := col.Metadata.(*column); ok {
		return column.Route(), column.Route().Symtab() == st, nil
	}
	if col.Qualifier.IsEmpty() {
		rb, err = st.searchResultColumn(col)
		switch {
		case err != nil:
			return nil, false, err
		case rb != nil:
			return rb, true, nil
		}
	}

	if rb = st.searchTables(col, autoResolve); rb != nil {
		return rb, true, nil
	}

	// Outer scope is not searched if ResultColumns has values.
	if len(st.ResultColumns) != 0 || st.Outer == nil {
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}

	// autoResolve only allowed for innermost scope.
	if rb, _, err = st.Outer.Find(col, false); err != nil {
		return nil, false, err
	}
	st.Externs = append(st.Externs, col)
	return rb, false, nil
}

// searchResultColumn looks for col in the results columns and
// returns the route if found.
func (st *symtab) searchResultColumn(col *sqlparser.ColName) (rb *route, err error) {
	var cursym *resultColumn
	for _, rc := range st.ResultColumns {
		if rc.alias.Equal(col.Name) {
			if cursym != nil {
				return nil, fmt.Errorf("ambiguous symbol reference: %v", sqlparser.String(col))
			}
			cursym = rc
			col.Metadata = rc.column
		}
	}
	if cursym != nil {
		return cursym.column.Route(), nil
	}
	return nil, nil
}

// searchTables looks for the column in the tables. If not found, it
// optimistically creates a reference if a table can be identified.
// If autoResolve is true and there is only one table present, then
// the column is assumed to be from that table.
func (st *symtab) searchTables(col *sqlparser.ColName, autoResolve bool) *route {
	// Identify the table.
	var t *table
	if col.Qualifier.IsEmpty() {
		// if autoResolve is set, and there's only one table,
		// we resolve to it.
		if autoResolve && len(st.tables) == 1 {
			// Loop executes once.
			for _, v := range st.tables {
				t = v
			}
		} else {
			return nil
		}
	} else {
		var ok bool
		t, ok = st.tables[col.Qualifier]
		if !ok {
			return nil
		}
	}

	// At this point, t should be set.
	c, ok := t.columns[col.Name.Lowered()]
	if !ok {
		c = &column{
			route: t.route,
			name:  col.Name,
			table: t,
		}
		t.columns[col.Name.Lowered()] = c
	}

	// At this point, c should be set.
	col.Metadata = c
	return c.Route()
}

// NewResultColumn creates a new resultColumn based on the supplied expression.
// The created symbol is not remembered until it is later set as ResultColumns
// after all select expressions are analyzed.
func (st *symtab) NewResultColumn(expr *sqlparser.NonStarExpr, rb *route) *resultColumn {
	rc := &resultColumn{
		alias: expr.As,
	}
	if col, ok := expr.Expr.(*sqlparser.ColName); ok {
		// If no alias was specified, then the base name
		// of the column becomes the alias.
		if rc.alias.IsEmpty() {
			rc.alias = col.Name
		}
		// If it's a col it should already have metadata.
		rc.column = col.Metadata.(*column)
	} else {
		// We don't generate an alias if the expression is non-trivial.
		// Just to be safe, generate an anonymous column for the expression.
		rc.column = &column{
			route: rb,
		}
	}
	return rc
}

// Vindex returns the vindex if the expression is a plain column reference
// that is part of the specified route, and has an associated vindex.
func (st *symtab) Vindex(expr sqlparser.Expr, scope *route, autoResolve bool) vindexes.Vindex {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil
	}
	if col.Metadata == nil {
		// Find will set the Metadata.
		if _, _, err := st.Find(col, autoResolve); err != nil {
			return nil
		}
	}
	c := col.Metadata.(*column)
	rb := c.Route()
	if rb != scope {
		return nil
	}
	return c.Vindex
}

// table is part of symtab.
// It represents a table alias in a FROM clause.
// The table points to a route into which we'll try to
// push the rest of the surrounding clauses.
// A table alias could also represent a subquery. But
// there's no difference in how it's treated compared to a normal
// table.
type table struct {
	alias   sqlparser.TableName
	columns map[string]*column
	route   *route
}

// resultColumn contains symbol info about a select expression. If the
// expression represents an underlying column, then it points to it.
// Otherwise, an anonymous column is created as place-holder.
type resultColumn struct {
	// alias will represent the unqualified symbol name for that expression.
	// If the statement provides an explicit alias, that name will be used.
	// If the expression is a simple column, then the base name of the
	// column will be used as the alias. If the expression is non-trivial,
	// alias will be empty, and cannot be referenced from other parts of
	// the query.
	alias  sqlparser.ColIdent
	column *column
}
