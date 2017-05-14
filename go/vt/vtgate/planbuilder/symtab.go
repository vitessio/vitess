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
	return &sqlparser.AliasedExpr{
		Expr: &sqlparser.ColName{
			Metadata:  c,
			Qualifier: sqlparser.TableName{Name: c.table.alias.Name},
			Name:      c.name,
		},
	}
}

// symtab represents the symbol table for a SELECT statement
// or a subquery. The symtab evolves over time. Each symtab starts off
// with one table. As a query is analyzed, multiple independent
// symtabs are created, and they are later merged as each
// sub-expression of a FROM clause is merged.
//
// A symtab maintains uniqueColumns a list of unique vindex
// column names. These names can be resolved without the
// need to qualify them by their table names. If there are
// duplicates during a merge, those columns are removed from
// the unique list, thereby disallowing unqualifed references
// to such columns.
//
// After a select expression is analyzed, the
// ResultColumns field is set. In the case of a subquery, the
// Outer field points to the outer symtab. Any symbols that
// are not resolved locally are added to the Externs field,
// which is later used to figure out if the subquery can be
// merged with an outer route.
type symtab struct {
	tables map[sqlparser.TableName]*table
	// uniqueColumns has the column name as key
	// and points at the columns that tables contains.
	uniqueColumns map[string]*column
	// singleRoute is set only if all the symbols in
	// the symbol table are part of the same route.
	// The route is set at creation time to be the
	// the same as the route it was built for. As
	// symbols are added through Merge, the route
	// is validated against the newer symbols. If any
	// of them have a different route, the value is
	// set to nil.
	singleRoute   *route
	ResultColumns []*resultColumn
	Outer         *symtab
	Externs       []*sqlparser.ColName
	VSchema       VSchema
}

// newSymtab creates a new symtab initialized
// to containe just one route.
func newSymtab(vschema VSchema, rb *route) *symtab {
	return &symtab{
		tables:        make(map[sqlparser.TableName]*table),
		uniqueColumns: make(map[string]*column),
		VSchema:       vschema,
		singleRoute:   rb,
	}
}

// InitWithAlias initializes the symtab with a table alias.
// It panics if symtab already contains tables. Additional tables are
// added to the symtab through calls to Merge.
func (st *symtab) InitWithAlias(alias sqlparser.TableName, vindexTable *vindexes.Table, rb *route) {
	if len(st.tables) != 0 {
		panic(fmt.Sprintf("BUG: symtab already contains tables: %v", st.tables))
	}
	table := &table{
		alias:   alias,
		columns: make(map[string]*column),
		route:   rb,
	}

	// Pre-create vindex columns
	for _, cv := range vindexTable.ColumnVindexes {
		c := &column{
			route:  rb,
			Vindex: cv.Vindex,
			name:   cv.Column,
			table:  table,
		}
		table.columns[cv.Column.Lowered()] = c
		if c.Vindex != nil {
			st.uniqueColumns[cv.Column.Lowered()] = c
		}
	}
	st.tables[alias] = table
	st.singleRoute = rb
}

// Merge merges the new symtab into the current one.
// Duplicate table aliases return an error.
// uniqueColumns is updated, but duplicates
// are removed.
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
		if t.route.Resolve() != st.singleRoute {
			st.singleRoute = nil
		}
		if _, ok := st.tables[k]; ok {
			return fmt.Errorf("duplicate symbol: %s", sqlparser.String(k))
		}
		st.tables[k] = t

		// update the uniqueColumns list, and eliminate
		// duplicate symbols if found.
		for colname, c := range t.columns {
			if c.Vindex == nil {
				continue
			}
			if _, ok := st.uniqueColumns[colname]; ok {
				delete(st.uniqueColumns, colname)
				continue
			}
			st.uniqueColumns[colname] = c
		}
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
// If a reference is found, col.Metadata is set to point
// to it. Subsequent searches will reuse this metadata.
//
// Unqualified columns are searched in the following order:
// 1. ResultColumns
// 2. uniqueColumns
// 3. symtab has only one table. The column is presumed to
// belong to that table.
// 4. symtab has more than one table, but all tables belong
// to the same route. An anonymous column is created against
// the current route.
// If all the above fail, an error is returned. This means
// that an unqualified reference can only be locally resolved.
//
// For qualified columns, we first look for the table. If one
// is found, we look for a column in the pre-existing list.
// If one is not found, we optimistically create an entry
// presuming that the table has such a column. If this is
// not the case, the query will fail when sent to vttablet.
// If the table is not found in the local scope, the search
// is continued in the outer scope, but only if ResultColumns
// is not set (this is MySQL behavior).
//
// For symbols that were found locally, isLocal is returned
// as true. Otherwise, it's returned as false and the symbol
// gets added to the Externs list, which can later be used
// to decide where to push-down the subquery.
func (st *symtab) Find(col *sqlparser.ColName) (rb *route, isLocal bool, err error) {
	// Return previously cached info if present.
	if column, ok := col.Metadata.(*column); ok {
		return column.Route(), column.Route().Symtab() == st, nil
	}

	// Unqualified column case.
	if col.Qualifier.IsEmpty() {
		// Step 1. Search ResultColumns.
		c, err := st.searchResultColumn(col)
		if err != nil {
			return nil, false, err
		}
		if c != nil {
			col.Metadata = c
			return c.Route(), true, nil
		}
	}

	// Steps 2-4 performed by searchTables.
	c, err := st.searchTables(col)
	if err != nil {
		return nil, false, err
	}
	if c != nil {
		col.Metadata = c
		return c.Route(), true, nil
	}

	if st.Outer == nil {
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}
	// Search is not continued if ResultColumns already has values:
	// select a ... having ... (select b ... having a...). In this case,
	// a (in having) should not match the outer-most 'a'. This is to
	// match MySQL's behavior.
	if len(st.ResultColumns) != 0 {
		return nil, false, fmt.Errorf("symbol %s not found in subquery", sqlparser.String(col))
	}

	if rb, _, err = st.Outer.Find(col); err != nil {
		return nil, false, err
	}
	st.Externs = append(st.Externs, col)
	return rb, false, nil
}

// searchResultColumn looks for col in the results columns and
// returns the route if found.
func (st *symtab) searchResultColumn(col *sqlparser.ColName) (c *column, err error) {
	var cursym *resultColumn
	for _, rc := range st.ResultColumns {
		if rc.alias.Equal(col.Name) {
			if cursym != nil {
				return nil, fmt.Errorf("ambiguous symbol reference: %v", sqlparser.String(col))
			}
			cursym = rc
		}
	}
	if cursym != nil {
		return cursym.column, nil
	}
	return nil, nil
}

// searchTables looks for the column in the tables. The search order
// is as described in Find.
func (st *symtab) searchTables(col *sqlparser.ColName) (*column, error) {
	var t *table
	if col.Qualifier.IsEmpty() {
		// Search uniqueColumns first. If found, our job is done.
		if c, ok := st.uniqueColumns[col.Name.Lowered()]; ok {
			return c, nil
		}

		switch {
		case len(st.tables) == 1:
			// If there's only one table match against it.
			// Loop executes once to match the only table.
			for _, v := range st.tables {
				t = v
			}
			// No return: break out.
		case st.singleRoute != nil:
			// If there's only one route, create an anonymous symbol.
			return &column{route: st.singleRoute}, nil
		default:
			// If none of the above, the symbol is unresolvable.
			return nil, fmt.Errorf("symbol %s not found", sqlparser.String(col))
		}
	} else {
		var ok bool
		t, ok = st.tables[col.Qualifier]
		if !ok {
			return nil, nil
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
	return c, nil
}

// NewResultColumn creates a new resultColumn based on the supplied expression.
// The created symbol is not remembered until it is later set as ResultColumns
// after all select expressions are analyzed.
func (st *symtab) NewResultColumn(expr *sqlparser.AliasedExpr, rb *route) *resultColumn {
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
func (st *symtab) Vindex(expr sqlparser.Expr, scope *route) vindexes.Vindex {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil
	}
	if col.Metadata == nil {
		// Find will set the Metadata.
		if _, _, err := st.Find(col); err != nil {
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
