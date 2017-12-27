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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// symtab represents the symbol table for a SELECT statement
// or a subquery. The symtab evolves over time.
// As a query is analyzed, multiple independent
// symtabs are created, and they are later merged as each
// sub-expression of a FROM clause is merged.
//
// A symtab maintains uniqueColumns, which is a list of unique
// vindex column names. These names can be resolved without the
// need to qualify them by their table names. If there are
// duplicates during a merge, those columns are removed from
// the unique list, thereby disallowing unqualifed references
// to such columns.
//
// After a select expression is analyzed, the
// ResultColumns field is set. In the case of a subquery, the
// Outer field points to the outer symtab. Any symbols that
// are not resolved locally are added to the Externs field,
// which is later used to determine if the subquery can be
// merged with an outer route.
type symtab struct {
	// if the symtab was merged with another one. The
	// redirect is set to point to the merged symtab.
	// Resolve is used to find the target.
	redirect *symtab

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
	singleRoute *route

	ResultColumns []*resultColumn
	Outer         *symtab
	Externs       []*sqlparser.ColName
	VSchema       VSchema
}

// newSymtab creates a new symtab.
func newSymtab(vschema VSchema) *symtab {
	return &symtab{
		tables:        make(map[sqlparser.TableName]*table),
		uniqueColumns: make(map[string]*column),
		VSchema:       vschema,
	}
}

// newSymtab creates a new symtab initialized
// to contain just one route.
func newSymtabWithRoute(vschema VSchema, rb *route) *symtab {
	return &symtab{
		tables:        make(map[sqlparser.TableName]*table),
		uniqueColumns: make(map[string]*column),
		VSchema:       vschema,
		singleRoute:   rb,
	}
}

func (st *symtab) Resolve() *symtab {
	if st.redirect != nil {
		return st.redirect.Resolve()
	}
	return st
}

// AddVindexTable creates a table from a vindex table
// and adds it to symtab.
func (st *symtab) AddVindexTable(alias sqlparser.TableName, vindexTable *vindexes.Table, rb *route) error {
	t := &table{
		alias:   alias,
		columns: make(map[string]*column),
		origin:  rb,
	}

	for _, col := range vindexTable.Columns {
		t.columns[col.Name.Lowered()] = &column{
			origin: rb,
			name:   col.Name,
			typ:    col.Type,
			table:  t,
		}
	}

	for _, cv := range vindexTable.ColumnVindexes {
		for i, cvcol := range cv.Columns {
			var vindex vindexes.Vindex
			if i == 0 {
				// For now, only the first column is used for vindex Map functions.
				vindex = cv.Vindex
			}
			lowered := cvcol.Lowered()
			if col, ok := t.columns[lowered]; ok {
				col.Vindex = vindex
				continue
			}
			t.columns[lowered] = &column{
				origin: rb,
				Vindex: vindex,
				name:   cvcol,
				table:  t,
			}
		}
	}

	if ai := vindexTable.AutoIncrement; ai != nil {
		lowered := ai.Column.Lowered()
		if _, ok := t.columns[lowered]; !ok {
			t.columns[lowered] = &column{
				origin: rb,
				name:   ai.Column,
				table:  t,
			}
		}
	}
	return st.AddTable(t)
}

// Merge merges the new symtab into the current one.
// Duplicate table aliases return an error.
// uniqueColumns is updated, but duplicates are removed.
// The function panics if ResultColumns or Externs contain values.
// This is because symbol tables are allowed to merge only during
// analysis for the FROM clause. At that time, there should be
// no ResultColumns or Externs.
func (st *symtab) Merge(newsyms *symtab) error {
	for _, t := range newsyms.tables {
		if err := st.AddTable(t); err != nil {
			return err
		}
	}
	newsyms.redirect = st
	return nil
}

// AddTable adds a table to symtab.
func (st *symtab) AddTable(t *table) error {
	if rb, ok := t.origin.(*route); !ok || rb.Resolve() != st.singleRoute {
		st.singleRoute = nil
	}
	if _, ok := st.tables[t.alias]; ok {
		return fmt.Errorf("duplicate symbol: %s", sqlparser.String(t.alias))
	}
	st.tables[t.alias] = t

	// update the uniqueColumns list, and eliminate
	// duplicate symbols if found.
	for colname, c := range t.columns {
		if _, ok := st.uniqueColumns[colname]; ok {
			// Keep the entry, but make it nil. This will
			// ensure that yet another column of the same name
			// doesn't get added back in.
			st.uniqueColumns[colname] = nil
			continue
		}
		st.uniqueColumns[colname] = c
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

// Find returns the columnOriginator for the symbol referenced by col.
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
func (st *symtab) Find(col *sqlparser.ColName) (origin columnOriginator, isLocal bool, err error) {
	// Return previously cached info if present.
	if column, ok := col.Metadata.(*column); ok {
		return column.Origin(), column.Origin().Symtab() == st, nil
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
			return c.Origin(), true, nil
		}
	}

	// Steps 2-4 performed by searchTables.
	c, err := st.searchTables(col)
	if err != nil {
		return nil, false, err
	}
	if c != nil {
		col.Metadata = c
		return c.Origin(), true, nil
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

	if origin, _, err = st.Outer.Find(col); err != nil {
		return nil, false, err
	}
	st.Externs = append(st.Externs, col)
	return origin, false, nil
}

// searchResultColumn looks for col in the results columns.
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
	// @@ syntax is only allowed for dual tables, in which case there should be
	// only one in the symtab. So, such expressions will be implicitly matched.
	if col.Qualifier.IsEmpty() || strings.HasPrefix(col.Qualifier.Name.String(), "@@") {
		// Search uniqueColumns first. If found, our job is done.
		// Check for nil because there can be nil entries if there
		// are duplicate columns across multiple tables.
		if c := st.uniqueColumns[col.Name.Lowered()]; c != nil {
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
			return &column{origin: st.singleRoute}, nil
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
		// We know all the column names of a subquery. Might as well return an error if it's not found.
		if _, ok := t.origin.(*subquery); ok {
			return nil, fmt.Errorf("symbol %s is referencing a non-existent column of the subquery", sqlparser.String(col))
		}
		c = &column{
			origin: t.origin,
			name:   col.Name,
			table:  t,
		}
		t.columns[col.Name.Lowered()] = c
	}
	return c, nil
}

// NewResultColumn creates a new resultColumn based on the supplied expression.
// The created symbol is not remembered until it is later set as ResultColumns
// after all select expressions are analyzed.
func (st *symtab) NewResultColumn(expr *sqlparser.AliasedExpr, origin columnOriginator) *resultColumn {
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
			origin: origin,
		}
	}
	return rc
}

// ResultFromNumber returns the result column index based on the column
// order expression.
func ResultFromNumber(resultColumns []*resultColumn, val *sqlparser.SQLVal) (int, error) {
	if val.Type != sqlparser.IntVal {
		return 0, errors.New("column number is not an int")
	}
	num, err := strconv.ParseInt(string(val.Val), 0, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing column number: %s", sqlparser.String(val))
	}
	if num < 1 || num > int64(len(resultColumns)) {
		return 0, fmt.Errorf("column number out of range: %d", num)
	}
	return int(num - 1), nil
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
	if c.Origin() != scope {
		return nil
	}
	return c.Vindex
}

// ResolveSymbols resolves all column references against symtab.
// This makes sure that they all have their Metadata initialized.
// If a symbol cannot be resolved or if the expression contains
// a subquery, an error is returned.
func (st *symtab) ResolveSymbols(node sqlparser.SQLNode) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if _, _, err := st.Find(node); err != nil {
				return false, err
			}
		case *sqlparser.Subquery:
			return false, errors.New("subqueries disallowed")
		}
		return true, nil
	}, node)
}

// table is part of symtab.
// It represents a table alias in a FROM clause. It points
// to the columnOriginator that represents it.
type table struct {
	alias   sqlparser.TableName
	columns map[string]*column
	origin  columnOriginator
}

// column represents a unique symbol in the query that other
// parts can refer to. A column can be a table column or
// a result column (select expression). Every column
// contains the columnOriginator it originates from.
// Columns for table columns are created or reused by
// symtab through the Find function.
//
// Columns are created by columnOriginator objects.
//
// Anonymous columns can be created by symtab to represent
// ambiguous column references, but whose route can still be
// identified. For example, in the case of 'select a from t1, t2',
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
	origin columnOriginator
	Vindex vindexes.Vindex
	name   sqlparser.ColIdent
	typ    querypb.Type
	table  *table

	// colnum is set only for primitives that can return a
	// subset of their internal result like subquery or vindexFunc.
	colnum int
}

// Origin returns the route that originates the column.
func (c *column) Origin() columnOriginator {
	// If it's a route, we have to resolve it.
	if rb, ok := c.origin.(*route); ok {
		return rb.Resolve()
	}
	return c.origin
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
