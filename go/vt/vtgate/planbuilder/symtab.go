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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var errNoTable = errors.New("no table info")

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
	tables     map[sqlparser.TableName]*table
	tableNames []sqlparser.TableName

	// uniqueColumns has the column name as key
	// and points at the columns that tables contains.
	uniqueColumns map[string]*column

	// singleRoute is set only if all the symbols in
	// the symbol table are part of the same route.
	singleRoute *route

	ResultColumns []*resultColumn
	Outer         *symtab
	Externs       []*sqlparser.ColName
}

// newSymtab creates a new symtab.
func newSymtab() *symtab {
	return &symtab{
		tables:        make(map[sqlparser.TableName]*table),
		uniqueColumns: make(map[string]*column),
	}
}

// newSymtab creates a new symtab initialized
// to contain just one route.
func newSymtabWithRoute(rb *route) *symtab {
	return &symtab{
		tables:        make(map[sqlparser.TableName]*table),
		uniqueColumns: make(map[string]*column),
		singleRoute:   rb,
	}
}

// AddVSchemaTable takes a list of vschema tables as input and
// creates a table with multiple route options. It returns a
// list of vindex maps, one for each input.
func (st *symtab) AddVSchemaTable(alias sqlparser.TableName, vschemaTables []*vindexes.Table, rb *route) (vindexMaps []map[*column]vindexes.Vindex, err error) {
	t := &table{
		alias:  alias,
		origin: rb,
	}

	vindexMaps = make([]map[*column]vindexes.Vindex, len(vschemaTables))
	for i, vst := range vschemaTables {
		// The following logic allows the first table to be authoritative while the rest
		// are not. But there's no need to reveal this flexibility to the user.
		if i != 0 && vst.ColumnListAuthoritative && !t.isAuthoritative {
			return nil, fmt.Errorf("intermixing of authoritative and non-authoritative tables not allowed: %v", vst.Name)
		}

		for _, col := range vst.Columns {
			if _, err := t.mergeColumn(col.Name, &column{
				origin: rb,
				st:     st,
				typ:    col.Type,
			}); err != nil {
				return nil, err
			}
		}
		if i == 0 && vst.ColumnListAuthoritative {
			// This will prevent new columns from being added.
			t.isAuthoritative = true
		}

		var vindexMap map[*column]vindexes.Vindex
		for _, cv := range vst.ColumnVindexes {
			for j, cvcol := range cv.Columns {
				col, err := t.mergeColumn(cvcol, &column{
					origin: rb,
					st:     st,
				})
				if err != nil {
					return nil, err
				}
				if j == 0 {
					// For now, only the first column is used for vindex Map functions.
					if vindexMap == nil {
						vindexMap = make(map[*column]vindexes.Vindex)
					}
					vindexMap[col] = cv.Vindex
				}
			}
		}
		vindexMaps[i] = vindexMap

		if ai := vst.AutoIncrement; ai != nil {
			if _, ok := t.columns[ai.Column.Lowered()]; !ok {
				if _, err := t.mergeColumn(ai.Column, &column{
					origin: rb,
					st:     st,
				}); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := st.AddTable(t); err != nil {
		return nil, err
	}
	return vindexMaps, nil
}

// Merge merges the new symtab into the current one.
// Duplicate table aliases return an error.
// uniqueColumns is updated, but duplicates are removed.
// Merges are only performed during the FROM clause analysis.
// At this point, only tables and uniqueColumns are set.
// All other fields are ignored.
func (st *symtab) Merge(newsyms *symtab) error {
	if st.tableNames == nil || newsyms.tableNames == nil {
		// If any side of symtab has anonymous tables,
		// we treat the merged symtab as having anonymous tables.
		return nil
	}
	for _, t := range newsyms.tables {
		if err := st.AddTable(t); err != nil {
			return err
		}
	}
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
	st.tableNames = append(st.tableNames, t.alias)

	// update the uniqueColumns list, and eliminate
	// duplicate symbols if found.
	for colname, c := range t.columns {
		c.st = st
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

// AllTables returns an ordered list of all current tables.
func (st *symtab) AllTables() []*table {
	if len(st.tableNames) == 0 {
		return nil
	}
	tables := make([]*table, 0, len(st.tableNames))
	for _, tname := range st.tableNames {
		tables = append(tables, st.tables[tname])
	}
	return tables
}

// FindTable finds a table in symtab. This function is specifically used
// for expanding 'select a.*' constructs. If you're in a subquery,
// you're most likely referring to a table in the local 'from' clause.
// For this reason, the search is only performed in the current scope.
// This may be a deviation from the formal definition of SQL, but there
// are currently no use cases that require the full support.
func (st *symtab) FindTable(tname sqlparser.TableName) (*table, error) {
	if st.tableNames == nil {
		// Unreachable because current code path checks for this condition
		// before invoking this function.
		return nil, errNoTable
	}
	t, ok := st.tables[tname]
	if !ok {
		return nil, fmt.Errorf("table %v not found", sqlparser.String(tname))
	}
	return t, nil
}

// SetResultColumns sets the result columns.
func (st *symtab) SetResultColumns(rcs []*resultColumn) {
	for _, rc := range rcs {
		rc.column.st = st
	}
	st.ResultColumns = rcs
}

// Find returns the builder for the symbol referenced by col.
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
func (st *symtab) Find(col *sqlparser.ColName) (origin builder, isLocal bool, err error) {
	// Return previously cached info if present.
	if column, ok := col.Metadata.(*column); ok {
		return column.Origin(), column.st == st, nil
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
			return &column{origin: st.singleRoute, st: st}, nil
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
		if t.isAuthoritative {
			return nil, fmt.Errorf("symbol %s not found in table or subquery", sqlparser.String(col))
		}
		c = &column{
			origin: t.Origin(),
			st:     st,
		}
		t.addColumn(col.Name, c)
	}
	return c, nil
}

// ResultFromNumber returns the result column index based on the column
// order expression.
func ResultFromNumber(rcs []*resultColumn, val *sqlparser.SQLVal) (int, error) {
	if val.Type != sqlparser.IntVal {
		return 0, errors.New("column number is not an int")
	}
	num, err := strconv.ParseInt(string(val.Val), 0, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing column number: %s", sqlparser.String(val))
	}
	if num < 1 || num > int64(len(rcs)) {
		return 0, fmt.Errorf("column number out of range: %d", num)
	}
	return int(num - 1), nil
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
			return false, errors.New("unsupported: subqueries disallowed in GROUP or ORDER BY")
		}
		return true, nil
	}, node)
}

// table is part of symtab.
// It represents a table alias in a FROM clause. It points
// to the builder that represents it.
type table struct {
	alias           sqlparser.TableName
	columns         map[string]*column
	columnNames     []sqlparser.ColIdent
	isAuthoritative bool
	origin          builder
}

func (t *table) addColumn(alias sqlparser.ColIdent, c *column) {
	if t.columns == nil {
		t.columns = make(map[string]*column)
	}
	lowered := alias.Lowered()
	// Dups are allowed, but first one wins if referenced.
	if _, ok := t.columns[lowered]; !ok {
		c.colNumber = len(t.columnNames)
		t.columns[lowered] = c
	}
	t.columnNames = append(t.columnNames, alias)
}

// mergeColumn merges or creates a new column for the table.
// If the table is authoritative and the column doesn't already
// exist, it returns an error. If the table is not authoritative,
// the column is added if not already present.
func (t *table) mergeColumn(alias sqlparser.ColIdent, c *column) (*column, error) {
	if t.columns == nil {
		t.columns = make(map[string]*column)
	}
	lowered := alias.Lowered()
	if col, ok := t.columns[lowered]; ok {
		return col, nil
	}
	if t.isAuthoritative {
		return nil, fmt.Errorf("column %v not found in %v", sqlparser.String(alias), sqlparser.String(t.alias))
	}
	c.colNumber = len(t.columnNames)
	t.columns[lowered] = c
	t.columnNames = append(t.columnNames, alias)
	return c, nil
}

// Origin returns the route that originates the table.
func (t *table) Origin() builder {
	// If it's a route, we have to resolve it.
	if rb, ok := t.origin.(*route); ok {
		return rb.Resolve()
	}
	return t.origin
}

// column represents a unique symbol in the query that other
// parts can refer to.
// Every column contains the builder it originates from.
//
// Two columns are equal if their pointer values match.
//
// For subquery and vindexFunc, the colNumber is also set because
// the column order is known and unchangeable.
type column struct {
	origin    builder
	st        *symtab
	typ       querypb.Type
	colNumber int
}

// Origin returns the route that originates the column.
func (c *column) Origin() builder {
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

// NewResultColumn creates a new resultColumn based on the supplied expression.
// The created symbol is not remembered until it is later set as ResultColumns
// after all select expressions are analyzed.
func newResultColumn(expr *sqlparser.AliasedExpr, origin builder) *resultColumn {
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
