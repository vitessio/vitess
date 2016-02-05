// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// symtab contains the symbols for a SELECT
// statement.
type symtab struct {
	tables     []*tableAlias
	Colsyms    []*colsym
	FirstRoute *routeBuilder
	Externs    []*sqlparser.ColName
	Outer      *symtab
	Schema     *Schema
}

// colsym contains symbol info about a select expression.
type colsym struct {
	Alias      sqlparser.SQLName
	Underlying sqlparser.ColName
	Route      *routeBuilder
	Vindex     Vindex
	Symtab     *symtab
}

func newColsym(st *symtab) *colsym {
	return &colsym{
		Route:  st.FirstRoute,
		Symtab: st,
	}
}

// tableAlias is part of symtab.
// It represnts a table alias in a FROM clause.
// TODO(sougou): Update comments after the struct is finalized.
type tableAlias struct {
	// Name represents the name of the alias.
	Name sqlparser.SQLName
	// Keyspace points to the keyspace to which this
	// alias belongs.
	Keyspace *Keyspace
	// CoVindexes is the list of column Vindexes for this alias.
	ColVindexes []*ColVindex
	// Route points to the routeBuilder object under which this alias
	// was created.
	Route  *routeBuilder
	Symtab *symtab
}

// FindVindex returns the vindex if one was found for the column.
func (t *tableAlias) FindVindex(name sqlparser.SQLName) Vindex {
	for _, colVindex := range t.ColVindexes {
		if string(name) == colVindex.Col {
			return colVindex.Vindex
		}
	}
	return nil
}

// newSymtab creates a new symtab initialized
// to contain the provided table alias.
func newSymtab(alias sqlparser.SQLName, table *Table, route *routeBuilder, schema *Schema) *symtab {
	st := &symtab{
		FirstRoute: route,
		Schema:     schema,
	}
	st.tables = []*tableAlias{{
		Name:        alias,
		Keyspace:    table.Keyspace,
		ColVindexes: table.ColVindexes,
		Route:       route,
		Symtab:      st,
	}}
	return st
}

// Add merges the new symbol table into the current one
// without merging their routes. This means that the new symbols
// will belong to different routes
func (st *symtab) Add(newsyms *symtab) error {
	for _, t := range newsyms.tables {
		if found := st.findTable(t.Name); found != nil {
			return errors.New("duplicate symbols")
		}
		t.Symtab = st
		st.tables = append(st.tables, t)
	}
	return nil
}

func (st *symtab) findTable(alias sqlparser.SQLName) *tableAlias {
	for i, t := range st.tables {
		if t.Name == alias {
			return st.tables[i]
		}
	}
	return nil
}

// Merge merges the new symbol table into the current one and makes
// all the tables part of the specified routeBuilder.
func (st *symtab) Merge(newsyms *symtab, route *routeBuilder) error {
	for _, t := range st.tables {
		t.Route = route
	}
	for _, t := range newsyms.tables {
		if found := st.findTable(t.Name); found != nil {
			return errors.New("duplicate symbols")
		}
		t.Route = route
		t.Symtab = st
		st.tables = append(st.tables, t)
	}
	return nil
}

// SetRHS removes the ColVindexes from the aliases signifying
// that they cannot be used to make routing decisions. This is
// called if the table is in the RHS of a LEFT JOIN.
func (st *symtab) SetRHS() {
	for _, t := range st.tables {
		t.ColVindexes = nil
	}
}

// Find identifies the table referenced in the column expression.
// If a reference is found, the column's Metadata is set to point
// it. Subsequent searches will reuse this meatadata.
// Find also returns the ColVindex if one exists for the column.
// If a scope is specified, then the search is done only within
// that route builder's scope. Otherwise, it's the global scope.
// If autoResolve is true, and there is only one table in the symbol table,
// then an unqualified reference is assumed to be implicitly against
// that table. The table info doesn't contain the full list of columns.
// So, any column reference is presumed valid until execution time.
func (st *symtab) Find(col *sqlparser.ColName, autoResolve bool) (*routeBuilder, error) {
	switch meta := col.Metadata.(type) {
	case *colsym:
		return meta.Route, nil
	case *tableAlias:
		return meta.Route, nil
	}
	if len(st.Colsyms) != 0 {
		name := sqlparser.SQLName(sqlparser.String(col))
		for _, colsym := range st.Colsyms {
			if name == colsym.Alias {
				col.Metadata = colsym
				return colsym.Route, nil
			}
		}
		st.Externs = append(st.Externs, col)
		if st.Outer != nil {
			// autoResolve only allowed for innermost scope.
			return st.Outer.Find(col, false)
		}
		return nil, errors.New("symbol not found")
	}
	qualifier := col.Qualifier
	if qualifier == "" && autoResolve {
		if len(st.tables) != 1 {
			return nil, errors.New("symbol not found")
		}
		for _, t := range st.tables {
			qualifier = t.Name
			break
		}
	}
	alias := st.findTable(qualifier)
	if alias == nil {
		st.Externs = append(st.Externs, col)
		if st.Outer != nil {
			// autoResolve only allowed for innermost scope.
			return st.Outer.Find(col, false)
		}
		return nil, errors.New("symbol not found")
	}
	col.Metadata = alias
	return alias.Route, nil
}

// Vindex returns the vindex if the expression has an
// associated Vindex.
func (st *symtab) Vindex(expr sqlparser.Expr, scope *routeBuilder, autoResolve bool) Vindex {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil
	}
	if col.Metadata == nil {
		_, err := st.Find(col, autoResolve)
		if err != nil {
			return nil
		}
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		if scope != nil && scope != meta.Route {
			return nil
		}
		return meta.Vindex
	case *tableAlias:
		if scope != nil && scope != meta.Route {
			return nil
		}
		return meta.FindVindex(col.Name)
	}
	return nil
}

// IsValue returns true if the expression can be treated as a value
// for the current scope.
// Unresolved references are treated as value
func (st *symtab) IsValue(expr sqlparser.ValExpr, scope *routeBuilder) bool {
	switch node := expr.(type) {
	case *sqlparser.ColName:
		// If route is in scope, it's a local column ref, not a value.
		if route, _ := st.Find(node, true); route != nil {
			return route != scope
		}
		return true
	case sqlparser.ValArg, sqlparser.StrVal, sqlparser.NumVal:
		return true
	}
	return false
}

// InScope returns true if the column reference is in scope of the
// current symbol table.
func (st *symtab) InScopeRoute(col *sqlparser.ColName) *routeBuilder {
	if col.Metadata == nil {
		_, err := st.Find(col, false)
		if err != nil {
			return nil
		}
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		if meta.Symtab == st {
			return meta.Route
		}
	case *tableAlias:
		if meta.Symtab == st {
			return meta.Route
		}
	}
	return nil
}

// Reroute re-points the specified route to the new one.
func (st *symtab) Reroute(o, n *routeBuilder) {
	for _, t := range st.tables {
		if t.Route == o {
			t.Route = n
		}
	}
	for _, c := range st.Colsyms {
		if c.Route == o {
			c.Route = n
		}
	}
}
