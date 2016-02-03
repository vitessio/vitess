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
}

// colsym contains symbol info about a select expression.
type colsym struct {
	Alias      sqlparser.SQLName
	Underlying sqlparser.ColName
	Route      *routeBuilder
	Vindex     Vindex
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
	Route *routeBuilder
}

// newSymtab creates a new symtab initialized
// to contain the provided table alias.
func newSymtab(alias sqlparser.SQLName, table *Table, route *routeBuilder) *symtab {
	return &symtab{
		tables: []*tableAlias{{
			Name:        alias,
			Keyspace:    table.Keyspace,
			ColVindexes: table.ColVindexes,
			Route:       route,
		}},
		FirstRoute: route,
	}
}

// Add merges the new symbol table into the current one
// without merging their routes. This means that the new symbols
// will belong to different routes
func (st *symtab) Add(newsyms *symtab) error {
	for _, t := range newsyms.tables {
		if found := st.find(t.Name); found != nil {
			return errors.New("duplicate symbols")
		}
		st.tables = append(st.tables, t)
	}
	return nil
}

func (st *symtab) find(alias sqlparser.SQLName) *tableAlias {
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
		if found := st.find(t.Name); found != nil {
			return errors.New("duplicate symbols")
		}
		t.Route = route
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

// FindColumn identifies the table referenced in the column expression.
// It also returns the ColVindex if one exists for the column.
// If a scope is specified, then the search is done only within
// that route builder's scope. Otherwise, it's the global scope.
// If autoResolve is true, and there is only one table in the symbol table,
// then an unqualified reference is assumed to be implicitly against
// that table. The table info doesn't contain the full list of columns.
// So, any column reference is presumed valid until execution time.
func (st *symtab) FindColumn(expr sqlparser.Expr, scope *routeBuilder, autoResolve bool) (*routeBuilder, Vindex) {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil, nil
	}
	if len(st.Colsyms) != 0 {
		name := sqlparser.SQLName(sqlparser.String(col))
		for _, col := range st.Colsyms {
			if name == col.Alias {
				if scope != nil && scope != col.Route {
					return nil, nil
				}
				return col.Route, col.Vindex
			}
		}
	}
	qualifier := col.Qualifier
	if qualifier == "" && autoResolve {
		if len(st.tables) != 1 {
			return nil, nil
		}
		for _, t := range st.tables {
			qualifier = t.Name
			break
		}
	}
	alias := st.find(qualifier)
	if alias == nil {
		return nil, nil
	}
	if scope != nil && scope != alias.Route {
		return nil, nil
	}
	for _, colVindex := range alias.ColVindexes {
		if string(col.Name) == colVindex.Col {
			return alias.Route, colVindex.Vindex
		}
	}
	return alias.Route, nil
}

// IsValue returns true if the expression can be treated as a value
// for the current scope.
// Unresolved references are treated as value
func (st *symtab) IsValue(expr sqlparser.ValExpr, scope *routeBuilder) bool {
	switch node := expr.(type) {
	case *sqlparser.ColName:
		// If it's a valid column reference, it can't be treated as value.
		if route, _ := st.FindColumn(node, scope, true); route != nil {
			return false
		}
		return true
	case sqlparser.ValArg, sqlparser.StrVal, sqlparser.NumVal:
		return true
	}
	return false
}
