// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// SymbolTable contains the symbols for a SELECT
// statement.
type SymbolTable struct {
	tables        map[sqlparser.SQLName]*TableAlias
	SelectSymbols []SelectSymbol
	FirstRoute    *RouteBuilder
}

// SelectSymbol contains symbol info about a select expression.
type SelectSymbol struct {
	Alias  sqlparser.SQLName
	Route  *RouteBuilder
	Vindex Vindex
}

// TableAlias is part of SymbolTable.
// It represnts a table alias in a FROM clause.
// TODO(sougou): Update comments after the struct is finalized.
type TableAlias struct {
	// Name represents the name of the alias.
	Name sqlparser.SQLName
	// Keyspace points to the keyspace to which this
	// alias belongs.
	Keyspace *Keyspace
	// CoVindexes is the list of column Vindexes for this alias.
	ColVindexes []*ColVindex
	// Route points to the RouteBuilder object under which this alias
	// was created.
	Route *RouteBuilder
}

// NewSymbolTable creates a new SymbolTable initialized
// to contain the provided table alias.
func NewSymbolTable(alias sqlparser.SQLName, table *Table, route *RouteBuilder) *SymbolTable {
	return &SymbolTable{
		tables: map[sqlparser.SQLName]*TableAlias{
			alias: {
				Name:        alias,
				Keyspace:    table.Keyspace,
				ColVindexes: table.ColVindexes,
				Route:       route,
			},
		},
		FirstRoute: route,
	}
}

// Add merges the new symbol table into the current one
// without merging their routes. This means that the new symbols
// will belong to different routes
func (smt *SymbolTable) Add(symbols *SymbolTable) error {
	for k, v := range symbols.tables {
		if _, found := smt.tables[k]; found {
			return errors.New("duplicate symbols")
		}
		smt.tables[k] = v
	}
	return nil
}

// Merge merges the new symbol table into the current one and makes
// all the tables part of the specified RouteBuilder.
func (smt *SymbolTable) Merge(symbols *SymbolTable, route *RouteBuilder) error {
	for _, v := range smt.tables {
		v.Route = route
	}
	for k, v := range symbols.tables {
		if _, found := smt.tables[k]; found {
			return errors.New("duplicate symbols")
		}
		smt.tables[k] = v
		v.Route = route
	}
	return nil
}

// SetRHS removes the ColVindexes from the aliases signifying
// that they cannot be used to make routing decisions. This is
// called if the table is in the RHS of a LEFT JOIN.
func (smt *SymbolTable) SetRHS() {
	for _, v := range smt.tables {
		v.ColVindexes = nil
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
func (smt *SymbolTable) FindColumn(expr sqlparser.Expr, scope *RouteBuilder, autoResolve bool) (*RouteBuilder, Vindex) {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil, nil
	}
	if len(smt.SelectSymbols) != 0 {
		name := sqlparser.SQLName(sqlparser.String(col))
		for _, selectSymbol := range smt.SelectSymbols {
			if name == selectSymbol.Alias {
				if scope != nil && scope != selectSymbol.Route {
					return nil, nil
				}
				return selectSymbol.Route, selectSymbol.Vindex
			}
		}
	}
	qualifier := col.Qualifier
	if qualifier == "" && autoResolve {
		if len(smt.tables) != 1 {
			return nil, nil
		}
		for k := range smt.tables {
			qualifier = k
			break
		}
	}
	alias, ok := smt.tables[qualifier]
	if !ok {
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
func (smt *SymbolTable) IsValue(expr sqlparser.ValExpr, scope *RouteBuilder) bool {
	switch node := expr.(type) {
	case *sqlparser.ColName:
		// If it's a valid column reference, it can't be treated as value.
		if route, _ := smt.FindColumn(node, scope, true); route != nil {
			return false
		}
		return true
	case sqlparser.ValArg, sqlparser.StrVal, sqlparser.NumVal:
		return true
	}
	return false
}
