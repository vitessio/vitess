// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// SymbolTable contains the symbols for a SELECT
// statement. If it's for a subquery, it points to
// an outer scope.
type SymbolTable struct {
	tables map[sqlparser.SQLName]*TableAlias
	outer  *SymbolTable
}

// TableAlias is part of SymbolTable.
// It represnts a table alias in a FROM clause.
type TableAlias struct {
	// Name represents the name of the alias.
	Name sqlparser.SQLName
	// Keyspace points to the keyspace to which this
	// alias belongs.
	Keyspace *Keyspace
	// CoVindexes is the list of column Vindexes for this alisas.
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
	}
}

// AddChunk merges the new symbol table into the current one
// without mergine their routes. This means that the new symbols
// will belong to a different chunk (or route).
func (smt *SymbolTable) AddChunk(symbols *SymbolTable) error {
	for k, v := range symbols.tables {
		if _, found := smt.tables[k]; found {
			return errors.New("duplicate symbols")
		}
		smt.tables[k] = v
	}
	return nil
}

// Merge merges the new symbol table into the current as part of
// the same chunk. So, all symbols will be changed to point to the new
// RouteBuilder.
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
