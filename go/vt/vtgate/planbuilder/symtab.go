// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// symtab contains the symbols for a SELECT statement.
// In the case of a subquery, the symtab points to the
// symtab of the outer query.
type symtab struct {
	tables  []*tableAlias
	Colsyms []*colsym
	Externs []*sqlparser.ColName
	Outer   *symtab
	VSchema *VSchema
}

// colsym contains symbol info about a select expression.
type colsym struct {
	Alias      sqlparser.SQLName
	route      *routeBuilder
	symtab     *symtab
	Underlying colref
	Vindex     Vindex
}

func newColsym(route *routeBuilder, st *symtab) *colsym {
	return &colsym{
		route:  route,
		symtab: st,
	}
}

func (cs *colsym) Route() *routeBuilder {
	return cs.route.Resolve()
}

// colref uniquely identifies a column reference.
type colref struct {
	metadata interface{}
	name     sqlparser.SQLName
}

func newColref(col *sqlparser.ColName) colref {
	if col.Metadata == nil {
		panic("unexpected")
	}
	return colref{
		metadata: col.Metadata,
		name:     col.Name,
	}
}

// tableAlias is part of symtab.
// It represnts a table alias in a FROM clause.
// TODO(sougou): Update comments after the struct is finalized.
type tableAlias struct {
	Alias  sqlparser.SQLName
	route  *routeBuilder
	symtab *symtab
	// Keyspace points to the keyspace to which this
	// alias belongs.
	Keyspace *Keyspace
	// CoVindexes is the list of column Vindexes for this alias.
	ColVindexes []*ColVindex
}

func (t *tableAlias) Route() *routeBuilder {
	return t.route.Resolve()
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
func newSymtab(vschema *VSchema) *symtab {
	return &symtab{
		VSchema: vschema,
	}
}

// AddAlias adds a table alias to symtab.
func (st *symtab) AddAlias(alias sqlparser.SQLName, table *Table, route *routeBuilder) error {
	if found := st.findTable(alias); found != nil {
		return errors.New("duplicate symbols")
	}
	st.tables = append(st.tables, &tableAlias{
		Alias:       alias,
		route:       route,
		symtab:      st,
		Keyspace:    table.Keyspace,
		ColVindexes: table.ColVindexes,
	})
	return nil
}

// Add merges the new symbol table into the current one
// without merging their routes. This means that the new symbols
// will belong to different routes
func (st *symtab) Add(newsyms *symtab) error {
	for _, t := range newsyms.tables {
		if found := st.findTable(t.Alias); found != nil {
			return errors.New("duplicate symbols")
		}
		t.symtab = st
		st.tables = append(st.tables, t)
	}
	return nil
}

func (st *symtab) findTable(alias sqlparser.SQLName) *tableAlias {
	for i, t := range st.tables {
		if t.Alias == alias {
			return st.tables[i]
		}
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

// Find returns the routeBuilder for the symbol referenced by col.
// If a reference is found, the column's Metadata is set to point
// it. Subsequent searches will reuse this meatadata.
// If autoResolve is true, and there is only one table in the symbol table,
// then an unqualified reference is assumed to be implicitly against
// that table. The table info doesn't contain the full list of columns.
// So, any column reference is presumed valid. If a Colsyms scope is
// present, then the table scope is not searched. If a symbol is found
// in the current symtab, then isLocal is set to true. Otherwise, the
// search is continued in the outer symtab. If so, isLocal will be set
// to false. If the symbol was not found, an error is returned.
// isLocal must be checked before you can push-down (or pull-out)
// a construct.
func (st *symtab) Find(col *sqlparser.ColName, autoResolve bool) (route *routeBuilder, isLocal bool, err error) {
	switch m := col.Metadata.(type) {
	case *colsym:
		return m.Route(), m.symtab == st, nil
	case *tableAlias:
		return m.Route(), m.symtab == st, nil
	}
	if len(st.Colsyms) != 0 {
		name := sqlparser.SQLName(sqlparser.String(col))
		for _, colsym := range st.Colsyms {
			if name == colsym.Alias {
				col.Metadata = colsym
				return colsym.Route(), true, nil
			}
		}
		st.Externs = append(st.Externs, col)
		if st.Outer != nil {
			// autoResolve only allowed for innermost scope.
			route, _, err = st.Outer.Find(col, false)
			return route, false, err
		}
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}
	qualifier := col.Qualifier
	if qualifier == "" && autoResolve {
		if len(st.tables) != 1 {
			return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
		}
		for _, t := range st.tables {
			qualifier = t.Alias
			break
		}
	}
	alias := st.findTable(qualifier)
	if alias == nil {
		st.Externs = append(st.Externs, col)
		if st.Outer != nil {
			// autoResolve only allowed for innermost scope.
			route, _, err = st.Outer.Find(col, false)
			return route, false, err
		}
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}
	col.Metadata = alias
	return alias.Route(), true, nil
}

// Vindex returns the vindex if the expression has an associated Vindex,
// but only if it's within the scope of routeBuilder. Because of this
// restriction, this function can be used to make push decisions.
func (st *symtab) Vindex(expr sqlparser.Expr, scope *routeBuilder, autoResolve bool) Vindex {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil
	}
	if col.Metadata == nil {
		_, _, err := st.Find(col, autoResolve)
		if err != nil {
			return nil
		}
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		if scope != meta.Route() {
			return nil
		}
		return meta.Vindex
	case *tableAlias:
		if scope != meta.Route() {
			return nil
		}
		return meta.FindVindex(col.Name)
	}
	return nil
}
