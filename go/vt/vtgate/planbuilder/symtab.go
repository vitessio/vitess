// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// symtab contains the symbols for a SELECT statement.
// In the case of a subquery, the symtab points to the
// symtab of the outer query. The symtab evolves over time,
// and its behavior changes accordingly. When it starts out,
// it contains only table aliases. The Find function looks
// for symbols there. After the select expressions are analyzed,
// Colsyms are set. From that point onwards, only Colsyms are
// searched by the Find function. This means that post-processing
// constructs like GROUP BY, etc. can only reference the select
// expressions. Note that the outer symtab may still be in the
// "table alias phase", in which case, only those symbols will
// be searched for that symtab.
// For any column reference that was successfully resolved, the
// information is persisted as Metadata in the sqlparser.ColName
// structure. This ensures that the changing state of the symtab
// does not change the meaning of a previously resolved column
// reference.
// If a searched symbol was found in an outer symtab, then the
// reference is added to the Externs field. This information will
// be used by the outer query to compute the correct target route
// that a subquery can be merged with, if possible.
// In the case of a subquery, a symtab exists only while it's
// analyzed. Although it's discarded after the analysis, column
// references continue to point to the symbols created by the
// symtab. These symbols, in turn, point to the route that they're part of.
// If a decision is made to merge a subquery with an outer query's
// route, then the associated route is redirected to the outer route.
// Effectively, this makes the inner route the same as the outer
// route. Consequently, this makes other routes that point to
// the inner route to be the same as the outermost route also.
// This method of redirection allows us to handle multiple levels
// of nested subqueries. Without this redirection, we'd have to
// keep track of every symtab created, and recursively chase them
// down every time a subquery merges with an outer query.
type symtab struct {
	tables  []*tableAlias
	Colsyms []*colsym
	Externs []*sqlparser.ColName
	Outer   *symtab
	VSchema *VSchema
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
		return fmt.Errorf("duplicate symbol: %s", alias)
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

// Merge merges the new symtab into the current one.
// Duplicate aliases return an error.
// The routes of the table aliases are not affected.
// Merge is only allowed in the early stages of symtab.
// The function panics if Colsyms or Externs contain values.
func (st *symtab) Merge(newsyms *symtab) error {
	if st.Colsyms != nil || newsyms.Colsyms != nil {
		panic("unexpected colsyms")
	}
	if st.Externs != nil || newsyms.Externs != nil {
		panic("unexpected Externs")
	}
	for _, t := range newsyms.tables {
		if found := st.findTable(t.Alias); found != nil {
			return fmt.Errorf("duplicate symbol: %s", t.Alias)
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

// Find returns the route for the symbol referenced by col.
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
// If a symbol was found in an outer scope, then the column reference
// is added to the Externs field.
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
		if st.Outer != nil {
			// autoResolve only allowed for innermost scope.
			route, _, err = st.Outer.Find(col, false)
			if err == nil {
				st.Externs = append(st.Externs, col)
			}
			return route, false, err
		}
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}
	qualifier := col.Qualifier
	if qualifier == "" && autoResolve && len(st.tables) == 1 {
		for _, t := range st.tables {
			qualifier = t.Alias
			break
		}
	}
	alias := st.findTable(qualifier)
	if alias == nil {
		if st.Outer != nil {
			// autoResolve only allowed for innermost scope.
			route, _, err = st.Outer.Find(col, false)
			if err == nil {
				st.Externs = append(st.Externs, col)
			}
			return route, false, err
		}
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}
	col.Metadata = alias
	return alias.Route(), true, nil
}

// Vindex returns the vindex if the expression is a plain column reference
// that is part of the specified route, and has an associated vindex.
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
	panic("unreachable")
}

// tableAlias is part of symtab.
// It represnts a table alias in a FROM clause.
// The tableAlias contains a backpointer to the symtab to which
// it belongs. This is necessary because column references directly
// point to a tableAlias, and sometimes, we need to know the symtab
// to which it belongs.
// The tableAlias also points to a route into which we'll try to
// push the rest of the surrounding clauses.
// A table alias could also represent a sbquery, as long as it
// can be executed as a route. Once a subquery gets encapsulated
// as a route, there's no difference in how it's treated
// compared to a normal table. Subqueries that require the use
// of a join primitive cannot be used by this data structure, and
// therefore, are not supported.
type tableAlias struct {
	Alias       sqlparser.SQLName
	route       *routeBuilder
	symtab      *symtab
	Keyspace    *Keyspace
	ColVindexes []*ColVindex
}

// Route returns the resolved route for a tableAlias.
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

// colsym contains symbol info about a select expression. Just like
// a tableAlias, colsym also contains a backpointer to the symtab,
// and a pointer to the route that would compute or fetch this value.
// In the future, it could point to primitives other than a route.
// A colsym may not have an alias, in which case, it cannot be referenced
// by ohter parts of the query.
// If the expression is a plain column reference, then the 'Underlying' field
// is set to the column it refers. If the referenced column has a Vindex,
// the Vindex field is also accordingly set.
type colsym struct {
	Alias      sqlparser.SQLName
	route      *routeBuilder
	symtab     *symtab
	Underlying colref
	Vindex     Vindex
}

// newColsym builds a colsym for the specified route and symtab.
// Other parameters are optionally set based on additional metadata
// gathered.
func newColsym(route *routeBuilder, st *symtab) *colsym {
	return &colsym{
		route:  route,
		symtab: st,
	}
}

// Route returns the resolved route for a colsym..
func (cs *colsym) Route() *routeBuilder {
	return cs.route.Resolve()
}

// colref uniquely identifies a column reference. For a column
// reference like 'a.b', the qualifier 'a' could be ambiguous due
// to scoping rules. Effectively, colref changes such references
// to 'symbol.a', where symbol physically points to a tableAlias
// or colsym. This representation makes a colref unambiguous.
type colref struct {
	metadata interface{}
	name     sqlparser.SQLName
}

// newColref builds a colref from a sqlparser.ColName that was
// previously resolved. Otherwise, it panics.
func newColref(col *sqlparser.ColName) colref {
	if col.Metadata == nil {
		panic("unexpected nil in metadata")
	}
	return colref{
		metadata: col.Metadata,
		name:     col.Name,
	}
}
