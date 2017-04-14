// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
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
	tables  []*tabsym
	Colsyms []*colsym
	Externs []*sqlparser.ColName
	Outer   *symtab
	VSchema VSchema
}

// newSymtab creates a new symtab initialized
// to contain the provided table alias.
func newSymtab(vschema VSchema) *symtab {
	return &symtab{
		VSchema: vschema,
	}
}

// AddAlias adds a table alias to symtab.
func (st *symtab) AddAlias(alias *sqlparser.TableName, astName sqlparser.TableIdent, table *vindexes.Table, rb *route) {
	st.tables = append(st.tables, &tabsym{
		Alias:          alias,
		ASTName:        astName,
		route:          rb,
		symtab:         st,
		Keyspace:       table.Keyspace,
		ColumnVindexes: table.ColumnVindexes,
	})
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
			return fmt.Errorf("duplicate symbol: %s", sqlparser.String(t.Alias))
		}
		t.symtab = st
		st.tables = append(st.tables, t)
	}
	return nil
}

func (st *symtab) findTable(alias *sqlparser.TableName) *tabsym {
	for i, t := range st.tables {
		if t.Alias.Equal(alias) {
			return st.tables[i]
		}
	}
	return nil
}

// SetRHS removes the ColumnVindexes from the aliases signifying
// that they cannot be used to make routing decisions. This is
// called if the table is in the RHS of a LEFT JOIN.
func (st *symtab) SetRHS() {
	for _, t := range st.tables {
		t.ColumnVindexes = nil
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
func (st *symtab) Find(col *sqlparser.ColName, autoResolve bool) (rb *route, isLocal bool, err error) {
	if m, ok := col.Metadata.(sym); ok {
		return m.Route(), m.Symtab() == st, nil
	}
	if len(st.Colsyms) != 0 {
		if col.Qualifier.IsEmpty() {
			rb, err = st.searchColsyms(col)
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
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}

	if rb = st.searchTables(col, autoResolve); rb != nil {
		return rb, true, nil
	}
	if st.Outer == nil {
		return nil, false, fmt.Errorf("symbol %s not found", sqlparser.String(col))
	}
	// autoResolve only allowed for innermost scope.
	if rb, _, err = st.Outer.Find(col, false); err != nil {
		return nil, false, err
	}
	st.Externs = append(st.Externs, col)
	return rb, false, nil
}

func (st *symtab) searchColsyms(col *sqlparser.ColName) (rb *route, err error) {
	var cursym *colsym
	for _, colsym := range st.Colsyms {
		if colsym.Alias.Equal(col.Name) {
			if cursym != nil {
				return nil, fmt.Errorf("ambiguous symbol reference: %v", sqlparser.String(col))
			}
			cursym = colsym
			col.Metadata = colsym
		}
	}
	if cursym != nil {
		return cursym.Route(), nil
	}
	return nil, nil
}

func (st *symtab) searchTables(col *sqlparser.ColName, autoResolve bool) *route {
	if col.Qualifier.IsEmpty() {
		if autoResolve && len(st.tables) == 1 {
			col.Metadata = st.tables[0]
			return st.tables[0].Route()
		}
		return nil
	}
	// TODO(sougou): this search should ideally match the search
	// style provided by VSchema, where the default keyspace must be
	// used if one is not provided.
	alias := st.findTable(col.Qualifier)
	if alias == nil {
		return nil
	}
	col.Metadata = alias
	return alias.Route()
}

// Vindex returns the vindex if the expression is a plain column reference
// that is part of the specified route, and has an associated vindex.
func (st *symtab) Vindex(expr sqlparser.Expr, scope *route, autoResolve bool) vindexes.Vindex {
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
	case *tabsym:
		if scope != meta.Route() {
			return nil
		}
		return meta.FindVindex(col.Name)
	}
	panic("unreachable")
}

// sym defines the interface that must be satisfied by
// all symbols in symtab
type sym interface {
	newColRef(col *sqlparser.ColName) colref
	Route() *route
	Symtab() *symtab
}

// tabsym is part of symtab.
// It represnts a table alias in a FROM clause.
// The tabsym points to a route into which we'll try to
// push the rest of the surrounding clauses.
// A table alias could also represent a sbquery. But
// there's no difference in how it's treated compared to a normal
// table. Consequently, ubqueries that cannot be executed
// as a route are currently not supported.
// While the Alias is used for resolving column references, the
// ASTName is used for code generation. The difference between
// the two is that the ASTName strips out the keyspace qualifier
// from the table name, which is something that VTTablet and MySQL
// can't recognize.
type tabsym struct {
	Alias          *sqlparser.TableName
	ASTName        sqlparser.TableIdent
	route          *route
	symtab         *symtab
	Keyspace       *vindexes.Keyspace
	ColumnVindexes []*vindexes.ColumnVindex
}

func (t *tabsym) newColRef(col *sqlparser.ColName) colref {
	return colref{
		Meta: t,
		name: col.Name.Lowered(),
	}
}

func (t *tabsym) Route() *route {
	return t.route.Resolve()
}

func (t *tabsym) Symtab() *symtab {
	return t.symtab
}

// FindVindex returns the vindex if one was found for the column.
func (t *tabsym) FindVindex(name sqlparser.ColIdent) vindexes.Vindex {
	for _, colVindex := range t.ColumnVindexes {
		if colVindex.Column.Equal(name) {
			return colVindex.Vindex
		}
	}
	return nil
}

// colsym contains symbol info about a select expression. Just like
// a tabsym, colsym also contains a backpointer to the symtab,
// and a pointer to the route that would compute or fetch this value.
// In the future, it could point to primitives other than a route.
// If the expression is a plain column reference, then the 'Underlying' field
// is set to the column it refers. If the referenced column has a Vindex,
// the Vindex field is also accordingly set.
type colsym struct {
	// Alias will represent the unqualified symbol name for that expression.
	// If the statement provides an explicit alias, that name will be used.
	// Otherwise, one will be generated. If the expression is a simple
	// column, then the base name of the column will be used as the alias.
	Alias sqlparser.ColIdent

	route      *route
	symtab     *symtab
	Underlying colref
	Vindex     vindexes.Vindex
}

// newColsym builds a colsym for the specified route and symtab.
// Other parameters are optionally set based on additional metadata
// gathered.
func newColsym(rb *route, st *symtab) *colsym {
	return &colsym{
		route:  rb,
		symtab: st,
	}
}

func (cs *colsym) newColRef(col *sqlparser.ColName) colref {
	return colref{
		Meta: cs,
	}
}

func (cs *colsym) Route() *route {
	return cs.route.Resolve()
}

func (cs *colsym) Symtab() *symtab {
	return cs.symtab
}

// colref uniquely identifies a column reference. For a column
// reference like 'a.b', the qualifier 'a' could be ambiguous due
// to scoping rules. Effectively, colref changes such references
// to 'symbol.a', where symbol physically points to a tabsym
// or colsym. This representation makes a colref unambiguous.
// The name field must be relied upon only for comparison. Use
// the Name method for generating SQL.
type colref struct {
	Meta sym
	name string
}

// newColref builds a colref from a sqlparser.ColName that was
// previously resolved. Otherwise, it panics.
func newColref(col *sqlparser.ColName) colref {
	return col.Metadata.(sym).newColRef(col)
}

// Route returns the route for the colref.
func (cr colref) Route() *route {
	return cr.Meta.Route()
}

// Name returns the name of the colref.
func (cr colref) Name() sqlparser.ColIdent {
	return sqlparser.NewColIdent(cr.name)
}
