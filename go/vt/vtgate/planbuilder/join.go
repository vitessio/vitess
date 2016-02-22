// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

// joinBuilder is used to build a Join primitive.
// It's used to buid a normal join or a left join
// operation.
type joinBuilder struct {
	// LeftOrder and RightOrder store the order
	// of the left node and right node. The Order
	// of this join will be the same as RightOrder.
	// This information is used for traversal.
	LeftOrder, RightOrder int
	// Left and Right are the nodes for the join.
	Left, Right planBuilder
	symtab      *symtab
	// Colsyms specifies the colsyms supplied by this
	// join.
	Colsyms []*colsym
	// Join is the join plan.
	Join *Join
}

// Symtab returns the associated symtab.
func (jb *joinBuilder) Symtab() *symtab {
	return jb.symtab
}

// Order returns the order of the node.
func (jb *joinBuilder) Order() int {
	return jb.RightOrder
}

// PushSelect pushes the select expression into the join and
// recursively down.
func (jb *joinBuilder) PushSelect(expr *sqlparser.NonStarExpr, route *routeBuilder) (colsym *colsym, colnum int, err error) {
	if route.Order() <= jb.LeftOrder {
		colsym, colnum, err = jb.Left.PushSelect(expr, route)
		if err != nil {
			return nil, 0, err
		}
		jb.Join.Cols = append(jb.Join.Cols, -colnum-1)
	} else {
		colsym, colnum, err = jb.Right.PushSelect(expr, route)
		if err != nil {
			return nil, 0, err
		}
		jb.Join.Cols = append(jb.Join.Cols, colnum+1)
	}
	jb.Colsyms = append(jb.Colsyms, colsym)
	return colsym, len(jb.Colsyms) - 1, nil
}

// SupplyVar updates the join to make it supply the requested
// column as a join variable. If the column is not already in
// its list, it requests the LHS node to supply it using SupplyCol.
func (jb *joinBuilder) SupplyVar(col *sqlparser.ColName, varname string) {
	if _, ok := jb.Join.Vars[varname]; ok {
		// Looks like somebody else already requested this.
		return
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		for i, colsym := range jb.Colsyms {
			if jb.Join.Cols[i] > 0 {
				continue
			}
			if meta == colsym {
				jb.Join.Vars[varname] = -jb.Join.Cols[i] - 1
				return
			}
		}
		panic("unexpected: column not found")
	case *tableAlias:
		ref := newColref(col)
		for i, colsym := range jb.Colsyms {
			if jb.Join.Cols[i] > 0 {
				continue
			}
			if colsym.Underlying == ref {
				jb.Join.Vars[varname] = -jb.Join.Cols[i] - 1
				return
			}
		}
		jb.Join.Vars[varname] = jb.Left.SupplyCol(col)
		return
	}
	panic("unreachable")
}

// SupplyCol changes the join to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (jb *joinBuilder) SupplyCol(col *sqlparser.ColName) int {
	// We already know it's a tableAlias.
	meta := col.Metadata.(*tableAlias)
	ref := newColref(col)
	for i, colsym := range jb.Colsyms {
		if colsym.Underlying == ref {
			return i
		}
	}
	routeNumber := meta.Route().Order()
	if routeNumber <= jb.LeftOrder {
		ret := jb.Left.SupplyCol(col)
		jb.Join.Cols = append(jb.Join.Cols, -ret-1)
	} else {
		ret := jb.Right.SupplyCol(col)
		jb.Join.Cols = append(jb.Join.Cols, ret+1)
	}
	jb.Colsyms = append(jb.Colsyms, &colsym{Underlying: ref})
	return len(jb.Join.Cols) - 1
}
