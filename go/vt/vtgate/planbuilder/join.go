// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

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
func (jb *joinBuilder) PushSelect(expr *sqlparser.NonStarExpr, route *routeBuilder) (colsym *colsym, colnum int) {
	if route.Order() <= jb.LeftOrder {
		colsym, colnum = jb.Left.PushSelect(expr, route)
		jb.Join.Cols = append(jb.Join.Cols, -colnum-1)
	} else {
		colsym, colnum = jb.Right.PushSelect(expr, route)
		jb.Join.Cols = append(jb.Join.Cols, colnum+1)
	}
	jb.Colsyms = append(jb.Colsyms, colsym)
	return colsym, len(jb.Colsyms) - 1
}

// SupplyVar updates the join to make it supply the requested
// column as a join variable. If the column is not already in
// its list, it requests the LHS node to supply it using SupplyCol.
func (jb *joinBuilder) SupplyVar(col *sqlparser.ColName, varname string) {
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
		panic("unexpected")
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
	panic("unexpected")
}

// SupplyCol changes the join to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (jb *joinBuilder) SupplyCol(col *sqlparser.ColName) int {
	switch meta := col.Metadata.(type) {
	case *colsym:
		panic("unexpected")
	case *tableAlias:
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
	panic("unexpected")
}

// MarshalJSON marshals joinBuilder into a readable form.
// It's used for testing and diagnostics. The representation
// cannot be used to reconstruct a joinBuilder.
func (jb *joinBuilder) MarshalJSON() ([]byte, error) {
	marshalJoin := struct {
		LeftOrder   int
		RightOrder  int
		Left, Right planBuilder
		Join        *Join
	}{
		LeftOrder:  jb.LeftOrder,
		RightOrder: jb.RightOrder,
		Left:       jb.Left,
		Right:      jb.Right,
		Join: &Join{
			IsLeft: jb.Join.IsLeft,
			Cols:   jb.Join.Cols,
			Vars:   jb.Join.Vars,
		},
	}
	return json.Marshal(marshalJoin)
}
