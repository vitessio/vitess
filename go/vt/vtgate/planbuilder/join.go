// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

// join is used to build a Join primitive.
// It's used to buid a normal join or a left join
// operation.
type join struct {
	// LeftOrder and RightOrder store the order
	// of the left node and right node. The Order
	// of this join will be the same as RightOrder.
	// This information is used for traversal.
	LeftOrder, RightOrder int
	// Left and Right are the nodes for the join.
	Left, Right builder
	symtab      *symtab
	// Colsyms specifies the colsyms supplied by this
	// join.
	Colsyms []*colsym
	ejoin   *engine.Join
}

// newJoin makes a new joinBuilder using the two nodes. ajoin can be nil
// if the join is on a ',' operator.
func newJoin(lhs, rhs builder, ajoin *sqlparser.JoinTableExpr) (*join, error) {
	// This function converts ON clauses to WHERE clauses. The WHERE clause
	// scope can see all tables, whereas the ON clause can only see the
	// participants of the JOIN. However, since the ON clause doesn't allow
	// external references, and the FROM clause doesn't allow duplicates,
	// it's safe to perform this conversion and still expect the same behavior.

	err := lhs.Symtab().Merge(rhs.Symtab())
	if err != nil {
		return nil, err
	}
	rhs.SetSymtab(lhs.Symtab())
	rhs.SetOrder(lhs.Order())
	opcode := engine.NormalJoin
	if ajoin != nil && ajoin.Join == sqlparser.LeftJoinStr {
		opcode = engine.LeftJoin
	}
	jb := &join{
		LeftOrder:  lhs.Order(),
		RightOrder: rhs.Order(),
		Left:       lhs,
		Right:      rhs,
		symtab:     lhs.Symtab(),
		ejoin: &engine.Join{
			Opcode: opcode,
			Left:   lhs.Primitive(),
			Right:  rhs.Primitive(),
			Vars:   make(map[string]int),
		},
	}
	if ajoin == nil {
		return jb, nil
	}
	if opcode == engine.LeftJoin {
		err := pushFilter(ajoin.On, rhs, sqlparser.WhereStr)
		if err != nil {
			return nil, err
		}
		rhs.SetRHS()
		return jb, nil
	}
	err = pushFilter(ajoin.On, jb, sqlparser.WhereStr)
	if err != nil {
		return nil, err
	}
	return jb, nil
}

// Symtab returns the associated symtab.
func (jb *join) Symtab() *symtab {
	return jb.symtab
}

// SetSymtab sets the symtab for the current node and its
// non-subquery children.
func (jb *join) SetSymtab(symtab *symtab) {
	jb.symtab = symtab
	jb.Left.SetSymtab(symtab)
	jb.Right.SetSymtab(symtab)
}

// Order returns the order of the node.
func (jb *join) Order() int {
	return jb.RightOrder
}

// SetOrder sets the order for the unerlying routes.
func (jb *join) SetOrder(order int) {
	jb.Left.SetOrder(order)
	jb.LeftOrder = jb.Left.Order()
	jb.Right.SetOrder(jb.LeftOrder)
	jb.RightOrder = jb.Right.Order()
}

// Primitve returns the built primitive.
func (jb *join) Primitive() engine.Primitive {
	return jb.ejoin
}

// Leftmost returns the leftmost route.
func (jb *join) Leftmost() *route {
	return jb.Left.Leftmost()
}

// Join creates new joined node using the two plans.
func (jb *join) Join(rhs builder, ajoin *sqlparser.JoinTableExpr) (builder, error) {
	return newJoin(jb, rhs, ajoin)
}

// SetRHS sets all underlying routes to RHS.
func (jb *join) SetRHS() {
	jb.Left.SetRHS()
	jb.Right.SetRHS()
}

// PushSelect pushes the select expression into the join and
// recursively down.
func (jb *join) PushSelect(expr *sqlparser.NonStarExpr, rb *route) (colsym *colsym, colnum int, err error) {
	if rb.Order() <= jb.LeftOrder {
		colsym, colnum, err = jb.Left.PushSelect(expr, rb)
		if err != nil {
			return nil, 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, -colnum-1)
	} else {
		colsym, colnum, err = jb.Right.PushSelect(expr, rb)
		if err != nil {
			return nil, 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, colnum+1)
	}
	jb.Colsyms = append(jb.Colsyms, colsym)
	return colsym, len(jb.Colsyms) - 1, nil
}

// PushOrderByNull pushes misc constructs to the underlying routes.
func (jb *join) PushOrderByNull() {
	jb.Left.PushOrderByNull()
	jb.Right.PushOrderByNull()
}

// PushMisc pushes misc constructs to the underlying routes.
func (jb *join) PushMisc(sel *sqlparser.Select) {
	jb.Left.PushMisc(sel)
	jb.Right.PushMisc(sel)
}

// Wireup performs the wireup for join.
func (jb *join) Wireup(bldr builder, jt *jointab) error {
	err := jb.Right.Wireup(bldr, jt)
	if err != nil {
		return err
	}
	return jb.Left.Wireup(bldr, jt)
}

// SupplyVar updates the join to make it supply the requested
// column as a join variable. If the column is not already in
// its list, it requests the LHS node to supply it using SupplyCol.
func (jb *join) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	if from > jb.LeftOrder {
		jb.Right.SupplyVar(from, to, col, varname)
		return
	}
	if to <= jb.LeftOrder {
		jb.Left.SupplyVar(from, to, col, varname)
		return
	}
	if _, ok := jb.ejoin.Vars[varname]; ok {
		// Looks like somebody else already requested this.
		return
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		for i, colsym := range jb.Colsyms {
			if jb.ejoin.Cols[i] > 0 {
				continue
			}
			if meta == colsym {
				jb.ejoin.Vars[varname] = -jb.ejoin.Cols[i] - 1
				return
			}
		}
		panic("unexpected: column not found")
	case *tabsym:
		ref := newColref(col)
		for i, colsym := range jb.Colsyms {
			if jb.ejoin.Cols[i] > 0 {
				continue
			}
			if colsym.Underlying == ref {
				jb.ejoin.Vars[varname] = -jb.ejoin.Cols[i] - 1
				return
			}
		}
		jb.ejoin.Vars[varname] = jb.Left.SupplyCol(ref)
		return
	}
	panic("unreachable")
}

// SupplyCol changes the join to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (jb *join) SupplyCol(ref colref) int {
	for i, colsym := range jb.Colsyms {
		if colsym.Underlying == ref {
			return i
		}
	}
	routeNumber := ref.Route().Order()
	if routeNumber <= jb.LeftOrder {
		ret := jb.Left.SupplyCol(ref)
		jb.ejoin.Cols = append(jb.ejoin.Cols, -ret-1)
	} else {
		ret := jb.Right.SupplyCol(ref)
		jb.ejoin.Cols = append(jb.ejoin.Cols, ret+1)
	}
	jb.Colsyms = append(jb.Colsyms, &colsym{Underlying: ref})
	return len(jb.ejoin.Cols) - 1
}
