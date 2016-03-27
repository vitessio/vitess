// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
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
	join *engine.Join
}

// newJoinBuilder makes a new joinBuilder using the two nodes.
func newJoinBuilder(lhs, rhs planBuilder, join *sqlparser.JoinTableExpr) (*joinBuilder, error) {
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
	isLeft := false
	if join.Join == sqlparser.LeftJoinStr {
		isLeft = true
	}
	jb := &joinBuilder{
		LeftOrder:  lhs.Order(),
		RightOrder: rhs.Order(),
		Left:       lhs,
		Right:      rhs,
		symtab:     lhs.Symtab(),
		join: &engine.Join{
			IsLeft: isLeft,
			Left:   lhs.Primitive(),
			Right:  rhs.Primitive(),
			Vars:   make(map[string]int),
		},
	}
	if isLeft {
		err := pushFilter(join.On, rhs, sqlparser.WhereStr)
		if err != nil {
			return nil, err
		}
		rhs.SetRHS()
		return jb, nil
	}
	err = pushFilter(join.On, jb, sqlparser.WhereStr)
	if err != nil {
		return nil, err
	}
	return jb, nil
}

// Symtab returns the associated symtab.
func (jb *joinBuilder) Symtab() *symtab {
	return jb.symtab
}

// SetSymtab sets the symtab for the current node and its
// non-subquery children.
func (jb *joinBuilder) SetSymtab(symtab *symtab) {
	jb.symtab = symtab
	jb.Left.SetSymtab(symtab)
	jb.Right.SetSymtab(symtab)
}

// Order returns the order of the node.
func (jb *joinBuilder) Order() int {
	return jb.RightOrder
}

// SetOrder sets the order for the unerlying routes.
func (jb *joinBuilder) SetOrder(order int) {
	jb.Left.SetOrder(order)
	jb.LeftOrder = jb.Left.Order()
	jb.Right.SetOrder(jb.LeftOrder)
	jb.RightOrder = jb.Right.Order()
}

// Primitve returns the built primitive.
func (jb *joinBuilder) Primitive() engine.Primitive {
	return jb.join
}

// Leftmost returns the leftmost route.
func (jb *joinBuilder) Leftmost() *routeBuilder {
	return jb.Left.Leftmost()
}

// Join creates new joined node using the two plans.
func (jb *joinBuilder) Join(rhs planBuilder, join *sqlparser.JoinTableExpr) (planBuilder, error) {
	return newJoinBuilder(jb, rhs, join)
}

// SetRHS sets all underlying routes to RHS.
func (jb *joinBuilder) SetRHS() {
	jb.Left.SetRHS()
	jb.Right.SetRHS()
}

// PushSelect pushes the select expression into the join and
// recursively down.
func (jb *joinBuilder) PushSelect(expr *sqlparser.NonStarExpr, route *routeBuilder) (colsym *colsym, colnum int, err error) {
	if route.Order() <= jb.LeftOrder {
		colsym, colnum, err = jb.Left.PushSelect(expr, route)
		if err != nil {
			return nil, 0, err
		}
		jb.join.Cols = append(jb.join.Cols, -colnum-1)
	} else {
		colsym, colnum, err = jb.Right.PushSelect(expr, route)
		if err != nil {
			return nil, 0, err
		}
		jb.join.Cols = append(jb.join.Cols, colnum+1)
	}
	jb.Colsyms = append(jb.Colsyms, colsym)
	return colsym, len(jb.Colsyms) - 1, nil
}

// PushMisc pushes misc constructs to the underlying routes.
func (jb *joinBuilder) PushMisc(sel *sqlparser.Select) {
	jb.Left.PushMisc(sel)
	jb.Right.PushMisc(sel)
}

// Wireup performs the wireup for joinBuilder.
func (jb *joinBuilder) Wireup(plan planBuilder, jt *jointab) error {
	err := jb.Right.Wireup(plan, jt)
	if err != nil {
		return err
	}
	return jb.Left.Wireup(plan, jt)
}

// SupplyVar updates the join to make it supply the requested
// column as a join variable. If the column is not already in
// its list, it requests the LHS node to supply it using SupplyCol.
func (jb *joinBuilder) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	if from > jb.LeftOrder {
		jb.Right.SupplyVar(from, to, col, varname)
		return
	}
	if to <= jb.LeftOrder {
		jb.Left.SupplyVar(from, to, col, varname)
		return
	}
	if _, ok := jb.join.Vars[varname]; ok {
		// Looks like somebody else already requested this.
		return
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		for i, colsym := range jb.Colsyms {
			if jb.join.Cols[i] > 0 {
				continue
			}
			if meta == colsym {
				jb.join.Vars[varname] = -jb.join.Cols[i] - 1
				return
			}
		}
		panic("unexpected: column not found")
	case *tableAlias:
		ref := newColref(col)
		for i, colsym := range jb.Colsyms {
			if jb.join.Cols[i] > 0 {
				continue
			}
			if colsym.Underlying == ref {
				jb.join.Vars[varname] = -jb.join.Cols[i] - 1
				return
			}
		}
		jb.join.Vars[varname] = jb.Left.SupplyCol(col)
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
		jb.join.Cols = append(jb.join.Cols, -ret-1)
	} else {
		ret := jb.Right.SupplyCol(col)
		jb.join.Cols = append(jb.join.Cols, ret+1)
	}
	jb.Colsyms = append(jb.Colsyms, &colsym{Underlying: ref})
	return len(jb.join.Cols) - 1
}
