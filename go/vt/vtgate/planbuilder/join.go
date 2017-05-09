/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

// join is used to build a Join primitive.
// It's used to build a normal join or a left join
// operation.
type join struct {
	// leftMaxOrder and rightMaxOrder store the max order
	// of the left node and right node. This is essentially
	// used for a b-tree style traversal towards the target route.
	// Let us assume the following execution tree:
	//      Ja
	//     /  \
	//    /    \
	//   Jb     Jc
	//  / \    /  \
	// R1  R2  Jd  R5
	//        / \
	//        R3 R4
	//
	// R1-R5 are routes. Their numbers indicate execution
	// order: R1 is executed first, then it's R2, which will
	// be joined at Jb, etc.
	//
	// The values for left and right max order for the join
	// nodes will be:
	//     left right
	// Jb: 1    2
	// Jd: 3    4
	// Jc: 4    5
	// Ja: 2    5
	// The route to R3 would be:
	// Go right from Ja->Jc because Left(Ja)==2, which is <3.
	// Go left from Jc->Jd because Left(Jc)==4, which is >=3.
	// Go left from Jd->R3 because Left(Jd)==3, the destination.
	//
	// There are many use cases for these Orders. Look for 'isOnLeft'
	// to see how these numbers are used. 'isOnLeft' is a convenience
	// function to help with traversal.
	// The MaxOrder for a join is the same as rightMaxOrder.
	// A join can currently not be a destination. It therefore does
	// not have its own Order.
	leftMaxOrder, rightMaxOrder int
	// Left and Right are the nodes for the join.
	Left, Right builder
	symtab      *symtab
	// ResultColumns specifies the result columns supplied by this
	// join.
	ResultColumns []*resultColumn
	ejoin         *engine.Join
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
	rhs.SetOrder(lhs.MaxOrder())
	opcode := engine.NormalJoin
	if ajoin != nil && ajoin.Join == sqlparser.LeftJoinStr {
		opcode = engine.LeftJoin
	}
	jb := &join{
		leftMaxOrder:  lhs.MaxOrder(),
		rightMaxOrder: rhs.MaxOrder(),
		Left:          lhs,
		Right:         rhs,
		symtab:        lhs.Symtab(),
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

// MaxOrder returns the max order of the node.
func (jb *join) MaxOrder() int {
	return jb.rightMaxOrder
}

// SetOrder sets the order for the underlying routes.
func (jb *join) SetOrder(order int) {
	jb.Left.SetOrder(order)
	jb.leftMaxOrder = jb.Left.MaxOrder()
	jb.Right.SetOrder(jb.leftMaxOrder)
	jb.rightMaxOrder = jb.Right.MaxOrder()
}

// Primitive returns the built primitive.
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
func (jb *join) PushSelect(expr *sqlparser.AliasedExpr, rb *route) (rc *resultColumn, colnum int, err error) {
	if jb.isOnLeft(rb.Order) {
		rc, colnum, err = jb.Left.PushSelect(expr, rb)
		if err != nil {
			return nil, 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, -colnum-1)
	} else {
		rc, colnum, err = jb.Right.PushSelect(expr, rb)
		if err != nil {
			return nil, 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, colnum+1)
	}
	jb.ResultColumns = append(jb.ResultColumns, rc)
	return rc, len(jb.ResultColumns) - 1, nil
}

// PushOrderByNull pushes 'ORDER BY NULL' to the underlying routes.
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
	if !jb.isOnLeft(from) {
		jb.Right.SupplyVar(from, to, col, varname)
		return
	}
	if jb.isOnLeft(to) {
		jb.Left.SupplyVar(from, to, col, varname)
		return
	}
	if _, ok := jb.ejoin.Vars[varname]; ok {
		// Looks like somebody else already requested this.
		return
	}
	c := col.Metadata.(*column)
	for i, rc := range jb.ResultColumns {
		if jb.ejoin.Cols[i] > 0 {
			continue
		}
		if rc.column == c {
			jb.ejoin.Vars[varname] = -jb.ejoin.Cols[i] - 1
			return
		}
	}
	_, jb.ejoin.Vars[varname] = jb.Left.SupplyCol(col.Metadata.(*column))
}

// SupplyCol changes the join to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (jb *join) SupplyCol(c *column) (rs *resultColumn, colnum int) {
	for i, rs := range jb.ResultColumns {
		if rs.column == c {
			return rs, i
		}
	}

	routeNumber := c.Route().Order
	var sourceCol int
	if jb.isOnLeft(routeNumber) {
		rs, sourceCol = jb.Left.SupplyCol(c)
		jb.ejoin.Cols = append(jb.ejoin.Cols, -sourceCol-1)
	} else {
		rs, sourceCol = jb.Right.SupplyCol(c)
		jb.ejoin.Cols = append(jb.ejoin.Cols, sourceCol+1)
	}
	jb.ResultColumns = append(jb.ResultColumns, rs)
	return rs, len(jb.ejoin.Cols) - 1
}

// isOnLeft returns true if the specified route number
// is on the left side of the join. If false, it means
// the node is on the right.
func (jb *join) isOnLeft(nodeNum int) bool {
	return nodeNum <= jb.leftMaxOrder
}
