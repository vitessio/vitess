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
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

var _ builder = (*join)(nil)

// join is used to build a Join primitive.
// It's used to build a normal join or a left join
// operation.
type join struct {
	symtab        *symtab
	resultColumns []*resultColumn

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

	ejoin *engine.Join
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
		return jb, nil
	}
	err = pushFilter(ajoin.On, jb, sqlparser.WhereStr)
	if err != nil {
		return nil, err
	}
	return jb, nil
}

// Symtab satisfies the builder interface.
func (jb *join) Symtab() *symtab {
	return jb.symtab.Resolve()
}

// MaxOrder satisfies the builder interface.
func (jb *join) MaxOrder() int {
	return jb.rightMaxOrder
}

// SetOrder satisfies the builder interface.
func (jb *join) SetOrder(order int) {
	jb.Left.SetOrder(order)
	jb.leftMaxOrder = jb.Left.MaxOrder()
	jb.Right.SetOrder(jb.leftMaxOrder)
	jb.rightMaxOrder = jb.Right.MaxOrder()
}

// Primitive satisfies the builder interface.
func (jb *join) Primitive() engine.Primitive {
	return jb.ejoin
}

// Leftmost satisfies the builder interface.
func (jb *join) Leftmost() columnOriginator {
	return jb.Left.Leftmost()
}

// ResultColumns satisfies the builder interface.
func (jb *join) ResultColumns() []*resultColumn {
	return jb.resultColumns
}

// PushFilter satisfies the builder interface.
func (jb *join) PushFilter(filter sqlparser.Expr, whereType string, origin columnOriginator) error {
	if jb.isOnLeft(origin.Order()) {
		return jb.Left.PushFilter(filter, whereType, origin)
	}
	if jb.ejoin.Opcode == engine.LeftJoin {
		return errors.New("unsupported: cross-shard left join and where clause")
	}
	return jb.Right.PushFilter(filter, whereType, origin)
}

// PushSelect satisfies the builder interface.
func (jb *join) PushSelect(expr *sqlparser.AliasedExpr, origin columnOriginator) (rc *resultColumn, colnum int, err error) {
	if jb.isOnLeft(origin.Order()) {
		rc, colnum, err = jb.Left.PushSelect(expr, origin)
		if err != nil {
			return nil, 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, -colnum-1)
	} else {
		// Pushing of non-trivial expressions not allowed for RHS of left joins.
		if _, ok := expr.Expr.(*sqlparser.ColName); !ok && jb.ejoin.Opcode == engine.LeftJoin {
			return nil, 0, errors.New("unsupported: cross-shard left join and column expressions")
		}

		rc, colnum, err = jb.Right.PushSelect(expr, origin)
		if err != nil {
			return nil, 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, colnum+1)
	}
	jb.resultColumns = append(jb.resultColumns, rc)
	return rc, len(jb.resultColumns) - 1, nil
}

// PushOrderByNull satisfies the builder interface.
func (jb *join) PushOrderByNull() {
	jb.Left.PushOrderByNull()
	jb.Right.PushOrderByNull()
}

// SetUpperLimit satisfies the builder interface.
// The call is ignored because results get multiplied
// as they join with others. So, it's hard to reliably
// predict if a limit push down will work correctly.
func (jb *join) SetUpperLimit(_ *sqlparser.SQLVal) {
}

// PushMisc satisfies the builder interface.
func (jb *join) PushMisc(sel *sqlparser.Select) {
	jb.Left.PushMisc(sel)
	jb.Right.PushMisc(sel)
}

// Wireup satisfies the builder interface.
func (jb *join) Wireup(bldr builder, jt *jointab) error {
	err := jb.Right.Wireup(bldr, jt)
	if err != nil {
		return err
	}
	return jb.Left.Wireup(bldr, jt)
}

// SupplyVar satisfies the builder interface.
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
	for i, rc := range jb.resultColumns {
		if jb.ejoin.Cols[i] > 0 {
			continue
		}
		if rc.column == c {
			jb.ejoin.Vars[varname] = -jb.ejoin.Cols[i] - 1
			return
		}
	}
	_, jb.ejoin.Vars[varname] = jb.Left.SupplyCol(col)
}

// SupplyCol satisfies the builder interface.
func (jb *join) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int) {
	c := col.Metadata.(*column)
	for i, rc := range jb.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}

	routeNumber := c.Origin().Order()
	var sourceCol int
	if jb.isOnLeft(routeNumber) {
		rc, sourceCol = jb.Left.SupplyCol(col)
		jb.ejoin.Cols = append(jb.ejoin.Cols, -sourceCol-1)
	} else {
		rc, sourceCol = jb.Right.SupplyCol(col)
		jb.ejoin.Cols = append(jb.ejoin.Cols, sourceCol+1)
	}
	jb.resultColumns = append(jb.resultColumns, rc)
	return rc, len(jb.ejoin.Cols) - 1
}

// isOnLeft returns true if the specified route number
// is on the left side of the join. If false, it means
// the node is on the right.
func (jb *join) isOnLeft(nodeNum int) bool {
	return nodeNum <= jb.leftMaxOrder
}
