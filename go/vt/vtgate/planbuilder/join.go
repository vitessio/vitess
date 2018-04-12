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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*join)(nil)

// join is used to build a Join primitive.
// It's used to build a normal join or a left join
// operation.
type join struct {
	symtab        *symtab
	order         int
	resultColumns []*resultColumn

	// leftOrder stores the order number of the left node. This is
	// used for a b-tree style traversal towards the target route.
	// Let us assume the following execution tree:
	//      J9
	//     /  \
	//    /    \
	//   J3     J8
	//  / \    /  \
	// R1  R2  J6  R7
	//        / \
	//        R4 R5
	//
	// In the above trees, the suffix numbers indicate the
	// execution order. The leftOrder for the joins will then
	// be as follows:
	// J3: 1
	// J6: 4
	// J8: 6
	// J9: 3
	//
	// The route to R4 would be:
	// Go right from J9->J8 because Left(J9)==3, which is <4.
	// Go left from J8->J6 because Left(J8)==6, which is >=4.
	// Go left from J6->R4 because Left(J6)==4, the destination.
	// Look for 'isOnLeft' to see how these numbers are used.
	leftOrder int

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

	if err := lhs.Symtab().Merge(rhs.Symtab()); err != nil {
		return nil, err
	}
	opcode := engine.NormalJoin
	if ajoin != nil && ajoin.Join == sqlparser.LeftJoinStr {
		opcode = engine.LeftJoin
	}
	jb := &join{
		order:     rhs.Order() + 1,
		leftOrder: lhs.Order(),
		Left:      lhs,
		Right:     rhs,
		symtab:    lhs.Symtab(),
		ejoin: &engine.Join{
			Opcode: opcode,
			Left:   lhs.Primitive(),
			Right:  rhs.Primitive(),
			Vars:   make(map[string]int),
		},
	}
	jb.Reorder(0)
	if ajoin == nil {
		return jb, nil
	}
	if ajoin.Condition.Using != nil {
		return nil, errors.New("unsupported: join with USING(column_list) clause")
	}

	if opcode == engine.LeftJoin {
		if err := pushFilter(ajoin.Condition.On, rhs, sqlparser.WhereStr); err != nil {
			return nil, err
		}
		return jb, nil
	}
	if err := pushFilter(ajoin.Condition.On, jb, sqlparser.WhereStr); err != nil {
		return nil, err
	}
	return jb, nil
}

// Symtab satisfies the builder interface.
func (jb *join) Symtab() *symtab {
	return jb.symtab.Resolve()
}

// Order satisfies the builder interface.
func (jb *join) Order() int {
	return jb.order
}

// Reorder satisfies the builder interface.
func (jb *join) Reorder(order int) {
	jb.Left.Reorder(order)
	jb.leftOrder = jb.Left.Order()
	jb.Right.Reorder(jb.leftOrder)
	jb.order = jb.Right.Order() + 1
}

// Primitive satisfies the builder interface.
func (jb *join) Primitive() engine.Primitive {
	return jb.ejoin
}

// Leftmost satisfies the builder interface.
func (jb *join) Leftmost() builder {
	return jb.Left.Leftmost()
}

// ResultColumns satisfies the builder interface.
func (jb *join) ResultColumns() []*resultColumn {
	return jb.resultColumns
}

// PushFilter satisfies the builder interface.
func (jb *join) PushFilter(filter sqlparser.Expr, whereType string, origin builder) error {
	if jb.isOnLeft(origin.Order()) {
		return jb.Left.PushFilter(filter, whereType, origin)
	}
	if jb.ejoin.Opcode == engine.LeftJoin {
		return errors.New("unsupported: cross-shard left join and where clause")
	}
	return jb.Right.PushFilter(filter, whereType, origin)
}

// PushSelect satisfies the builder interface.
func (jb *join) PushSelect(expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colnum int, err error) {
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

// PushOrderByRand satisfies the builder interface.
func (jb *join) PushOrderByRand() {
	jb.Left.PushOrderByRand()
	jb.Right.PushOrderByRand()
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
	return nodeNum <= jb.leftOrder
}
