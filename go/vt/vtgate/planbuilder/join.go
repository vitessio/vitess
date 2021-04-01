/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/semantics"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*join)(nil)

// join is used to build a Join primitive.
// It's used to build a normal join or a left join
// operation.
type join struct {
	order         int
	resultColumns []*resultColumn
	weightStrings map[*resultColumn]int

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
	Left, Right logicalPlan

	ejoin *engine.Join
}

// newJoin makes a new join using the two planBuilder. ajoin can be nil
// if the join is on a ',' operator. lpb will contain the resulting join.
// rpb will be discarded.
func newJoin(lpb, rpb *primitiveBuilder, ajoin *sqlparser.JoinTableExpr, reservedVars sqlparser.BindVars) error {
	// This function converts ON clauses to WHERE clauses. The WHERE clause
	// scope can see all tables, whereas the ON clause can only see the
	// participants of the JOIN. However, since the ON clause doesn't allow
	// external references, and the FROM clause doesn't allow duplicates,
	// it's safe to perform this conversion and still expect the same behavior.

	opcode := engine.NormalJoin
	if ajoin != nil {
		switch {
		case ajoin.Join == sqlparser.LeftJoinType:
			opcode = engine.LeftJoin

			// For left joins, we have to push the ON clause into the RHS.
			// We do this before creating the join primitive.
			// However, variables of LHS need to be visible. To allow this,
			// we mark the LHS symtab as outer scope to the RHS, just like
			// a subquery. This make the RHS treat the LHS symbols as external.
			// This will prevent constructs from escaping out of the rpb scope.
			// At this point, the LHS symtab also contains symbols of the RHS.
			// But the RHS will hide those, as intended.
			rpb.st.Outer = lpb.st
			if err := rpb.pushFilter(ajoin.Condition.On, sqlparser.WhereStr, reservedVars); err != nil {
				return err
			}
		case ajoin.Condition.Using != nil:
			return errors.New("unsupported: join with USING(column_list) clause for complex queries")
		}
	}
	lpb.plan = &join{
		weightStrings: make(map[*resultColumn]int),
		Left:          lpb.plan,
		Right:         rpb.plan,
		ejoin: &engine.Join{
			Opcode: opcode,
			Vars:   make(map[string]int),
		},
	}
	lpb.plan.Reorder(0)
	if ajoin == nil || opcode == engine.LeftJoin {
		return nil
	}
	return lpb.pushFilter(ajoin.Condition.On, sqlparser.WhereStr, reservedVars)
}

// Order implements the logicalPlan interface
func (jb *join) Order() int {
	return jb.order
}

// Reorder implements the logicalPlan interface
func (jb *join) Reorder(order int) {
	jb.Left.Reorder(order)
	jb.leftOrder = jb.Left.Order()
	jb.Right.Reorder(jb.leftOrder)
	jb.order = jb.Right.Order() + 1
}

// Primitive implements the logicalPlan interface
func (jb *join) Primitive() engine.Primitive {
	jb.ejoin.Left = jb.Left.Primitive()
	jb.ejoin.Right = jb.Right.Primitive()
	return jb.ejoin
}

// ResultColumns implements the logicalPlan interface
func (jb *join) ResultColumns() []*resultColumn {
	return jb.resultColumns
}

// Wireup implements the logicalPlan interface
func (jb *join) Wireup(plan logicalPlan, jt *jointab) error {
	err := jb.Right.Wireup(plan, jt)
	if err != nil {
		return err
	}
	return jb.Left.Wireup(plan, jt)
}

// Wireup2 implements the logicalPlan interface
func (jb *join) WireupV4(semTable *semantics.SemTable) error {
	err := jb.Right.WireupV4(semTable)
	if err != nil {
		return err
	}
	return jb.Left.WireupV4(semTable)
}

// SupplyVar implements the logicalPlan interface
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

// SupplyCol implements the logicalPlan interface
func (jb *join) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
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

// SupplyWeightString implements the logicalPlan interface
func (jb *join) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	rc := jb.resultColumns[colNumber]
	if weightcolNumber, ok := jb.weightStrings[rc]; ok {
		return weightcolNumber, nil
	}
	routeNumber := rc.column.Origin().Order()
	if jb.isOnLeft(routeNumber) {
		sourceCol, err := jb.Left.SupplyWeightString(-jb.ejoin.Cols[colNumber] - 1)
		if err != nil {
			return 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, -sourceCol-1)
	} else {
		sourceCol, err := jb.Right.SupplyWeightString(jb.ejoin.Cols[colNumber] - 1)
		if err != nil {
			return 0, err
		}
		jb.ejoin.Cols = append(jb.ejoin.Cols, sourceCol+1)
	}
	jb.resultColumns = append(jb.resultColumns, rc)
	jb.weightStrings[rc] = len(jb.ejoin.Cols) - 1
	return len(jb.ejoin.Cols) - 1, nil
}

// Rewrite implements the logicalPlan interface
func (jb *join) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "join: wrong number of inputs")
	}
	jb.Left = inputs[0]
	jb.Right = inputs[1]
	return nil
}

// Solves implements the logicalPlan interface
func (jb *join) ContainsTables() semantics.TableSet {
	return jb.Left.ContainsTables().Merge(jb.Right.ContainsTables())
}

// Inputs implements the logicalPlan interface
func (jb *join) Inputs() []logicalPlan {
	return []logicalPlan{jb.Left, jb.Right}
}

// isOnLeft returns true if the specified route number
// is on the left side of the join. If false, it means
// the node is on the right.
func (jb *join) isOnLeft(nodeNum int) bool {
	return nodeNum <= jb.leftOrder
}
