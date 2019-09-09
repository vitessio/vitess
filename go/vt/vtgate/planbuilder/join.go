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

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*join)(nil)

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
	Left, Right builder

	Cols     []int          `json:",omitempty"`
	Vars     map[string]int `json:",omitempty"`
	Opcode   engine.JoinOpcode
	joinImpl joinStyle
}

// Instead of storing a boolean saying if we are dealing with a hash join or a nested loop join, we use
// this joinStyle interface, and call on it in the two phases where we need different behaviour per join
type joinStyle interface {
	wireUp(jb *join) error
	createPrimitive(jb *join, l, r engine.Primitive) engine.Primitive
}

// newJoin makes a new join using the two planBuilder. ajoin can be nil
// if the join is on a ',' operator. lpb will contain the resulting join.
// rpb will be discarded.
func newJoin(lpb, rpb *primitiveBuilder, ajoin *sqlparser.JoinTableExpr, useHashJoin bool) error {
	// This function converts ON clauses to WHERE clauses. The WHERE clause
	// scope can see all tables, whereas the ON clause can only see the
	// participants of the JOIN. However, since the ON clause doesn't allow
	// external references, and the FROM clause doesn't allow duplicates,
	// it's safe to perform this conversion and still expect the same behavior.

	opcode := engine.InnerJoin
	if ajoin != nil {
		switch {
		case ajoin.Join == sqlparser.LeftJoinStr:
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
			if err := rpb.pushFilter(ajoin.Condition.On, sqlparser.WhereStr); err != nil {
				return err
			}
		case ajoin.Condition.Using != nil:
			return errors.New("unsupported: join with USING(column_list) clause")
		}
	}

	joinStruct, err := createJoinStruct(lpb, rpb, opcode, useHashJoin, ajoin)
	if err != nil {
		return err
	}
	lpb.bldr = joinStruct
	lpb.bldr.Reorder(0)

	if ajoin == nil || opcode == engine.LeftJoin || useHashJoin {
		return nil
	}
	return lpb.pushFilter(ajoin.Condition.On, sqlparser.WhereStr)
}

func createJoinStruct(lpb, rpb *primitiveBuilder, opcode engine.JoinOpcode, useHashJoin bool, ajoin *sqlparser.JoinTableExpr) (*join, error) {
	j := join{
		weightStrings: make(map[*resultColumn]int),
		Left:          lpb.bldr,
		Right:         rpb.bldr,
		Vars:          make(map[string]int),
		Opcode:        opcode,
	}

	if useHashJoin {
		if ajoin == nil {
			return nil, vterrors.New(vtrpc.Code_INTERNAL, "need an ajoin to create a hash join")
		}
		comparisons, err := extractJoinComparisons(ajoin.Condition.On)
		if err != nil {
			return nil, err
		}

		j.joinImpl = &hashJoin{comparisonExprs: comparisons}
	} else {
		j.joinImpl = &nestedLoopJoin{}
	}
	return &j, nil
}

var _ joinStyle = (*hashJoin)(nil)

// extractJoinComparisons recursively walks down an AND tree to produce an array with all the equals comparisons
func extractJoinComparisons(expr sqlparser.Expr) ([]*sqlparser.ComparisonExpr, error) {
	cmp, isComparison := expr.(*sqlparser.ComparisonExpr)
	if isComparison {
		if cmp.Operator != "=" {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "can only handle simple equality between columns using hash joins - got %s", cmp.Operator)
		}

		return []*sqlparser.ComparisonExpr{cmp}, nil
	}

	and, isAnd := expr.(*sqlparser.AndExpr)
	if isAnd {
		lft, err := extractJoinComparisons(and.Left)
		if err != nil {
			return nil, err
		}
		rgt, err := extractJoinComparisons(and.Right)
		if err != nil {
			return nil, err
		}
		return append(lft, rgt...), nil
	}
	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "can only handle one or more equality comparisons between columns using hash joins - got %s", expr)
}

type hashJoin struct {
	comparisonExprs []*sqlparser.ComparisonExpr // the ON predicate
	left, right     []int                       // the column offsets for the key comparisons
}

func (hj *hashJoin) wireUp(jb *join) error {
	var lftResult []int
	var rgtResult []int
	for _, cmp := range hj.comparisonExprs {
		leftCol, leftOk := cmp.Left.(*sqlparser.ColName)
		rightCol, rightOk := cmp.Right.(*sqlparser.ColName)
		if !leftOk || !rightOk {
			return vterrors.New(vtrpc.Code_INTERNAL, "can only handle simple equality between columns using hash joins")
		}
		_, lftColOffset := jb.Left.SupplyCol(leftCol)
		_, rgtColOffset := jb.Right.SupplyCol(rightCol)
		lftResult = append(lftResult, lftColOffset)
		rgtResult = append(rgtResult, rgtColOffset)
	}

	hj.left = lftResult
	hj.right = rgtResult
	return nil
}

func (hj *hashJoin) createPrimitive(jb *join, l, r engine.Primitive) engine.Primitive {
	return &engine.HashJoin{
		Left:          l,
		Right:         r,
		Cols:          jb.Cols,
		LeftJoinCols:  hj.left,
		RightJoinCols: hj.right,
	}
}

var _ joinStyle = (*nestedLoopJoin)(nil)

type nestedLoopJoin struct{}

func (nestedLoopJoin) wireUp(jb *join) error {
	return nil // do nothing
}

func (nestedLoopJoin) createPrimitive(jb *join, l, r engine.Primitive) engine.Primitive {
	return &engine.NestedLoopJoin{
		Opcode: jb.Opcode,
		Vars:   jb.Vars,
		Cols:   jb.Cols,
		Left:   l,
		Right:  r,
	}
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
	return jb.joinImpl.createPrimitive(jb, jb.Left.Primitive(), jb.Right.Primitive())
}

// First satisfies the builder interface.
func (jb *join) First() builder {
	return jb.Left.First()
}

// ResultColumns satisfies the builder interface.
func (jb *join) ResultColumns() []*resultColumn {
	return jb.resultColumns
}

// PushFilter satisfies the builder interface.
func (jb *join) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, origin builder) error {
	if jb.isOnLeft(origin.Order()) {
		return jb.Left.PushFilter(pb, filter, whereType, origin)
	}
	if jb.Opcode == engine.LeftJoin {
		return errors.New("unsupported: cross-shard left join and where clause")
	}
	return jb.Right.PushFilter(pb, filter, whereType, origin)
}

// PushSelect satisfies the builder interface.
func (jb *join) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	if jb.isOnLeft(origin.Order()) {
		rc, colNumber, err = jb.Left.PushSelect(pb, expr, origin)
		if err != nil {
			return nil, 0, err
		}
		jb.Cols = append(jb.Cols, -colNumber-1)
	} else {
		// Pushing of non-trivial expressions not allowed for RHS of left joins.
		if _, ok := expr.Expr.(*sqlparser.ColName); !ok && jb.Opcode == engine.LeftJoin {
			return nil, 0, errors.New("unsupported: cross-shard left join and column expressions")
		}

		rc, colNumber, err = jb.Right.PushSelect(pb, expr, origin)
		if err != nil {
			return nil, 0, err
		}
		jb.Cols = append(jb.Cols, colNumber+1)
	}
	jb.resultColumns = append(jb.resultColumns, rc)
	return rc, len(jb.resultColumns) - 1, nil
}

// MakeDistinct satisfies the builder interface.
func (jb *join) MakeDistinct() error {
	return errors.New("unsupported: distinct on cross-shard join")
}

// PushGroupBy satisfies the builder interface.
func (jb *join) PushGroupBy(groupBy sqlparser.GroupBy) error {
	if (groupBy) == nil {
		return nil
	}
	return errors.New("unupported: group by on cross-shard join")
}

// PushOrderBy satisfies the builder interface.
func (jb *join) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
	isSpecial := false
	switch len(orderBy) {
	case 0:
		isSpecial = true
	case 1:
		if _, ok := orderBy[0].Expr.(*sqlparser.NullVal); ok {
			isSpecial = true
		} else if f, ok := orderBy[0].Expr.(*sqlparser.FuncExpr); ok {
			if f.Name.Lowered() == "rand" {
				isSpecial = true
			}
		}
	}
	if isSpecial {
		l, err := jb.Left.PushOrderBy(orderBy)
		if err != nil {
			return nil, err
		}
		jb.Left = l
		r, err := jb.Right.PushOrderBy(orderBy)
		if err != nil {
			return nil, err
		}
		jb.Right = r
		return jb, nil
	}

	for _, order := range orderBy {
		if node, ok := order.Expr.(*sqlparser.SQLVal); ok {
			// This block handles constructs that use ordinals for 'ORDER BY'. For example:
			// SELECT a, b, c FROM t1, t2 ORDER BY 1, 2, 3.
			num, err := ResultFromNumber(jb.ResultColumns(), node)
			if err != nil {
				return nil, err
			}
			if jb.ResultColumns()[num].column.Origin().Order() > jb.Left.Order() {
				return newMemorySort(jb, orderBy)
			}
		} else {
			// Analyze column references within the expression to make sure they all
			// go to the left.
			err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ColName:
					if node.Metadata.(*column).Origin().Order() > jb.Left.Order() {
						return false, errors.New("unsupported: order by spans across shards")
					}
				case *sqlparser.Subquery:
					// Unreachable because ResolveSymbols perfoms this check up above.
					return false, errors.New("unsupported: order by has subquery")
				}
				return true, nil
			}, order.Expr)
			if err != nil {
				return newMemorySort(jb, orderBy)
			}
		}
	}

	// There were no errors. We can push the order by to the left-most route.
	l, err := jb.Left.PushOrderBy(orderBy)
	if err != nil {
		return nil, err
	}
	jb.Left = l
	// Still need to push an empty order by to the right.
	r, err := jb.Right.PushOrderBy(nil)
	if err != nil {
		return nil, err
	}
	jb.Right = r
	return jb, nil
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
	err = jb.Left.Wireup(bldr, jt)
	if err != nil {
		return err
	}
	return jb.joinImpl.wireUp(jb)
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
	if _, ok := jb.Vars[varname]; ok {
		// Looks like somebody else already requested this.
		return
	}
	c := col.Metadata.(*column)
	for i, rc := range jb.resultColumns {
		if jb.Cols[i] > 0 {
			continue
		}
		if rc.column == c {
			jb.Vars[varname] = -jb.Cols[i] - 1
			return
		}
	}
	_, jb.Vars[varname] = jb.Left.SupplyCol(col)
}

// SupplyCol satisfies the builder interface.
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
		jb.Cols = append(jb.Cols, -sourceCol-1)
	} else {
		rc, sourceCol = jb.Right.SupplyCol(col)
		jb.Cols = append(jb.Cols, sourceCol+1)
	}
	jb.resultColumns = append(jb.resultColumns, rc)
	return rc, len(jb.Cols) - 1
}

// SupplyWeightString satisfies the builder interface.
func (jb *join) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	rc := jb.resultColumns[colNumber]
	if weightcolNumber, ok := jb.weightStrings[rc]; ok {
		return weightcolNumber, nil
	}
	routeNumber := rc.column.Origin().Order()
	if jb.isOnLeft(routeNumber) {
		sourceCol, err := jb.Left.SupplyWeightString(-jb.Cols[colNumber] - 1)
		if err != nil {
			return 0, err
		}
		jb.Cols = append(jb.Cols, -sourceCol-1)
	} else {
		sourceCol, err := jb.Right.SupplyWeightString(jb.Cols[colNumber] - 1)
		if err != nil {
			return 0, err
		}
		jb.Cols = append(jb.Cols, sourceCol+1)
	}
	jb.resultColumns = append(jb.resultColumns, rc)
	jb.weightStrings[rc] = len(jb.Cols) - 1
	return len(jb.Cols) - 1, nil
}

// isOnLeft returns true if the specified route number
// is on the left side of the join. If false, it means
// the node is on the right.
func (jb *join) isOnLeft(nodeNum int) bool {
	return nodeNum <= jb.leftOrder
}
