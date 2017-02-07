package planbuilder

import (
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

// unionbuilder builds a UnionAll primitive
type unionbuilder struct {
	Left, Right builder
	symtab      *symtab
	eunion      *engine.UnionAll
}

func newUnionBuilder(lhs, rhs builder) (*unionbuilder, error) {
	return &unionbuilder{
		Left:   lhs,
		Right:  rhs,
		symtab: nil,
		eunion: &engine.UnionAll{
			Left:   lhs.Primitive(),
			Right:  rhs.Primitive(),
		},
	}, nil
}

// Symtab returns the associated symtab.
func (ub *unionbuilder) Symtab() *symtab {
	panic("unreachable")
}

// SetSymtab sets the symtab for the current node and
// its non-subquery children.
func (ub *unionbuilder) SetSymtab(*symtab) {
	panic("unreachable")
}

// Order is a number that signifies execution order.
// A lower Order number Route is executed before a
// higher one. For a node that contains other nodes,
// the Order represents the highest order of the leaf
// nodes. This function is used to travel from a root
// node to a target node.
func (ub *unionbuilder) Order() int {
	panic("unreachable")
}

// SetOrder sets the order for the underlying routes.
func (ub *unionbuilder) SetOrder(int) {
	panic("unreachable")
}

// Primitve returns the underlying primitive.
func (ub *unionbuilder) Primitive() engine.Primitive {
	return ub.eunion
}

// Leftmost returns the leftmost route.
func (ub *unionbuilder) Leftmost() *route {
	panic("unreachable")
}

// Join joins the two builder objects. The outcome
// can be a new builder or a modified one.
func (ub *unionbuilder) Join(rhs builder, ajoin *sqlparser.JoinTableExpr) (builder, error) {
	return newJoin(ub, rhs, ajoin)
}

// SetRHS marks all routes under this node as RHS due
// to a left join. Such nodes have restrictions on what
// can be pushed into them. This should not propagate
// to subqueries.
func (ub *unionbuilder) SetRHS() {
	panic("unreachable")
}

// PushSelect pushes the select expression through the tree
// all the way to the route that colsym points to.
// PushSelect is similar to SupplyCol except that it always
// adds a new column, whereas SupplyCol can reuse an existing
// column. The function must return a colsym for the expression
// and the column number of the result.
func (ub *unionbuilder) PushSelect(expr *sqlparser.NonStarExpr, rb *route) (colsym *colsym, colnum int, err error) {
	panic("unreachable")
}

// PushOrderByNull pushes the special case ORDER By NULL to
// all routes. It's safe to push down this clause because it's
// just on optimization hint.
func (ub *unionbuilder) PushOrderByNull() {
	panic("unreachable")
}

// PushMisc pushes miscelleaneous constructs to all the routes.
// This should not propagate to subqueries.
func (ub *unionbuilder) PushMisc(sel *sqlparser.Select) {
	panic("unreachable")
}

// Wireup performs the wire-up work. Nodes should be traversed
// from right to left because the rhs nodes can request vars from
// the lhs nodes.
func (ub *unionbuilder) Wireup(bldr builder, jt *jointab) error {
	err := ub.Right.Wireup(bldr, jt)
	if err != nil {
		return err
	}
	return ub.Left.Wireup(bldr, jt)
}

// SupplyVar finds the common root between from and to. If it's
// the common root, it supplies the requested var to the rhs tree.
func (ub *unionbuilder) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("unreachable")
}

// SupplyCol will be used for the wire-up process. This function
// takes a column reference as input, changes the primitive
// to supply the requested column and returns the column number of
// the result for it. The request is passed down recursively
// as needed.
func (ub *unionbuilder) SupplyCol(ref colref) int{
	panic("unreachable")
}
