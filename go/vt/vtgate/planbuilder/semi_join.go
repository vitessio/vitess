package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*semiJoin)(nil)

// semiJoin is the logicalPlan for engine.SemiJoin.
// This gets built if a rhs is correlated and can
// be pulled out but requires some variables to be supplied from outside.
type semiJoin struct {
	gen4Plan
	rhs  logicalPlan
	lhs  logicalPlan
	vars map[string]int
	cols []int
}

// newSemiJoin builds a new semiJoin.
func newSemiJoin(lhs, rhs logicalPlan, vars map[string]int) *semiJoin {
	return &semiJoin{
		rhs:  rhs,
		lhs:  lhs,
		vars: vars,
	}
}

// Primitive implements the logicalPlan interface
func (ps *semiJoin) Primitive() engine.Primitive {
	return &engine.SemiJoin{
		Left:  ps.lhs.Primitive(),
		Right: ps.rhs.Primitive(),
		Vars:  ps.vars,
		Cols:  ps.cols,
	}
}

// WireupGen4 implements the logicalPlan interface
func (ps *semiJoin) WireupGen4(semTable *semantics.SemTable) error {
	if err := ps.lhs.WireupGen4(semTable); err != nil {
		return err
	}
	return ps.rhs.WireupGen4(semTable)
}

// Rewrite implements the logicalPlan interface
func (ps *semiJoin) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "semiJoin: wrong number of inputs")
	}
	ps.lhs = inputs[0]
	ps.rhs = inputs[1]
	return nil
}

// ContainsTables implements the logicalPlan interface
func (ps *semiJoin) ContainsTables() semantics.TableSet {
	return ps.lhs.ContainsTables().Merge(ps.rhs.ContainsTables())
}

// Inputs implements the logicalPlan interface
func (ps *semiJoin) Inputs() []logicalPlan {
	return []logicalPlan{ps.lhs, ps.rhs}
}

// OutputColumns implements the logicalPlan interface
func (ps *semiJoin) OutputColumns() []sqlparser.SelectExpr {
	return ps.lhs.OutputColumns()
}
