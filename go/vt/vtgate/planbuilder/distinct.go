package planbuilder

import (
	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*distinct)(nil)

// distinct is the logicalPlan for engine.Distinct.
type distinct struct {
	logicalPlanCommon
	ColCollations []collations.ID
}

func newDistinct(source logicalPlan, colCollations []collations.ID) logicalPlan {
	return &distinct{
		logicalPlanCommon: newBuilderCommon(source),
		ColCollations:     colCollations,
	}
}

func (d *distinct) Primitive() engine.Primitive {
	return &engine.Distinct{
		Source:        d.input.Primitive(),
		ColCollations: d.ColCollations,
	}
}

// Rewrite implements the logicalPlan interface
func (d *distinct) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "distinct: wrong number of inputs")
	}
	d.input = inputs[0]
	return nil
}

// Inputs implements the logicalPlan interface
func (d *distinct) Inputs() []logicalPlan {
	return []logicalPlan{d.input}
}
