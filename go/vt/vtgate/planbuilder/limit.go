package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*limit)(nil)

// limit is the logicalPlan for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type limit struct {
	logicalPlanCommon
	elimit *engine.Limit
}

// newLimit builds a new limit.
func newLimit(plan logicalPlan) *limit {
	return &limit{
		logicalPlanCommon: newBuilderCommon(plan),
		elimit:            &engine.Limit{},
	}
}

// Primitive implements the logicalPlan interface
func (l *limit) Primitive() engine.Primitive {
	l.elimit.Input = l.input.Primitive()
	return l.elimit
}
