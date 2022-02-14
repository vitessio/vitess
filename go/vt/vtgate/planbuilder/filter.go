package planbuilder

import (
	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// filter is the logicalPlan for engine.Filter.
	filter struct {
		logicalPlanCommon
		efilter *engine.Filter
	}

	simpleConverterLookup struct {
		ctx               *plancontext.PlanningContext
		plan              logicalPlan
		canPushProjection bool
	}
)

var _ logicalPlan = (*filter)(nil)
var _ evalengine.ConverterLookup = (*simpleConverterLookup)(nil)

func (s *simpleConverterLookup) ColumnLookup(col *sqlparser.ColName) (int, error) {
	offset, added, err := pushProjection(s.ctx, &sqlparser.AliasedExpr{Expr: col}, s.plan, true, true, false)
	if err != nil {
		return 0, err
	}
	if added && !s.canPushProjection {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column should not be pushed to projection while doing a column lookup")
	}
	return offset, nil
}

func (s *simpleConverterLookup) CollationForExpr(expr sqlparser.Expr) collations.ID {
	return s.ctx.SemTable.CollationForExpr(expr)
}

func (s *simpleConverterLookup) DefaultCollation() collations.ID {
	return s.ctx.SemTable.Collation
}

// newFilter builds a new filter.
func newFilter(ctx *plancontext.PlanningContext, plan logicalPlan, expr sqlparser.Expr) (*filter, error) {
	scl := &simpleConverterLookup{
		ctx:  ctx,
		plan: plan,
	}
	predicate, err := evalengine.Convert(expr, scl)
	if err != nil {
		return nil, err
	}
	return &filter{
		logicalPlanCommon: newBuilderCommon(plan),
		efilter: &engine.Filter{
			Predicate:    predicate,
			ASTPredicate: expr,
		},
	}, nil
}

// Primitive implements the logicalPlan interface
func (l *filter) Primitive() engine.Primitive {
	l.efilter.Input = l.input.Primitive()
	return l.efilter
}
