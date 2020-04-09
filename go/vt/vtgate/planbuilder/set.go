package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildSetPlan(sql string, stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	var setOps []engine.SetOp

	for _, expr := range stmt.Exprs {
		switch expr.Name.AtCount() {
		case sqlparser.SingleAt:
		default:
			return nil, ErrPlanNotSupported
		}
		pv, err := sqlparser.NewPlanValue(expr.Expr)
		if err != nil {
			return nil, err
		}
		setOps = append(setOps, &engine.UserDefinedVariable{
			Name:      expr.Name.Lowered(),
			PlanValue: pv,
		})
	}
	return &engine.Set{
		Ops: setOps,
	}, nil
}
