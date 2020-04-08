package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildSetPlan(sql string, stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	//keyValues, scope, err := sqlparser.ExtractSetValues(sql)
	//if err != nil {
	//	return nil, err
	//}
	//if scope == sqlparser.GlobalStr {
	//	return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
	//}
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
			Name:  expr.Name.Lowered(),
			Value: pv,
		})
	}
	//for key, value := range keyValues {
	//	switch key.Scope {
	//	case sqlparser.VariableStr:
	//		setOps = append(setOps, &engine.UserDefinedVariable{
	//			Value: sqltypes.PlanValue{
	//				Key:     key.Key,
	//				Value:   value,
	//				ListKey: "",
	//				Values:  nil,
	//			},
	//		})
	//	default:
	//		return nil, ErrPlanNotSupported
	//	}
	//}
	return &engine.Set{
		Ops: setOps,
	}, nil
}
