/*
Copyright 2020 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildSetPlan(sql string, stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	var setOps []engine.SetOp
	var setOp engine.SetOp
	var err error

	if stmt.Scope == sqlparser.GlobalStr {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
	}

	for _, expr := range stmt.Exprs {
		switch expr.Name.AtCount() {
		case sqlparser.NoAt:
			planFunc, ok := sysVarPlanningFunc[expr.Name.Lowered()]
			if !ok {
				return nil, ErrPlanNotSupported
			}
			setOp, err = planFunc(expr)
			if err != nil {
				return nil, err
			}
		case sqlparser.SingleAt:
			pv, err := sqlparser.NewPlanValue(expr.Expr)
			if err != nil {
				return nil, err
			}
			setOp = &engine.UserDefinedVariable{
				Name:      expr.Name.Lowered(),
				PlanValue: pv,
			}
		default:
			return nil, ErrPlanNotSupported
		}
		setOps = append(setOps, setOp)
	}
	return &engine.Set{
		Ops: setOps,
	}, nil
}

var sysVarPlanningFunc = map[string]func(expr *sqlparser.SetExpr) (*engine.SysVarIgnore, error){}

func init() {
	sysVarPlanningFunc["debug"] = buildSetOpIgnore
}

func buildSetOpIgnore(expr *sqlparser.SetExpr) (*engine.SysVarIgnore, error) {
	pv, err := sqlparser.NewPlanValue(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &engine.SysVarIgnore{
		Name:      expr.Name.Lowered(),
		PlanValue: pv,
	}, nil
}
