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
