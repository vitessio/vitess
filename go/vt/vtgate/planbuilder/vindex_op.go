/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func transformVindexPlan(ctx *plancontext.PlanningContext, op *operators.Vindex) (logicalPlan, error) {
	single, ok := op.Vindex.(vindexes.SingleColumn)
	if !ok {
		return nil, vterrors.VT12001("multi-column vindexes not supported")
	}

	expr, err := evalengine.Translate(op.Value, &evalengine.Config{
		Collation: ctx.SemTable.Collation,
	})
	if err != nil {
		return nil, err
	}
	plan := &vindexFunc{
		order:   1,
		tableID: op.Solved,
		eVindexFunc: &engine.VindexFunc{
			Opcode: op.OpCode,
			Vindex: single,
			Value:  expr,
		},
	}

	for _, col := range op.Columns {
		_, err := plan.SupplyProjection(&sqlparser.AliasedExpr{
			Expr: col,
			As:   sqlparser.IdentifierCI{},
		}, false)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}
