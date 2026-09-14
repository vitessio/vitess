/*
Copyright 2023 The Vitess Authors.

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
	"context"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func prepareStmt(pStmt *sqlparser.PrepareStmt) (*planResult, error) {
	prep := &engine.PrepareStmt{
		Name: pStmt.Name.Lowered(),
	}
	switch expr := pStmt.Statement.(type) {
	case *sqlparser.Literal:
		prep.Query = expr.Val
	case *sqlparser.Variable:
		prep.UserDefinedVariable = expr.Name.Lowered()
	default:
		return nil, vterrors.VT13002("prepare statement should not have : %T", pStmt.Statement)
	}

	return newPlanResult(prep), nil
}

func buildExecuteStmtPlan(ctx context.Context, vschema plancontext.VSchema, eStmt *sqlparser.ExecuteStmt) (*planResult, error) {
	stmtName := eStmt.Name.Lowered()
	prepareData := vschema.GetPrepareData(stmtName)
	if prepareData == nil {
		return nil, vterrors.VT09011(stmtName, "EXECUTE")
	}
	if int(prepareData.ParamsCount) != len(eStmt.Arguments) {
		return nil, vterrors.VT03025("EXECUTE")
	}

	plan, err := vschema.PlanPrepareStatement(ctx, prepareData.PrepareStatement)
	if err != nil {
		return nil, err
	}

	return &planResult{
		primitive: &engine.ExecStmt{
			Params: eStmt.Arguments,
			Input:  plan.Instructions,
		},
		tables: plan.TablesUsed,
	}, nil
}

func dropPreparedStatement(stmt *sqlparser.DeallocateStmt) (*planResult, error) {
	return newPlanResult(&engine.DeallocateStmt{
		Name: stmt.Name.Lowered(),
	}), nil
}
