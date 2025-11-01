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
	"regexp"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// regexParams checks that argument names are in the form v1, v2, v3...
var regexParams = regexp.MustCompile(`^v\d+`)

func prepareStmt(ctx context.Context, vschema plancontext.VSchema, pStmt *sqlparser.PrepareStmt) (*planResult, error) {
	stmtName := pStmt.Name.Lowered()
	vschema.ClearPrepareData(stmtName)

	var pQuery string
	var err error
	switch expr := pStmt.Statement.(type) {
	case *sqlparser.Literal:
		pQuery = expr.Val
	case *sqlparser.Variable:
		pQuery, err = fetchUDVValue(vschema, expr.Name.Lowered())
	case *sqlparser.Argument:
		udv, _ := strings.CutPrefix(expr.Name, sqlparser.UserDefinedVariableName)
		pQuery, err = fetchUDVValue(vschema, udv)
	default:
		return nil, vterrors.VT13002("prepare statement should not have : %T", pStmt.Statement)
	}
	if err != nil {
		return nil, err
	}

	plan, err := vschema.PlanPrepareStatement(ctx, pQuery)
	if err != nil {
		return nil, err
	}

	vschema.StorePrepareData(stmtName, &vtgatepb.PrepareData{
		PrepareStatement: plan.Original,
		ParamsCount:      int32(plan.ParamsCount),
	})

	return &planResult{
		primitive: engine.NewRowsPrimitive(nil, nil),
		tables:    plan.TablesUsed,
	}, nil
}

func fetchUDVValue(vschema plancontext.VSchema, udv string) (string, error) {
	bv := vschema.GetUDV(udv)
	if bv == nil {
		return "", vterrors.VT03024(udv)
	}
	val, err := sqltypes.BindVariableToValue(bv)
	if err != nil {
		return "", err
	}
	return val.ToString(), nil
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

func dropPreparedStatement(
	vschema plancontext.VSchema,
	stmt *sqlparser.DeallocateStmt,
) (*planResult, error) {
	stmtName := stmt.Name.Lowered()
	prepareData := vschema.GetPrepareData(stmtName)
	if prepareData == nil {
		return nil, vterrors.VT09011(stmtName, "DEALLOCATE PREPARE")
	}

	vschema.ClearPrepareData(stmtName)
	return &planResult{
		primitive: engine.NewRowsPrimitive(nil, nil),
	}, nil
}
