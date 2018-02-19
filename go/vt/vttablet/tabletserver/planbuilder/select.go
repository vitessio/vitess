/*
Copyright 2018 Google Inc.

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
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func analyzeSelect(sel *sqlparser.Select, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:     PlanPassSelect,
		FieldQuery: GenerateFieldQuery(sel),
		FullQuery:  GenerateLimitQuery(sel),
	}
	if sel.Lock != "" {
		plan.PlanID = PlanSelectLock
	}

	tableName := analyzeFrom(sel.From)
	if tableName.IsEmpty() {
		if err := verifyNoGetLock(sel); err != nil {
			return nil, err
		}
		return plan, nil
	}
	if _, err := plan.setTable(tableName, tables); err != nil {
		return nil, err
	}

	if altPlan, err := analyzeNextValue(sel, plan); altPlan != nil || err != nil {
		return altPlan, err
	}
	if altPlan, err := analyzeGetLock(sel, plan); altPlan != nil || err != nil {
		return altPlan, err
	}
	return plan, nil
}

func analyzeNextValue(sel *sqlparser.Select, plan *Plan) (*Plan, error) {
	nextVal, ok := sel.SelectExprs[0].(sqlparser.Nextval)
	if !ok {
		return nil, nil
	}

	if plan.Table.Type != schema.Sequence {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v is not a sequence", plan.Table.Name)
	}
	plan.PlanID = PlanNextval
	v, err := sqlparser.NewPlanValue(nextVal.Expr)
	if err != nil {
		return nil, err
	}
	plan.PKValues = []sqltypes.PlanValue{v}
	plan.FieldQuery = nil
	plan.FullQuery = nil
	return plan, nil
}

func analyzeGetLock(sel *sqlparser.Select, plan *Plan) (*Plan, error) {
	if plan.Table.Name.String() != "dual" {
		return nil, verifyNoGetLock(sel)
	}
	if len(sel.SelectExprs) != 1 {
		return nil, verifyNoGetLock(sel)
	}
	aliased, ok := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, verifyNoGetLock(sel)
	}
	fun, ok := aliased.Expr.(*sqlparser.FuncExpr)
	if !ok {
		return nil, verifyNoGetLock(sel)
	}
	if fun.Name.EqualString("get_lock") {
		plan.PlanID = PlanGetLock
	}
	if fun.Name.EqualString("release_lock") {
		plan.PlanID = PlanReleaseLock
	}
	var err error
	if plan.PlanID == PlanGetLock || plan.PlanID == PlanReleaseLock {
		plan.PKValues, err = extractFuncValues(fun)
		if err != nil {
			return nil, err
		}
		return plan, nil
	}

	return nil, verifyNoGetLock(sel)
}

func verifyNoGetLock(sel *sqlparser.Select) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.FuncExpr:
			if node.Name.EqualString("get_lock") {
				return false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "GET_LOCK can only be used in a simple SELECT")
			}
			if node.Name.EqualString("release_lock") {
				return false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "RELEASE_LOCK can only be used in a simple SELECT")
			}
		}
		return true, nil
	}, sel)
}

func extractFuncValues(fun *sqlparser.FuncExpr) ([]sqltypes.PlanValue, error) {
	var pvs []sqltypes.PlanValue
	for _, selExpr := range fun.Exprs {
		aliased, ok := selExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid syntax for GET_LOCK or RELEASE_LOCK")
		}
		if !sqlparser.IsValue(aliased.Expr) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "GET_LOCK or RELEASE_LOCK require simple value params")
		}
		pv, err := sqlparser.NewPlanValue(aliased.Expr)
		if err != nil {
			return nil, err
		}
		pvs = append(pvs, pv)
	}
	return pvs, nil
}
