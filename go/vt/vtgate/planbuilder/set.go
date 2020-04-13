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
	"fmt"

	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var sysVarPlanningFunc = map[string]func(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error){}

func init() {
	sysVarPlanningFunc["debug"] = buildSetOpIgnore
	sysVarPlanningFunc["sql_mode"] = buildSetOpCheckAndIgnore
}

func buildSetPlan(sql string, stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	var setOps []engine.SetOp
	var setOp engine.SetOp
	var err error

	if stmt.Scope == sqlparser.GlobalStr {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
	}

	for _, expr := range stmt.Exprs {
		switch expr.Name.AtCount() {
		case sqlparser.SingleAt:
			pv, err := sqlparser.NewPlanValue(expr.Expr)
			if err != nil {
				return nil, err
			}
			setOp = &engine.UserDefinedVariable{
				Name:      expr.Name.Lowered(),
				PlanValue: pv,
			}
		case sqlparser.DoubleAt:
			planFunc, ok := sysVarPlanningFunc[expr.Name.Lowered()]
			if !ok {
				return nil, ErrPlanNotSupported
			}
			setOp, err = planFunc(expr, vschema)
			if err != nil {
				return nil, err
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

func buildSetOpIgnore(expr *sqlparser.SetExpr, _ ContextVSchema) (engine.SetOp, error) {
	pv, err := sqlparser.NewPlanValue(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &engine.SysVarIgnore{
		Name:      expr.Name.Lowered(),
		PlanValue: pv,
	}, nil
}

func buildSetOpCheckAndIgnore(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error) {
	resolveExpr := true
	pv, err := sqlparser.NewPlanValue(expr.Expr)
	if err != nil {
		resolveExpr = false
	}
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}

	dest := vschema.Destination()
	if dest == nil {
		dest = key.DestinationAnyShard{}
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", expr.Expr)

	return &engine.SysVarCheckAndIgnore{
		Name:               expr.Name.Lowered(),
		Keyspace:           keyspace,
		TargetDestination:  dest,
		CurrentSysVarQuery: fmt.Sprintf("select @@%s from dual", expr.Name.Lowered()),
		ResolveExpr:        resolveExpr,
		PlanValue:          pv,
		NewSysVarQuery:     fmt.Sprintf("select %s from dual", buf.String()),
	}, nil
}
