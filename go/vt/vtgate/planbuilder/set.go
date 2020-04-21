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
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var sysVarPlanningFunc = map[string]func(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error){}

func init() {
	sysVarPlanningFunc["default_storage_engine"] = buildSetOpIgnore
	sysVarPlanningFunc["sql_mode"] = buildSetOpCheckAndIgnore
}

func buildSetPlan(sql string, stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	var setOps []engine.SetOp
	var setOp engine.SetOp
	var err error

	for _, expr := range stmt.Exprs {
		switch expr.Scope {
		case sqlparser.GlobalStr:
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
		case "":
			pv, err := sqlparser.NewPlanValue(expr.Expr)
			if err != nil {
				return nil, err
			}
			setOp = &engine.UserDefinedVariable{
				Name:      expr.Name.Lowered(),
				PlanValue: pv,
			}
		case sqlparser.SessionStr:
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
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", expr.Expr)

	return &engine.SysVarIgnore{
		Name: expr.Name.Lowered(),
		Expr: buf.String(),
	}, nil
}

func buildSetOpCheckAndIgnore(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error) {
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		//TODO: Record warning for switching plan construct.
		if strings.HasPrefix(err.Error(), "no keyspace in database name specified") {
			return buildSetOpIgnore(expr, vschema)
		}
		return nil, err
	}

	dest := vschema.Destination()
	if dest == nil {
		dest = key.DestinationAnyShard{}
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", expr.Expr)

	return &engine.SysVarCheckAndIgnore{
		Name:              expr.Name.Lowered(),
		Keyspace:          keyspace,
		TargetDestination: dest,
		Expr:              buf.String(),
	}, nil
}
