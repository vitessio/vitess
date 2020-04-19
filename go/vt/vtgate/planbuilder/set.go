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

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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

func buildSetPlan(stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	var setOps []engine.SetOp
	var setOp engine.SetOp
	var err error

	var tabletExpressions []*sqlparser.SetExpr
	for _, expr := range stmt.Exprs {
		switch expr.Scope {
		case sqlparser.GlobalStr:
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
		case "":
			exp, err := sqlparser.Convert(expr.Expr)
			if err == nil {
				setOp = &engine.UserDefinedVariable{
					Name: expr.Name.Lowered(),
					Expr: exp,
				}
				setOps = append(setOps, setOp)
			} else {
				if err == sqlparser.ExprNotSupported {
					tabletExpressions = append(tabletExpressions, expr)
				} else {
					return nil, err
				}
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
			setOps = append(setOps, setOp)
		default:
			return nil, ErrPlanNotSupported
		}
	}

	var input engine.Primitive
	if len(tabletExpressions) == 0 {
		input = &engine.SingleRow{}
	} else {
		newSetOpts, primitive, err := planTabletInput(vschema, tabletExpressions)
		if err != nil {
			return nil, err
		}
		input = primitive
		setOps = append(setOps, newSetOpts...)
	}

	return &engine.Set{
		Ops:   setOps,
		Input: input,
	}, nil
}

func planTabletInput(vschema ContextVSchema, tabletExpressions []*sqlparser.SetExpr) ([]engine.SetOp, engine.Primitive, error) {
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, nil, err
	}

	dest := vschema.Destination()
	if dest == nil {
		dest = key.DestinationAnyShard{}
	}

	var setOps []engine.SetOp
	query := ""
	for i, e := range tabletExpressions {
		setOp := &engine.UserDefinedVariable{
			Name: e.Name.Lowered(),
			Expr: &evalengine.Column{Offset: i},
		}

		setOps = append(setOps, setOp)
		if query == "" {
			query = "select "
		} else {
			query += ", "
		}
		query += sqlparser.String(e.Expr)
	}

	primitive := &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: dest,
		Query:             query,
		IsDML:             false,
		SingleShardOnly:   true,
	}
	return setOps, primitive, nil
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

	return &engine.SysVarCheckAndIgnore{
		Name:              expr.Name.Lowered(),
		Keyspace:          keyspace,
		TargetDestination: dest,
		Expr:              sqlparser.String(expr.Expr),
	}, nil
}
