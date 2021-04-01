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
	"strings"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type (
	planFunc = func(expr *sqlparser.SetExpr, vschema ContextVSchema, ec *expressionConverter) (engine.SetOp, error)

	setting struct {
		name         string
		boolean      bool
		defaultValue evalengine.Expr

		// this allows identifiers (a.k.a. ColName) from the AST to be handled as if they are strings.
		// SET transaction_mode = two_pc => SET transaction_mode = 'two_pc'
		identifierAsString bool
	}
)

var sysVarPlanningFunc = map[string]planFunc{}

func buildSetPlan(stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	var setOps []engine.SetOp
	var err error

	ec := new(expressionConverter)

	for _, expr := range stmt.Exprs {
		// AST struct has been prepared before getting here, so no scope here means that
		// we have a UDV. If the original query didn't explicitly specify the scope, it
		// would have been explictly set to sqlparser.SessionStr before reaching this
		// phase of planning
		switch expr.Scope {
		case sqlparser.GlobalScope:
			setOp, err := planSysVarCheckIgnore(expr, vschema, true)
			if err != nil {
				return nil, err
			}
			setOps = append(setOps, setOp)
		case sqlparser.ImplicitScope:
			evalExpr, err := ec.convert(expr.Expr /*boolean*/, false /*identifierAsString*/, false)
			if err != nil {
				return nil, err
			}
			setOp := &engine.UserDefinedVariable{
				Name: expr.Name.Lowered(),
				Expr: evalExpr,
			}
			setOps = append(setOps, setOp)
		case sqlparser.SessionScope:
			planFunc, ok := sysVarPlanningFunc[expr.Name.Lowered()]
			if !ok {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.UnknownSystemVariable, "Unknown system variable '%s'", sqlparser.String(expr))
			}
			setOp, err := planFunc(expr, vschema, ec)
			if err != nil {
				return nil, err
			}
			setOps = append(setOps, setOp)
		default:
			return nil, ErrPlanNotSupported
		}
	}

	input, err := ec.source(vschema)
	if err != nil {
		return nil, err
	}

	return &engine.Set{
		Ops:   setOps,
		Input: input,
	}, nil
}

func buildSetOpReadOnly(s setting) planFunc {
	return func(expr *sqlparser.SetExpr, schema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.IncorrectGlobalLocalVar, "Variable '%s' is a read only variable", expr.Name)
	}
}

func buildNotSupported(setting) planFunc {
	return func(expr *sqlparser.SetExpr, schema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s: system setting is not supported", expr.Name)
	}
}

func buildSetOpIgnore(s setting) planFunc {
	return func(expr *sqlparser.SetExpr, vschema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		value, err := extractValue(expr, s.boolean)
		if err != nil {
			return nil, err
		}
		return &engine.SysVarIgnore{
			Name: expr.Name.Lowered(),
			Expr: value,
		}, nil
	}
}

func buildSetOpCheckAndIgnore(s setting) planFunc {
	return func(expr *sqlparser.SetExpr, schema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		return planSysVarCheckIgnore(expr, schema, s.boolean)
	}
}

func planSysVarCheckIgnore(expr *sqlparser.SetExpr, schema ContextVSchema, boolean bool) (engine.SetOp, error) {
	keyspace, dest, err := resolveDestination(schema)
	if err != nil {
		return nil, err
	}
	value, err := extractValue(expr, boolean)
	if err != nil {
		return nil, err
	}

	return &engine.SysVarCheckAndIgnore{
		Name:              expr.Name.Lowered(),
		Keyspace:          keyspace,
		TargetDestination: dest,
		Expr:              value,
	}, nil
}

func buildSetOpReservedConn(s setting) planFunc {
	return func(expr *sqlparser.SetExpr, vschema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		if !vschema.SysVarSetEnabled() {
			return planSysVarCheckIgnore(expr, vschema, s.boolean)
		}
		ks, err := vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
		value, err := extractValue(expr, s.boolean)
		if err != nil {
			return nil, err
		}

		return &engine.SysVarReservedConn{
			Name:              expr.Name.Lowered(),
			Keyspace:          ks,
			TargetDestination: vschema.Destination(),
			Expr:              value,
		}, nil
	}
}

const defaultNotSupportedErrFmt = "DEFAULT not supported for @@%s"

func buildSetOpVitessAware(s setting) planFunc {
	return func(astExpr *sqlparser.SetExpr, vschema ContextVSchema, ec *expressionConverter) (engine.SetOp, error) {
		var err error
		var runtimeExpr evalengine.Expr

		_, isDefault := astExpr.Expr.(*sqlparser.Default)
		if isDefault {
			if s.defaultValue == nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, defaultNotSupportedErrFmt, astExpr.Name)
			}
			runtimeExpr = s.defaultValue
		} else {
			runtimeExpr, err = ec.convert(astExpr.Expr, s.boolean, s.identifierAsString)
			if err != nil {
				return nil, err
			}
		}

		return &engine.SysVarSetAware{
			Name: astExpr.Name.Lowered(),
			Expr: runtimeExpr,
		}, nil
	}
}

func resolveDestination(vschema ContextVSchema) (*vindexes.Keyspace, key.Destination, error) {
	keyspace, err := vschema.AnyKeyspace()
	if err != nil {
		return nil, nil, err
	}

	dest := vschema.Destination()
	if dest == nil {
		dest = key.DestinationAnyShard{}
	}
	return keyspace, dest, nil
}

func extractValue(expr *sqlparser.SetExpr, boolean bool) (string, error) {
	switch node := expr.Expr.(type) {
	case *sqlparser.Literal:
		if node.Type == sqlparser.StrVal && boolean {
			switch strings.ToLower(string(node.Val)) {
			case "on":
				return "1", nil
			case "off":
				return "0", nil
			}
		}
	case *sqlparser.ColName:
		// this is a little of a hack. it's used when the setting is not a normal expression, but rather
		// an enumeration, such as utf8, utf8mb4, etc
		if node.Name.AtCount() == sqlparser.NoAt {
			switch node.Name.Lowered() {
			case "on":
				return "1", nil
			case "off":
				return "0", nil
			}
			return fmt.Sprintf("'%s'", sqlparser.String(expr.Expr)), nil
		}
	case *sqlparser.Default:
		return "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, defaultNotSupportedErrFmt, expr.Name)
	}

	return sqlparser.String(expr.Expr), nil
}
