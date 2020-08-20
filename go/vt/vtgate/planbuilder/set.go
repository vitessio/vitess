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
	var setOp engine.SetOp
	var err error

	ec := new(expressionConverter)

	for _, expr := range stmt.Exprs {
		switch expr.Scope {
		case sqlparser.GlobalStr:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported global scope in set: %s", sqlparser.String(expr))
			// AST struct has been prepared before getting here, so no scope here means that
			// we have a UDV. If the original query didn't explicitly specify the scope, it
			// would have been explictly set to sqlparser.SessionStr before reaching this
			// phase of planning
		case "":
			evalExpr, err := ec.convert(expr.Expr /*boolean*/, false /*identifierAsString*/, false)
			if err != nil {
				return nil, err
			}
			setOp = &engine.UserDefinedVariable{
				Name: expr.Name.Lowered(),
				Expr: evalExpr,
			}

			setOps = append(setOps, setOp)
		case sqlparser.SessionStr:
			planFunc, ok := sysVarPlanningFunc[expr.Name.Lowered()]
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported construct in set: %s", sqlparser.String(expr))
			}
			setOp, err = planFunc(expr, vschema, ec)
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

func buildNotSupported(setting) planFunc {
	return func(expr *sqlparser.SetExpr, schema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s: system setting is not supported", expr.Name)
	}
}

func buildSetOpIgnore(s setting) planFunc {
	return func(expr *sqlparser.SetExpr, vschema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		return &engine.SysVarIgnore{
			Name: expr.Name.Lowered(),
			Expr: extractValue(expr, s.boolean),
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

	return &engine.SysVarCheckAndIgnore{
		Name:              expr.Name.Lowered(),
		Keyspace:          keyspace,
		TargetDestination: dest,
		Expr:              extractValue(expr, boolean),
	}, nil
}

func expressionOkToDelegateToTablet(e sqlparser.Expr) bool {
	valid := true
	sqlparser.Rewrite(e, nil, func(cursor *sqlparser.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *sqlparser.Subquery, *sqlparser.TimestampFuncExpr, *sqlparser.CurTimeFuncExpr:
			valid = false
			return false
		case *sqlparser.FuncExpr:
			_, ok := validFuncs[n.Name.Lowered()]
			valid = ok
			return ok
		case *sqlparser.ColName:
			valid = n.Name.AtCount() == 2
			return false
		}
		return true
	})
	return valid
}

func buildSetOpVarSet(s setting) planFunc {
	return func(expr *sqlparser.SetExpr, vschema ContextVSchema, _ *expressionConverter) (engine.SetOp, error) {
		if !vschema.SysVarSetEnabled() {
			return planSysVarCheckIgnore(expr, vschema, s.boolean)
		}
		ks, err := vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}

		return &engine.SysVarSet{
			Name:              expr.Name.Lowered(),
			Keyspace:          ks,
			TargetDestination: vschema.Destination(),
			Expr:              extractValue(expr, s.boolean),
		}, nil
	}
}

func buildSetOpVitessAware(s setting) planFunc {
	return func(astExpr *sqlparser.SetExpr, vschema ContextVSchema, ec *expressionConverter) (engine.SetOp, error) {
		var err error
		var runtimeExpr evalengine.Expr

		_, isDefault := astExpr.Expr.(*sqlparser.Default)
		if isDefault {
			if s.defaultValue == nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "don't know default value for %s", astExpr.Name)
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

func extractValue(expr *sqlparser.SetExpr, boolean bool) string {
	switch node := expr.Expr.(type) {
	case *sqlparser.SQLVal:
		if node.Type == sqlparser.StrVal && boolean {
			switch strings.ToLower(string(node.Val)) {
			case "on":
				return "1"
			case "off":
				return "0"
			}
		}
	case *sqlparser.ColName:
		// this is a little of a hack. it's used when the setting is not a normal expression, but rather
		// an enumeration, such as utf8, utf8mb4, etc
		if node.Name.AtCount() == sqlparser.NoAt {
			switch node.Name.Lowered() {
			case "on":
				return "1"
			case "off":
				return "0"
			}
			return fmt.Sprintf("'%s'", sqlparser.String(expr.Expr))
		}
	}

	return sqlparser.String(expr.Expr)
}

// whitelist of functions knows to be safe to pass through to mysql for evaluation
// this list tries to not include functions that might return different results on different tablets
var validFuncs = map[string]interface{}{
	"if":               nil,
	"ifnull":           nil,
	"nullif":           nil,
	"abs":              nil,
	"acos":             nil,
	"asin":             nil,
	"atan2":            nil,
	"atan":             nil,
	"ceil":             nil,
	"ceiling":          nil,
	"conv":             nil,
	"cos":              nil,
	"cot":              nil,
	"crc32":            nil,
	"degrees":          nil,
	"div":              nil,
	"exp":              nil,
	"floor":            nil,
	"ln":               nil,
	"log":              nil,
	"log10":            nil,
	"log2":             nil,
	"mod":              nil,
	"pi":               nil,
	"pow":              nil,
	"power":            nil,
	"radians":          nil,
	"rand":             nil,
	"round":            nil,
	"sign":             nil,
	"sin":              nil,
	"sqrt":             nil,
	"tan":              nil,
	"truncate":         nil,
	"adddate":          nil,
	"addtime":          nil,
	"convert_tz":       nil,
	"date":             nil,
	"date_add":         nil,
	"date_format":      nil,
	"date_sub":         nil,
	"datediff":         nil,
	"day":              nil,
	"dayname":          nil,
	"dayofmonth":       nil,
	"dayofweek":        nil,
	"dayofyear":        nil,
	"extract":          nil,
	"from_days":        nil,
	"from_unixtime":    nil,
	"get_format":       nil,
	"hour":             nil,
	"last_day":         nil,
	"makedate":         nil,
	"maketime":         nil,
	"microsecond":      nil,
	"minute":           nil,
	"month":            nil,
	"monthname":        nil,
	"period_add":       nil,
	"period_diff":      nil,
	"quarter":          nil,
	"sec_to_time":      nil,
	"second":           nil,
	"str_to_date":      nil,
	"subdate":          nil,
	"subtime":          nil,
	"time_format":      nil,
	"time_to_sec":      nil,
	"timediff":         nil,
	"timestampadd":     nil,
	"timestampdiff":    nil,
	"to_days":          nil,
	"to_seconds":       nil,
	"week":             nil,
	"weekday":          nil,
	"weekofyear":       nil,
	"year":             nil,
	"yearweek":         nil,
	"ascii":            nil,
	"bin":              nil,
	"bit_length":       nil,
	"char":             nil,
	"char_length":      nil,
	"character_length": nil,
	"concat":           nil,
	"concat_ws":        nil,
	"elt":              nil,
	"export_set":       nil,
	"field":            nil,
	"find_in_set":      nil,
	"format":           nil,
	"from_base64":      nil,
	"hex":              nil,
	"insert":           nil,
	"instr":            nil,
	"lcase":            nil,
	"left":             nil,
	"length":           nil,
	"load_file":        nil,
	"locate":           nil,
	"lower":            nil,
	"lpad":             nil,
	"ltrim":            nil,
	"make_set":         nil,
	"mid":              nil,
	"oct":              nil,
	"octet_length":     nil,
	"ord":              nil,
	"position":         nil,
	"quote":            nil,
	"repeat":           nil,
	"replace":          nil,
	"reverse":          nil,
	"right":            nil,
	"rpad":             nil,
	"rtrim":            nil,
	"soundex":          nil,
	"space":            nil,
	"strcmp":           nil,
	"substr":           nil,
	"substring":        nil,
	"substring_index":  nil,
	"to_base64":        nil,
	"trim":             nil,
	"ucase":            nil,
	"unhex":            nil,
	"upper":            nil,
	"weight_string":    nil,
}
