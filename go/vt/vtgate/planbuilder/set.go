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

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var sysVarPlanningFunc = map[string]func(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error){}

func init() {
	sysVarPlanningFunc["default_storage_engine"] = buildSetOpIgnore
	sysVarPlanningFunc["sql_mode"] = buildSetOpCheckAndIgnore
	sysVarPlanningFunc["sql_safe_updates"] = buildSetOpVarSet
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
			} else {
				if err != sqlparser.ExprNotSupported {
					return nil, err
				}
				if !expressionOkToDelegateToTablet(expr.Expr) {
					return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "expression not supported for SET: %s", sqlparser.String(expr.Expr))
				}
				setOp = &engine.UserDefinedVariable{
					Name: expr.Name.Lowered(),
					Expr: &evalengine.Column{Offset: len(tabletExpressions)},
				}
				tabletExpressions = append(tabletExpressions, expr)
			}
			setOps = append(setOps, setOp)
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
		primitive, err := planTabletInput(vschema, tabletExpressions)
		if err != nil {
			return nil, err
		}
		input = primitive
	}

	return &engine.Set{
		Ops:   setOps,
		Input: input,
	}, nil
}

func planTabletInput(vschema ContextVSchema, tabletExpressions []*sqlparser.SetExpr) (engine.Primitive, error) {
	ks, dest, err := resolveDestination(vschema)
	if err != nil {
		return nil, err
	}

	var expr []string
	for _, e := range tabletExpressions {
		expr = append(expr, sqlparser.String(e.Expr))
	}
	query := fmt.Sprintf("select %s from dual", strings.Join(expr, ","))

	primitive := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
		IsDML:             false,
		SingleShardOnly:   true,
	}
	return primitive, nil
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
	keyspace, dest, err := resolveDestination(vschema)
	if err != nil {
		return nil, err
	}

	return &engine.SysVarCheckAndIgnore{
		Name:              expr.Name.Lowered(),
		Keyspace:          keyspace,
		TargetDestination: dest,
		Expr:              sqlparser.String(expr.Expr),
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
		}
		return true
	})
	return valid
}

func buildSetOpVarSet(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error) {
	ks, dest, err := resolveDestination(vschema)
	if err != nil {
		return nil, err
	}

	return &engine.SysVarSet{
		Name:              expr.Name.Lowered(),
		Keyspace:          ks,
		TargetDestination: dest,
		Expr:              sqlparser.String(expr.Expr),
	}, nil
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
