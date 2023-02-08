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

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
)

type (
	builtinJsonExtract struct {
		CallExpr
	}

	builtinJsonUnquote struct {
		CallExpr
	}

	builtinJsonObject struct {
		CallExpr
	}

	builtinJsonArray struct {
		CallExpr
	}

	builtinJsonDepth struct {
		CallExpr
	}

	builtinJsonLength struct {
		CallExpr
	}

	builtinJsonContainsPath struct {
		CallExpr
	}
)

var _ Expr = (*builtinJsonExtract)(nil)
var _ Expr = (*builtinJsonUnquote)(nil)
var _ Expr = (*builtinJsonObject)(nil)
var _ Expr = (*builtinJsonArray)(nil)
var _ Expr = (*builtinJsonDepth)(nil)
var _ Expr = (*builtinJsonLength)(nil)
var _ Expr = (*builtinJsonContainsPath)(nil)

var errInvalidPathForTransform = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "In this situation, path expressions may not contain the * and ** tokens or an array range.")

func (call *builtinJsonExtract) eval(env *ExpressionEnv) (eval, error) {
	args, err := call.args(env)
	if err != nil {
		return nil, err
	}

	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}

	doc, err := intoJson(call.Method, args[0])
	if err != nil {
		return nil, err
	}

	var matches = make([]*json.Value, 0, 4)
	var multi = len(args) > 2

	for _, p := range args[1:] {
		path, err := intoJsonPath(p)
		if err != nil {
			return nil, err
		}

		if path.ContainsWildcards() {
			multi = true
		}

		path.Match(doc, func(v *json.Value) {
			matches = append(matches, v)
		})
	}

	if len(matches) == 0 {
		return nil, nil
	}
	if len(matches) == 1 && !multi {
		return matches[0], nil
	}
	return json.NewArray(matches), nil
}

func (call *builtinJsonExtract) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.TypeJSON, f
}

func (call *builtinJsonUnquote) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}
	j, err := intoJson(call.Method, arg)
	if err != nil {
		return nil, err
	}
	if b, ok := j.StringBytes(); ok {
		return newEvalRaw(sqltypes.Blob, b, collationJSON), nil
	}
	return newEvalRaw(sqltypes.Blob, j.MarshalTo(nil), collationJSON), nil
}

func (call *builtinJsonUnquote) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Blob, f
}

func (call *builtinJsonObject) eval(env *ExpressionEnv) (eval, error) {
	j := json.NewObject()
	obj, _ := j.Object()

	for i := 0; i < len(call.Arguments); i += 2 {
		key, err := call.Arguments[i].eval(env)
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "JSON documents may not contain NULL member names.")
		}
		val, err := call.Arguments[i+1].eval(env)
		if err != nil {
			return nil, err
		}

		key1, err := evalToVarchar(key, collations.CollationUtf8mb4ID, true)
		if err != nil {
			return nil, err
		}
		val1, err := evalToJson(val)
		if err != nil {
			return nil, err
		}

		obj.Set(key1.string(), val1, json.Set)
	}
	return j, nil
}

func (call *builtinJsonObject) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	return sqltypes.TypeJSON, 0
}

func (call *builtinJsonArray) eval(env *ExpressionEnv) (eval, error) {
	ary := make([]*json.Value, 0, len(call.Arguments))
	for _, arg := range call.Arguments {
		arg, err := arg.eval(env)
		if err != nil {
			return nil, err
		}
		arg1, err := evalToJson(arg)
		if err != nil {
			return nil, err
		}
		ary = append(ary, arg1)
	}
	return json.NewArray(ary), nil
}

func (call *builtinJsonArray) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	return sqltypes.TypeJSON, 0
}

func (call *builtinJsonDepth) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}
	j, err := intoJson("JSON_DEPTH", arg)
	if err != nil {
		return nil, err
	}
	return newEvalInt64(int64(j.Depth())), nil
}

func (call *builtinJsonDepth) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
}

func (call *builtinJsonLength) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.Arguments[0].eval(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	j, err := intoJson("JSON_LENGTH", arg)
	if err != nil {
		return nil, err
	}

	var length int

	if len(call.Arguments) == 2 {
		path, err := call.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}
		if path == nil {
			return nil, nil
		}
		jp, err := intoJsonPath(path)
		if err != nil {
			return nil, err
		}
		jp.Match(j, func(value *json.Value) {
			length += value.Len()
		})
	} else {
		length = j.Len()
	}

	return newEvalInt64(int64(length)), nil
}

func (call *builtinJsonLength) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
}

func (call *builtinJsonContainsPath) eval(env *ExpressionEnv) (eval, error) {
	args, err := call.args(env)
	if err != nil {
		return nil, err
	}
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}

	doc, err := intoJson("JSON_CONTAINS_PATH", args[0])
	if err != nil {
		return nil, err
	}

	all, err := intoOneOrAll("JSON_CONTAINS_PATH", args[1])
	if err != nil {
		return nil, err
	}

	for _, path := range args[2:] {
		jp, err := intoJsonPath(path)
		if err != nil {
			return nil, err
		}
		var matched bool
		jp.Match(doc, func(*json.Value) { matched = true })
		if matched && !all {
			return newEvalBool(true), nil
		}
		if !matched && all {
			return newEvalBool(false), nil
		}
	}
	return newEvalBool(all), nil
}

func intoOneOrAll(fname string, e eval) (bool, error) {
	switch evalToBinary(e).string() {
	case "one":
		return false, nil
	case "all":
		return true, nil
	default:
		return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "The oneOrAll argument to %s may take these values: 'one' or 'all'.", fname)
	}
}

func (call *builtinJsonContainsPath) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
}
