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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
)

type (
	builtinJSONExtract struct {
		CallExpr
	}

	builtinJSONUnquote struct {
		CallExpr
	}

	builtinJSONObject struct {
		CallExpr
	}

	builtinJSONArray struct {
		CallExpr
	}

	builtinJSONDepth struct {
		CallExpr
	}

	builtinJSONLength struct {
		CallExpr
	}

	builtinJSONContainsPath struct {
		CallExpr
	}

	builtinJSONKeys struct {
		CallExpr
	}
)

var _ Expr = (*builtinJSONExtract)(nil)
var _ Expr = (*builtinJSONUnquote)(nil)
var _ Expr = (*builtinJSONObject)(nil)
var _ Expr = (*builtinJSONArray)(nil)
var _ Expr = (*builtinJSONDepth)(nil)
var _ Expr = (*builtinJSONLength)(nil)
var _ Expr = (*builtinJSONContainsPath)(nil)
var _ Expr = (*builtinJSONKeys)(nil)

var errInvalidPathForTransform = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "In this situation, path expressions may not contain the * and ** tokens or an array range.")

func (call *builtinJSONExtract) eval(env *ExpressionEnv) (eval, error) {
	args, err := call.args(env)
	if err != nil {
		return nil, err
	}

	if args[0] == nil {
		return nil, nil
	}

	doc, err := intoJSON(call.Method, args[0])
	if err != nil {
		return nil, err
	}

	return builtin_JSON_EXTRACT(doc, args[1:])
}

func builtin_JSON_EXTRACT(doc *json.Value, paths []eval) (eval, error) {
	matches := make([]*json.Value, 0, 4)
	multi := len(paths) > 1

	for _, p := range paths {
		if p == nil {
			return nil, nil
		}

		path, err := intoJSONPath(p)
		if err != nil {
			return nil, err
		}

		if path.ContainsWildcards() {
			multi = true
		}

		path.Match(doc, true, func(v *json.Value) {
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

func (call *builtinJSONExtract) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.TypeJSON, f
}

func (call *builtinJSONUnquote) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}
	j, err := intoJSON(call.Method, arg)
	if err != nil {
		return nil, err
	}
	if b, ok := j.StringBytes(); ok {
		return newEvalRaw(sqltypes.Blob, b, collationJSON), nil
	}
	return newEvalRaw(sqltypes.Blob, j.MarshalTo(nil), collationJSON), nil
}

func (call *builtinJSONUnquote) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Blob, f
}

var errJSONKeyIsNil = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "JSON documents may not contain NULL member names.")

func (call *builtinJSONObject) eval(env *ExpressionEnv) (eval, error) {
	j := json.NewObject()
	obj, _ := j.Object()

	for i := 0; i < len(call.Arguments); i += 2 {
		key, err := call.Arguments[i].eval(env)
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, errJSONKeyIsNil
		}
		key1, err := evalToVarchar(key, collations.CollationUtf8mb4ID, true)
		if err != nil {
			return nil, err
		}

		val, err := call.Arguments[i+1].eval(env)
		if err != nil {
			return nil, err
		}
		val1, err := argToJSON(val)
		if err != nil {
			return nil, err
		}

		obj.Set(key1.string(), val1, json.Set)
	}
	return j, nil
}

func (call *builtinJSONObject) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.TypeJSON, 0
}

func (call *builtinJSONArray) eval(env *ExpressionEnv) (eval, error) {
	ary := make([]*json.Value, 0, len(call.Arguments))
	for _, arg := range call.Arguments {
		arg, err := arg.eval(env)
		if err != nil {
			return nil, err
		}
		arg1, err := argToJSON(arg)
		if err != nil {
			return nil, err
		}
		ary = append(ary, arg1)
	}
	return json.NewArray(ary), nil
}

func (call *builtinJSONArray) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.TypeJSON, 0
}

func (call *builtinJSONDepth) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}
	j, err := intoJSON("JSON_DEPTH", arg)
	if err != nil {
		return nil, err
	}
	return newEvalInt64(int64(j.Depth())), nil
}

func (call *builtinJSONDepth) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f
}

func (call *builtinJSONLength) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.Arguments[0].eval(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	j, err := intoJSON("JSON_LENGTH", arg)
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
		jp, err := intoJSONPath(path)
		if err != nil {
			return nil, err
		}
		jp.Match(j, true, func(value *json.Value) {
			length += value.Len()
		})
	} else {
		length = j.Len()
	}

	return newEvalInt64(int64(length)), nil
}

func (call *builtinJSONLength) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f
}

func (call *builtinJSONContainsPath) eval(env *ExpressionEnv) (eval, error) {
	args, err := call.args(env)
	if err != nil {
		return nil, err
	}
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}

	doc, err := intoJSON("JSON_CONTAINS_PATH", args[0])
	if err != nil {
		return nil, err
	}

	match, err := intoOneOrAll("JSON_CONTAINS_PATH", evalToBinary(args[1]).string())
	if err != nil {
		return nil, err
	}

	for _, path := range args[2:] {
		jp, err := intoJSONPath(path)
		if err != nil {
			return nil, err
		}
		var matched bool
		jp.Match(doc, true, func(*json.Value) { matched = true })
		if matched && match == jsonMatchOne {
			return newEvalBool(true), nil
		}
		if !matched && match == jsonMatchAll {
			return newEvalBool(false), nil
		}
	}
	return newEvalBool(match == jsonMatchAll), nil
}

type jsonMatch int8

const (
	jsonMatchInvalid           = -1
	jsonMatchOne     jsonMatch = 0
	jsonMatchAll     jsonMatch = 1
)

func (jm jsonMatch) String() string {
	switch jm {
	case jsonMatchOne:
		return "one"
	case jsonMatchAll:
		return "all"
	default:
		return "<INVALID>"
	}
}

func intoOneOrAll(fname, value string) (jsonMatch, error) {
	switch value {
	case "one":
		return jsonMatchOne, nil
	case "all":
		return jsonMatchAll, nil
	default:
		return jsonMatchInvalid, errOneOrAll(fname)
	}
}

func errOneOrAll(fname string) error {
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "The oneOrAll argument to %s may take these values: 'one' or 'all'.", fname)
}

func (call *builtinJSONContainsPath) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f
}

func (call *builtinJSONKeys) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.Arguments[0].eval(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	j, err := intoJSON("JSON_KEYS", arg)
	if err != nil {
		return nil, err
	}

	var obj *json.Object
	if len(call.Arguments) == 2 {
		path, err := call.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}
		if path == nil {
			return nil, nil
		}
		jp, err := intoJSONPath(path)
		if err != nil {
			return nil, err
		}
		if jp.ContainsWildcards() {
			return nil, errInvalidPathForTransform
		}
		jp.Match(j, false, func(value *json.Value) {
			obj, _ = value.Object()
		})
	} else {
		obj, _ = j.Object()
	}
	if obj == nil {
		return nil, nil
	}

	var keys []*json.Value
	obj.Visit(func(key []byte, _ *json.Value) {
		keys = append(keys, json.NewString(key))
	})
	return json.NewArray(keys), nil
}

func (call *builtinJSONKeys) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.TypeJSON, f | flagNullable
}
