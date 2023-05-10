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
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

func (call *builtinJSONExtract) compile(c *compiler) (ctype, error) {
	doct, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	if slices2.All(call.Arguments[1:], func(expr Expr) bool { return expr.constant() }) {
		paths := make([]*json.Path, 0, len(call.Arguments[1:]))

		for _, arg := range call.Arguments[1:] {
			jp, err := c.jsonExtractPath(arg)
			if err != nil {
				return ctype{}, err
			}
			paths = append(paths, jp)
		}

		jt, err := c.compileParseJSON("JSON_EXTRACT", doct, 1)
		if err != nil {
			return ctype{}, err
		}

		c.asm.Fn_JSON_EXTRACT0(paths)
		return jt, nil
	}

	return ctype{}, c.unsupported(call)
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

func (call *builtinJSONUnquote) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	_, err = c.compileParseJSON("JSON_UNQUOTE", arg, 1)
	if err != nil {
		return ctype{}, err
	}

	c.asm.Fn_JSON_UNQUOTE()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Blob, Flag: flagNullable, Col: collationJSON}, nil
}

var errJSONKeyIsNil = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "JSON documents may not contain NULL member names.")

func (call *builtinJSONObject) eval(env *ExpressionEnv) (eval, error) {
	var obj json.Object
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
	return json.NewObject(obj), nil
}

func (call *builtinJSONObject) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.TypeJSON, 0
}

func (call *builtinJSONObject) compile(c *compiler) (ctype, error) {
	for i := 0; i < len(call.Arguments); i += 2 {
		key, err := call.Arguments[i].compile(c)
		if err != nil {
			return ctype{}, err
		}
		if err := c.compileToJSONKey(key); err != nil {
			return ctype{}, err
		}
		val, err := call.Arguments[i+1].compile(c)
		if err != nil {
			return ctype{}, err
		}
		_, err = c.compileArgToJSON(val, 1)
		if err != nil {
			return ctype{}, err
		}
	}
	c.asm.Fn_JSON_OBJECT(len(call.Arguments))
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
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

func (call *builtinJSONArray) compile(c *compiler) (ctype, error) {
	for _, arg := range call.Arguments {
		tt, err := arg.compile(c)
		if err != nil {
			return ctype{}, err
		}

		_, err = c.compileArgToJSON(tt, 1)
		if err != nil {
			return ctype{}, err
		}
	}
	c.asm.Fn_JSON_ARRAY(len(call.Arguments))
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
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

func (call *builtinJSONDepth) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(call)
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

func (call *builtinJSONLength) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(call)
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

func (call *builtinJSONContainsPath) compile(c *compiler) (ctype, error) {
	doct, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	if !call.Arguments[1].constant() {
		return ctype{}, c.unsupported(call)
	}

	if !slices2.All(call.Arguments[2:], func(expr Expr) bool { return expr.constant() }) {
		return ctype{}, c.unsupported(call)
	}

	match, err := c.jsonExtractOneOrAll("JSON_CONTAINS_PATH", call.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	paths := make([]*json.Path, 0, len(call.Arguments[2:]))

	for _, arg := range call.Arguments[2:] {
		jp, err := c.jsonExtractPath(arg)
		if err != nil {
			return ctype{}, err
		}
		paths = append(paths, jp)
	}

	_, err = c.compileParseJSON("JSON_CONTAINS_PATH", doct, 1)
	if err != nil {
		return ctype{}, err
	}

	c.asm.Fn_JSON_CONTAINS_PATH(match, paths)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
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
	obj.Visit(func(key string, _ *json.Value) {
		keys = append(keys, json.NewString(key))
	})
	return json.NewArray(keys), nil
}

func (call *builtinJSONKeys) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.TypeJSON, f | flagNullable
}

func (call *builtinJSONKeys) compile(c *compiler) (ctype, error) {
	doc, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	_, err = c.compileParseJSON("JSON_KEYS", doc, 1)
	if err != nil {
		return ctype{}, err
	}

	var jp *json.Path
	if len(call.Arguments) == 2 {
		jp, err = c.jsonExtractPath(call.Arguments[1])
		if err != nil {
			return ctype{}, err
		}
		if jp.ContainsWildcards() {
			return ctype{}, errInvalidPathForTransform
		}
	}

	c.asm.Fn_JSON_KEYS(jp)
	return ctype{Type: sqltypes.TypeJSON, Flag: flagNullable, Col: collationJSON}, nil
}
