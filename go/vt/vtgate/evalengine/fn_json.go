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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
)

type (
	builtinJsonExtract struct {
		CallExpr
	}

	builtinJsonUnquote struct {
		CallExpr
	}
)

var _ Expr = (*builtinJsonExtract)(nil)
var _ Expr = (*builtinJsonUnquote)(nil)

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
	var multi bool

	for _, p := range args[1:] {
		path, err := intoJsonPath(p)
		if err != nil {
			return nil, err
		}

		if path.Multi() {
			multi = true
		}

		path.Match(doc, func(v *json.Value) {
			matches = append(matches, v)
		})
	}

	switch len(matches) {
	case 0:
		return nil, nil
	case 1:
		if !multi {
			return (*evalJson)(matches[0]), nil
		}
		fallthrough
	default:
		return (*evalJson)(json.NewArray(matches)), nil
	}
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
	if bytes, ok := j.StringBytes(); ok {
		return newEvalRaw(sqltypes.Blob, bytes, collationJSON), nil
	}
	return newEvalRaw(sqltypes.Blob, j.MarshalTo(nil), collationJSON), nil
}

func (call *builtinJsonUnquote) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Blob, f
}
