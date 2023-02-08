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
	"bytes"
	"encoding/base64"

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
)

var _ Expr = (*builtinJsonExtract)(nil)
var _ Expr = (*builtinJsonUnquote)(nil)
var _ Expr = (*builtinJsonObject)(nil)
var _ Expr = (*builtinJsonArray)(nil)
var _ Expr = (*builtinJsonDepth)(nil)
var _ Expr = (*builtinJsonLength)(nil)

func evalBinaryToJson(e *evalBytes) *evalJson {
	const prefix = "base64:type15:"

	dst := make([]byte, len(prefix)+mysqlBase64.EncodedLen(len(e.bytes)))
	copy(dst, prefix)
	base64.StdEncoding.Encode(dst[len(prefix):], e.bytes)
	return (*evalJson)(json.NewString(dst))
}

func evalToJson(e eval) (*evalJson, error) {
	switch e := e.(type) {
	case nil:
		return (*evalJson)(json.ValueNull), nil
	case *evalJson:
		return e, nil
	case *evalFloat:
		f := e.toRawBytes()
		if bytes.IndexByte(f, '.') < 0 {
			f = append(f, '.', '0')
		}
		return (*evalJson)(json.NewNumber(f)), nil
	case evalNumeric:
		if e == evalBoolTrue {
			return (*evalJson)(json.ValueTrue), nil
		}
		if e == evalBoolFalse {
			return (*evalJson)(json.ValueFalse), nil
		}
		return (*evalJson)(json.NewNumber(e.toRawBytes())), nil
	case *evalBytes:
		if sqltypes.IsBinary(e.sqlType()) {
			return evalBinaryToJson(e), nil
		}

		jsonText, err := collations.ConvertForJSON(nil, e.bytes, collations.Local().LookupByID(e.col.Collation))
		if err != nil {
			return nil, err
		}

		var p json.Parser
		j, err := p.ParseBytes(jsonText)
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Invalid JSON text in argument 1 to function cast_as_json")
		}
		return (*evalJson)(j), nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", e.sqlType())
	}
}

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

		obj.Set(key1.string(), (*json.Value)(val1), json.Set)
	}
	return (*evalJson)(j), nil
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
		ary = append(ary, (*json.Value)(arg1))
	}
	return (*evalJson)(json.NewArray(ary)), nil
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
