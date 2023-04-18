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
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func isEncodingJSONSafe(col collations.ID) bool {
	switch col.Get().Charset().(type) {
	case charset.Charset_utf8mb4, charset.Charset_utf8mb3, charset.Charset_binary:
		return true
	default:
		return false
	}
}

func (c *compiler) compileParseJSON(fn string, doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
	case sqltypes.VarChar, sqltypes.VarBinary:
		c.asm.Parse_j(offset)
	default:
		return ctype{}, errJSONType(fn)
	}
	return ctype{Type: sqltypes.TypeJSON, Flag: doct.Flag, Col: collationJSON}, nil
}

func (c *compiler) compileToJSON(doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
		return doct, nil
	case sqltypes.Float64:
		c.asm.Convert_fj(offset)
	case sqltypes.Int64:
		c.asm.Convert_ij(offset, doct.Flag&flagIsBoolean != 0)
	case sqltypes.Uint64:
		c.asm.Convert_uj(offset)
	case sqltypes.Decimal:
		c.asm.Convert_dj(offset)
	case sqltypes.VarChar:
		c.asm.Convert_cj(offset)
	case sqltypes.VarBinary:
		c.asm.Convert_bj(offset)
	case sqltypes.Null:
		c.asm.Convert_Nj(offset)
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Time:
		c.asm.Convert_Tj(offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", doct.Type)
	}
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func (c *compiler) compileArgToJSON(doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
		return doct, nil
	case sqltypes.Float64:
		c.asm.Convert_fj(offset)
	case sqltypes.Int64:
		c.asm.Convert_ij(offset, doct.Flag&flagIsBoolean != 0)
	case sqltypes.Uint64:
		c.asm.Convert_uj(offset)
	case sqltypes.Decimal:
		c.asm.Convert_dj(offset)
	case sqltypes.VarChar:
		c.asm.ConvertArg_cj(offset)
	case sqltypes.VarBinary:
		c.asm.Convert_bj(offset)
	case sqltypes.Null:
		c.asm.Convert_Nj(offset)
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Time:
		c.asm.Convert_Tj(offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", doct.Type)
	}
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func (c *compiler) compileFn_JSON_ARRAY(call *builtinJSONArray) (ctype, error) {
	for _, arg := range call.Arguments {
		tt, err := c.compileExpr(arg)
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

func (c *compiler) compileFn_JSON_OBJECT(call *builtinJSONObject) (ctype, error) {
	for i := 0; i < len(call.Arguments); i += 2 {
		key, err := c.compileExpr(call.Arguments[i])
		if err != nil {
			return ctype{}, err
		}
		if err := c.compileToJSONKey(key); err != nil {
			return ctype{}, err
		}
		val, err := c.compileExpr(call.Arguments[i+1])
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

func (c *compiler) compileToJSONKey(key ctype) error {
	if key.Type == sqltypes.Null {
		return errJSONKeyIsNil
	}
	if key.Type == sqltypes.VarChar && isEncodingJSONSafe(key.Col.Collation) {
		return nil
	}
	if key.Type == sqltypes.VarBinary {
		return nil
	}
	c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	return nil
}

func (c *compiler) jsonExtractPath(expr Expr) (*json.Path, error) {
	path, ok := expr.(*Literal)
	if !ok {
		return nil, errJSONPath
	}
	pathBytes, ok := path.inner.(*evalBytes)
	if !ok {
		return nil, errJSONPath
	}
	var parser json.PathParser
	return parser.ParseBytes(pathBytes.bytes)
}

func (c *compiler) jsonExtractOneOrAll(fname string, expr Expr) (jsonMatch, error) {
	lit, ok := expr.(*Literal)
	if !ok {
		return jsonMatchInvalid, errOneOrAll(fname)
	}
	b, ok := lit.inner.(*evalBytes)
	if !ok {
		return jsonMatchInvalid, errOneOrAll(fname)
	}
	return intoOneOrAll(fname, b.string())
}

func (c *compiler) compileFn_JSON_EXTRACT(call *builtinJSONExtract) (ctype, error) {
	doct, err := c.compileExpr(call.Arguments[0])
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

func (c *compiler) compileFn_JSON_UNQUOTE(call *builtinJSONUnquote) (ctype, error) {
	arg, err := c.compileExpr(call.Arguments[0])
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

func (c *compiler) compileFn_JSON_CONTAINS_PATH(call *builtinJSONContainsPath) (ctype, error) {
	doct, err := c.compileExpr(call.Arguments[0])
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

func (c *compiler) compileFn_JSON_KEYS(call *builtinJSONKeys) (ctype, error) {
	doc, err := c.compileExpr(call.Arguments[0])
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
