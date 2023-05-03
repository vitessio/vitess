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
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	builtinToBase64 struct {
		CallExpr
		collate collations.ID
	}

	builtinFromBase64 struct {
		CallExpr
	}
)

var _ Expr = (*builtinToBase64)(nil)
var _ Expr = (*builtinFromBase64)(nil)

// MySQL wraps every 76 characters with a newline. That maps
// to a 57 byte input. So we encode here in blocks of 57 bytes
// with then each a newline.
var mysqlBase64OutLineLength = 76
var mysqlBase64InLineLength = (mysqlBase64OutLineLength / 4) * 3

func mysqlBase64Encode(in []byte) []byte {
	newlines := len(in) / mysqlBase64InLineLength
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(in))+newlines)
	out := encoded
	for len(in) > mysqlBase64InLineLength {
		base64.StdEncoding.Encode(out, in[:mysqlBase64InLineLength])
		in = in[mysqlBase64InLineLength:]
		out[mysqlBase64OutLineLength] = '\n'
		out = out[mysqlBase64OutLineLength+1:]
	}
	base64.StdEncoding.Encode(out, in)
	return encoded
}

func mysqlBase64Decode(in []byte) ([]byte, error) {
	in = bytes.Trim(in, " \t\r\n")
	decoded := make([]byte, len(in)/4*3)

	n, err := base64.StdEncoding.Decode(decoded, in)
	if err != nil {
		return nil, err
	}
	return decoded[:n], nil
}

func (call *builtinToBase64) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b := evalToBinary(arg)
	encoded := mysqlBase64Encode(b.bytes)

	if arg.SQLType() == sqltypes.Blob || arg.SQLType() == sqltypes.TypeJSON {
		return newEvalRaw(sqltypes.Text, encoded, defaultCoercionCollation(call.collate)), nil
	}
	return newEvalText(encoded, defaultCoercionCollation(call.collate)), nil
}

func (call *builtinToBase64) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	tt, f := call.Arguments[0].typeof(env, fields)
	if tt == sqltypes.Blob || tt == sqltypes.TypeJSON {
		return sqltypes.Text, f
	}
	return sqltypes.VarChar, f
}

func (call *builtinToBase64) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	t := sqltypes.VarChar
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, t, 0, false)
	}

	col := collations.TypedCollation{
		Collation:    c.cfg.Collation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}

	c.asm.Fn_TO_BASE64(t, col)
	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: col}, nil
}

func (call *builtinFromBase64) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b := evalToBinary(arg)
	decoded, err := mysqlBase64Decode(b.bytes)
	if err != nil {
		return nil, nil
	}
	if arg.SQLType() == sqltypes.Text || arg.SQLType() == sqltypes.TypeJSON {
		return newEvalRaw(sqltypes.Blob, decoded, collationBinary), nil
	}
	return newEvalBinary(decoded), nil
}

func (call *builtinFromBase64) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	tt, f := call.Arguments[0].typeof(env, fields)
	if tt == sqltypes.Text || tt == sqltypes.TypeJSON {
		return sqltypes.Blob, f | flagNullable
	}
	return sqltypes.VarBinary, f | flagNullable
}

func (call *builtinFromBase64) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	t := sqltypes.VarBinary
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Blob
	}

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, t, 0, false)
	}

	c.asm.Fn_FROM_BASE64(t)
	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: collationBinary}, nil
}
