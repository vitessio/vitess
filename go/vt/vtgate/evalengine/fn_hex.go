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
	"vitess.io/vitess/go/mysql/hex"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
)

type builtinHex struct {
	CallExpr
	collate collations.ID
}

var _ IR = (*builtinHex)(nil)

func (call *builtinHex) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	var encoded []byte
	switch arg := arg.(type) {
	case *evalBytes:
		encoded = hex.EncodeBytes(arg.bytes)
	case evalNumeric:
		encoded = hex.EncodeUint(uint64(arg.toInt64().i))
	default:
		encoded = hex.EncodeBytes(arg.ToRawBytes())
	}
	if arg.SQLType() == sqltypes.Blob || arg.SQLType() == sqltypes.TypeJSON {
		return newEvalRaw(sqltypes.Text, encoded, typedCoercionCollation(sqltypes.Text, call.collate)), nil
	}
	return newEvalText(encoded, typedCoercionCollation(sqltypes.VarChar, call.collate)), nil
}

func (call *builtinHex) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)
	t := sqltypes.VarChar
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}
	col := typedCoercionCollation(t, c.collation)

	switch {
	case sqltypes.IsNumber(str.Type):
		c.asm.Fn_HEX_d(col)
	case str.isTextual():
		c.asm.Fn_HEX_c(t, col)
	default:
		c.asm.Convert_xc(1, t, c.collation, nil)
		c.asm.Fn_HEX_c(t, col)
	}

	c.asm.jumpDestination(skip)

	return ctype{Type: t, Flag: nullableFlags(str.Flag), Col: col}, nil
}

type builtinUnhex struct {
	CallExpr
}

var _ IR = (*builtinUnhex)(nil)

func hexDecodeJSON(j *evalJSON) ([]byte, bool) {
	switch j.Type() {
	case json.TypeNumber:
		u, ok := j.Uint64()
		if ok {
			return hex.DecodeUint(u), true
		} else {
			return nil, false
		}
	default:
		b := j.ToRawBytes()
		decoded := make([]byte, hex.DecodedLen(b))
		err := hex.DecodeBytes(decoded, b)
		if err != nil {
			return nil, false
		}
		return decoded, true
	}
}

func (call *builtinUnhex) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	var decoded []byte
	switch arg := arg.(type) {
	case *evalBytes:
		decoded = make([]byte, hex.DecodedLen(arg.bytes))
		err := hex.DecodeBytes(decoded, arg.bytes)
		if err != nil {
			return nil, nil
		}
	case *evalInt64:
		if arg.i < 0 {
			return nil, nil
		}
		decoded = hex.DecodeUint(uint64(arg.i))
	case *evalUint64:
		decoded = hex.DecodeUint(arg.u)
	case *evalDecimal:
		b := arg.ToRawBytes()
		decoded = make([]byte, hex.DecodedLen(b))
		err := hex.DecodeBytes(decoded, b)
		if err != nil {
			return nil, nil
		}
	case *evalFloat:
		f := arg.f
		if f != float64(int64(f)) {
			return nil, nil
		}
		decoded = hex.DecodeUint(uint64(arg.f))
	case *evalJSON:
		var ok bool
		decoded, ok = hexDecodeJSON(arg)
		if !ok {
			return nil, nil
		}
	default:
		b := evalToBinary(arg)
		decoded = make([]byte, hex.DecodedLen(b.bytes))
		err := hex.DecodeBytes(decoded, b.bytes)
		if err != nil {
			return nil, nil
		}
	}

	switch arg.SQLType() {
	case sqltypes.Text, sqltypes.Blob, sqltypes.TypeJSON:
		return newEvalRaw(sqltypes.Blob, decoded, collationBinary), nil
	}
	return newEvalBinary(decoded), nil
}

func (call *builtinUnhex) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)
	t := sqltypes.VarBinary
	if str.Type == sqltypes.Text || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Blob
	}

	switch {
	case sqltypes.IsSigned(str.Type):
		c.asm.Fn_UNHEX_i(t)
	case sqltypes.IsUnsigned(str.Type):
		c.asm.Fn_UNHEX_u(t)
	case sqltypes.IsFloat(str.Type):
		c.asm.Fn_UNHEX_f(t)
	case str.isTextual():
		c.asm.Fn_UNHEX_b(t)
	case str.Type == sqltypes.TypeJSON:
		c.asm.Fn_UNHEX_j(t)
	default:
		c.asm.Convert_xb(1, t, nil)
		c.asm.Fn_UNHEX_b(t)
	}

	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: collationBinary, Flag: flagNullable}, nil
}
