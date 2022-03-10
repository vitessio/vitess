/*
Copyright 2022 The Vitess Authors.

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
	"strconv"
	"unicode"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	builtinAscii     struct{}
	builtinBin       struct{}
	builtinBitLength struct{}
)

func (builtinAscii) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	toascii := &args[0]
	if toascii.isNull() {
		result.setNull()
		return
	}

	toascii.makeBinary()
	if len(toascii.bytes()) > 0 {
		result.setInt64(int64(toascii.bytes()[0]))
	} else {
		result.setInt64(0)
	}
}

func (builtinAscii) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("ASCII")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

func (builtinBin) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	tobin := &args[0]
	if tobin.isNull() {
		result.setNull()
		return
	}

	var raw []byte
	switch t := tobin.typeof(); {
	case sqltypes.IsNumber(t):
		tobin.makeUnsignedIntegral()
		raw = strconv.AppendUint(raw, tobin.uint64(), 2)
	case sqltypes.IsText(t):
		input := tobin.string()
		index := len(input)
		if index == 0 {
			break
		}
		for i, ch := range input {
			if !unicode.IsDigit(ch) {
				index = i
				break
			}
		}
		intstr := input[:index]
		toint64, _ := strconv.ParseInt(intstr, 10, 64)
		raw = strconv.AppendUint(raw, uint64(toint64), 2)
	default:
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported BIN argument: %s", t.String()))
	}

	result.setRaw(sqltypes.VarChar, raw, collations.TypedCollation{
		Collation:    env.DefaultCollation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	})
}

func (builtinBin) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("BIN")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}

func (builtinBitLength) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	arg1 := &args[0]
	if arg1.isNull() {
		result.setNull()
		return
	}

	switch t := arg1.typeof(); {
	case sqltypes.IsQuoted(t):
		result.setInt64(int64(8 * len(arg1.bytes())))
	default:
		result.setInt64(int64(8 * len(arg1.toRawBytes())))
	}
}

func (builtinBitLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("BIT_LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}
