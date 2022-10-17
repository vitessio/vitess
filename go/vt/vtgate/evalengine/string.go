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
	"bytes"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type builtinLower struct{}

func (builtinLower) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]

	switch {
	case inarg.isNull():
		result.setNull()

	case sqltypes.IsNumber(inarg.typeof()):
		inarg.makeTextual(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.bytes(), inarg.collation())

	default:
		coll := collations.Local().LookupByID(inarg.collation().Collation)
		csa, ok := coll.(collations.CaseAwareCollation)
		if !ok {
			throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented"))
		}

		dst := csa.ToLower(nil, inarg.bytes())
		result.setRaw(sqltypes.VarChar, dst, inarg.collation())
	}
}

func (builtinLower) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("LOWER")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}

type builtinLcase struct {
	builtinLower
}

func (builtinLcase) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("LCASE")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}

type builtinUpper struct{}

func (builtinUpper) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]

	switch {
	case inarg.isNull():
		result.setNull()

	case sqltypes.IsNumber(inarg.typeof()):
		inarg.makeTextual(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.bytes(), inarg.collation())

	default:
		coll := collations.Local().LookupByID(inarg.collation().Collation)
		csa, ok := coll.(collations.CaseAwareCollation)
		if !ok {
			throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented"))
		}

		dst := csa.ToUpper(nil, inarg.bytes())
		result.setRaw(sqltypes.VarChar, dst, inarg.collation())
	}
}

func (builtinUpper) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("UPPER")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}

type builtinUcase struct {
	builtinUpper
}

func (builtinUcase) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("UCASE")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}

type builtinCharLength struct{}

func (builtinCharLength) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	if inarg.isNull() {
		result.setNull()
		return
	}

	coll := collations.Local().LookupByID(inarg.collation().Collation)
	cnt := collations.Length(coll, inarg.toRawBytes())
	result.setInt64(int64(cnt))
}

func (builtinCharLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("CHAR_LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type builtinCharacterLength struct {
	builtinCharLength
}

func (builtinCharacterLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("CHARACTER_LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type builtinOctetLength struct{}

func (builtinOctetLength) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	if inarg.isNull() {
		result.setNull()
		return
	}

	cnt := len(inarg.toRawBytes())
	result.setInt64(int64(cnt))
}

func (builtinOctetLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type builtinLength struct {
	builtinOctetLength
}

func (builtinLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("OCTET_LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type builtinBitLength struct {
}

func (builtinBitLength) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	if inarg.isNull() {
		result.setNull()
		return
	}

	cnt := len(inarg.toRawBytes())
	result.setInt64(int64(cnt * 8))
}

func (builtinBitLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("BIT_LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type builtinASCII struct {
}

func (builtinASCII) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	if inarg.isNull() {
		result.setNull()
		return
	}

	inarg.makeBinary()
	bs := inarg.bytes()
	if len(bs) > 0 {
		result.setInt64(int64(bs[0]))
	} else {
		result.setInt64(0)
	}
}

func (builtinASCII) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("ASCII")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type builtinRepeat struct {
}

func (builtinRepeat) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	repeatTime := &args[1]
	if inarg.isNull() || repeatTime.isNull() {
		result.setNull()
		return
	}

	if sqltypes.IsNumber(inarg.typeof()) {
		inarg.makeTextual(env.DefaultCollation)
	}

	repeatTime.makeSignedIntegral()
	repeat := int(repeatTime.int64())
	if repeat < 0 {
		repeat = 0
	}

	result.setRaw(sqltypes.VarChar, bytes.Repeat(inarg.bytes(), repeat), inarg.collation())
}

func (builtinRepeat) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 2 {
		throwArgError("REPEAT")
	}
	_, f1 := args[0].typeof(env)
	// typecheck the right-hand argument but ignore its flags
	args[1].typeof(env)

	return sqltypes.VarChar, f1
}
