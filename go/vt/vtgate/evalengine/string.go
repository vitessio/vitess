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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type builtinLower struct{}

func (builtinLower) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	raw := inarg.value().Raw()
	t := inarg.typeof()

	if inarg.isNull() {
		result.setNull()
		return
	}

	coll := collations.Local().LookupByID(inarg.Collation())

	if sqltypes.IsNumber(t) {
		inarg.makeTextualAndConvert(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.Value().Raw(), inarg.collation())
	} else if csa, ok := coll.(collations.CaseAwareCollation); ok {
		var dst []byte
		dst = csa.ToLower(dst, raw)
		result.setRaw(sqltypes.VarChar, dst, inarg.collation())
	} else {
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented"))
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
	raw := inarg.value().Raw()
	t := inarg.typeof()

	if inarg.isNull() {
		result.setNull()
		return
	}

	coll := collations.Local().LookupByID(inarg.Collation())

	if sqltypes.IsNumber(t) {
		inarg.makeTextualAndConvert(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.Value().Raw(), inarg.collation())
	} else if csa, ok := coll.(collations.CaseAwareCollation); ok {
		var dst []byte
		dst = csa.ToUpper(dst, raw)
		result.setRaw(sqltypes.VarChar, dst, inarg.collation())
	} else {
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented"))
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
	t := inarg.typeof()
	raw := inarg.value().Raw()

	if inarg.isNull() {
		result.setNull()
		return
	}

	coll := collations.Local().LookupByID(inarg.collation().Collation)

	if sqltypes.IsNumber(t) {
		inarg.makeTextualAndConvert(env.DefaultCollation)
		cnt := int64(len(inarg.value().Raw()))
		result.setInt64(cnt)
	} else if cnt := collations.CharLen(coll, raw); cnt >= 0 {
		result.setInt64(int64(cnt))
	} else {
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented"))
	}

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

	cnt := len(inarg.value().RawStr())
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
