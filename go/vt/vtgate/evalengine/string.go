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
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
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

	if sqltypes.IsNumber(t) {
		inarg.makeTextualAndConvert(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.Value().Raw(), inarg.collation())
	} else {
		var dst []byte
		dst = collations.ToLower(inarg.Collation(), raw, dst)
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

type builtinLcase struct{}

func (builtinLcase) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	raw := inarg.value().Raw()
	t := inarg.typeof()

	if inarg.isNull() {
		result.setNull()
		return
	}

	if sqltypes.IsNumber(t) {
		inarg.makeTextualAndConvert(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.Value().Raw(), inarg.collation())
	} else {
		var dst []byte
		dst = collations.ToLower(inarg.Collation(), raw, dst)
		result.setRaw(sqltypes.VarChar, dst, inarg.collation())
	}
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

	if sqltypes.IsNumber(t) {
		inarg.makeTextualAndConvert(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.Value().Raw(), inarg.collation())
	} else {
		var dst []byte
		dst = collations.ToUpper(inarg.Collation(), raw, dst)
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

type builtinUcase struct{}

func (builtinUcase) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	raw := inarg.value().Raw()
	t := inarg.typeof()

	if inarg.isNull() {
		result.setNull()
		return
	}

	if sqltypes.IsNumber(t) {
		inarg.makeTextualAndConvert(env.DefaultCollation)
		result.setRaw(sqltypes.VarChar, inarg.Value().Raw(), inarg.collation())
	} else {
		var dst []byte
		dst = collations.ToUpper(inarg.Collation(), raw, dst)
		result.setRaw(sqltypes.VarChar, dst, inarg.collation())
	}
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

	cnt := strings.Count(inarg.value().RawStr(), "") - 1
	result.setInt64(int64(cnt))
}

type builtinCharacterLength struct{}

func (builtinCharLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("Char_Length")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

func (builtinCharacterLength) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	if inarg.isNull() {
		result.setNull()
		return
	}

	cnt := strings.Count(inarg.value().RawStr(), "") - 1
	result.setInt64(int64(cnt))
}

func (builtinCharacterLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("CHARACTER_LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type builtinLength struct{}

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

type builtinOctetLength struct{}

func (builtinLength) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	if inarg.isNull() {
		result.setNull()
		return
	}

	cnt := len(inarg.value().RawStr())
	result.setInt64(int64(cnt))
}

func (builtinLength) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("OCTET_LENGTH")
	}
	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}
