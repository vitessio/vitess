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

type (
	builtinLower struct{}
	builtinLcase struct{}
)

func (builtinLower) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	t := inarg.typeof()
	if inarg.isNull() {
		result.setNull()
		return
	}

	if sqltypes.IsText(t) {
		tolower := strings.ToLower(inarg.value().RawStr())
		result.setString(tolower, inarg.collation())
	} else {
		tolower := inarg.value().RawStr()
		inarg.makeTextualAndConvert(collations.CollationUtf8mb4ID)
		result.setString(tolower, inarg.collation())
	}
}

func (builtinLower) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("LOWER")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}

func (builtinLcase) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	inarg := &args[0]
	t := inarg.typeof()
	if inarg.isNull() {
		result.setNull()
		return
	}

	if sqltypes.IsText(t) {
		tolower := strings.ToLower(inarg.value().RawStr())
		result.setString(tolower, inarg.collation())
	} else {
		tolower := inarg.value().RawStr()
		inarg.makeTextualAndConvert(collations.CollationUtf8mb4ID)
		result.setString(tolower, inarg.collation())
	}
}

func (builtinLcase) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("LCASE")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}
