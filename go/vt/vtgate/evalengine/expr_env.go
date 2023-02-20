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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars         map[string]*querypb.BindVariable
		DefaultCollation collations.ID

		// Row and Fields should line up
		Row    []sqltypes.Value
		Fields []*querypb.Field
	}
)

func (env *ExpressionEnv) Evaluate(expr Expr) (EvalResult, error) {
	if env == nil {
		panic("ExpressionEnv == nil")
	}
	e, err := expr.eval(env)
	return EvalResult{e}, err
}

func (env *ExpressionEnv) TypeOf(expr Expr) (ty sqltypes.Type, err error) {
	ty, _ = expr.typeof(env)
	return
}

func (env *ExpressionEnv) collation() collations.TypedCollation {
	return collations.TypedCollation{
		Collation:    env.DefaultCollation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}
}

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv() *ExpressionEnv {
	return EnvWithBindVars(map[string]*querypb.BindVariable{}, collations.Unknown)
}

// EnvWithBindVars returns an expression environment with no current row, but with bindvars
func EnvWithBindVars(bindVars map[string]*querypb.BindVariable, coll collations.ID) *ExpressionEnv {
	if coll == collations.Unknown {
		coll = collations.Default()
	}
	return &ExpressionEnv{BindVars: bindVars, DefaultCollation: coll}
}
