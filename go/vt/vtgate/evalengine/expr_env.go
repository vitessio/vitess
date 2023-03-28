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
	"errors"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		vm vmstate

		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
		Tz       *time.Location

		// internal state
		now time.Time
	}
)

func (env *ExpressionEnv) time(utc bool) time.Time {
	if env.now.IsZero() {
		env.now = time.Now()
	}
	if utc {
		return env.now.UTC()
	}
	if env.Tz != nil {
		return env.now.In(env.Tz)
	}
	return env.now
}

func (env *ExpressionEnv) Evaluate(expr Expr) (EvalResult, error) {
	env.now = time.Time{}
	if p, ok := expr.(*CompiledExpr); ok {
		return env.EvaluateVM(p)
	}
	e, err := expr.eval(env)
	return EvalResult{e}, err
}

var ErrAmbiguousType = errors.New("the type of this expression cannot be statically computed")

func (env *ExpressionEnv) TypeOf(expr Expr, fields []*querypb.Field) (sqltypes.Type, error) {
	ty, f := expr.typeof(env, fields)
	if f&flagAmbiguousType != 0 {
		return ty, ErrAmbiguousType
	}
	return ty, nil
}

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv() *ExpressionEnv {
	return NewExpressionEnv(nil, nil)
}

// NewExpressionEnv returns an expression environment with no current row, but with bindvars
func NewExpressionEnv(bindVars map[string]*querypb.BindVariable, tz *time.Location) *ExpressionEnv {
	return &ExpressionEnv{BindVars: bindVars, Tz: tz}
}
