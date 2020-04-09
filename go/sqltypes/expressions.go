/*
Copyright 2020 The Vitess Authors.

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

package sqltypes

import (
	"strconv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type ExpressionEnv struct {
	BindVars map[string]*querypb.BindVariable
	Row      []Value
}

type EvalResult = numeric

func (e EvalResult) Value() Value {
	return castFromNumeric(e, e.typ)
}

type Expr interface {
	Evaluate(env ExpressionEnv) (EvalResult, error)
}

var _ Expr = (*LiteralInt)(nil)
var _ Expr = (*BindVariable)(nil)

type LiteralInt struct {
	Val []byte
}

func (l *LiteralInt) Evaluate(env ExpressionEnv) (EvalResult, error) {
	ival, err := strconv.ParseInt(string(l.Val), 10, 64)
	if err != nil {
		ival = 0
	}
	return numeric{typ: Int64, ival: ival}, nil
}

type BindVariable struct {
	Key string
}

func (b *BindVariable) Evaluate(env ExpressionEnv) (EvalResult, error) {
	val, ok := env.BindVars[b.Key]
	if !ok {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Bind variable not found")
	}
	ival, err := strconv.ParseInt(string(val.Value), 10, 64)
	if err != nil {
		ival = 0
	}
	return numeric{typ: Int64, ival: ival}, nil

}

var _ Expr = (*Addition)(nil)
var _ Expr = (*Subtraction)(nil)
var _ Expr = (*Multiplication)(nil)
var _ Expr = (*Division)(nil)

type Addition struct {
	Left, Right Expr
}

func (a *Addition) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := a.Left.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := a.Right.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return addNumericWithError(lVal, rVal)
}

type Subtraction struct {
	Left, Right Expr
}

func (s *Subtraction) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := s.Left.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := s.Right.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return subtractNumericWithError(lVal, rVal)
}

type Multiplication struct {
	Left, Right Expr
}

func (m *Multiplication) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := m.Left.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := m.Right.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return multiplyNumericWithError(lVal, rVal)
}

type Division struct {
	Left, Right Expr
}

func (d *Division) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := d.Left.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := d.Right.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return divideNumericWithError(lVal, rVal)
}
