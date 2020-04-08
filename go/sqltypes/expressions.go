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

var _ Expr = (*SQLVal)(nil)

type SQLVal struct {
	Type ValType
	Val  []byte
}

func (s *SQLVal) Evaluate(env ExpressionEnv) (EvalResult, error) {
	switch s.Type {
	case IntVal:
		ival, err := strconv.ParseInt(string(s.Val), 10, 64)
		if err != nil {
			ival = 0
		}
		return numeric{typ: Int64, ival: ival}, nil
	case ValArg:
		val := env.BindVars[string(s.Val[1:])]
		ival, err := strconv.ParseInt(string(val.Value), 10, 64)
		if err != nil {
			ival = 0
		}
		return numeric{typ: Int64, ival: ival}, nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not yet implemented")
}

type ValType int

const (
	StrVal = ValType(iota)
	IntVal
	FloatVal
	HexNum
	HexVal
	ValArg
	BitVal
)

var _ Expr = (*add)(nil)

type add struct {
	l, r Expr
}

type subtract struct {
	l, r Expr
}

func (a *add) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := a.l.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := a.r.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return addNumericWithError(lVal, rVal)
}

func (s *subtract) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := s.l.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := s.r.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return subtractNumericWithError(lVal, rVal)
}
