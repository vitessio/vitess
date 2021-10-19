/*
Copyright 2021 The Vitess Authors.

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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	Comparison struct {
		Left, Right Expr
	}
)

func evaluateSideOfComparison(expr Expr, env ExpressionEnv) (EvalResult, error) {
	val, err := expr.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	if val.typ == sqltypes.Null {
		return NullEvalResult(), nil
	}
	return makeNumeric(val), nil
}

// Evaluate implements the Expr interface
func (e *Comparison) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := evaluateSideOfComparison(e.Left, env)
	if lVal.typ == sqltypes.Null || err != nil {
		return lVal, err
	}

	rVal, err := evaluateSideOfComparison(e.Right, env)
	if rVal.typ == sqltypes.Null || err != nil {
		return rVal, err
	}

	numeric, err := compareNumeric(lVal, rVal)
	if err != nil {
		return EvalResult{}, err
	}
	value := int64(0)
	if numeric == 0 {
		value = 1
	}
	return EvalResult{
		typ:  sqltypes.Int32,
		ival: value,
	}, nil
}

// Type implements the Expr interface
func (e *Comparison) Type(ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_INT32, nil
}

// String implements the Expr interface
func (e *Comparison) String() string {
	return e.Left.String() + " = " + e.Right.String()
}
