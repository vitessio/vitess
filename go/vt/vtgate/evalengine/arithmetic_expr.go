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
)

type (
	ArithmeticExpr struct {
		BinaryExpr
		Op ArithmeticOp
	}

	// ArithmeticOp allows arithmetic expressions to not have to evaluate child expressions - this is done by the BinaryExpr
	ArithmeticOp interface {
		eval(left, right, out *EvalResult) error
		typeof(left sqltypes.Type) sqltypes.Type
		String() string
	}

	OpAddition       struct{}
	OpSubstraction   struct{}
	OpMultiplication struct{}
	OpDivision       struct{}
)

var _ ArithmeticOp = (*OpAddition)(nil)
var _ ArithmeticOp = (*OpSubstraction)(nil)
var _ ArithmeticOp = (*OpMultiplication)(nil)
var _ ArithmeticOp = (*OpDivision)(nil)

func (b *ArithmeticExpr) eval(env *ExpressionEnv, out *EvalResult) {
	var left, right EvalResult
	left.init(env, b.Left)
	right.init(env, b.Right)
	if left.null() || right.null() {
		out.setNull()
		return
	}
	if left.typeof() == sqltypes.Tuple || right.typeof() == sqltypes.Tuple {
		panic("failed to typecheck tuples")
	}
	if err := b.Op.eval(&left, &right, out); err != nil {
		throwEvalError(err)
	}
}

// typeof implements the Expr interface
func (b *ArithmeticExpr) typeof(env *ExpressionEnv) (sqltypes.Type, uint16) {
	t1, f1 := b.Left.typeof(env)
	_, f2 := b.Right.typeof(env)

	// TODO: this is not really accurate
	return b.Op.typeof(t1), f1 | f2
}

func (a *OpAddition) eval(left, right, out *EvalResult) error {
	return addNumericWithError(left, right, out)
}
func (a *OpAddition) typeof(left sqltypes.Type) sqltypes.Type { return left }
func (a *OpAddition) String() string                          { return "+" }

func (s *OpSubstraction) eval(left, right, out *EvalResult) error {
	return subtractNumericWithError(left, right, out)
}
func (s *OpSubstraction) typeof(left sqltypes.Type) sqltypes.Type { return left }
func (s *OpSubstraction) String() string                          { return "-" }

func (m *OpMultiplication) eval(left, right, out *EvalResult) error {
	return multiplyNumericWithError(left, right, out)
}
func (m *OpMultiplication) typeof(left sqltypes.Type) sqltypes.Type { return left }
func (m *OpMultiplication) String() string                          { return "*" }

func (d *OpDivision) eval(left, right, out *EvalResult) error {
	return divideNumericWithError(left, right, true, out)
}
func (d *OpDivision) typeof(sqltypes.Type) sqltypes.Type { return sqltypes.Float64 }
func (d *OpDivision) String() string                     { return "/" }
