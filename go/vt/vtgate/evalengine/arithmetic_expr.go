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

	ArithmeticOp interface {
		eval(left, right, out *EvalResult) error
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
	if left.isNull() || right.isNull() {
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

func makeNumericalType(t sqltypes.Type, f flag) sqltypes.Type {
	if sqltypes.IsNumber(t) {
		return t
	}
	if t == sqltypes.VarBinary && (f&flagHex) != 0 {
		return sqltypes.Uint64
	}
	return sqltypes.Float64
}

// typeof implements the Expr interface
func (b *ArithmeticExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	t1, f1 := b.Left.typeof(env)
	t2, f2 := b.Right.typeof(env)
	flags := f1 | f2

	t1 = makeNumericalType(t1, f1)
	t2 = makeNumericalType(t2, f2)

	switch b.Op.(type) {
	case *OpDivision:
		if t1 == sqltypes.Float64 || t2 == sqltypes.Float64 {
			return sqltypes.Float64, flags
		}
		return sqltypes.Decimal, flags
	}

	switch t1 {
	case sqltypes.Int64:
		switch t2 {
		case sqltypes.Uint64, sqltypes.Float64, sqltypes.Decimal:
			return t2, flags
		}
	case sqltypes.Uint64:
		switch t2 {
		case sqltypes.Float64, sqltypes.Decimal:
			return t2, flags
		}
	case sqltypes.Decimal:
		if t2 == sqltypes.Float64 {
			return t2, flags
		}
	}
	return t1, flags
}

func (a *OpAddition) eval(left, right, out *EvalResult) error {
	return addNumericWithError(left, right, out)
}
func (a *OpAddition) String() string { return "+" }

func (s *OpSubstraction) eval(left, right, out *EvalResult) error {
	return subtractNumericWithError(left, right, out)
}
func (s *OpSubstraction) String() string { return "-" }

func (m *OpMultiplication) eval(left, right, out *EvalResult) error {
	return multiplyNumericWithError(left, right, out)
}
func (m *OpMultiplication) String() string { return "*" }

func (d *OpDivision) eval(left, right, out *EvalResult) error {
	return divideNumericWithError(left, right, true, out)
}
func (d *OpDivision) String() string { return "/" }
