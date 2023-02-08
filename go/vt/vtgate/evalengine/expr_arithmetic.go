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
	"vitess.io/vitess/go/sqltypes"
)

type (
	ArithmeticExpr struct {
		BinaryExpr
		Op opArith
	}

	NegateExpr struct {
		UnaryExpr
	}

	opArith interface {
		eval(left, right eval) (eval, error)
		String() string
	}

	opArithAdd struct{}
	opArithSub struct{}
	opArithMul struct{}
	opArithDiv struct{}
)

var _ Expr = (*ArithmeticExpr)(nil)

var _ opArith = (*opArithAdd)(nil)
var _ opArith = (*opArithSub)(nil)
var _ opArith = (*opArithMul)(nil)
var _ opArith = (*opArithDiv)(nil)

func (b *ArithmeticExpr) eval(env *ExpressionEnv) (eval, error) {
	left, right, err := b.arguments(env)
	if left == nil || right == nil || err != nil {
		return nil, err
	}
	return b.Op.eval(left, right)
}

func makeNumericalType(t sqltypes.Type, f typeFlag) sqltypes.Type {
	if sqltypes.IsNumber(t) {
		return t
	}
	if t == sqltypes.VarBinary && (f&flagHex) != 0 {
		return sqltypes.Uint64
	}
	return sqltypes.Float64
}

// typeof implements the Expr interface
func (b *ArithmeticExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	t1, f1 := b.Left.typeof(env)
	t2, f2 := b.Right.typeof(env)
	flags := f1 | f2

	t1 = makeNumericalType(t1, f1)
	t2 = makeNumericalType(t2, f2)

	switch b.Op.(type) {
	case *opArithDiv:
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

func (a *opArithAdd) eval(left, right eval) (eval, error) {
	return addNumericWithError(left, right)
}
func (a *opArithAdd) String() string { return "+" }

func (s *opArithSub) eval(left, right eval) (eval, error) {
	return subtractNumericWithError(left, right)
}
func (s *opArithSub) String() string { return "-" }

func (m *opArithMul) eval(left, right eval) (eval, error) {
	return multiplyNumericWithError(left, right)
}
func (m *opArithMul) String() string { return "*" }

func (d *opArithDiv) eval(left, right eval) (eval, error) {
	return divideNumericWithError(left, right, true)
}
func (d *opArithDiv) String() string { return "/" }

func (n *NegateExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := n.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	return evalToNumeric(e).negate(), nil
}

func (n *NegateExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	tt, f := n.Inner.typeof(env)
	switch tt {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		if f&flagIntegerOvf != 0 {
			return sqltypes.Decimal, f & ^flagIntegerRange
		}
		if f&flagIntegerCap != 0 {
			return sqltypes.Int64, (f | flagIntegerUdf) & ^flagIntegerCap
		}
		return sqltypes.Int64, f
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		if f&flagIntegerUdf != 0 {
			return sqltypes.Decimal, f & ^flagIntegerRange
		}
		return sqltypes.Int64, f
	case sqltypes.Decimal:
		return sqltypes.Decimal, f
	}
	return sqltypes.Float64, f
}
