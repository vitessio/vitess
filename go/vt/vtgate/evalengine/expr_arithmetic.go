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
	querypb "vitess.io/vitess/go/vt/proto/query"
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

	opArithAdd    struct{}
	opArithSub    struct{}
	opArithMul    struct{}
	opArithDiv    struct{}
	opArithIntDiv struct{}
	opArithMod    struct{}
)

var _ Expr = (*ArithmeticExpr)(nil)

var _ opArith = (*opArithAdd)(nil)
var _ opArith = (*opArithSub)(nil)
var _ opArith = (*opArithMul)(nil)
var _ opArith = (*opArithDiv)(nil)
var _ opArith = (*opArithIntDiv)(nil)
var _ opArith = (*opArithMod)(nil)

func (b *ArithmeticExpr) eval(env *ExpressionEnv) (eval, error) {
	left, err := b.Left.eval(env)
	if left == nil || err != nil {
		return nil, err
	}

	right, err := b.Right.eval(env)
	if right == nil || err != nil {
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
	if sqltypes.IsDateOrTime(t) {
		return sqltypes.Int64
	}
	return sqltypes.Float64
}

// typeof implements the Expr interface
func (b *ArithmeticExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t1, f1 := b.Left.typeof(env, fields)
	t2, f2 := b.Right.typeof(env, fields)
	flags := f1 | f2

	t1 = makeNumericalType(t1, f1)
	t2 = makeNumericalType(t2, f2)

	switch b.Op.(type) {
	case *opArithDiv:
		if t1 == sqltypes.Float64 || t2 == sqltypes.Float64 {
			return sqltypes.Float64, flags | flagNullable
		}
		return sqltypes.Decimal, flags | flagNullable
	case *opArithIntDiv:
		if t1 == sqltypes.Uint64 || t2 == sqltypes.Uint64 {
			return sqltypes.Uint64, flags | flagNullable
		}
		return sqltypes.Int64, flags | flagNullable
	case *opArithMod:
		if t1 == sqltypes.Float64 || t2 == sqltypes.Float64 {
			return sqltypes.Float64, flags | flagNullable
		}
		if t1 == sqltypes.Decimal || t2 == sqltypes.Decimal {
			return sqltypes.Decimal, flags | flagNullable
		}
		return t1, flags | flagNullable
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

func (op *opArithAdd) eval(left, right eval) (eval, error) {
	return addNumericWithError(left, right)
}
func (op *opArithAdd) String() string { return "+" }

func (op *opArithSub) eval(left, right eval) (eval, error) {
	return subtractNumericWithError(left, right)
}
func (op *opArithSub) String() string { return "-" }

func (op *opArithMul) eval(left, right eval) (eval, error) {
	return multiplyNumericWithError(left, right)
}
func (op *opArithMul) String() string { return "*" }

func (op *opArithDiv) eval(left, right eval) (eval, error) {
	return divideNumericWithError(left, right, true)
}
func (op *opArithDiv) String() string { return "/" }

func (op *opArithIntDiv) eval(left, right eval) (eval, error) {
	return integerDivideNumericWithError(left, right, true)
}
func (op *opArithIntDiv) String() string { return "DIV" }

func (op *opArithMod) eval(left, right eval) (eval, error) {
	return modNumericWithError(left, right, true)
}
func (op *opArithMod) String() string { return "DIV" }

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

func (n *NegateExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	tt, f := n.Inner.typeof(env, fields)
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
