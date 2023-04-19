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
		compile(c *compiler, left, right Expr) (ctype, error)
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

func (b *ArithmeticExpr) compile(c *compiler) (ctype, error) {
	return b.Op.compile(c, b.Left, b.Right)
}

func (op *opArithAdd) eval(left, right eval) (eval, error) {
	return addNumericWithError(left, right)
}
func (op *opArithAdd) String() string { return "+" }

func (op *opArithAdd) compile(c *compiler, left, right Expr) (ctype, error) {
	lt, err := left.compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip2 := c.compileNullCheck1r(rt)

	lt = c.compileToNumeric(lt, 2, sqltypes.Float64)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64)
	lt, rt, swap = c.compileNumericPriority(lt, rt)

	var sumtype sqltypes.Type

	switch lt.Type {
	case sqltypes.Int64:
		c.asm.Add_ii()
		sumtype = sqltypes.Int64
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Add_ui(swap)
		case sqltypes.Uint64:
			c.asm.Add_uu()
		}
		sumtype = sqltypes.Uint64
	case sqltypes.Decimal:
		if swap {
			c.compileToDecimal(rt, 2)
		} else {
			c.compileToDecimal(rt, 1)
		}
		c.asm.Add_dd()
		sumtype = sqltypes.Decimal
	case sqltypes.Float64:
		if swap {
			c.compileToFloat(rt, 2)
		} else {
			c.compileToFloat(rt, 1)
		}
		c.asm.Add_ff()
		sumtype = sqltypes.Float64
	}

	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sumtype, Col: collationNumeric}, nil
}

func (op *opArithSub) eval(left, right eval) (eval, error) {
	return subtractNumericWithError(left, right)
}
func (op *opArithSub) String() string { return "-" }

func (op *opArithSub) compile(c *compiler, left, right Expr) (ctype, error) {
	lt, err := left.compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2, sqltypes.Float64)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64)

	var subtype sqltypes.Type

	switch lt.Type {
	case sqltypes.Int64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Sub_ii()
			subtype = sqltypes.Int64
		case sqltypes.Uint64:
			c.asm.Sub_iu()
			subtype = sqltypes.Uint64
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.asm.Sub_ff()
			subtype = sqltypes.Float64
		case sqltypes.Decimal:
			c.compileToDecimal(lt, 2)
			c.asm.Sub_dd()
			subtype = sqltypes.Decimal
		}
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Sub_ui()
			subtype = sqltypes.Uint64
		case sqltypes.Uint64:
			c.asm.Sub_uu()
			subtype = sqltypes.Uint64
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.asm.Sub_ff()
			subtype = sqltypes.Float64
		case sqltypes.Decimal:
			c.compileToDecimal(lt, 2)
			c.asm.Sub_dd()
			subtype = sqltypes.Decimal
		}
	case sqltypes.Float64:
		c.compileToFloat(rt, 1)
		c.asm.Sub_ff()
		subtype = sqltypes.Float64
	case sqltypes.Decimal:
		switch rt.Type {
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.asm.Sub_ff()
			subtype = sqltypes.Float64
		default:
			c.compileToDecimal(rt, 1)
			c.asm.Sub_dd()
			subtype = sqltypes.Decimal
		}
	}

	if subtype == 0 {
		panic("did not compile?")
	}

	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: subtype, Col: collationNumeric}, nil
}

func (op *opArithMul) eval(left, right eval) (eval, error) {
	return multiplyNumericWithError(left, right)
}

func (op *opArithMul) String() string { return "*" }

func (op *opArithMul) compile(c *compiler, left, right Expr) (ctype, error) {
	lt, err := left.compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2, sqltypes.Float64)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64)
	lt, rt, swap = c.compileNumericPriority(lt, rt)

	var multype sqltypes.Type

	switch lt.Type {
	case sqltypes.Int64:
		c.asm.Mul_ii()
		multype = sqltypes.Int64
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Mul_ui(swap)
		case sqltypes.Uint64:
			c.asm.Mul_uu()
		}
		multype = sqltypes.Uint64
	case sqltypes.Float64:
		if swap {
			c.compileToFloat(rt, 2)
		} else {
			c.compileToFloat(rt, 1)
		}
		c.asm.Mul_ff()
		multype = sqltypes.Float64
	case sqltypes.Decimal:
		if swap {
			c.compileToDecimal(rt, 2)
		} else {
			c.compileToDecimal(rt, 1)
		}
		c.asm.Mul_dd()
		multype = sqltypes.Decimal
	}

	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: multype, Col: collationNumeric}, nil
}

func (op *opArithDiv) eval(left, right eval) (eval, error) {
	return divideNumericWithError(left, right, true)
}

func (op *opArithDiv) String() string { return "/" }

func (op *opArithDiv) compile(c *compiler, left, right Expr) (ctype, error) {
	lt, err := left.compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := right.compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip2 := c.compileNullCheck1r(rt)

	lt = c.compileToNumeric(lt, 2, sqltypes.Float64)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64)

	ct := ctype{Col: collationNumeric, Flag: flagNullable}
	if lt.Type == sqltypes.Float64 || rt.Type == sqltypes.Float64 {
		ct.Type = sqltypes.Float64
		c.compileToFloat(lt, 2)
		c.compileToFloat(rt, 1)
		c.asm.Div_ff()
	} else {
		ct.Type = sqltypes.Decimal
		c.compileToDecimal(lt, 2)
		c.compileToDecimal(rt, 1)
		c.asm.Div_dd()
	}
	c.asm.jumpDestination(skip1, skip2)
	return ct, nil
}

func (op *opArithIntDiv) eval(left, right eval) (eval, error) {
	return integerDivideNumericWithError(left, right)
}

func (op *opArithIntDiv) String() string { return "DIV" }

func (op *opArithIntDiv) compile(c *compiler, left, right Expr) (ctype, error) {
	lt, err := left.compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2, sqltypes.Decimal)
	rt = c.compileToNumeric(rt, 1, sqltypes.Decimal)

	ct := ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagNullable}
	switch lt.Type {
	case sqltypes.Int64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.IntDiv_ii()
		case sqltypes.Uint64:
			ct.Type = sqltypes.Uint64
			c.asm.IntDiv_iu()
		case sqltypes.Float64:
			c.asm.Convert_xd(2, 0, 0)
			c.asm.Convert_xd(1, 0, 0)
			c.asm.IntDiv_di()
		case sqltypes.Decimal:
			c.asm.Convert_xd(2, 0, 0)
			c.asm.IntDiv_di()
		}
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.IntDiv_ui()
		case sqltypes.Uint64:
			ct.Type = sqltypes.Uint64
			c.asm.IntDiv_uu()
		case sqltypes.Float64:
			c.asm.Convert_xd(2, 0, 0)
			c.asm.Convert_xd(1, 0, 0)
			c.asm.IntDiv_du()
		case sqltypes.Decimal:
			c.asm.Convert_xd(2, 0, 0)
			c.asm.IntDiv_du()
		}
	case sqltypes.Float64:
		switch rt.Type {
		case sqltypes.Decimal:
			c.asm.Convert_xd(2, 0, 0)
			c.asm.IntDiv_di()
		case sqltypes.Uint64:
			ct.Type = sqltypes.Uint64
			c.asm.Convert_xd(2, 0, 0)
			c.asm.Convert_xd(1, 0, 0)
			c.asm.IntDiv_du()
		default:
			c.asm.Convert_xd(2, 0, 0)
			c.asm.Convert_xd(1, 0, 0)
			c.asm.IntDiv_di()
		}
	case sqltypes.Decimal:
		switch rt.Type {
		case sqltypes.Decimal:
			c.asm.IntDiv_di()
		case sqltypes.Uint64:
			ct.Type = sqltypes.Uint64
			c.asm.Convert_xd(1, 0, 0)
			c.asm.IntDiv_du()
		default:
			c.asm.Convert_xd(1, 0, 0)
			c.asm.IntDiv_di()
		}
	}
	c.asm.jumpDestination(skip1, skip2)
	return ct, nil
}

func (op *opArithMod) eval(left, right eval) (eval, error) {
	return modNumericWithError(left, right, true)
}

func (op *opArithMod) String() string { return "DIV" }

func (op *opArithMod) compile(c *compiler, left, right Expr) (ctype, error) {
	lt, err := left.compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2, sqltypes.Float64)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64)

	ct := ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagNullable}
	switch lt.Type {
	case sqltypes.Int64:
		ct.Type = sqltypes.Int64
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Mod_ii()
		case sqltypes.Uint64:
			c.asm.Mod_iu()
		case sqltypes.Float64:
			ct.Type = sqltypes.Float64
			c.asm.Convert_xf(2)
			c.asm.Mod_ff()
		case sqltypes.Decimal:
			ct.Type = sqltypes.Decimal
			c.asm.Convert_xd(2, 0, 0)
			c.asm.Mod_dd()
		}
	case sqltypes.Uint64:
		ct.Type = sqltypes.Uint64
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Mod_ui()
		case sqltypes.Uint64:
			c.asm.Mod_uu()
		case sqltypes.Float64:
			ct.Type = sqltypes.Float64
			c.asm.Convert_xf(2)
			c.asm.Mod_ff()
		case sqltypes.Decimal:
			ct.Type = sqltypes.Decimal
			c.asm.Convert_xd(2, 0, 0)
			c.asm.Mod_dd()
		}
	case sqltypes.Decimal:
		ct.Type = sqltypes.Decimal
		switch rt.Type {
		case sqltypes.Float64:
			ct.Type = sqltypes.Float64
			c.asm.Convert_xf(2)
			c.asm.Mod_ff()
		default:
			c.asm.Convert_xd(1, 0, 0)
			c.asm.Mod_dd()
		}
	case sqltypes.Float64:
		ct.Type = sqltypes.Float64
		c.asm.Convert_xf(1)
		c.asm.Mod_ff()
	}

	c.asm.jumpDestination(skip1, skip2)
	return ct, nil
}

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

func (expr *NegateExpr) compile(c *compiler) (ctype, error) {
	arg, err := expr.Inner.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	arg = c.compileToNumeric(arg, 1, sqltypes.Float64)
	var neg sqltypes.Type

	switch arg.Type {
	case sqltypes.Int64:
		neg = sqltypes.Int64
		c.asm.Neg_i()
	case sqltypes.Uint64:
		if arg.Flag&flagHex != 0 {
			neg = sqltypes.Float64
			c.asm.Neg_hex()
		} else {
			neg = sqltypes.Int64
			c.asm.Neg_u()
		}
	case sqltypes.Float64:
		neg = sqltypes.Float64
		c.asm.Neg_f()
	case sqltypes.Decimal:
		neg = sqltypes.Decimal
		c.asm.Neg_d()
	default:
		panic("unexpected Numeric type")
	}

	c.asm.jumpDestination(skip)
	return ctype{Type: neg, Col: collationNumeric}, nil
}
