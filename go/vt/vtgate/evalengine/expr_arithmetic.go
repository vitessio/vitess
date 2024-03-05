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
		compile(c *compiler, left, right IR) (ctype, error)
		String() string
	}

	opArithAdd    struct{}
	opArithSub    struct{}
	opArithMul    struct{}
	opArithDiv    struct{}
	opArithIntDiv struct{}
	opArithMod    struct{}
)

var _ IR = (*ArithmeticExpr)(nil)

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

func (b *ArithmeticExpr) compile(c *compiler) (ctype, error) {
	return b.Op.compile(c, b.Left, b.Right)
}

func (op *opArithAdd) eval(left, right eval) (eval, error) {
	return addNumericWithError(left, right)
}
func (op *opArithAdd) String() string { return "+" }

func (op *opArithAdd) compile(c *compiler, left, right IR) (ctype, error) {
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

	lt = c.compileToNumeric(lt, 2, sqltypes.Float64, true)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64, true)
	lt, rt, swap = c.compileNumericPriority(lt, rt)

	ct := ctype{Flag: nullableFlags(lt.Flag | rt.Flag), Col: collationNumeric}

	switch lt.Type {
	case sqltypes.Int64:
		c.asm.Add_ii()
		ct.Type = sqltypes.Int64
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Add_ui(swap)
		case sqltypes.Uint64:
			c.asm.Add_uu()
		}
		ct.Type = sqltypes.Uint64
	case sqltypes.Decimal:
		if swap {
			c.compileToDecimal(rt, 2)
		} else {
			c.compileToDecimal(rt, 1)
		}
		c.asm.Add_dd()
		ct.Type = sqltypes.Decimal
		ct.Scale = max(lt.Scale, rt.Scale)
	case sqltypes.Float64:
		if swap {
			c.compileToFloat(rt, 2)
		} else {
			c.compileToFloat(rt, 1)
		}
		c.asm.Add_ff()
		ct.Type = sqltypes.Float64
	}

	c.asm.jumpDestination(skip1, skip2)
	return ct, nil
}

func (op *opArithSub) eval(left, right eval) (eval, error) {
	return subtractNumericWithError(left, right)
}
func (op *opArithSub) String() string { return "-" }

func (op *opArithSub) compile(c *compiler, left, right IR) (ctype, error) {
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
	lt = c.compileToNumeric(lt, 2, sqltypes.Float64, true)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64, true)

	ct := ctype{Flag: nullableFlags(lt.Flag | rt.Flag), Col: collationNumeric}
	switch lt.Type {
	case sqltypes.Int64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Sub_ii()
			ct.Type = sqltypes.Int64
		case sqltypes.Uint64:
			c.asm.Sub_iu()
			ct.Type = sqltypes.Uint64
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.asm.Sub_ff()
			ct.Type = sqltypes.Float64
		case sqltypes.Decimal:
			c.compileToDecimal(lt, 2)
			c.asm.Sub_dd()
			ct.Type = sqltypes.Decimal
			ct.Scale = max(lt.Scale, rt.Scale)
		}
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Sub_ui()
			ct.Type = sqltypes.Uint64
		case sqltypes.Uint64:
			c.asm.Sub_uu()
			ct.Type = sqltypes.Uint64
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.asm.Sub_ff()
			ct.Type = sqltypes.Float64
		case sqltypes.Decimal:
			c.compileToDecimal(lt, 2)
			c.asm.Sub_dd()
			ct.Type = sqltypes.Decimal
			ct.Scale = max(lt.Scale, rt.Scale)
		}
	case sqltypes.Float64:
		c.compileToFloat(rt, 1)
		c.asm.Sub_ff()
		ct.Type = sqltypes.Float64
	case sqltypes.Decimal:
		switch rt.Type {
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.asm.Sub_ff()
			ct.Type = sqltypes.Float64
		default:
			c.compileToDecimal(rt, 1)
			c.asm.Sub_dd()
			ct.Type = sqltypes.Decimal
			ct.Scale = max(lt.Scale, rt.Scale)
		}
	}

	if ct.Type == 0 {
		panic("did not compile?")
	}

	c.asm.jumpDestination(skip1, skip2)
	return ct, nil
}

func (op *opArithMul) eval(left, right eval) (eval, error) {
	return multiplyNumericWithError(left, right)
}

func (op *opArithMul) String() string { return "*" }

func (op *opArithMul) compile(c *compiler, left, right IR) (ctype, error) {
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
	lt = c.compileToNumeric(lt, 2, sqltypes.Float64, true)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64, true)
	lt, rt, swap = c.compileNumericPriority(lt, rt)

	ct := ctype{Flag: nullableFlags(lt.Flag | rt.Flag), Col: collationNumeric}
	switch lt.Type {
	case sqltypes.Int64:
		c.asm.Mul_ii()
		ct.Type = sqltypes.Int64
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.Mul_ui(swap)
		case sqltypes.Uint64:
			c.asm.Mul_uu()
		}
		ct.Type = sqltypes.Uint64
	case sqltypes.Float64:
		if swap {
			c.compileToFloat(rt, 2)
		} else {
			c.compileToFloat(rt, 1)
		}
		c.asm.Mul_ff()
		ct.Type = sqltypes.Float64
	case sqltypes.Decimal:
		if swap {
			c.compileToDecimal(rt, 2)
		} else {
			c.compileToDecimal(rt, 1)
		}
		c.asm.Mul_dd()
		ct.Type = sqltypes.Decimal
		ct.Scale = lt.Scale + rt.Scale
	}

	c.asm.jumpDestination(skip1, skip2)
	return ct, nil
}

func (op *opArithDiv) eval(left, right eval) (eval, error) {
	return divideNumericWithError(left, right, true)
}

func (op *opArithDiv) String() string { return "/" }

func (op *opArithDiv) compile(c *compiler, left, right IR) (ctype, error) {
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

	lt = c.compileToNumeric(lt, 2, sqltypes.Float64, true)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64, true)

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
		ct.Scale = lt.Scale + divPrecisionIncrement
	}
	c.asm.jumpDestination(skip1, skip2)
	return ct, nil
}

func (op *opArithIntDiv) eval(left, right eval) (eval, error) {
	return integerDivideNumericWithError(left, right)
}

func (op *opArithIntDiv) String() string { return "DIV" }

func (op *opArithIntDiv) compile(c *compiler, left, right IR) (ctype, error) {
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
	lt = c.compileToNumeric(lt, 2, sqltypes.Decimal, true)
	rt = c.compileToNumeric(rt, 1, sqltypes.Decimal, true)

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

func (op *opArithMod) compile(c *compiler, left, right IR) (ctype, error) {
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
	lt = c.compileToNumeric(lt, 2, sqltypes.Float64, true)
	rt = c.compileToNumeric(rt, 1, sqltypes.Float64, true)

	ct := ctype{Col: collationNumeric, Flag: flagNullable}
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
			ct.Scale = max(lt.Scale, rt.Scale)
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
			ct.Scale = max(lt.Scale, rt.Scale)
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
	return evalToNumeric(e, false).negate(), nil
}

func (expr *NegateExpr) compile(c *compiler) (ctype, error) {
	arg, err := expr.Inner.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	arg = c.compileToNumeric(arg, 1, sqltypes.Float64, false)
	var neg sqltypes.Type

	switch arg.Type {
	case sqltypes.Int64:
		if arg.Flag&flagBit != 0 {
			neg = sqltypes.Float64
			c.asm.Neg_bit()
		} else {
			neg = sqltypes.Int64
			c.asm.Neg_i()
		}
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
	return ctype{
		Type:  neg,
		Flag:  nullableFlags(arg.Flag),
		Size:  arg.Size,
		Scale: arg.Scale,
		Col:   collationNumeric,
	}, nil
}

func nullableFlags(flag typeFlag) typeFlag {
	return flag & (flagNull | flagNullable)
}
