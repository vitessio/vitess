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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func (c *compiler) compileNegate(expr *NegateExpr) (ctype, error) {
	arg, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	arg = c.compileToNumeric(arg, 1)
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

func (c *compiler) compileArithmetic(expr *ArithmeticExpr) (ctype, error) {
	switch expr.Op.(type) {
	case *opArithAdd:
		return c.compileArithmeticAdd(expr.Left, expr.Right)
	case *opArithSub:
		return c.compileArithmeticSub(expr.Left, expr.Right)
	case *opArithMul:
		return c.compileArithmeticMul(expr.Left, expr.Right)
	case *opArithDiv:
		return c.compileArithmeticDiv(expr.Left, expr.Right)
	case *opArithIntDiv:
		return c.compileArithmeticIntDiv(expr.Left, expr.Right)
	case *opArithMod:
		return c.compileArithmeticMod(expr.Left, expr.Right)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "not implemented")
	}
}

func (c *compiler) compileNumericPriority(lt, rt ctype) (ctype, ctype, bool) {
	switch lt.Type {
	case sqltypes.Int64:
		if rt.Type == sqltypes.Uint64 || rt.Type == sqltypes.Float64 || rt.Type == sqltypes.Decimal {
			return rt, lt, true
		}
	case sqltypes.Uint64:
		if rt.Type == sqltypes.Float64 || rt.Type == sqltypes.Decimal {
			return rt, lt, true
		}
	case sqltypes.Decimal:
		if rt.Type == sqltypes.Float64 {
			return rt, lt, true
		}
	}
	return lt, rt, false
}

func (c *compiler) compileArithmeticAdd(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip2 := c.compileNullCheck1r(rt)

	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)
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

func (c *compiler) compileArithmeticSub(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)

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

func (c *compiler) compileArithmeticMul(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)
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

func (c *compiler) compileArithmeticDiv(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}
	skip2 := c.compileNullCheck1r(rt)

	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)

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

func (c *compiler) compileArithmeticIntDiv(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)

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

func (c *compiler) compileArithmeticMod(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(lt)

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(rt)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)

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
