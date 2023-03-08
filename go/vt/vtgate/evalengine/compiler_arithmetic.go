package evalengine

import "vitess.io/vitess/go/sqltypes"

func (c *compiler) compileNegate(expr *NegateExpr) (ctype, error) {
	arg, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.asm.jumpFrom()
	c.asm.NullCheck1(skip)

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
	default:
		panic("unexpected arithmetic operator")
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

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip := c.asm.jumpFrom()
	c.asm.NullCheck2(skip)
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

	c.asm.jumpDestination(skip)
	return ctype{Type: sumtype, Col: collationNumeric}, nil
}

func (c *compiler) compileArithmeticSub(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip := c.asm.jumpFrom()
	c.asm.NullCheck2(skip)
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

	c.asm.jumpDestination(skip)
	return ctype{Type: subtype, Col: collationNumeric}, nil
}

func (c *compiler) compileArithmeticMul(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip := c.asm.jumpFrom()
	c.asm.NullCheck2(skip)
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

	c.asm.jumpDestination(skip)
	return ctype{Type: multype, Col: collationNumeric}, nil
}

func (c *compiler) compileArithmeticDiv(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip := c.asm.jumpFrom()
	c.asm.NullCheck2(skip)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)

	if lt.Type == sqltypes.Float64 || rt.Type == sqltypes.Float64 {
		c.compileToFloat(lt, 2)
		c.compileToFloat(rt, 1)
		c.asm.Div_ff()
		return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
	} else {
		c.compileToDecimal(lt, 2)
		c.compileToDecimal(rt, 1)
		c.asm.Div_dd()
		return ctype{Type: sqltypes.Decimal, Col: collationNumeric}, nil
	}
}
