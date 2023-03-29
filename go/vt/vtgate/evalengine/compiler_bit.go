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

import "vitess.io/vitess/go/sqltypes"

func (c *compiler) compileBitwise(expr *BitwiseExpr) (ctype, error) {
	switch expr.Op.(type) {
	case *opBitAnd:
		return c.compileBitwiseOp(expr.Left, expr.Right, c.asm.BitOp_and_bb, c.asm.BitOp_and_uu)
	case *opBitOr:
		return c.compileBitwiseOp(expr.Left, expr.Right, c.asm.BitOp_or_bb, c.asm.BitOp_or_uu)
	case *opBitXor:
		return c.compileBitwiseOp(expr.Left, expr.Right, c.asm.BitOp_xor_bb, c.asm.BitOp_xor_uu)
	case *opBitShl:
		return c.compileBitwiseShift(expr.Left, expr.Right, -1)
	case *opBitShr:
		return c.compileBitwiseShift(expr.Left, expr.Right, 1)
	default:
		panic("unexpected arithmetic operator")
	}
}

func (c *compiler) compileBitwiseOp(left Expr, right Expr, asm_ins_bb, asm_ins_uu func()) (ctype, error) {
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

	if lt.Type == sqltypes.VarBinary && rt.Type == sqltypes.VarBinary {
		if !lt.isHexOrBitLiteral() || !rt.isHexOrBitLiteral() {
			asm_ins_bb()
			c.asm.jumpDestination(skip1, skip2)
			return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
		}
	}

	lt = c.compileToBitwiseUint64(lt, 2)
	rt = c.compileToBitwiseUint64(rt, 1)

	asm_ins_uu()
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.Uint64, Col: collationNumeric}, nil
}

func (c *compiler) compileBitwiseShift(left Expr, right Expr, i int) (ctype, error) {
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

	if lt.Type == sqltypes.VarBinary && !lt.isHexOrBitLiteral() {
		_ = c.compileToUint64(rt, 1)
		if i < 0 {
			c.asm.BitShiftLeft_bu()
		} else {
			c.asm.BitShiftRight_bu()
		}
		c.asm.jumpDestination(skip1, skip2)
		return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
	}

	_ = c.compileToBitwiseUint64(lt, 2)
	_ = c.compileToUint64(rt, 1)

	if i < 0 {
		c.asm.BitShiftLeft_uu()
	} else {
		c.asm.BitShiftRight_uu()
	}

	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.Uint64, Col: collationNumeric}, nil
}

func (c *compiler) compileFn_BIT_COUNT(expr *builtinBitCount) (ctype, error) {
	ct, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(ct)

	if ct.Type == sqltypes.VarBinary && !ct.isHexOrBitLiteral() {
		c.asm.BitCount_b()
		c.asm.jumpDestination(skip)
		return ctype{Type: sqltypes.Int64, Col: collationBinary}, nil
	}

	_ = c.compileToBitwiseUint64(ct, 1)
	c.asm.BitCount_u()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationBinary}, nil
}

func (c *compiler) compileBitwiseNot(expr *BitwiseNotExpr) (ctype, error) {
	ct, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(ct)

	if ct.Type == sqltypes.VarBinary && !ct.isHexOrBitLiteral() {
		c.asm.BitwiseNot_b()
		c.asm.jumpDestination(skip)
		return ct, nil
	}

	ct = c.compileToBitwiseUint64(ct, 1)
	c.asm.BitwiseNot_u()
	c.asm.jumpDestination(skip)
	return ct, nil
}
