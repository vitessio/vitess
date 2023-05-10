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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	BitwiseExpr struct {
		BinaryExpr
		Op opBit
	}

	BitwiseNotExpr struct {
		UnaryExpr
	}

	opBit interface {
		BitwiseOp() string
	}

	opBitBinary interface {
		opBit
		numeric(left, right uint64) uint64
		binary(left, right []byte) []byte
	}

	opBitShift interface {
		opBit
		numeric(num, shift uint64) uint64
		binary(num []byte, shift uint64) []byte
	}

	opBitAnd struct{}
	opBitOr  struct{}
	opBitXor struct{}
	opBitShl struct{}
	opBitShr struct{}
)

var _ Expr = (*BitwiseExpr)(nil)
var _ Expr = (*BitwiseNotExpr)(nil)

func (b *BitwiseNotExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := b.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	if e, ok := e.(*evalBytes); ok && e.isBinary() && !e.isHexOrBitLiteral() {
		in := e.bytes
		out := make([]byte, len(in))
		for i := range in {
			out[i] = ^in[i]
		}
		return newEvalBinary(out), nil
	}

	eu := evalToInt64(e)
	return newEvalUint64(^uint64(eu.i)), nil
}

func (b *BitwiseNotExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	tt, f := b.Inner.typeof(env, fields)
	if tt == sqltypes.VarBinary && f&(flagHex|flagBit) == 0 {
		return sqltypes.VarBinary, f
	}
	return sqltypes.Uint64, f
}

func (expr *BitwiseNotExpr) compile(c *compiler) (ctype, error) {
	ct, err := expr.Inner.compile(c)
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

func (o opBitShr) BitwiseOp() string                { return ">>" }
func (o opBitShr) numeric(num, shift uint64) uint64 { return num >> shift }

func (o opBitShr) binary(num []byte, shift uint64) []byte {
	var (
		bits   = int(shift % 8)
		bytes  = int(shift / 8)
		length = len(num)
		out    = make([]byte, length)
	)

	for i := length - 1; i >= 0; i-- {
		switch {
		case i > bytes:
			out[i] = num[i-bytes-1] << (8 - bits)
			fallthrough
		case i == bytes:
			out[i] |= num[i-bytes] >> bits
		}
	}
	return out
}

func (o opBitShl) BitwiseOp() string                { return "<<" }
func (o opBitShl) numeric(num, shift uint64) uint64 { return num << shift }

func (o opBitShl) binary(num []byte, shift uint64) []byte {
	var (
		bits   = int(shift % 8)
		bytes  = int(shift / 8)
		length = len(num)
		out    = make([]byte, length)
	)

	for i := 0; i < length; i++ {
		pos := i + bytes + 1
		switch {
		case pos < length:
			out[i] = num[pos] >> (8 - bits)
			fallthrough
		case pos == length:
			out[i] |= num[pos-1] << bits
		}
	}
	return out
}

func (o opBitXor) numeric(left, right uint64) uint64 { return left ^ right }

func (o opBitXor) binary(left, right []byte) (out []byte) {
	out = make([]byte, len(left))
	for i := range out {
		out[i] = left[i] ^ right[i]
	}
	return
}

func (o opBitXor) BitwiseOp() string { return "^" }

func (o opBitOr) numeric(left, right uint64) uint64 { return left | right }

func (o opBitOr) binary(left, right []byte) (out []byte) {
	out = make([]byte, len(left))
	for i := range out {
		out[i] = left[i] | right[i]
	}
	return
}

func (o opBitOr) BitwiseOp() string { return "|" }

func (o opBitAnd) numeric(left, right uint64) uint64 { return left & right }

func (o opBitAnd) binary(left, right []byte) (out []byte) {
	out = make([]byte, len(left))
	for i := range out {
		out[i] = left[i] & right[i]
	}
	return
}

func (o opBitAnd) BitwiseOp() string { return "&" }

var errBitwiseOperandsLength = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Binary operands of bitwise operators must be of equal length")

func (bit *BitwiseExpr) eval(env *ExpressionEnv) (eval, error) {
	l, err := bit.Left.eval(env)
	if l == nil || err != nil {
		return nil, err
	}

	r, err := bit.Right.eval(env)
	if r == nil || err != nil {
		return nil, err
	}

	switch op := bit.Op.(type) {
	case opBitBinary:
		/*
			The result type depends on whether the arguments are evaluated as binary strings or numbers:
			Binary-string evaluation occurs when the arguments have a binary string type, and at least one of them is
			not a hexadecimal literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument
			conversion to unsigned 64-bit integers as necessary. Binary-string evaluation produces a binary string of
			the same length as the arguments. If the arguments have unequal lengths, an ER_INVALID_BITWISE_OPERANDS_SIZE
			error occurs. Numeric evaluation produces an unsigned 64-bit integer.
		*/
		if l, ok := l.(*evalBytes); ok && l.isBinary() {
			if r, ok := r.(*evalBytes); ok && r.isBinary() {
				if !l.isHexOrBitLiteral() || !r.isHexOrBitLiteral() {
					b1 := l.bytes
					b2 := r.bytes
					if len(b1) != len(b2) {
						return nil, errBitwiseOperandsLength
					}
					return newEvalBinary(op.binary(b1, b2)), nil
				}
			}
		}

		lu := evalToInt64(l)
		ru := evalToInt64(r)
		return newEvalUint64(op.numeric(uint64(lu.i), uint64(ru.i))), nil

	case opBitShift:
		/*
			The result type depends on whether the bit argument is evaluated as a binary string or number:
			Binary-string evaluation occurs when the bit argument has a binary string type, and is not a hexadecimal
			literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to an
			unsigned 64-bit integer as necessary.
		*/
		if l, ok := l.(*evalBytes); ok && l.isBinary() && !l.isHexOrBitLiteral() {
			ru := evalToInt64(r)
			return newEvalBinary(op.binary(l.bytes, uint64(ru.i))), nil
		}
		lu := evalToInt64(l)
		ru := evalToInt64(r)
		return newEvalUint64(op.numeric(uint64(lu.i), uint64(ru.i))), nil

	default:
		panic("unexpected bit operation")
	}
}

func (bit *BitwiseExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t1, f1 := bit.Left.typeof(env, fields)
	t2, f2 := bit.Right.typeof(env, fields)

	switch bit.Op.(type) {
	case opBitBinary:
		if t1 == sqltypes.VarBinary && t2 == sqltypes.VarBinary &&
			(f1&(flagHex|flagBit) == 0 || f2&(flagHex|flagBit) == 0) {
			return sqltypes.VarBinary, f1 | f2
		}
	case opBitShift:
		if t1 == sqltypes.VarBinary && (f1&(flagHex|flagBit)) == 0 {
			return sqltypes.VarBinary, f1 | f2
		}
	}

	return sqltypes.Uint64, f1 | f2
}

func (expr *BitwiseExpr) compileBinary(c *compiler, asm_ins_bb, asm_ins_uu func()) (ctype, error) {
	lt, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(lt)

	rt, err := expr.Right.compile(c)
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

func (expr *BitwiseExpr) compileShift(c *compiler, i int) (ctype, error) {
	lt, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(lt)

	rt, err := expr.Right.compile(c)
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

func (expr *BitwiseExpr) compile(c *compiler) (ctype, error) {
	switch expr.Op.(type) {
	case *opBitAnd:
		return expr.compileBinary(c, c.asm.BitOp_and_bb, c.asm.BitOp_and_uu)
	case *opBitOr:
		return expr.compileBinary(c, c.asm.BitOp_or_bb, c.asm.BitOp_or_uu)
	case *opBitXor:
		return expr.compileBinary(c, c.asm.BitOp_xor_bb, c.asm.BitOp_xor_uu)
	case *opBitShl:
		return expr.compileShift(c, -1)
	case *opBitShr:
		return expr.compileShift(c, 1)
	default:
		panic("unexpected arithmetic operator")
	}
}

var _ opBitBinary = (*opBitAnd)(nil)
var _ opBitBinary = (*opBitOr)(nil)
var _ opBitBinary = (*opBitXor)(nil)
var _ opBitShift = (*opBitShl)(nil)
var _ opBitShift = (*opBitShr)(nil)
