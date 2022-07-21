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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	BitwiseExpr struct {
		BinaryExpr
		Op BitwiseOp
	}

	BitwiseNotExpr struct {
		UnaryExpr
	}

	BitwiseOp interface {
		BitwiseOp() string
	}

	BitwiseBinaryOp interface {
		BitwiseOp
		numeric(left, right uint64) uint64
		binary(left, right []byte) []byte
	}

	BitwiseShiftOp interface {
		BitwiseOp
		numeric(num, shift uint64) uint64
		binary(num []byte, shift uint64) []byte
	}

	OpBitAnd        struct{}
	OpBitOr         struct{}
	OpBitXor        struct{}
	OpBitShiftLeft  struct{}
	OpBitShiftRight struct{}
)

func (b *BitwiseNotExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var inner EvalResult
	inner.init(env, b.Inner)

	if inner.isNull() {
		result.setNull()
		return
	}

	if inner.isBitwiseBinaryString() {
		in := inner.bytes()
		out := make([]byte, len(in))

		for i := range in {
			out[i] = ^in[i]
		}

		result.setRaw(sqltypes.VarBinary, out, collationBinary)
	} else {
		inner.makeUnsignedIntegral()
		result.setUint64(^inner.uint64())
	}
}

func (b *BitwiseNotExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	tt, f := b.Inner.typeof(env)
	if tt == sqltypes.VarBinary && f&(flagHex|flagBit) == 0 {
		return sqltypes.VarBinary, f
	}
	return sqltypes.Uint64, f
}

func (o OpBitShiftRight) BitwiseOp() string                { return ">>" }
func (o OpBitShiftRight) numeric(num, shift uint64) uint64 { return num >> shift }

func (o OpBitShiftRight) binary(num []byte, shift uint64) []byte {
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

func (o OpBitShiftLeft) BitwiseOp() string                { return "<<" }
func (o OpBitShiftLeft) numeric(num, shift uint64) uint64 { return num << shift }

func (o OpBitShiftLeft) binary(num []byte, shift uint64) []byte {
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

func (o OpBitXor) numeric(left, right uint64) uint64 { return left ^ right }

func (o OpBitXor) binary(left, right []byte) (out []byte) {
	out = make([]byte, len(left))
	for i := range out {
		out[i] = left[i] ^ right[i]
	}
	return
}

func (o OpBitXor) BitwiseOp() string { return "^" }

func (o OpBitOr) numeric(left, right uint64) uint64 { return left | right }

func (o OpBitOr) binary(left, right []byte) (out []byte) {
	out = make([]byte, len(left))
	for i := range out {
		out[i] = left[i] | right[i]
	}
	return
}

func (o OpBitOr) BitwiseOp() string { return "|" }

func (o OpBitAnd) numeric(left, right uint64) uint64 { return left & right }

func (o OpBitAnd) binary(left, right []byte) (out []byte) {
	out = make([]byte, len(left))
	for i := range out {
		out[i] = left[i] & right[i]
	}
	return
}

func (o OpBitAnd) BitwiseOp() string { return "&" }

func (bit *BitwiseExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var l, r EvalResult

	l.init(env, bit.Left)
	r.init(env, bit.Right)

	if l.isNull() || r.isNull() {
		result.setNull()
		return
	}

	switch op := bit.Op.(type) {
	case BitwiseBinaryOp:
		/*
			The result type depends on whether the arguments are evaluated as binary strings or numbers:
			Binary-string evaluation occurs when the arguments have a binary string type, and at least one of them is
			not a hexadecimal literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument
			conversion to unsigned 64-bit integers as necessary. Binary-string evaluation produces a binary string of
			the same length as the arguments. If the arguments have unequal lengths, an ER_INVALID_BITWISE_OPERANDS_SIZE
			error occurs. Numeric evaluation produces an unsigned 64-bit integer.
		*/
		if l.typeof() == sqltypes.VarBinary && r.typeof() == sqltypes.VarBinary && (!l.hasFlag(flagHex|flagBit) || !r.hasFlag(flagHex|flagBit)) {
			b1 := l.bytes()
			b2 := r.bytes()

			if len(b1) != len(b2) {
				throwEvalError(vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Binary operands of bitwise operators must be of equal length"))
			}
			result.setRaw(sqltypes.VarBinary, op.binary(b1, b2), collationBinary)
		} else {
			l.makeUnsignedIntegral()
			r.makeUnsignedIntegral()
			result.setUint64(op.numeric(l.uint64(), r.uint64()))
		}

	case BitwiseShiftOp:
		/*
			The result type depends on whether the bit argument is evaluated as a binary string or number:
			Binary-string evaluation occurs when the bit argument has a binary string type, and is not a hexadecimal
			literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to an
			unsigned 64-bit integer as necessary.
		*/
		if l.isBitwiseBinaryString() {
			r.makeUnsignedIntegral()
			result.setRaw(sqltypes.VarBinary, op.binary(l.bytes(), r.uint64()), collationBinary)
		} else {
			l.makeUnsignedIntegral()
			r.makeUnsignedIntegral()
			result.setUint64(op.numeric(l.uint64(), r.uint64()))
		}
	}
}

func (bit *BitwiseExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	t1, f1 := bit.Left.typeof(env)
	t2, f2 := bit.Right.typeof(env)

	switch bit.Op.(type) {
	case BitwiseBinaryOp:
		if t1 == sqltypes.VarBinary && t2 == sqltypes.VarBinary &&
			(f1&(flagHex|flagBit) == 0 || f2&(flagHex|flagBit) == 0) {
			return sqltypes.VarBinary, f1 | f2
		}
	case BitwiseShiftOp:
		if t1 == sqltypes.VarBinary && (f1&(flagHex|flagBit)) == 0 {
			return sqltypes.VarBinary, f1 | f2
		}
	}

	return sqltypes.Uint64, f1 | f2
}

var _ BitwiseBinaryOp = (*OpBitAnd)(nil)
var _ BitwiseBinaryOp = (*OpBitOr)(nil)
var _ BitwiseBinaryOp = (*OpBitXor)(nil)
var _ BitwiseShiftOp = (*OpBitShiftLeft)(nil)
var _ BitwiseShiftOp = (*OpBitShiftRight)(nil)
