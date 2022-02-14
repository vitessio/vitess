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

	BitWiseShiftOp interface {
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

	if inner.null() {
		result.setNull()
		return
	}

	if inner.bitwiseBinaryString() {
		in := inner.bytes()
		out := make([]byte, len(in))

		for i := range in {
			out[i] = ^in[i]
		}

		result.setRaw(sqltypes.VarBinary, out, collationBinary)
	} else {
		inner.makeIntegral()
		result.setUint64(^inner.uint64())
	}
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

	if l.null() || r.null() {
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
			l.makeIntegral()
			r.makeIntegral()
			result.setUint64(op.numeric(l.uint64(), r.uint64()))
		}

	case BitWiseShiftOp:
		/*
			The result type depends on whether the bit argument is evaluated as a binary string or number:
			Binary-string evaluation occurs when the bit argument has a binary string type, and is not a hexadecimal
			literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to an
			unsigned 64-bit integer as necessary.
		*/
		if l.bitwiseBinaryString() {
			r.makeIntegral()
			result.setRaw(sqltypes.VarBinary, op.binary(l.bytes(), r.uint64()), collationBinary)
		} else {
			l.makeIntegral()
			r.makeIntegral()
			result.setUint64(op.numeric(l.uint64(), r.uint64()))
		}
	}
}

func (bit *BitwiseExpr) typeof(env *ExpressionEnv) sqltypes.Type {
	return sqltypes.Uint64
}

var _ BitwiseBinaryOp = (*OpBitAnd)(nil)
var _ BitwiseBinaryOp = (*OpBitOr)(nil)
var _ BitwiseBinaryOp = (*OpBitXor)(nil)
var _ BitWiseShiftOp = (*OpBitShiftLeft)(nil)
var _ BitWiseShiftOp = (*OpBitShiftRight)(nil)
