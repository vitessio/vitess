package evalengine

import (
	"bytes"
	"math"
	"math/bits"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
)

func cmpnum[N interface{ int64 | uint64 | float64 }](a, b N) int {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}

func (c *compiler) emit(f frame, asm string, args ...any) {
	if c.log != nil {
		c.log.Disasm(asm, args...)
	}
	c.ins = append(c.ins, f)
}

func (c *compiler) emitPushLiteral(lit eval) error {
	c.adjustStack(1)

	if lit == evalBoolTrue {
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = evalBoolTrue
			vm.sp++
			return 1
		}, "PUSH true")
		return nil
	}

	if lit == evalBoolFalse {
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = evalBoolFalse
			vm.sp++
			return 1
		}, "PUSH false")
		return nil
	}

	switch lit := lit.(type) {
	case *evalInt64:
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalInt64(lit.i)
			vm.sp++
			return 1
		}, "PUSH INT64(%s)", lit.ToRawBytes())
	case *evalUint64:
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalUint64(lit.u)
			vm.sp++
			return 1
		}, "PUSH UINT64(%s)", lit.ToRawBytes())
	case *evalFloat:
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalFloat(lit.f)
			vm.sp++
			return 1
		}, "PUSH FLOAT64(%s)", lit.ToRawBytes())
	case *evalDecimal:
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalDecimalWithPrec(lit.dec, lit.length)
			vm.sp++
			return 1
		}, "PUSH DECIMAL(%s)", lit.ToRawBytes())
	case *evalBytes:
		c.emit(func(vm *VirtualMachine) int {
			b := vm.arena.newEvalBytesEmpty()
			*b = *lit
			vm.stack[vm.sp] = b
			vm.sp++
			return 1
		}, "PUSH VARCHAR(%q)", lit.ToRawBytes())
	default:
		return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported literal kind '%T'", lit)
	}

	return nil
}

func (c *compiler) emitSetBool(offset int, b bool) {
	if b {
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp-offset] = evalBoolTrue
			return 1
		}, "SET (SP-%d), BOOL(true)", offset)
	} else {
		c.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp-offset] = evalBoolFalse
			return 1
		}, "SET (SP-%d), BOOL(false)", offset)
	}
}

func (c *compiler) emitPushNull() {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = nil
		vm.sp++
		return 1
	}, "PUSH NULL")
}

func (c *compiler) emitPushColumn_i(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		var ival int64
		ival, vm.err = vm.row[offset].ToInt64()
		vm.stack[vm.sp] = vm.arena.newEvalInt64(ival)
		vm.sp++
		return 1
	}, "PUSH INT64(:%d)", offset)
}

func (c *compiler) emitPushColumn_u(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		var uval uint64
		uval, vm.err = vm.row[offset].ToUint64()
		vm.stack[vm.sp] = vm.arena.newEvalUint64(uval)
		vm.sp++
		return 1
	}, "PUSH UINT64(:%d)", offset)
}

func (c *compiler) emitPushColumn_f(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		var fval float64
		fval, vm.err = vm.row[offset].ToFloat64()
		vm.stack[vm.sp] = vm.arena.newEvalFloat(fval)
		vm.sp++
		return 1
	}, "PUSH FLOAT64(:%d)", offset)
}

func (c *compiler) emitPushColumn_d(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		var dec decimal.Decimal
		dec, vm.err = decimal.NewFromMySQL(vm.row[offset].Raw())
		vm.stack[vm.sp] = vm.arena.newEvalDecimal(dec, 0, 0)
		vm.sp++
		return 1
	}, "PUSH DECIMAL(:%d)", offset)
}

func (c *compiler) emitPushColumn_hexnum(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		var raw []byte
		raw, vm.err = parseHexNumber(vm.row[offset].Raw())
		vm.stack[vm.sp] = newEvalBytesHex(raw)
		vm.sp++
		return 1
	}, "PUSH HEXNUM(:%d)", offset)
}

func (c *compiler) emitPushColumn_hexval(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		hex := vm.row[offset].Raw()
		var raw []byte
		raw, vm.err = parseHexLiteral(hex[2 : len(hex)-1])
		vm.stack[vm.sp] = newEvalBytesHex(raw)
		vm.sp++
		return 1
	}, "PUSH HEXVAL(:%d)", offset)
}

func (c *compiler) emitPushColumn_text(offset int, col collations.TypedCollation) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalText(vm.row[offset].Raw(), col)
		vm.sp++
		return 1
	}, "PUSH VARCHAR(:%d) COLLATE %d", offset, col.Collation)
}

func (c *compiler) emitPushColumn_bin(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBinary(vm.row[offset].Raw())
		vm.sp++
		return 1
	}, "PUSH VARBINARY(:%d)", offset)
}

func (c *compiler) emitPushColumn_json(offset int) {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		var parser json.Parser
		vm.stack[vm.sp], vm.err = parser.ParseBytes(vm.row[offset].Raw())
		vm.sp++
		return 1
	}, "PUSH JSON(:%d)", offset)
}

func (c *compiler) emitConvert_hex(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		var ok bool
		vm.stack[vm.sp-offset], ok = vm.stack[vm.sp-offset].(*evalBytes).toNumericHex()
		if !ok {
			vm.err = errDeoptimize
		}
		return 1
	}, "CONV VARBINARY(SP-%d), HEX", offset)
}

func (c *compiler) emitConvert_if(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalInt64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalFloat(arg.toFloat0())
		return 1
	}, "CONV INT64(SP-%d), FLOAT64", offset)
}

func (c *compiler) emitConvert_uf(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalUint64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalFloat(arg.toFloat0())
		return 1
	}, "CONV UINT64(SP-%d), FLOAT64)", offset)
}

func (c *compiler) emitConvert_xf(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp-offset], _ = evalToNumeric(vm.stack[vm.sp-offset]).toFloat()
		return 1
	}, "CONV (SP-%d), FLOAT64", offset)
}

func (c *compiler) emitConvert_id(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalInt64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalDecimalWithPrec(decimal.NewFromInt(arg.i), 0)
		return 1
	}, "CONV INT64(SP-%d), FLOAT64", offset)
}

func (c *compiler) emitConvert_ud(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalUint64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalDecimalWithPrec(decimal.NewFromUint(arg.u), 0)
		return 1
	}, "CONV UINT64(SP-%d), FLOAT64)", offset)
}

func (c *compiler) emitConvert_xd(offset int, m, d int32) {
	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp-offset] = evalToNumeric(vm.stack[vm.sp-offset]).toDecimal(m, d)
		return 1
	}, "CONV (SP-%d), FLOAT64", offset)
}

func (c *compiler) emitParse_j(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		var p json.Parser
		arg := vm.stack[vm.sp-offset].(*evalBytes)
		vm.stack[vm.sp-offset], vm.err = p.ParseBytes(arg.bytes)
		return 1
	}, "PARSE_JSON VARCHAR(SP-%d)", offset)
}

func (c *compiler) emitAdd_ii() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.i, vm.err = mathAdd_ii0(l.i, r.i)
		vm.sp--
		return 1
	}, "ADD INT64(SP-2), INT64(SP-1)")
}

func (c *compiler) emitAdd_ui(swap bool) {
	c.adjustStack(-1)

	if swap {
		c.emit(func(vm *VirtualMachine) int {
			var u uint64
			l := vm.stack[vm.sp-1].(*evalUint64)
			r := vm.stack[vm.sp-2].(*evalInt64)
			u, vm.err = mathAdd_ui0(l.u, r.i)
			vm.stack[vm.sp-2] = vm.arena.newEvalUint64(u)
			vm.sp--
			return 1
		}, "ADD UINT64(SP-1), INT64(SP-2)")
	} else {
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalInt64)
			l.u, vm.err = mathAdd_ui0(l.u, r.i)
			vm.sp--
			return 1
		}, "ADD UINT64(SP-2), INT64(SP-1)")
	}
}

func (c *compiler) emitAdd_uu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u, vm.err = mathAdd_uu0(l.u, r.u)
		vm.sp--
		return 1
	}, "ADD UINT64(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitAdd_ff() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		l.f += r.f
		vm.sp--
		return 1
	}, "ADD FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (c *compiler) emitAdd_dd() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		mathAdd_dd0(l, r)
		vm.sp--
		return 1
	}, "ADD DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (c *compiler) emitSub_ii() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.i, vm.err = mathSub_ii0(l.i, r.i)
		vm.sp--
		return 1
	}, "SUB INT64(SP-2), INT64(SP-1)")
}

func (c *compiler) emitSub_iu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		r.u, vm.err = mathSub_iu0(l.i, r.u)
		vm.stack[vm.sp-2] = r
		vm.sp--
		return 1
	}, "SUB INT64(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitSub_ff() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		l.f -= r.f
		vm.sp--
		return 1
	}, "SUB FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (c *compiler) emitSub_dd() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		mathSub_dd0(l, r)
		vm.sp--
		return 1
	}, "SUB DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (c *compiler) emitSub_ui() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.u, vm.err = mathSub_ui0(l.u, r.i)
		vm.sp--
		return 1
	}, "SUB UINT64(SP-2), INT64(SP-1)")
}

func (c *compiler) emitSub_uu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u, vm.err = mathSub_uu0(l.u, r.u)
		vm.sp--
		return 1
	}, "SUB UINT64(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitMul_ii() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.i, vm.err = mathMul_ii0(l.i, r.i)
		vm.sp--
		return 1
	}, "MUL INT64(SP-2), INT64(SP-1)")
}

func (c *compiler) emitMul_ui(swap bool) {
	c.adjustStack(-1)

	if swap {
		c.emit(func(vm *VirtualMachine) int {
			var u uint64
			l := vm.stack[vm.sp-1].(*evalUint64)
			r := vm.stack[vm.sp-2].(*evalInt64)
			u, vm.err = mathMul_ui0(l.u, r.i)
			vm.stack[vm.sp-2] = vm.arena.newEvalUint64(u)
			vm.sp--
			return 1
		}, "MUL UINT64(SP-1), INT64(SP-2)")
	} else {
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalInt64)
			l.u, vm.err = mathMul_ui0(l.u, r.i)
			vm.sp--
			return 1
		}, "MUL UINT64(SP-2), INT64(SP-1)")
	}
}

func (c *compiler) emitMul_uu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u, vm.err = mathMul_uu0(l.u, r.u)
		vm.sp--
		return 1
	}, "MUL UINT64(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitMul_ff() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		l.f *= r.f
		vm.sp--
		return 1
	}, "MUL FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (c *compiler) emitMul_dd() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		mathMul_dd0(l, r)
		vm.sp--
		return 1
	}, "MUL DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (c *compiler) emitDiv_ff() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		if r.f == 0.0 {
			vm.stack[vm.sp-2] = nil
		} else {
			l.f, vm.err = mathDiv_ff0(l.f, r.f)
		}
		vm.sp--
		return 1
	}, "DIV FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (c *compiler) emitDiv_dd() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		if r.dec.IsZero() {
			vm.stack[vm.sp-2] = nil
		} else {
			mathDiv_dd0(l, r, divPrecisionIncrement)
		}
		vm.sp--
		return 1
	}, "DIV DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (c *compiler) emitNullCmp(j *jump) {
	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2]
		r := vm.stack[vm.sp-1]
		if l == nil || r == nil {
			if l == r {
				vm.flags.cmp = 0
			} else {
				vm.flags.cmp = 1
			}
			vm.sp -= 2
			return j.offset()
		}
		return 1
	}, "NULLCMP SP-1, SP-2")
}

func (c *compiler) emitNullCheck1(j *jump) {
	c.emit(func(vm *VirtualMachine) int {
		if vm.stack[vm.sp-1] == nil {
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1")
}

func (c *compiler) emitNullCheck2(j *jump) {
	c.emit(func(vm *VirtualMachine) int {
		if vm.stack[vm.sp-2] == nil || vm.stack[vm.sp-1] == nil {
			vm.stack[vm.sp-2] = nil
			vm.sp--
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1, SP-2")
}

func (c *compiler) emitCmp_eq() {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp == 0)
		vm.sp++
		return 1
	}, "CMPFLAG EQ")
}

func (c *compiler) emitCmp_ne() {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp != 0)
		vm.sp++
		return 1
	}, "CMPFLAG NE")
}

func (c *compiler) emitCmp_lt() {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp < 0)
		vm.sp++
		return 1
	}, "CMPFLAG LT")
}

func (c *compiler) emitCmp_le() {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp <= 0)
		vm.sp++
		return 1
	}, "CMPFLAG LE")
}

func (c *compiler) emitCmp_gt() {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp > 0)
		vm.sp++
		return 1
	}, "CMPFLAG GT")
}

func (c *compiler) emitCmp_ge() {
	c.adjustStack(1)

	c.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp >= 0)
		vm.sp++
		return 1
	}, "CMPFLAG GE")
}

func (c *compiler) emitCmpNum_ii() {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(l.i, r.i)
		return 1
	}, "CMP INT64(SP-2), INT64(SP-1)")
}

func (c *compiler) emitCmpNum_iu(left, right int) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalInt64)
		r := vm.stack[vm.sp-right].(*evalUint64)
		vm.sp -= 2
		if l.i < 0 {
			vm.flags.cmp = -1
		} else {
			vm.flags.cmp = cmpnum(uint64(l.i), r.u)
		}
		return 1
	}, "CMP INT64(SP-%d), UINT64(SP-%d)", left, right)
}

func (c *compiler) emitCmpNum_if(left, right int) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalInt64)
		r := vm.stack[vm.sp-right].(*evalFloat)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(float64(l.i), r.f)
		return 1
	}, "CMP INT64(SP-%d), FLOAT64(SP-%d)", left, right)
}

func (c *compiler) emitCmpNum_id(left, right int) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalInt64)
		r := vm.stack[vm.sp-right].(*evalDecimal)
		vm.sp -= 2
		vm.flags.cmp = decimal.NewFromInt(l.i).Cmp(r.dec)
		return 1
	}, "CMP INT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (c *compiler) emitCmpNum_uu() {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(l.u, r.u)
		return 1
	}, "CMP UINT64(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitCmpNum_uf(left, right int) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalUint64)
		r := vm.stack[vm.sp-right].(*evalFloat)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(float64(l.u), r.f)
		return 1
	}, "CMP UINT64(SP-%d), FLOAT64(SP-%d)", left, right)
}

func (c *compiler) emitCmpNum_ud(left, right int) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalUint64)
		r := vm.stack[vm.sp-right].(*evalDecimal)
		vm.sp -= 2
		vm.flags.cmp = decimal.NewFromUint(l.u).Cmp(r.dec)
		return 1
	}, "CMP UINT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (c *compiler) emitCmpNum_ff() {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(l.f, r.f)
		return 1
	}, "CMP FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (c *compiler) emitCmpNum_fd(left, right int) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalFloat)
		r := vm.stack[vm.sp-right].(*evalDecimal)
		vm.sp -= 2
		fval, ok := r.dec.Float64()
		if !ok {
			vm.err = errDecimalOutOfRange
		}
		vm.flags.cmp = cmpnum(l.f, fval)
		return 1
	}, "CMP FLOAT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (c *compiler) emitCmpNum_dd() {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		vm.sp -= 2
		vm.flags.cmp = l.dec.Cmp(r.dec)
		return 1
	}, "CMP DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (c *compiler) emitCmpString_collate(collation collations.Collation) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalBytes)
		r := vm.stack[vm.sp-1].(*evalBytes)
		vm.sp -= 2
		vm.flags.cmp = collation.Collate(l.bytes, r.bytes, false)
		return 1
	}, "CMP VARCHAR(SP-2), VARCHAR(SP-1) COLLATE '%s'", collation.Name())
}

func (c *compiler) emitCmpString_coerce(coercion *compiledCoercion) {
	c.adjustStack(-2)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalBytes)
		r := vm.stack[vm.sp-1].(*evalBytes)
		vm.sp -= 2

		var bl, br []byte
		bl, vm.err = coercion.left(nil, l.bytes)
		if vm.err != nil {
			return 0
		}
		br, vm.err = coercion.right(nil, r.bytes)
		if vm.err != nil {
			return 0
		}
		vm.flags.cmp = coercion.col.Collate(bl, br, false)
		return 1
	}, "CMP VARCHAR(SP-2), VARCHAR(SP-1) COERCE AND COLLATE '%s'", coercion.col.Name())
}

func (c *compiler) emitCollate(col collations.ID) {
	c.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalBytes)
		a.tt = int16(sqltypes.VarChar)
		a.col.Collation = col
		return 1
	}, "COLLATE VARCHAR(SP-1), %d", col)
}

func (c *compiler) emitFn_JSON_UNQUOTE() {
	c.emit(func(vm *VirtualMachine) int {
		j := vm.stack[vm.sp-1].(*evalJSON)
		b := vm.arena.newEvalBytesEmpty()
		b.tt = int16(sqltypes.Blob)
		b.col = collationJSON
		if jbytes, ok := j.StringBytes(); ok {
			b.bytes = jbytes
		} else {
			b.bytes = j.MarshalTo(nil)
		}
		vm.stack[vm.sp-1] = b
		return 1
	}, "FN JSON_UNQUOTE (SP-1)")
}

func (c *compiler) emitFn_JSON_EXTRACT0(jp []*json.Path) {
	multi := len(jp) > 1 || slices2.Any(jp, func(path *json.Path) bool { return path.ContainsWildcards() })

	if multi {
		c.emit(func(vm *VirtualMachine) int {
			matches := make([]*json.Value, 0, 4)
			arg := vm.stack[vm.sp-1].(*evalJSON)
			for _, jp := range jp {
				jp.Match(arg, true, func(value *json.Value) {
					matches = append(matches, value)
				})
			}
			if len(matches) == 0 {
				vm.stack[vm.sp-1] = nil
			} else {
				vm.stack[vm.sp-1] = json.NewArray(matches)
			}
			return 1
		}, "FN JSON_EXTRACT, SP-1, [static]")
	} else {
		c.emit(func(vm *VirtualMachine) int {
			var match *json.Value
			arg := vm.stack[vm.sp-1].(*evalJSON)
			jp[0].Match(arg, true, func(value *json.Value) {
				match = value
			})
			if match == nil {
				vm.stack[vm.sp-1] = nil
			} else {
				vm.stack[vm.sp-1] = match
			}
			return 1
		}, "FN JSON_EXTRACT, SP-1, [static]")
	}
}

func (c *compiler) emitFn_JSON_CONTAINS_PATH(match jsonMatch, paths []*json.Path) {
	switch match {
	case jsonMatchOne:
		c.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalJSON)
			matched := false
			for _, p := range paths {
				p.Match(arg, true, func(*json.Value) { matched = true })
				if matched {
					break
				}
			}
			vm.stack[vm.sp-1] = newEvalBool(matched)
			return 1
		}, "FN JSON_CONTAINS_PATH, SP-1, 'one', [static]")
	case jsonMatchAll:
		c.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalJSON)
			matched := true
			for _, p := range paths {
				matched = false
				p.Match(arg, true, func(*json.Value) { matched = true })
				if !matched {
					break
				}
			}
			vm.stack[vm.sp-1] = newEvalBool(matched)
			return 1
		}, "FN JSON_CONTAINS_PATH, SP-1, 'all', [static]")
	}
}

func (c *compiler) emitBitOp_bb(op bitwiseOp) {
	c.adjustStack(-1)

	switch op {
	case and:
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalBytes)
			r := vm.stack[vm.sp-1].(*evalBytes)
			if len(l.bytes) != len(r.bytes) {
				vm.err = errBitwiseOperandsLength
				return 0
			}
			for i := range l.bytes {
				l.bytes[i] = l.bytes[i] & r.bytes[i]
			}
			vm.sp--
			return 1
		}, "AND BINARY(SP-2), BINARY(SP-1)")
	case or:
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalBytes)
			r := vm.stack[vm.sp-1].(*evalBytes)
			if len(l.bytes) != len(r.bytes) {
				vm.err = errBitwiseOperandsLength
				return 0
			}
			for i := range l.bytes {
				l.bytes[i] = l.bytes[i] | r.bytes[i]
			}
			vm.sp--
			return 1
		}, "OR BINARY(SP-2), BINARY(SP-1)")
	case xor:
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalBytes)
			r := vm.stack[vm.sp-1].(*evalBytes)
			if len(l.bytes) != len(r.bytes) {
				vm.err = errBitwiseOperandsLength
				return 0
			}
			for i := range l.bytes {
				l.bytes[i] = l.bytes[i] ^ r.bytes[i]
			}
			vm.sp--
			return 1
		}, "XOR BINARY(SP-2), BINARY(SP-1)")
	}
}

func (c *compiler) emitBitOp_uu(op bitwiseOp) {
	c.adjustStack(-1)

	switch op {
	case and:
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalUint64)
			l.u = l.u & r.u
			vm.sp--
			return 1
		}, "AND UINT64(SP-2), UINT64(SP-1)")
	case or:
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalUint64)
			l.u = l.u | r.u
			vm.sp--
			return 1
		}, "OR UINT64(SP-2), UINT64(SP-1)")
	case xor:
		c.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalUint64)
			l.u = l.u ^ r.u
			vm.sp--
			return 1
		}, "XOR UINT64(SP-2), UINT64(SP-1)")
	}
}

func (c *compiler) emitConvert_ui(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalUint64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalInt64(int64(arg.u))
		return 1
	}, "CONV UINT64(SP-%d), INT64", offset)
}

func (c *compiler) emitConvert_iu(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalInt64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalUint64(uint64(arg.i))
		return 1
	}, "CONV INT64(SP-%d), UINT64", offset)
}

func (c *compiler) emitConvert_xi(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := evalToNumeric(vm.stack[vm.sp-offset])
		vm.stack[vm.sp-offset] = arg.toInt64()
		return 1
	}, "CONV (SP-%d), INT64", offset)
}

func (c *compiler) emitConvert_xu(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := evalToNumeric(vm.stack[vm.sp-offset])
		vm.stack[vm.sp-offset] = arg.toUint64()
		return 1
	}, "CONV (SP-%d), UINT64", offset)
}

func (c *compiler) emitConvert_xb(offset int, t sqltypes.Type, length int, hasLength bool) {
	if hasLength {
		c.emit(func(vm *VirtualMachine) int {
			arg := evalToBinary(vm.stack[vm.sp-offset])
			arg.truncateInPlace(length)
			arg.tt = int16(t)
			vm.stack[vm.sp-offset] = arg
			return 1
		}, "CONV (SP-%d), VARBINARY[%d]", offset, length)
	} else {
		c.emit(func(vm *VirtualMachine) int {
			arg := evalToBinary(vm.stack[vm.sp-offset])
			arg.tt = int16(t)
			vm.stack[vm.sp-offset] = arg
			return 1
		}, "CONV (SP-%d), VARBINARY", offset)
	}
}

func (c *compiler) emitConvert_fj(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalFloat)
		vm.stack[vm.sp-offset] = evalConvert_fj(arg)
		return 1
	}, "CONV FLOAT64(SP-%d), JSON")
}

func (c *compiler) emitConvert_nj(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(evalNumeric)
		vm.stack[vm.sp-offset] = evalConvert_nj(arg)
		return 1
	}, "CONV numeric(SP-%d), JSON")
}

func (c *compiler) emitConvert_cj(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalBytes)
		vm.stack[vm.sp-offset], vm.err = evalConvert_cj(arg)
		return 1
	}, "CONV VARCHAR(SP-%d), JSON")
}

func (c *compiler) emitConvert_bj(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalBytes)
		vm.stack[vm.sp-offset] = evalConvert_bj(arg)
		return 1
	}, "CONV VARBINARY(SP-%d), JSON")
}

func (c *compiler) emitConvert_xc(offset int, t sqltypes.Type, collation collations.ID, length int, hasLength bool) {
	if hasLength {
		c.emit(func(vm *VirtualMachine) int {
			arg, err := evalToVarchar(vm.stack[vm.sp-offset], collation, true)
			if err != nil {
				vm.stack[vm.sp-offset] = nil
			} else {
				arg.truncateInPlace(length)
				arg.tt = int16(t)
				vm.stack[vm.sp-offset] = arg
			}
			return 1
		}, "CONV (SP-%d), VARCHAR[%d]", offset, length)
	} else {
		c.emit(func(vm *VirtualMachine) int {
			arg, err := evalToVarchar(vm.stack[vm.sp-offset], collation, true)
			if err != nil {
				vm.stack[vm.sp-offset] = nil
			} else {
				arg.tt = int16(t)
				vm.stack[vm.sp-offset] = arg
			}
			return 1
		}, "CONV (SP-%d), VARCHAR", offset)
	}
}

func (c *compiler) emitBitShiftLeft_bu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalBytes)
		r := vm.stack[vm.sp-1].(*evalUint64)

		var (
			bits   = int(r.u & 7)
			bytes  = int(r.u >> 3)
			length = len(l.bytes)
			out    = make([]byte, length)
		)

		for i := 0; i < length; i++ {
			pos := i + bytes + 1
			switch {
			case pos < length:
				out[i] = l.bytes[pos] >> (8 - bits)
				fallthrough
			case pos == length:
				out[i] |= l.bytes[pos-1] << bits
			}
		}
		l.bytes = out

		vm.sp--
		return 1
	}, "BIT_SHL BINARY(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitBitShiftRight_bu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalBytes)
		r := vm.stack[vm.sp-1].(*evalUint64)

		var (
			bits   = int(r.u & 7)
			bytes  = int(r.u >> 3)
			length = len(l.bytes)
			out    = make([]byte, length)
		)

		for i := length - 1; i >= 0; i-- {
			switch {
			case i > bytes:
				out[i] = l.bytes[i-bytes-1] << (8 - bits)
				fallthrough
			case i == bytes:
				out[i] |= l.bytes[i-bytes] >> bits
			}
		}
		l.bytes = out

		vm.sp--
		return 1
	}, "BIT_SHR BINARY(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitBitShiftLeft_uu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u = l.u << r.u

		vm.sp--
		return 1
	}, "BIT_SHL UINT64(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitBitShiftRight_uu() {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u = l.u >> r.u

		vm.sp--
		return 1
	}, "BIT_SHR UINT64(SP-2), UINT64(SP-1)")
}

func (c *compiler) emitBitCount_b() {
	c.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalBytes)
		count := 0
		for _, b := range a.bytes {
			count += bits.OnesCount8(b)
		}
		vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(count))
		return 1
	}, "BIT_COUNT BINARY(SP-1)")
}

func (c *compiler) emitBitCount_u() {
	c.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalUint64)
		vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(bits.OnesCount64(a.u)))
		return 1
	}, "BIT_COUNT UINT64(SP-1)")
}

func (c *compiler) emitBitwiseNot_b() {
	c.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalBytes)
		for i := range a.bytes {
			a.bytes[i] = ^a.bytes[i]
		}
		return 1
	}, "BIT_NOT BINARY(SP-1)")
}

func (c *compiler) emitBitwiseNot_u() {
	c.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalUint64)
		a.u = ^a.u
		return 1
	}, "BIT_NOT UINT64(SP-1)")
}

func (c *compiler) emitCmpCase(cases int, hasElse bool, tt sqltypes.Type, cc collations.TypedCollation) {
	elseOffset := 0
	if hasElse {
		elseOffset = 1
	}

	stackDepth := 2*cases + elseOffset
	c.adjustStack(-(stackDepth - 1))

	c.emit(func(vm *VirtualMachine) int {
		end := vm.sp - elseOffset
		for sp := vm.sp - stackDepth; sp < end; sp += 2 {
			if vm.stack[sp].(*evalInt64).i != 0 {
				vm.stack[vm.sp-stackDepth], vm.err = evalCoerce(vm.stack[sp+1], tt, cc.Collation)
				goto done
			}
		}
		if elseOffset != 0 {
			vm.stack[vm.sp-stackDepth], vm.err = evalCoerce(vm.stack[vm.sp-1], tt, cc.Collation)
		} else {
			vm.stack[vm.sp-stackDepth] = nil
		}
	done:
		vm.sp -= stackDepth - 1
		return 1
	}, "CASE [%d cases, else = %v]", cases, hasElse)
}

func (c *compiler) emitConvert_iB(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && arg.(*evalInt64).i != 0)
		return 1
	}, "CONV INT64(SP-%d), BOOL", offset)
}

func (c *compiler) emitConvert_uB(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && arg.(*evalUint64).u != 0)
		return 1
	}, "CONV UINT64(SP-%d), BOOL", offset)
}

func (c *compiler) emitConvert_fB(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && arg.(*evalFloat).f != 0.0)
		return 1
	}, "CONV FLOAT64(SP-%d), BOOL", offset)
}

func (c *compiler) emitConvert_dB(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && !arg.(*evalDecimal).dec.IsZero())
		return 1
	}, "CONV DECIMAL(SP-%d), BOOL", offset)
}

func (c *compiler) emitConvert_bB(offset int) {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && parseStringToFloat(arg.(*evalBytes).string()) != 0.0)
		return 1
	}, "CONV VARBINARY(SP-%d), BOOL", offset)
}

func (c *compiler) emitNeg_i() {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalInt64)
		if arg.i == math.MinInt64 {
			vm.err = errDeoptimize
		} else {
			arg.i = -arg.i
		}
		return 1
	}, "NEG INT64(SP-1)")
}

func (c *compiler) emitNeg_hex() {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalUint64)
		vm.stack[vm.sp-1] = vm.arena.newEvalFloat(-float64(arg.u))
		return 1
	}, "NEG HEX(SP-1)")
}

func (c *compiler) emitNeg_u() {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalUint64)
		if arg.u > math.MaxInt64+1 {
			vm.err = errDeoptimize
		} else {
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(-int64(arg.u))
		}
		return 1
	}, "NEG UINT64(SP-1)")
}

func (c *compiler) emitNeg_f() {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalFloat)
		arg.f = -arg.f
		return 1
	}, "NEG FLOAT64(SP-1)")
}

func (c *compiler) emitNeg_d() {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalDecimal)
		arg.dec = arg.dec.NegInPlace()
		return 1
	}, "NEG DECIMAL(SP-1)")
}

func (c *compiler) emitFn_REPEAT(i int) {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		str := vm.stack[vm.sp-2].(*evalBytes)
		repeat := vm.stack[vm.sp-1].(*evalInt64)

		if repeat.i < 0 {
			repeat.i = 0
		}

		if !checkMaxLength(int64(len(str.bytes)), repeat.i) {
			vm.stack[vm.sp-2] = nil
			vm.sp--
			return 1
		}

		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.Repeat(str.bytes, int(repeat.i))
		vm.sp--
		return 1
	}, "FN REPEAT VARCHAR(SP-2) INT64(SP-1)")
}

func (c *compiler) emitFn_TO_BASE64(t sqltypes.Type, col collations.TypedCollation) {
	c.emit(func(vm *VirtualMachine) int {
		str := vm.stack[vm.sp-1].(*evalBytes)

		encoded := make([]byte, mysqlBase64.EncodedLen(len(str.bytes)))
		mysqlBase64.Encode(encoded, str.bytes)

		str.tt = int16(t)
		str.col = col
		str.bytes = encoded
		return 1
	}, "FN TO_BASE64 VARCHAR(SP-1)")
}

func (c *compiler) emitFromBase64() {
	c.emit(func(vm *VirtualMachine) int {
		str := vm.stack[vm.sp-1].(*evalBytes)

		decoded := make([]byte, mysqlBase64.DecodedLen(len(str.bytes)))

		n, err := mysqlBase64.Decode(decoded, str.bytes)
		if err != nil {
			vm.stack[vm.sp-1] = nil
			return 1
		}
		str.tt = int16(sqltypes.VarBinary)
		str.bytes = decoded[:n]
		return 1
	}, "FROM_BASE64 VARCHAR(SP-1)")
}

func (c *compiler) emitFn_LUCASE(upcase bool) {
	if upcase {
		c.emit(func(vm *VirtualMachine) int {
			str := vm.stack[vm.sp-1].(*evalBytes)

			coll := collations.Local().LookupByID(str.col.Collation)
			csa, ok := coll.(collations.CaseAwareCollation)
			if !ok {
				vm.err = vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "not implemented")
			} else {
				str.bytes = csa.ToUpper(nil, str.bytes)
			}
			str.tt = int16(sqltypes.VarChar)
			return 1
		}, "FN UPPER VARCHAR(SP-1)")
	} else {
		c.emit(func(vm *VirtualMachine) int {
			str := vm.stack[vm.sp-1].(*evalBytes)

			coll := collations.Local().LookupByID(str.col.Collation)
			csa, ok := coll.(collations.CaseAwareCollation)
			if !ok {
				vm.err = vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "not implemented")
			} else {
				str.bytes = csa.ToLower(nil, str.bytes)
			}
			str.tt = int16(sqltypes.VarChar)
			return 1
		}, "FN LOWER VARCHAR(SP-1)")
	}
}

func (c *compiler) emitFn_LENGTH(op lengthOp) {
	switch op {
	case charLen:
		c.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalBytes)

			if sqltypes.IsBinary(arg.SQLType()) {
				vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(len(arg.bytes)))
			} else {
				coll := collations.Local().LookupByID(arg.col.Collation)
				count := charset.Length(coll.Charset(), arg.bytes)
				vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(count))
			}
			return 1
		}, "FN CHAR_LENGTH VARCHAR(SP-1)")
	case byteLen:
		c.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalBytes)
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(len(arg.bytes)))
			return 1
		}, "FN LENGTH VARCHAR(SP-1)")
	case bitLen:
		c.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalBytes)
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(len(arg.bytes) * 8))
			return 1
		}, "FN BIT_LENGTH VARCHAR(SP-1)")
	}
}

func (c *compiler) emitFn_ASCII() {
	c.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalBytes)
		if len(arg.bytes) == 0 {
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(0)
		} else {
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(arg.bytes[0]))
		}
		return 1
	}, "FN ASCII VARCHAR(SP-1)")
}

func (c *compiler) emitLike_collate(expr *LikeExpr, collation collations.Collation) {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalBytes)
		r := vm.stack[vm.sp-1].(*evalBytes)
		vm.sp--

		match := expr.matchWildcard(l.bytes, r.bytes, collation.ID())
		if match {
			vm.stack[vm.sp-1] = evalBoolTrue
		} else {
			vm.stack[vm.sp-1] = evalBoolFalse
		}
		return 1
	}, "LIKE VARCHAR(SP-2), VARCHAR(SP-1) COLLATE '%s'", collation.Name())
}

func (c *compiler) emitLike_coerce(expr *LikeExpr, coercion *compiledCoercion) {
	c.adjustStack(-1)

	c.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalBytes)
		r := vm.stack[vm.sp-1].(*evalBytes)
		vm.sp--

		var bl, br []byte
		bl, vm.err = coercion.left(nil, l.bytes)
		if vm.err != nil {
			return 0
		}
		br, vm.err = coercion.right(nil, r.bytes)
		if vm.err != nil {
			return 0
		}

		match := expr.matchWildcard(bl, br, coercion.col.ID())
		if match {
			vm.stack[vm.sp-1] = evalBoolTrue
		} else {
			vm.stack[vm.sp-1] = evalBoolFalse
		}
		return 1
	}, "LIKE VARCHAR(SP-2), VARCHAR(SP-1) COERCE AND COLLATE '%s'", coercion.col.Name())
}
