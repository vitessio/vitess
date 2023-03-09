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

type jump struct {
	from, to int
}

func (j *jump) offset() int {
	return j.to - j.from
}

type assembler struct {
	ins   []frame
	log   AssemblerLog
	stack struct {
		cur int
		max int
	}
}

func (asm *assembler) jumpFrom() *jump {
	return &jump{from: len(asm.ins)}
}

func (asm *assembler) jumpDestination(j *jump) {
	j.to = len(asm.ins)
}

func (asm *assembler) adjustStack(offset int) {
	asm.stack.cur += offset
	if asm.stack.cur < 0 {
		panic("negative stack position")
	}
	if asm.stack.cur > asm.stack.max {
		asm.stack.max = asm.stack.cur
	}
	if asm.log != nil {
		asm.log.Stack(asm.stack.cur-offset, asm.stack.cur)
	}
}

func (asm *assembler) emit(f frame, instruction string, args ...any) {
	if asm.log != nil {
		asm.log.Instruction(instruction, args...)
	}
	asm.ins = append(asm.ins, f)
}

func (asm *assembler) Add_dd() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		mathAdd_dd0(l, r)
		vm.sp--
		return 1
	}, "ADD DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Add_ff() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		l.f += r.f
		vm.sp--
		return 1
	}, "ADD FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) Add_ii() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.i, vm.err = mathAdd_ii0(l.i, r.i)
		vm.sp--
		return 1
	}, "ADD INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Add_ui(swap bool) {
	asm.adjustStack(-1)

	if swap {
		asm.emit(func(vm *VirtualMachine) int {
			var u uint64
			l := vm.stack[vm.sp-1].(*evalUint64)
			r := vm.stack[vm.sp-2].(*evalInt64)
			u, vm.err = mathAdd_ui0(l.u, r.i)
			vm.stack[vm.sp-2] = vm.arena.newEvalUint64(u)
			vm.sp--
			return 1
		}, "ADD UINT64(SP-1), INT64(SP-2)")
	} else {
		asm.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalInt64)
			l.u, vm.err = mathAdd_ui0(l.u, r.i)
			vm.sp--
			return 1
		}, "ADD UINT64(SP-2), INT64(SP-1)")
	}
}

func (asm *assembler) Add_uu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u, vm.err = mathAdd_uu0(l.u, r.u)
		vm.sp--
		return 1
	}, "ADD UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitCount_b() {
	asm.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalBytes)
		count := 0
		for _, b := range a.bytes {
			count += bits.OnesCount8(b)
		}
		vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(count))
		return 1
	}, "BIT_COUNT BINARY(SP-1)")
}

func (asm *assembler) BitCount_u() {
	asm.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalUint64)
		vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(bits.OnesCount64(a.u)))
		return 1
	}, "BIT_COUNT UINT64(SP-1)")
}

func (asm *assembler) BitOp_bb(op bitwiseOp) {
	asm.adjustStack(-1)

	switch op {
	case and:
		asm.emit(func(vm *VirtualMachine) int {
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
		asm.emit(func(vm *VirtualMachine) int {
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
		asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) BitOp_uu(op bitwiseOp) {
	asm.adjustStack(-1)

	switch op {
	case and:
		asm.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalUint64)
			l.u = l.u & r.u
			vm.sp--
			return 1
		}, "AND UINT64(SP-2), UINT64(SP-1)")
	case or:
		asm.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalUint64)
			l.u = l.u | r.u
			vm.sp--
			return 1
		}, "OR UINT64(SP-2), UINT64(SP-1)")
	case xor:
		asm.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalUint64)
			l.u = l.u ^ r.u
			vm.sp--
			return 1
		}, "XOR UINT64(SP-2), UINT64(SP-1)")
	}
}

func (asm *assembler) BitShiftLeft_bu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) BitShiftLeft_uu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u = l.u << r.u

		vm.sp--
		return 1
	}, "BIT_SHL UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitShiftRight_bu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) BitShiftRight_uu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u = l.u >> r.u

		vm.sp--
		return 1
	}, "BIT_SHR UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitwiseNot_b() {
	asm.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalBytes)
		for i := range a.bytes {
			a.bytes[i] = ^a.bytes[i]
		}
		return 1
	}, "BIT_NOT BINARY(SP-1)")
}

func (asm *assembler) BitwiseNot_u() {
	asm.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalUint64)
		a.u = ^a.u
		return 1
	}, "BIT_NOT UINT64(SP-1)")
}

func (asm *assembler) Cmp_eq() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp == 0)
		vm.sp++
		return 1
	}, "CMPFLAG EQ")
}

func (asm *assembler) Cmp_eq_n() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		if vm.flags.null {
			vm.stack[vm.sp] = nil
		} else {
			vm.stack[vm.sp] = newEvalBool(vm.flags.cmp == 0)
		}
		vm.sp++
		return 1
	}, "CMPFLAG EQ [NULL]")
}

func (asm *assembler) Cmp_ge() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp >= 0)
		vm.sp++
		return 1
	}, "CMPFLAG GE")
}

func (asm *assembler) Cmp_ge_n() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		if vm.flags.null {
			vm.stack[vm.sp] = nil
		} else {
			vm.stack[vm.sp] = newEvalBool(vm.flags.cmp >= 0)
		}
		vm.sp++
		return 1
	}, "CMPFLAG GE [NULL]")
}

func (asm *assembler) Cmp_gt() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp > 0)
		vm.sp++
		return 1
	}, "CMPFLAG GT")
}

func (asm *assembler) Cmp_gt_n() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		if vm.flags.null {
			vm.stack[vm.sp] = nil
		} else {
			vm.stack[vm.sp] = newEvalBool(vm.flags.cmp > 0)
		}
		vm.sp++
		return 1
	}, "CMPFLAG GT [NULL]")
}

func (asm *assembler) Cmp_le() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp <= 0)
		vm.sp++
		return 1
	}, "CMPFLAG LE")
}

func (asm *assembler) Cmp_le_n() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		if vm.flags.null {
			vm.stack[vm.sp] = nil
		} else {
			vm.stack[vm.sp] = newEvalBool(vm.flags.cmp <= 0)
		}
		vm.sp++
		return 1
	}, "CMPFLAG LE [NULL]")
}

func (asm *assembler) Cmp_lt() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp < 0)
		vm.sp++
		return 1
	}, "CMPFLAG LT")
}

func (asm *assembler) Cmp_lt_n() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		if vm.flags.null {
			vm.stack[vm.sp] = nil
		} else {
			vm.stack[vm.sp] = newEvalBool(vm.flags.cmp < 0)
		}
		vm.sp++
		return 1
	}, "CMPFLAG LT [NULL]")
}
func (asm *assembler) Cmp_ne() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBool(vm.flags.cmp != 0)
		vm.sp++
		return 1
	}, "CMPFLAG NE")
}

func (asm *assembler) Cmp_ne_n() {
	asm.adjustStack(1)
	asm.emit(func(vm *VirtualMachine) int {
		if vm.flags.null {
			vm.stack[vm.sp] = nil
		} else {
			vm.stack[vm.sp] = newEvalBool(vm.flags.cmp != 0)
		}
		vm.sp++
		return 1
	}, "CMPFLAG NE [NULL]")
}

func (asm *assembler) CmpCase(cases int, hasElse bool, tt sqltypes.Type, cc collations.TypedCollation) {
	elseOffset := 0
	if hasElse {
		elseOffset = 1
	}

	stackDepth := 2*cases + elseOffset
	asm.adjustStack(-(stackDepth - 1))

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) CmpNum_dd() {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		vm.sp -= 2
		vm.flags.cmp = l.dec.Cmp(r.dec)
		return 1
	}, "CMP DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) CmpNum_fd(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) CmpNum_ff() {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(l.f, r.f)
		return 1
	}, "CMP FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) CmpNum_id(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalInt64)
		r := vm.stack[vm.sp-right].(*evalDecimal)
		vm.sp -= 2
		vm.flags.cmp = decimal.NewFromInt(l.i).Cmp(r.dec)
		return 1
	}, "CMP INT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_if(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalInt64)
		r := vm.stack[vm.sp-right].(*evalFloat)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(float64(l.i), r.f)
		return 1
	}, "CMP INT64(SP-%d), FLOAT64(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_ii() {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(l.i, r.i)
		return 1
	}, "CMP INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) CmpNum_iu(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) CmpNum_ud(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalUint64)
		r := vm.stack[vm.sp-right].(*evalDecimal)
		vm.sp -= 2
		vm.flags.cmp = decimal.NewFromUint(l.u).Cmp(r.dec)
		return 1
	}, "CMP UINT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_uf(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-left].(*evalUint64)
		r := vm.stack[vm.sp-right].(*evalFloat)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(float64(l.u), r.f)
		return 1
	}, "CMP UINT64(SP-%d), FLOAT64(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_uu() {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		vm.sp -= 2
		vm.flags.cmp = cmpnum(l.u, r.u)
		return 1
	}, "CMP UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) CmpString_coerce(coercion *compiledCoercion) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) CmpString_collate(collation collations.Collation) {
	asm.adjustStack(-2)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalBytes)
		r := vm.stack[vm.sp-1].(*evalBytes)
		vm.sp -= 2
		vm.flags.cmp = collation.Collate(l.bytes, r.bytes, false)
		return 1
	}, "CMP VARCHAR(SP-2), VARCHAR(SP-1) COLLATE '%s'", collation.Name())
}

func (asm *assembler) CmpTuple(fullEquality bool) {
	asm.adjustStack(-2)
	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalTuple)
		r := vm.stack[vm.sp-1].(*evalTuple)
		vm.sp -= 2
		vm.flags.cmp, vm.flags.null, vm.err = evalCompareMany(l.t, r.t, fullEquality)
		return 1
	}, "CMP TUPLE(SP-2), TUPLE(SP-1)")
}

func (asm *assembler) CmpTupleNullsafe() {
	asm.adjustStack(-1)
	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalTuple)
		r := vm.stack[vm.sp-1].(*evalTuple)

		var equals bool
		equals, vm.err = evalCompareTuplesNullSafe(l.t, r.t)

		vm.stack[vm.sp-2] = newEvalBool(equals)
		vm.sp -= 1
		return 1
	}, "CMP NULLSAFE TUPLE(SP-2), TUPLE(SP-1)")
}

func (asm *assembler) Collate(col collations.ID) {
	asm.emit(func(vm *VirtualMachine) int {
		a := vm.stack[vm.sp-1].(*evalBytes)
		a.tt = int16(sqltypes.VarChar)
		a.col.Collation = col
		return 1
	}, "COLLATE VARCHAR(SP-1), %d", col)
}

func (asm *assembler) Convert_bB(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && parseStringToFloat(arg.(*evalBytes).string()) != 0.0)
		return 1
	}, "CONV VARBINARY(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_bj(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalBytes)
		vm.stack[vm.sp-offset] = evalConvert_bj(arg)
		return 1
	}, "CONV VARBINARY(SP-%d), JSON")
}

func (asm *assembler) Convert_cj(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalBytes)
		vm.stack[vm.sp-offset], vm.err = evalConvert_cj(arg)
		return 1
	}, "CONV VARCHAR(SP-%d), JSON")
}

func (asm *assembler) Convert_dB(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && !arg.(*evalDecimal).dec.IsZero())
		return 1
	}, "CONV DECIMAL(SP-%d), BOOL", offset)
}

// Convert_dbit is a special instruction emission for converting
// a bigdecimal in preparation for a bitwise operation. In that case
// we need to convert the bigdecimal to an int64 and then cast to
// uint64 to ensure we match the behavior of MySQL.
func (asm *assembler) Convert_dbit(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := evalToNumeric(vm.stack[vm.sp-offset])
		vm.stack[vm.sp-offset] = vm.arena.newEvalUint64(uint64(arg.toInt64().i))
		return 1
	}, "CONV DECIMAL_BITWISE(SP-%d), UINT64", offset)
}

func (asm *assembler) Convert_fB(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && arg.(*evalFloat).f != 0.0)
		return 1
	}, "CONV FLOAT64(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_fj(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalFloat)
		vm.stack[vm.sp-offset] = evalConvert_fj(arg)
		return 1
	}, "CONV FLOAT64(SP-%d), JSON")
}

func (asm *assembler) Convert_hex(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		var ok bool
		vm.stack[vm.sp-offset], ok = vm.stack[vm.sp-offset].(*evalBytes).toNumericHex()
		if !ok {
			vm.err = errDeoptimize
		}
		return 1
	}, "CONV VARBINARY(SP-%d), HEX", offset)
}

func (asm *assembler) Convert_iB(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && arg.(*evalInt64).i != 0)
		return 1
	}, "CONV INT64(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_id(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalInt64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalDecimalWithPrec(decimal.NewFromInt(arg.i), 0)
		return 1
	}, "CONV INT64(SP-%d), FLOAT64", offset)
}

func (asm *assembler) Convert_if(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalInt64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalFloat(arg.toFloat0())
		return 1
	}, "CONV INT64(SP-%d), FLOAT64", offset)
}

func (asm *assembler) Convert_iu(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalInt64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalUint64(uint64(arg.i))
		return 1
	}, "CONV INT64(SP-%d), UINT64", offset)
}

func (asm *assembler) Convert_nj(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(evalNumeric)
		vm.stack[vm.sp-offset] = evalConvert_nj(arg)
		return 1
	}, "CONV numeric(SP-%d), JSON")
}

func (asm *assembler) Convert_Nj(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp-offset] = json.ValueNull
		return 1
	}, "CONV NULL(SP-%d), JSON")
}

func (asm *assembler) Convert_uB(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset]
		vm.stack[vm.sp-offset] = newEvalBool(arg != nil && arg.(*evalUint64).u != 0)
		return 1
	}, "CONV UINT64(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_ud(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalUint64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalDecimalWithPrec(decimal.NewFromUint(arg.u), 0)
		return 1
	}, "CONV UINT64(SP-%d), FLOAT64)", offset)
}

func (asm *assembler) Convert_uf(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalUint64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalFloat(arg.toFloat0())
		return 1
	}, "CONV UINT64(SP-%d), FLOAT64)", offset)
}

func (asm *assembler) Convert_ui(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-offset].(*evalUint64)
		vm.stack[vm.sp-offset] = vm.arena.newEvalInt64(int64(arg.u))
		return 1
	}, "CONV UINT64(SP-%d), INT64", offset)
}

func (asm *assembler) Convert_xb(offset int, t sqltypes.Type, length int, hasLength bool) {
	if hasLength {
		asm.emit(func(vm *VirtualMachine) int {
			arg := evalToBinary(vm.stack[vm.sp-offset])
			arg.truncateInPlace(length)
			arg.tt = int16(t)
			vm.stack[vm.sp-offset] = arg
			return 1
		}, "CONV (SP-%d), VARBINARY[%d]", offset, length)
	} else {
		asm.emit(func(vm *VirtualMachine) int {
			arg := evalToBinary(vm.stack[vm.sp-offset])
			arg.tt = int16(t)
			vm.stack[vm.sp-offset] = arg
			return 1
		}, "CONV (SP-%d), VARBINARY", offset)
	}
}

func (asm *assembler) Convert_xc(offset int, t sqltypes.Type, collation collations.ID, length int, hasLength bool) {
	if hasLength {
		asm.emit(func(vm *VirtualMachine) int {
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
		asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Convert_xd(offset int, m, d int32) {
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp-offset] = evalToNumeric(vm.stack[vm.sp-offset]).toDecimal(m, d)
		return 1
	}, "CONV (SP-%d), FLOAT64", offset)
}

func (asm *assembler) Convert_xf(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp-offset], _ = evalToNumeric(vm.stack[vm.sp-offset]).toFloat()
		return 1
	}, "CONV (SP-%d), FLOAT64", offset)
}
func (asm *assembler) Convert_xi(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := evalToNumeric(vm.stack[vm.sp-offset])
		vm.stack[vm.sp-offset] = arg.toInt64()
		return 1
	}, "CONV (SP-%d), INT64", offset)
}

func (asm *assembler) Convert_xu(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := evalToNumeric(vm.stack[vm.sp-offset])
		vm.stack[vm.sp-offset] = arg.toUint64()
		return 1
	}, "CONV (SP-%d), UINT64", offset)
}

func (asm *assembler) Div_dd() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Div_ff() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Fn_ASCII() {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalBytes)
		if len(arg.bytes) == 0 {
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(0)
		} else {
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(arg.bytes[0]))
		}
		return 1
	}, "FN ASCII VARCHAR(SP-1)")
}

func (asm *assembler) Fn_FROM_BASE64() {
	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Fn_HEX_c(t sqltypes.Type, col collations.TypedCollation) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalBytes)
		encoded := vm.arena.newEvalText(hexEncodeBytes(arg.bytes), col)
		encoded.tt = int16(t)
		vm.stack[vm.sp-1] = encoded
		return 1
	}, "FN HEX VARCHAR(SP-1)")
}

func (asm *assembler) Fn_HEX_d(col collations.TypedCollation) {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(evalNumeric)
		vm.stack[vm.sp-1] = vm.arena.newEvalText(hexEncodeUint(uint64(arg.toInt64().i)), col)
		return 1
	}, "FN HEX NUMERIC(SP-1)")
}

func (asm *assembler) Fn_JSON_ARRAY(args int) {
	asm.adjustStack(-(args - 1))
	asm.emit(func(vm *VirtualMachine) int {
		ary := make([]*json.Value, 0, args)
		for sp := vm.sp - args; sp < vm.sp; sp++ {
			ary = append(ary, vm.stack[sp].(*json.Value))
		}
		vm.stack[vm.sp-args] = json.NewArray(ary)
		vm.sp -= args - 1
		return 1
	}, "FN JSON_ARRAY (SP-%d)...(SP-1)", args)
}

func (asm *assembler) Fn_JSON_CONTAINS_PATH(match jsonMatch, paths []*json.Path) {
	switch match {
	case jsonMatchOne:
		asm.emit(func(vm *VirtualMachine) int {
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
		asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Fn_JSON_EXTRACT0(jp []*json.Path) {
	multi := len(jp) > 1 || slices2.Any(jp, func(path *json.Path) bool { return path.ContainsWildcards() })

	if multi {
		asm.emit(func(vm *VirtualMachine) int {
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
		asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Fn_JSON_OBJECT(args int) {
	asm.adjustStack(-(args - 1))
	asm.emit(func(vm *VirtualMachine) int {
		j := json.NewObject()
		obj, _ := j.Object()

		for sp := vm.sp - args; sp < vm.sp; sp += 2 {
			key := vm.stack[sp]
			val := vm.stack[sp+1]

			if key == nil {
				vm.err = errJSONKeyIsNil
				return 0
			}

			obj.Set(key.(*evalBytes).string(), val.(*evalJSON), json.Set)
		}
		vm.stack[vm.sp-args] = j
		vm.sp -= args - 1
		return 1
	}, "FN JSON_ARRAY (SP-%d)...(SP-1)", args)
}

func (asm *assembler) Fn_JSON_UNQUOTE() {
	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Fn_LENGTH(op lengthOp) {
	switch op {
	case charLen:
		asm.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalBytes)

			if sqltypes.IsBinary(arg.SQLType()) {
				vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(len(arg.bytes)))
			} else {
				coll := arg.col.Collation.Get()
				count := charset.Length(coll.Charset(), arg.bytes)
				vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(count))
			}
			return 1
		}, "FN CHAR_LENGTH VARCHAR(SP-1)")
	case byteLen:
		asm.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalBytes)
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(len(arg.bytes)))
			return 1
		}, "FN LENGTH VARCHAR(SP-1)")
	case bitLen:
		asm.emit(func(vm *VirtualMachine) int {
			arg := vm.stack[vm.sp-1].(*evalBytes)
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(int64(len(arg.bytes) * 8))
			return 1
		}, "FN BIT_LENGTH VARCHAR(SP-1)")
	}
}

func (asm *assembler) Fn_LUCASE(upcase bool) {
	if upcase {
		asm.emit(func(vm *VirtualMachine) int {
			str := vm.stack[vm.sp-1].(*evalBytes)

			coll := str.col.Collation.Get()
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
		asm.emit(func(vm *VirtualMachine) int {
			str := vm.stack[vm.sp-1].(*evalBytes)

			coll := str.col.Collation.Get()
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

func (asm *assembler) Fn_MULTICMP_b(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(vm *VirtualMachine) int {
		x := vm.stack[vm.sp-args].ToRawBytes()
		for sp := vm.sp - args + 1; sp < vm.sp; sp++ {
			y := vm.stack[sp].ToRawBytes()
			if lessThan == (bytes.Compare(y, x) < 0) {
				x = y
			}
		}
		vm.stack[vm.sp-args] = vm.arena.newEvalBinary(x)
		vm.sp -= args - 1
		return 1
	}, "FN MULTICMP VARBINARY(SP-%d)...VARBINARY(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_c(args int, lessThan bool, tc collations.TypedCollation) {
	col := tc.Collation.Get()

	asm.adjustStack(-(args - 1))
	asm.emit(func(vm *VirtualMachine) int {
		x := vm.stack[vm.sp-args].ToRawBytes()
		for sp := vm.sp - args + 1; sp < vm.sp; sp++ {
			y := vm.stack[sp].ToRawBytes()
			if lessThan == (col.Collate(y, x, false) < 0) {
				x = y
			}
		}
		vm.stack[vm.sp-args] = vm.arena.newEvalText(x, tc)
		vm.sp -= args - 1
		return 1
	}, "FN MULTICMP FLOAT64(SP-%d)...FLOAT64(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_d(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(vm *VirtualMachine) int {
		x := vm.stack[vm.sp-args].(*evalDecimal)
		xprec := x.length

		for sp := vm.sp - args + 1; sp < vm.sp; sp++ {
			y := vm.stack[sp].(*evalDecimal)
			if lessThan == (y.dec.Cmp(x.dec) < 0) {
				x = y
			}
			if y.length > xprec {
				xprec = y.length
			}
		}
		vm.stack[vm.sp-args] = vm.arena.newEvalDecimalWithPrec(x.dec, xprec)
		vm.sp -= args - 1
		return 1
	}, "FN MULTICMP DECIMAL(SP-%d)...DECIMAL(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_f(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(vm *VirtualMachine) int {
		x := vm.stack[vm.sp-args].(*evalFloat)
		for sp := vm.sp - args + 1; sp < vm.sp; sp++ {
			y := vm.stack[sp].(*evalFloat)
			if lessThan == (y.f < x.f) {
				x = y
			}
		}
		vm.stack[vm.sp-args] = x
		vm.sp -= args - 1
		return 1
	}, "FN MULTICMP FLOAT64(SP-%d)...FLOAT64(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_i(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(vm *VirtualMachine) int {
		x := vm.stack[vm.sp-args].(*evalInt64)
		for sp := vm.sp - args + 1; sp < vm.sp; sp++ {
			y := vm.stack[sp].(*evalInt64)
			if lessThan == (y.i < x.i) {
				x = y
			}
		}
		vm.stack[vm.sp-args] = x
		vm.sp -= args - 1
		return 1
	}, "FN MULTICMP INT64(SP-%d)...INT64(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_u(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(vm *VirtualMachine) int {
		x := vm.stack[vm.sp-args].(*evalUint64)
		for sp := vm.sp - args + 1; sp < vm.sp; sp++ {
			y := vm.stack[sp].(*evalUint64)
			if lessThan == (y.u < x.u) {
				x = y
			}
		}
		vm.stack[vm.sp-args] = x
		vm.sp -= args - 1
		return 1
	}, "FN MULTICMP UINT64(SP-%d)...UINT64(SP-1)", args)
}

func (asm *assembler) Fn_REPEAT(i int) {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Fn_TO_BASE64(t sqltypes.Type, col collations.TypedCollation) {
	asm.emit(func(vm *VirtualMachine) int {
		str := vm.stack[vm.sp-1].(*evalBytes)

		encoded := make([]byte, mysqlBase64.EncodedLen(len(str.bytes)))
		mysqlBase64.Encode(encoded, str.bytes)

		str.tt = int16(t)
		str.col = col
		str.bytes = encoded
		return 1
	}, "FN TO_BASE64 VARCHAR(SP-1)")
}

func (asm *assembler) Like_coerce(expr *LikeExpr, coercion *compiledCoercion) {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Like_collate(expr *LikeExpr, collation collations.Collation) {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) Mul_dd() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		mathMul_dd0(l, r)
		vm.sp--
		return 1
	}, "MUL DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Mul_ff() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		l.f *= r.f
		vm.sp--
		return 1
	}, "MUL FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) Mul_ii() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.i, vm.err = mathMul_ii0(l.i, r.i)
		vm.sp--
		return 1
	}, "MUL INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Mul_ui(swap bool) {
	asm.adjustStack(-1)

	if swap {
		asm.emit(func(vm *VirtualMachine) int {
			var u uint64
			l := vm.stack[vm.sp-1].(*evalUint64)
			r := vm.stack[vm.sp-2].(*evalInt64)
			u, vm.err = mathMul_ui0(l.u, r.i)
			vm.stack[vm.sp-2] = vm.arena.newEvalUint64(u)
			vm.sp--
			return 1
		}, "MUL UINT64(SP-1), INT64(SP-2)")
	} else {
		asm.emit(func(vm *VirtualMachine) int {
			l := vm.stack[vm.sp-2].(*evalUint64)
			r := vm.stack[vm.sp-1].(*evalInt64)
			l.u, vm.err = mathMul_ui0(l.u, r.i)
			vm.sp--
			return 1
		}, "MUL UINT64(SP-2), INT64(SP-1)")
	}
}

func (asm *assembler) Mul_uu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u, vm.err = mathMul_uu0(l.u, r.u)
		vm.sp--
		return 1
	}, "MUL UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) Neg_d() {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalDecimal)
		arg.dec = arg.dec.Neg()
		return 1
	}, "NEG DECIMAL(SP-1)")
}

func (asm *assembler) Neg_f() {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalFloat)
		arg.f = -arg.f
		return 1
	}, "NEG FLOAT64(SP-1)")
}

func (asm *assembler) Neg_hex() {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalUint64)
		vm.stack[vm.sp-1] = vm.arena.newEvalFloat(-float64(arg.u))
		return 1
	}, "NEG HEX(SP-1)")
}

func (asm *assembler) Neg_i() {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalInt64)
		if arg.i == math.MinInt64 {
			vm.err = errDeoptimize
		} else {
			arg.i = -arg.i
		}
		return 1
	}, "NEG INT64(SP-1)")
}

func (asm *assembler) Neg_u() {
	asm.emit(func(vm *VirtualMachine) int {
		arg := vm.stack[vm.sp-1].(*evalUint64)
		if arg.u > math.MaxInt64+1 {
			vm.err = errDeoptimize
		} else {
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(-int64(arg.u))
		}
		return 1
	}, "NEG UINT64(SP-1)")
}

func (asm *assembler) NullCheck1(j *jump) {
	asm.emit(func(vm *VirtualMachine) int {
		if vm.stack[vm.sp-1] == nil {
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1")
}

func (asm *assembler) NullCheck2(j *jump) {
	asm.emit(func(vm *VirtualMachine) int {
		if vm.stack[vm.sp-2] == nil || vm.stack[vm.sp-1] == nil {
			vm.stack[vm.sp-2] = nil
			vm.sp--
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1, SP-2")
}

func (asm *assembler) NullCmp(j *jump) {
	asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) PackTuple(tlen int) {
	asm.adjustStack(-(tlen - 1))
	asm.emit(func(vm *VirtualMachine) int {
		tuple := make([]eval, tlen)
		copy(tuple, vm.stack[vm.sp-tlen:])
		vm.stack[vm.sp-tlen] = &evalTuple{tuple}
		vm.sp -= tlen - 1
		return 1
	}, "TUPLE (SP-%d)...(SP-1)", tlen)
}

func (asm *assembler) Parse_j(offset int) {
	asm.emit(func(vm *VirtualMachine) int {
		var p json.Parser
		arg := vm.stack[vm.sp-offset].(*evalBytes)
		vm.stack[vm.sp-offset], vm.err = p.ParseBytes(arg.bytes)
		return 1
	}, "PARSE_JSON VARCHAR(SP-%d)", offset)
}

func (asm *assembler) PushColumn_bin(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalBinary(vm.row[offset].Raw())
		vm.sp++
		return 1
	}, "PUSH VARBINARY(:%d)", offset)
}

func (asm *assembler) PushColumn_d(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		var dec decimal.Decimal
		dec, vm.err = decimal.NewFromMySQL(vm.row[offset].Raw())
		vm.stack[vm.sp] = vm.arena.newEvalDecimal(dec, 0, 0)
		vm.sp++
		return 1
	}, "PUSH DECIMAL(:%d)", offset)
}

func (asm *assembler) PushColumn_f(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		var fval float64
		fval, vm.err = vm.row[offset].ToFloat64()
		vm.stack[vm.sp] = vm.arena.newEvalFloat(fval)
		vm.sp++
		return 1
	}, "PUSH FLOAT64(:%d)", offset)
}

func (asm *assembler) PushColumn_hexnum(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		var raw []byte
		raw, vm.err = parseHexNumber(vm.row[offset].Raw())
		vm.stack[vm.sp] = newEvalBytesHex(raw)
		vm.sp++
		return 1
	}, "PUSH HEXNUM(:%d)", offset)
}

func (asm *assembler) PushColumn_hexval(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		hex := vm.row[offset].Raw()
		var raw []byte
		raw, vm.err = parseHexLiteral(hex[2 : len(hex)-1])
		vm.stack[vm.sp] = newEvalBytesHex(raw)
		vm.sp++
		return 1
	}, "PUSH HEXVAL(:%d)", offset)
}

func (asm *assembler) PushColumn_i(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		var ival int64
		ival, vm.err = vm.row[offset].ToInt64()
		vm.stack[vm.sp] = vm.arena.newEvalInt64(ival)
		vm.sp++
		return 1
	}, "PUSH INT64(:%d)", offset)
}

func (asm *assembler) PushColumn_json(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		var parser json.Parser
		vm.stack[vm.sp], vm.err = parser.ParseBytes(vm.row[offset].Raw())
		vm.sp++
		return 1
	}, "PUSH JSON(:%d)", offset)
}

func (asm *assembler) PushColumn_text(offset int, col collations.TypedCollation) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = newEvalText(vm.row[offset].Raw(), col)
		vm.sp++
		return 1
	}, "PUSH VARCHAR(:%d) COLLATE %d", offset, col.Collation)
}

func (asm *assembler) PushColumn_u(offset int) {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		var uval uint64
		uval, vm.err = vm.row[offset].ToUint64()
		vm.stack[vm.sp] = vm.arena.newEvalUint64(uval)
		vm.sp++
		return 1
	}, "PUSH UINT64(:%d)", offset)
}

func (asm *assembler) PushLiteral(lit eval) error {
	asm.adjustStack(1)

	if lit == evalBoolTrue {
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = evalBoolTrue
			vm.sp++
			return 1
		}, "PUSH true")
		return nil
	}

	if lit == evalBoolFalse {
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = evalBoolFalse
			vm.sp++
			return 1
		}, "PUSH false")
		return nil
	}

	switch lit := lit.(type) {
	case *evalInt64:
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalInt64(lit.i)
			vm.sp++
			return 1
		}, "PUSH INT64(%s)", lit.ToRawBytes())
	case *evalUint64:
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalUint64(lit.u)
			vm.sp++
			return 1
		}, "PUSH UINT64(%s)", lit.ToRawBytes())
	case *evalFloat:
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalFloat(lit.f)
			vm.sp++
			return 1
		}, "PUSH FLOAT64(%s)", lit.ToRawBytes())
	case *evalDecimal:
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp] = vm.arena.newEvalDecimalWithPrec(lit.dec, lit.length)
			vm.sp++
			return 1
		}, "PUSH DECIMAL(%s)", lit.ToRawBytes())
	case *evalBytes:
		asm.emit(func(vm *VirtualMachine) int {
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

func (asm *assembler) PushNull() {
	asm.adjustStack(1)

	asm.emit(func(vm *VirtualMachine) int {
		vm.stack[vm.sp] = nil
		vm.sp++
		return 1
	}, "PUSH NULL")
}

func (asm *assembler) SetBool(offset int, b bool) {
	if b {
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp-offset] = evalBoolTrue
			return 1
		}, "SET (SP-%d), BOOL(true)", offset)
	} else {
		asm.emit(func(vm *VirtualMachine) int {
			vm.stack[vm.sp-offset] = evalBoolFalse
			return 1
		}, "SET (SP-%d), BOOL(false)", offset)
	}
}

func (asm *assembler) Sub_dd() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalDecimal)
		r := vm.stack[vm.sp-1].(*evalDecimal)
		mathSub_dd0(l, r)
		vm.sp--
		return 1
	}, "SUB DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Sub_ff() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalFloat)
		r := vm.stack[vm.sp-1].(*evalFloat)
		l.f -= r.f
		vm.sp--
		return 1
	}, "SUB FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) Sub_ii() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.i, vm.err = mathSub_ii0(l.i, r.i)
		vm.sp--
		return 1
	}, "SUB INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Sub_iu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalInt64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		r.u, vm.err = mathSub_iu0(l.i, r.u)
		vm.stack[vm.sp-2] = r
		vm.sp--
		return 1
	}, "SUB INT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) Sub_ui() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalInt64)
		l.u, vm.err = mathSub_ui0(l.u, r.i)
		vm.sp--
		return 1
	}, "SUB UINT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Sub_uu() {
	asm.adjustStack(-1)

	asm.emit(func(vm *VirtualMachine) int {
		l := vm.stack[vm.sp-2].(*evalUint64)
		r := vm.stack[vm.sp-1].(*evalUint64)
		l.u, vm.err = mathSub_uu0(l.u, r.u)
		vm.sp--
		return 1
	}, "SUB UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) Collation(col collations.TypedCollation) {
	asm.emit(func(vm *VirtualMachine) int {
		v := evalCollation(vm.stack[vm.sp-1])
		vm.stack[vm.sp-1] = vm.arena.newEvalText([]byte(v.Collation.Get().Name()), col)
		return 1
	}, "COLLATION (SP-1)")
}

func (asm *assembler) Fn_CEIL_f() {
	asm.emit(func(vm *VirtualMachine) int {
		f := vm.stack[vm.sp-1].(*evalFloat)
		f.f = math.Ceil(f.f)
		return 1
	}, "CEIL FLOAT64(SP-1)")
}

func (asm *assembler) Fn_CEIL_d() {
	asm.emit(func(vm *VirtualMachine) int {
		d := vm.stack[vm.sp-1].(*evalDecimal)
		c := d.dec.Ceil()
		i, valid := c.Int64()
		if valid {
			vm.stack[vm.sp-1] = vm.arena.newEvalInt64(i)
		} else {
			vm.err = errDeoptimize
		}
		return 1
	}, "CEIL DECIMAL(SP-1)")
}

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
