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
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"math"
	"math/bits"
	"strconv"
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

type jump struct {
	from, to int
}

func (j *jump) offset() int {
	return j.to - j.from
}

type assembler struct {
	ins   []frame
	log   CompilerLog
	stack struct {
		cur int
		max int
	}
}

func (asm *assembler) jumpFrom() *jump {
	return &jump{from: len(asm.ins)}
}

func (asm *assembler) jumpDestination(jumps ...*jump) {
	for _, j := range jumps {
		if j != nil {
			j.to = len(asm.ins)
		}
	}
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

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		mathAdd_dd0(l, r)
		env.vm.sp--
		return 1
	}, "ADD DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Add_ff() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalFloat)
		l.f += r.f
		env.vm.sp--
		return 1
	}, "ADD FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) Add_ii() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		l.i, env.vm.err = mathAdd_ii0(l.i, r.i)
		env.vm.sp--
		return 1
	}, "ADD INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Add_ui(swap bool) {
	asm.adjustStack(-1)

	if swap {
		asm.emit(func(env *ExpressionEnv) int {
			var u uint64
			l := env.vm.stack[env.vm.sp-1].(*evalUint64)
			r := env.vm.stack[env.vm.sp-2].(*evalInt64)
			u, env.vm.err = mathAdd_ui0(l.u, r.i)
			env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalUint64(u)
			env.vm.sp--
			return 1
		}, "ADD UINT64(SP-1), INT64(SP-2)")
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			l := env.vm.stack[env.vm.sp-2].(*evalUint64)
			r := env.vm.stack[env.vm.sp-1].(*evalInt64)
			l.u, env.vm.err = mathAdd_ui0(l.u, r.i)
			env.vm.sp--
			return 1
		}, "ADD UINT64(SP-2), INT64(SP-1)")
	}
}

func (asm *assembler) Add_uu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u, env.vm.err = mathAdd_uu0(l.u, r.u)
		env.vm.sp--
		return 1
	}, "ADD UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitCount_b() {
	asm.emit(func(env *ExpressionEnv) int {
		a := env.vm.stack[env.vm.sp-1].(*evalBytes)
		count := 0
		for _, b := range a.bytes {
			count += bits.OnesCount8(b)
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(count))
		return 1
	}, "BIT_COUNT BINARY(SP-1)")
}

func (asm *assembler) BitCount_u() {
	asm.emit(func(env *ExpressionEnv) int {
		a := env.vm.stack[env.vm.sp-1].(*evalUint64)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(bits.OnesCount64(a.u)))
		return 1
	}, "BIT_COUNT UINT64(SP-1)")
}

func (asm *assembler) BitOp_and_bb() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)
		if len(l.bytes) != len(r.bytes) {
			env.vm.err = errBitwiseOperandsLength
			return 0
		}
		for i := range l.bytes {
			l.bytes[i] = l.bytes[i] & r.bytes[i]
		}
		env.vm.sp--
		return 1
	}, "AND BINARY(SP-2), BINARY(SP-1)")
}

func (asm *assembler) BitOp_or_bb() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)
		if len(l.bytes) != len(r.bytes) {
			env.vm.err = errBitwiseOperandsLength
			return 0
		}
		for i := range l.bytes {
			l.bytes[i] = l.bytes[i] | r.bytes[i]
		}
		env.vm.sp--
		return 1
	}, "OR BINARY(SP-2), BINARY(SP-1)")
}

func (asm *assembler) BitOp_xor_bb() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)
		if len(l.bytes) != len(r.bytes) {
			env.vm.err = errBitwiseOperandsLength
			return 0
		}
		for i := range l.bytes {
			l.bytes[i] = l.bytes[i] ^ r.bytes[i]
		}
		env.vm.sp--
		return 1
	}, "XOR BINARY(SP-2), BINARY(SP-1)")
}

func (asm *assembler) BitOp_and_uu() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u = l.u & r.u
		env.vm.sp--
		return 1
	}, "AND UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitOp_or_uu() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u = l.u | r.u
		env.vm.sp--
		return 1
	}, "OR UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitOp_xor_uu() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u = l.u ^ r.u
		env.vm.sp--
		return 1
	}, "XOR UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitShiftLeft_bu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)

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

		env.vm.sp--
		return 1
	}, "BIT_SHL BINARY(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitShiftLeft_uu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u = l.u << r.u

		env.vm.sp--
		return 1
	}, "BIT_SHL UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitShiftRight_bu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)

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

		env.vm.sp--
		return 1
	}, "BIT_SHR BINARY(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitShiftRight_uu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u = l.u >> r.u

		env.vm.sp--
		return 1
	}, "BIT_SHR UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) BitwiseNot_b() {
	asm.emit(func(env *ExpressionEnv) int {
		a := env.vm.stack[env.vm.sp-1].(*evalBytes)
		for i := range a.bytes {
			a.bytes[i] = ^a.bytes[i]
		}
		return 1
	}, "BIT_NOT BINARY(SP-1)")
}

func (asm *assembler) BitwiseNot_u() {
	asm.emit(func(env *ExpressionEnv) int {
		a := env.vm.stack[env.vm.sp-1].(*evalUint64)
		a.u = ^a.u
		return 1
	}, "BIT_NOT UINT64(SP-1)")
}

func (asm *assembler) Cmp_eq() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp == 0)
		env.vm.sp++
		return 1
	}, "CMPFLAG EQ")
}

func (asm *assembler) Cmp_eq_n() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.flags.null {
			env.vm.stack[env.vm.sp] = nil
		} else {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp == 0)
		}
		env.vm.sp++
		return 1
	}, "CMPFLAG EQ [NULL]")
}

func (asm *assembler) Cmp_ge() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp >= 0)
		env.vm.sp++
		return 1
	}, "CMPFLAG GE")
}

func (asm *assembler) Cmp_ge_n() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.flags.null {
			env.vm.stack[env.vm.sp] = nil
		} else {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp >= 0)
		}
		env.vm.sp++
		return 1
	}, "CMPFLAG GE [NULL]")
}

func (asm *assembler) Cmp_gt() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp > 0)
		env.vm.sp++
		return 1
	}, "CMPFLAG GT")
}

func (asm *assembler) Cmp_gt_n() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.flags.null {
			env.vm.stack[env.vm.sp] = nil
		} else {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp > 0)
		}
		env.vm.sp++
		return 1
	}, "CMPFLAG GT [NULL]")
}

func (asm *assembler) Cmp_le() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp <= 0)
		env.vm.sp++
		return 1
	}, "CMPFLAG LE")
}

func (asm *assembler) Cmp_le_n() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.flags.null {
			env.vm.stack[env.vm.sp] = nil
		} else {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp <= 0)
		}
		env.vm.sp++
		return 1
	}, "CMPFLAG LE [NULL]")
}

func (asm *assembler) Cmp_lt() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp < 0)
		env.vm.sp++
		return 1
	}, "CMPFLAG LT")
}

func (asm *assembler) Cmp_lt_n() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.flags.null {
			env.vm.stack[env.vm.sp] = nil
		} else {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp < 0)
		}
		env.vm.sp++
		return 1
	}, "CMPFLAG LT [NULL]")
}
func (asm *assembler) Cmp_ne() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp != 0)
		env.vm.sp++
		return 1
	}, "CMPFLAG NE")
}

func (asm *assembler) Cmp_ne_n() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.flags.null {
			env.vm.stack[env.vm.sp] = nil
		} else {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalBool(env.vm.flags.cmp != 0)
		}
		env.vm.sp++
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

	asm.emit(func(env *ExpressionEnv) int {
		end := env.vm.sp - elseOffset
		for sp := env.vm.sp - stackDepth; sp < end; sp += 2 {
			if env.vm.stack[sp].(*evalInt64).i != 0 {
				env.vm.stack[env.vm.sp-stackDepth], env.vm.err = evalCoerce(env.vm.stack[sp+1], tt, cc.Collation)
				goto done
			}
		}
		if elseOffset != 0 {
			env.vm.stack[env.vm.sp-stackDepth], env.vm.err = evalCoerce(env.vm.stack[env.vm.sp-1], tt, cc.Collation)
		} else {
			env.vm.stack[env.vm.sp-stackDepth] = nil
		}
	done:
		env.vm.sp -= stackDepth - 1
		return 1
	}, "CASE [%d cases, else = %v]", cases, hasElse)
}

func (asm *assembler) CmpNum_dd() {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		env.vm.sp -= 2
		env.vm.flags.cmp = l.dec.Cmp(r.dec)
		return 1
	}, "CMP DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) CmpNum_fd(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-left].(*evalFloat)
		r := env.vm.stack[env.vm.sp-right].(*evalDecimal)
		env.vm.sp -= 2
		fval, ok := r.dec.Float64()
		if !ok {
			env.vm.err = errDecimalOutOfRange
		}
		env.vm.flags.cmp = cmpnum(l.f, fval)
		return 1
	}, "CMP FLOAT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_ff() {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalFloat)
		env.vm.sp -= 2
		env.vm.flags.cmp = cmpnum(l.f, r.f)
		return 1
	}, "CMP FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) CmpNum_id(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-left].(*evalInt64)
		r := env.vm.stack[env.vm.sp-right].(*evalDecimal)
		env.vm.sp -= 2
		env.vm.flags.cmp = decimal.NewFromInt(l.i).Cmp(r.dec)
		return 1
	}, "CMP INT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_if(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-left].(*evalInt64)
		r := env.vm.stack[env.vm.sp-right].(*evalFloat)
		env.vm.sp -= 2
		env.vm.flags.cmp = cmpnum(float64(l.i), r.f)
		return 1
	}, "CMP INT64(SP-%d), FLOAT64(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_ii() {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		env.vm.sp -= 2
		env.vm.flags.cmp = cmpnum(l.i, r.i)
		return 1
	}, "CMP INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) CmpNum_iu(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-left].(*evalInt64)
		r := env.vm.stack[env.vm.sp-right].(*evalUint64)
		env.vm.sp -= 2
		if l.i < 0 {
			env.vm.flags.cmp = -1
		} else {
			env.vm.flags.cmp = cmpnum(uint64(l.i), r.u)
		}
		return 1
	}, "CMP INT64(SP-%d), UINT64(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_ud(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-left].(*evalUint64)
		r := env.vm.stack[env.vm.sp-right].(*evalDecimal)
		env.vm.sp -= 2
		env.vm.flags.cmp = decimal.NewFromUint(l.u).Cmp(r.dec)
		return 1
	}, "CMP UINT64(SP-%d), DECIMAL(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_uf(left, right int) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-left].(*evalUint64)
		r := env.vm.stack[env.vm.sp-right].(*evalFloat)
		env.vm.sp -= 2
		env.vm.flags.cmp = cmpnum(float64(l.u), r.f)
		return 1
	}, "CMP UINT64(SP-%d), FLOAT64(SP-%d)", left, right)
}

func (asm *assembler) CmpNum_uu() {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		env.vm.sp -= 2
		env.vm.flags.cmp = cmpnum(l.u, r.u)
		return 1
	}, "CMP UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) CmpString_coerce(coercion *compiledCoercion) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.sp -= 2

		var bl, br []byte
		bl, env.vm.err = coercion.left(nil, l.bytes)
		if env.vm.err != nil {
			return 0
		}
		br, env.vm.err = coercion.right(nil, r.bytes)
		if env.vm.err != nil {
			return 0
		}
		env.vm.flags.cmp = coercion.col.Collate(bl, br, false)
		return 1
	}, "CMP VARCHAR(SP-2), VARCHAR(SP-1) COERCE AND COLLATE '%s'", coercion.col.Name())
}

func (asm *assembler) CmpString_collate(collation collations.Collation) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2]
		r := env.vm.stack[env.vm.sp-1]
		env.vm.sp -= 2
		env.vm.flags.cmp = collation.Collate(l.ToRawBytes(), r.ToRawBytes(), false)
		return 1
	}, "CMP VARCHAR(SP-2), VARCHAR(SP-1) COLLATE '%s'", collation.Name())
}

func (asm *assembler) CmpJSON() {
	asm.adjustStack(-2)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalJSON)
		r := env.vm.stack[env.vm.sp-1].(*evalJSON)
		env.vm.sp -= 2
		env.vm.flags.cmp, env.vm.err = compareJSONValue(l, r)
		return 1
	}, "CMP JSON(SP-2), JSON(SP-1)")
}

func (asm *assembler) CmpTuple(fullEquality bool) {
	asm.adjustStack(-2)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalTuple)
		r := env.vm.stack[env.vm.sp-1].(*evalTuple)
		env.vm.sp -= 2
		env.vm.flags.cmp, env.vm.flags.null, env.vm.err = evalCompareMany(l.t, r.t, fullEquality)
		return 1
	}, "CMP TUPLE(SP-2), TUPLE(SP-1)")
}

func (asm *assembler) CmpTupleNullsafe() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalTuple)
		r := env.vm.stack[env.vm.sp-1].(*evalTuple)

		var equals bool
		equals, env.vm.err = evalCompareTuplesNullSafe(l.t, r.t)

		env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalBool(equals)
		env.vm.sp -= 1
		return 1
	}, "CMP NULLSAFE TUPLE(SP-2), TUPLE(SP-1)")
}

func (asm *assembler) CmpDateString() {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2]
		r := env.vm.stack[env.vm.sp-1]
		env.vm.sp -= 2
		env.vm.flags.cmp = compareDateAndString(l, r)
		return 1
	}, "CMP DATE|STRING(SP-2), DATE|STRING(SP-1)")
}

func (asm *assembler) CmpDates() {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalTemporal)
		r := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.sp -= 2
		env.vm.flags.cmp = compareDates(l, r)
		return 1
	}, "CMP DATE(SP-2), DATE(SP-1)")
}

func (asm *assembler) Collate(col collations.ID) {
	asm.emit(func(env *ExpressionEnv) int {
		a := env.vm.stack[env.vm.sp-1].(*evalBytes)
		a.tt = int16(sqltypes.VarChar)
		a.col.Collation = col
		return 1
	}, "COLLATE VARCHAR(SP-1), %d", col)
}

func (asm *assembler) Convert_bB(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset]
		var f float64
		if arg != nil {
			f, _ = fastparse.ParseFloat64(arg.(*evalBytes).string())
		}
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(f != 0.0)
		return 1
	}, "CONV VARBINARY(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_TB(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset]
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(arg != nil && !arg.(*evalTemporal).isZero())
		return 1
	}, "CONV SQLTYPES(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_jB(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalJSON)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(arg.ToBoolean())
		return 1
	}, "CONV JSON(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_bj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalBytes)
		env.vm.stack[env.vm.sp-offset] = evalConvert_bj(arg)
		return 1
	}, "CONV VARBINARY(SP-%d), JSON", offset)
}

func (asm *assembler) ConvertArg_cj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalBytes)
		env.vm.stack[env.vm.sp-offset], env.vm.err = evalConvertArg_cj(arg)
		return 1
	}, "CONVA VARCHAR(SP-%d), JSON", offset)
}

func (asm *assembler) Convert_cj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalBytes)
		env.vm.stack[env.vm.sp-offset], env.vm.err = evalConvert_cj(arg)
		return 1
	}, "CONV VARCHAR(SP-%d), JSON", offset)
}

func (asm *assembler) Convert_Tj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalTemporal)
		env.vm.stack[env.vm.sp-offset] = arg.toJSON()
		return 1
	}, "CONV SQLTIME(SP-%d), JSON", offset)
}

func (asm *assembler) Convert_dB(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset]
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(arg != nil && !arg.(*evalDecimal).dec.IsZero())
		return 1
	}, "CONV DECIMAL(SP-%d), BOOL", offset)
}

// Convert_dbit is a special instruction emission for converting
// a bigdecimal in preparation for a bitwise operation. In that case
// we need to convert the bigdecimal to an int64 and then cast to
// uint64 to ensure we match the behavior of MySQL.
func (asm *assembler) Convert_dbit(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := evalToInt64(env.vm.stack[env.vm.sp-offset])
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalUint64(uint64(arg.i))
		return 1
	}, "CONV DECIMAL_BITWISE(SP-%d), UINT64", offset)
}

func (asm *assembler) Convert_fB(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset]
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(arg != nil && arg.(*evalFloat).f != 0.0)
		return 1
	}, "CONV FLOAT64(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_fj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalFloat)
		env.vm.stack[env.vm.sp-offset] = evalConvert_fj(arg)
		return 1
	}, "CONV FLOAT64(SP-%d), JSON")
}

func (asm *assembler) Convert_hex(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		var ok bool
		env.vm.stack[env.vm.sp-offset], ok = env.vm.stack[env.vm.sp-offset].(*evalBytes).toNumericHex()
		if !ok {
			env.vm.err = errDeoptimize
		}
		return 1
	}, "CONV VARBINARY(SP-%d), HEX", offset)
}

func (asm *assembler) Convert_Ti(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		v := env.vm.stack[env.vm.sp-offset].(*evalTemporal)
		if v.prec != 0 {
			env.vm.err = errDeoptimize
			return 1
		}
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalInt64(v.toInt64())
		return 1
	}, "CONV SQLTIME(SP-%d), INT64", offset)
}

func (asm *assembler) Convert_Tf(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		v := env.vm.stack[env.vm.sp-offset].(*evalTemporal)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalFloat(v.toFloat())
		return 1
	}, "CONV SQLTIME(SP-%d), FLOAT64", offset)
}

func (asm *assembler) Convert_iB(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset]
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(arg != nil && arg.(*evalInt64).i != 0)
		return 1
	}, "CONV INT64(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_id(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalInt64)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalDecimalWithPrec(decimal.NewFromInt(arg.i), 0)
		return 1
	}, "CONV INT64(SP-%d), FLOAT64", offset)
}

func (asm *assembler) Convert_if(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalInt64)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalFloat(arg.toFloat0())
		return 1
	}, "CONV INT64(SP-%d), FLOAT64", offset)
}

func (asm *assembler) Convert_iu(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalInt64)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalUint64(uint64(arg.i))
		return 1
	}, "CONV INT64(SP-%d), UINT64", offset)
}

func (asm *assembler) Clamp_u(offset int, val uint64) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalUint64)
		if arg.u > val {
			arg.u = val
		}
		return 1
	}, "CLAMP UINT64(SP-%d), UINT64", offset)
}

func (asm *assembler) Convert_ij(offset int, isBool bool) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalInt64)
		switch {
		case isBool && arg.i == 0:
			env.vm.stack[env.vm.sp-offset] = json.ValueFalse
		case isBool && arg.i == 1:
			env.vm.stack[env.vm.sp-offset] = json.ValueTrue
		default:
			env.vm.stack[env.vm.sp-offset] = json.NewNumber(string(arg.ToRawBytes()), json.NumberTypeSigned)
		}
		return 1
	}, "CONV INT64(SP-%d), JSON")
}

func (asm *assembler) Convert_uj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalUint64)
		env.vm.stack[env.vm.sp-offset] = json.NewNumber(string(arg.ToRawBytes()), json.NumberTypeUnsigned)
		return 1
	}, "CONV UINT64(SP-%d), JSON")
}

func (asm *assembler) Convert_dj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalDecimal)
		env.vm.stack[env.vm.sp-offset] = json.NewNumber(string(arg.ToRawBytes()), json.NumberTypeDecimal)
		return 1
	}, "CONV DECIMAL(SP-%d), JSON")
}

func (asm *assembler) Convert_Nj(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp-offset] = json.ValueNull
		return 1
	}, "CONV NULL(SP-%d), JSON")
}

func (asm *assembler) Convert_uB(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset]
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(arg != nil && arg.(*evalUint64).u != 0)
		return 1
	}, "CONV UINT64(SP-%d), BOOL", offset)
}

func (asm *assembler) Convert_ud(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalUint64)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalDecimalWithPrec(decimal.NewFromUint(arg.u), 0)
		return 1
	}, "CONV UINT64(SP-%d), FLOAT64)", offset)
}

func (asm *assembler) Convert_uf(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalUint64)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalFloat(arg.toFloat0())
		return 1
	}, "CONV UINT64(SP-%d), FLOAT64)", offset)
}

func (asm *assembler) Convert_ui(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalUint64)
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalInt64(int64(arg.u))
		return 1
	}, "CONV UINT64(SP-%d), INT64", offset)
}

func (asm *assembler) Convert_xb(offset int, t sqltypes.Type, length int, hasLength bool) {
	if hasLength {
		asm.emit(func(env *ExpressionEnv) int {
			arg := evalToBinary(env.vm.stack[env.vm.sp-offset])
			arg.truncateInPlace(length)
			arg.tt = int16(t)
			env.vm.stack[env.vm.sp-offset] = arg
			return 1
		}, "CONV (SP-%d), VARBINARY[%d]", offset, length)
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			arg := evalToBinary(env.vm.stack[env.vm.sp-offset])
			arg.tt = int16(t)
			env.vm.stack[env.vm.sp-offset] = arg
			return 1
		}, "CONV (SP-%d), VARBINARY", offset)
	}
}

func (asm *assembler) Convert_xc(offset int, t sqltypes.Type, collation collations.ID, length int, hasLength bool) {
	if hasLength {
		asm.emit(func(env *ExpressionEnv) int {
			arg, err := evalToVarchar(env.vm.stack[env.vm.sp-offset], collation, true)
			if err != nil {
				env.vm.stack[env.vm.sp-offset] = nil
			} else {
				arg.truncateInPlace(length)
				arg.tt = int16(t)
				env.vm.stack[env.vm.sp-offset] = arg
			}
			return 1
		}, "CONV (SP-%d), VARCHAR[%d]", offset, length)
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			arg, err := evalToVarchar(env.vm.stack[env.vm.sp-offset], collation, true)
			if err != nil {
				env.vm.stack[env.vm.sp-offset] = nil
			} else {
				arg.tt = int16(t)
				env.vm.stack[env.vm.sp-offset] = arg
			}
			return 1
		}, "CONV (SP-%d), VARCHAR", offset)
	}
}

func (asm *assembler) Convert_xce(offset int, t sqltypes.Type, collation collations.ID) {
	asm.emit(func(env *ExpressionEnv) int {
		arg, err := evalToVarchar(env.vm.stack[env.vm.sp-offset], collation, true)
		if err != nil {
			env.vm.stack[env.vm.sp-offset] = nil
			env.vm.err = err
		} else {
			arg.tt = int16(t)
			env.vm.stack[env.vm.sp-offset] = arg
		}
		return 1
	}, "CONVE (SP-%d), VARCHAR", offset)
}

func (asm *assembler) Convert_xd(offset int, m, d int32) {
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp-offset] = evalToDecimal(env.vm.stack[env.vm.sp-offset], m, d)
		return 1
	}, "CONV (SP-%d), DECIMAL", offset)
}

func (asm *assembler) Convert_xf(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp-offset], _ = evalToFloat(env.vm.stack[env.vm.sp-offset])
		return 1
	}, "CONV (SP-%d), FLOAT64", offset)
}

func (asm *assembler) Convert_xi(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := evalToInt64(env.vm.stack[env.vm.sp-offset])
		env.vm.stack[env.vm.sp-offset] = arg
		return 1
	}, "CONV (SP-%d), INT64", offset)
}

func (asm *assembler) Convert_xu(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := evalToInt64(env.vm.stack[env.vm.sp-offset])
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalUint64(uint64(arg.i))
		return 1
	}, "CONV (SP-%d), UINT64", offset)
}

func (asm *assembler) Convert_xD(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		// Need to explicitly check here or we otherwise
		// store a nil wrapper in an interface vs. a direct
		// nil.
		d := evalToDate(env.vm.stack[env.vm.sp-offset])
		if d == nil {
			env.vm.stack[env.vm.sp-offset] = nil
		} else {
			env.vm.stack[env.vm.sp-offset] = d
		}
		return 1
	}, "CONV (SP-%d), DATE", offset)
}

func (asm *assembler) Convert_xD_nz(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		// Need to explicitly check here or we otherwise
		// store a nil wrapper in an interface vs. a direct
		// nil.
		d := evalToDate(env.vm.stack[env.vm.sp-offset])
		if d == nil || d.isZero() {
			env.vm.stack[env.vm.sp-offset] = nil
		} else {
			env.vm.stack[env.vm.sp-offset] = d
		}
		return 1
	}, "CONV (SP-%d), DATE(NOZERO)", offset)
}

func (asm *assembler) Convert_xDT(offset, prec int) {
	asm.emit(func(env *ExpressionEnv) int {
		// Need to explicitly check here or we otherwise
		// store a nil wrapper in an interface vs. a direct
		// nil.
		dt := evalToDateTime(env.vm.stack[env.vm.sp-offset], prec)
		if dt == nil {
			env.vm.stack[env.vm.sp-offset] = nil
		} else {
			env.vm.stack[env.vm.sp-offset] = dt
		}
		return 1
	}, "CONV (SP-%d), DATETIME", offset)
}

func (asm *assembler) Convert_xDT_nz(offset, prec int) {
	asm.emit(func(env *ExpressionEnv) int {
		// Need to explicitly check here or we otherwise
		// store a nil wrapper in an interface vs. a direct
		// nil.
		dt := evalToDateTime(env.vm.stack[env.vm.sp-offset], prec)
		if dt == nil || dt.isZero() {
			env.vm.stack[env.vm.sp-offset] = nil
		} else {
			env.vm.stack[env.vm.sp-offset] = dt
		}
		return 1
	}, "CONV (SP-%d), DATETIME(NOZERO)", offset)
}

func (asm *assembler) Convert_xT(offset, prec int) {
	asm.emit(func(env *ExpressionEnv) int {
		t := evalToTime(env.vm.stack[env.vm.sp-offset], prec)
		if t == nil {
			env.vm.stack[env.vm.sp-offset] = nil
		} else {
			env.vm.stack[env.vm.sp-offset] = t
		}
		return 1
	}, "CONV (SP-%d), TIME", offset)
}

func (asm *assembler) Convert_tp(offset, prec int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalTemporal)
		arg.dt = arg.dt.Round(prec)
		arg.prec = uint8(prec)
		return 1
	}, "CONV (SP-%d), PRECISION", offset)
}

func (asm *assembler) Div_dd() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		if r.dec.IsZero() {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			mathDiv_dd0(l, r, divPrecisionIncrement)
		}
		env.vm.sp--
		return 1
	}, "DIV DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Div_ff() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalFloat)
		if r.f == 0.0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.f, env.vm.err = mathDiv_ff0(l.f, r.f)
		}
		env.vm.sp--
		return 1
	}, "DIV FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) IntDiv_ii() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if r.i == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.i = l.i / r.i
		}
		env.vm.sp--
		return 1
	}, "INTDIV INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) IntDiv_iu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		if r.u == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			r.u, env.vm.err = mathIntDiv_iu0(l.i, r.u)
			env.vm.stack[env.vm.sp-2] = r
		}
		env.vm.sp--
		return 1
	}, "INTDIV INT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) IntDiv_ui() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if r.i == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.u, env.vm.err = mathIntDiv_ui0(l.u, r.i)
		}
		env.vm.sp--
		return 1
	}, "INTDIV UINT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) IntDiv_uu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		if r.u == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.u = l.u / r.u
		}
		env.vm.sp--
		return 1
	}, "INTDIV UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) IntDiv_di() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		if r.dec.IsZero() {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			var res int64
			res, env.vm.err = mathIntDiv_di0(l, r)
			env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalInt64(res)
		}
		env.vm.sp--
		return 1
	}, "INTDIV DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) IntDiv_du() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		if r.dec.IsZero() {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			var res uint64
			res, env.vm.err = mathIntDiv_du0(l, r)
			env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalUint64(res)
		}
		env.vm.sp--
		return 1
	}, "UINTDIV DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Mod_ii() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if r.i == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.i = l.i % r.i
		}
		env.vm.sp--
		return 1
	}, "MOD INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Mod_iu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		if r.u == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.i = mathMod_iu0(l.i, r.u)
		}
		env.vm.sp--
		return 1
	}, "MOD INT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) Mod_ui() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if r.i == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.u, env.vm.err = mathMod_ui0(l.u, r.i)
		}
		env.vm.sp--
		return 1
	}, "MOD UINT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Mod_uu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		if r.u == 0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.u = l.u % r.u
		}
		env.vm.sp--
		return 1
	}, "MOD UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) Mod_ff() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalFloat)
		if r.f == 0.0 {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.f = math.Mod(l.f, r.f)
		}
		env.vm.sp--
		return 1
	}, "MOD FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) Mod_dd() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		if r.dec.IsZero() {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			l.dec, l.length = mathMod_dd0(l, r)
		}
		env.vm.sp--
		return 1
	}, "MOD DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Fn_ASCII() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)
		if len(arg.bytes) == 0 {
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(0)
		} else {
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.bytes[0]))
		}
		return 1
	}, "FN ASCII VARCHAR(SP-1)")
}

func (asm *assembler) Fn_ORD(col collations.ID) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(charOrd(arg.bytes, col))
		return 1
	}, "FN ORD VARCHAR(SP-1)")
}

func (asm *assembler) Fn_CEIL_d() {
	asm.emit(func(env *ExpressionEnv) int {
		d := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		c := d.dec.Ceil()
		i, valid := c.Int64()
		if valid {
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(i)
		} else {
			env.vm.err = errDeoptimize
		}
		return 1
	}, "FN CEIL DECIMAL(SP-1)")
}

func (asm *assembler) Fn_CEIL_f() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Ceil(f.f)
		return 1
	}, "FN CEIL FLOAT64(SP-1)")
}

func (asm *assembler) Fn_FLOOR_d() {
	asm.emit(func(env *ExpressionEnv) int {
		d := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		c := d.dec.Floor()
		i, valid := c.Int64()
		if valid {
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(i)
		} else {
			env.vm.err = errDeoptimize
		}
		return 1
	}, "FN FLOOR DECIMAL(SP-1)")
}

func (asm *assembler) Fn_FLOOR_f() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Floor(f.f)
		return 1
	}, "FN FLOOR FLOAT64(SP-1)")
}

func (asm *assembler) Fn_ABS_i() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if f.i >= 0 {
			return 1
		}
		if f.i == math.MinInt64 {
			env.vm.err = vterrors.NewErrorf(vtrpc.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "BIGINT value is out of range")
			return 1
		}
		f.i = -f.i
		return 1
	}, "FN ABS INT64(SP-1)")
}

func (asm *assembler) Fn_ABS_d() {
	asm.emit(func(env *ExpressionEnv) int {
		d := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		d.dec = d.dec.Abs()
		return 1
	}, "FN ABS DECIMAL(SP-1)")
}

func (asm *assembler) Fn_ABS_f() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		if f.f >= 0 {
			return 1
		}
		f.f = -f.f
		return 1
	}, "FN ABS FLOAT64(SP-1)")
}

func (asm *assembler) Fn_PI() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalFloat(math.Pi)
		env.vm.sp++
		return 1
	}, "FN PI")
}

func (asm *assembler) Fn_ACOS() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		if f.f < -1 || f.f > 1 {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		f.f = math.Acos(f.f)
		return 1
	}, "FN ACOS FLOAT64(SP-1)")
}

func (asm *assembler) Fn_ASIN() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		if f.f < -1 || f.f > 1 {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		f.f = math.Asin(f.f)
		return 1
	}, "FN ASIN FLOAT64(SP-1)")
}

func (asm *assembler) Fn_ATAN() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Atan(f.f)
		return 1
	}, "FN ATAN FLOAT64(SP-1)")
}

func (asm *assembler) Fn_ATAN2() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		f1 := env.vm.stack[env.vm.sp-2].(*evalFloat)
		f2 := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f1.f = math.Atan2(f1.f, f2.f)
		env.vm.sp--
		return 1
	}, "FN ATAN2 FLOAT64(SP-2) FLOAT64(SP-1)")
}

func (asm *assembler) Fn_COS() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Cos(f.f)
		return 1
	}, "FN COS FLOAT64(SP-1)")
}

func (asm *assembler) Fn_COT() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = 1.0 / math.Tan(f.f)
		return 1
	}, "FN COT FLOAT64(SP-1)")
}

func (asm *assembler) Fn_SIN() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Sin(f.f)
		return 1
	}, "FN SIN FLOAT64(SP-1)")
}

func (asm *assembler) Fn_TAN() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Tan(f.f)
		return 1
	}, "FN TAN FLOAT64(SP-1)")
}

func (asm *assembler) Fn_DEGREES() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = f.f * (180 / math.Pi)
		return 1
	}, "FN DEGREES FLOAT64(SP-1)")
}

func (asm *assembler) Fn_RADIANS() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = f.f * (math.Pi / 180)
		return 1
	}, "FN RADIANS FLOAT64(SP-1)")
}

func (asm *assembler) Fn_EXP() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Exp(f.f)
		if !isFinite(f.f) {
			env.vm.stack[env.vm.sp-1] = nil
		}
		return 1
	}, "FN EXP FLOAT64(SP-1)")
}

func (asm *assembler) Fn_LN() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		var ok bool
		f.f, ok = math_log(f.f)
		if !ok {
			env.vm.stack[env.vm.sp-1] = nil
		}
		return 1
	}, "FN LN FLOAT64(SP-1)")
}

func (asm *assembler) Fn_LOG() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		var ok bool
		f1 := env.vm.stack[env.vm.sp-2].(*evalFloat)
		f2 := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f1.f, ok = math_logN(f1.f, f2.f)
		if !ok {
			env.vm.stack[env.vm.sp-2] = nil
		}
		env.vm.sp--
		return 1
	}, "FN LOG FLOAT64(SP-1)")
}

func (asm *assembler) Fn_LOG10() {
	asm.emit(func(env *ExpressionEnv) int {
		var ok bool
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f, ok = math_log10(f.f)
		if !ok {
			env.vm.stack[env.vm.sp-1] = nil
		}
		return 1
	}, "FN LOG10 FLOAT64(SP-1)")
}

func (asm *assembler) Fn_LOG2() {
	asm.emit(func(env *ExpressionEnv) int {
		var ok bool
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f, ok = math_log2(f.f)
		if !ok {
			env.vm.stack[env.vm.sp-1] = nil
		}
		return 1
	}, "FN LOG2 FLOAT64(SP-1)")
}

func (asm *assembler) Fn_POW() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		f1 := env.vm.stack[env.vm.sp-2].(*evalFloat)
		f2 := env.vm.stack[env.vm.sp-1].(*evalFloat)

		f1.f = math.Pow(f1.f, f2.f)
		if !isFinite(f1.f) {
			env.vm.stack[env.vm.sp-2] = nil
		}
		env.vm.sp--
		return 1
	}, "FN POW FLOAT64(SP-1)")
}

func (asm *assembler) Fn_SIGN_i() {
	asm.emit(func(env *ExpressionEnv) int {
		i := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if i.i < 0 {
			i.i = -1
		} else if i.i > 0 {
			i.i = 1
		} else {
			i.i = 0
		}
		return 1
	}, "FN SIGN INT64(SP-1)")
}

func (asm *assembler) Fn_SIGN_u() {
	asm.emit(func(env *ExpressionEnv) int {
		u := env.vm.stack[env.vm.sp-1].(*evalUint64)
		a := int64(0)
		if u.u > 0 {
			a = 1
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(a)
		return 1
	}, "FN SIGN UINT64(SP-1)")
}

func (asm *assembler) Fn_SIGN_f() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		a := int64(0)
		if f.f < 0 {
			a = -1
		} else if f.f > 0 {
			a = 1
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(a)
		return 1
	}, "FN SIGN FLOAT64(SP-1)")
}

func (asm *assembler) Fn_SIGN_d() {
	asm.emit(func(env *ExpressionEnv) int {
		d := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		a := int64(d.dec.Sign())
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(a)
		return 1
	}, "FN SIGN FLOAT64(SP-1)")
}

func (asm *assembler) Fn_SQRT() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Sqrt(f.f)
		if !isFinite(f.f) {
			env.vm.stack[env.vm.sp-1] = nil
		}
		return 1
	}, "FN SQRT FLOAT64(SP-1)")
}

func (asm *assembler) Fn_ROUND1_f() {
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f.f = math.Round(f.f)
		return 1
	}, "FN ROUND FLOAT64(SP-1)")
}

func (asm *assembler) Fn_ROUND1_d() {
	asm.emit(func(env *ExpressionEnv) int {
		d := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		d.dec = d.dec.Round(0)
		d.length = 0
		return 1
	}, "FN ROUND DECIMAL(SP-1)")
}

func (asm *assembler) Fn_ROUND2_i() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		i := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)

		i.i = roundSigned(i.i, r.i)
		env.vm.sp--
		return 1
	}, "FN ROUND INT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_ROUND2_u() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		u := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)

		u.u = roundUnsigned(u.u, r.i)
		env.vm.sp--
		return 1
	}, "FN ROUND INT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_ROUND2_f() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if r.i == 0 {
			f.f = math.Round(f.f)
			env.vm.sp--
			return 1
		}

		r.i = clampRounding(r.i)
		factor := math.Pow(10, float64(r.i))
		if factor == 0.0 {
			f.f = 0.0
			env.vm.sp--
			return 1
		}
		f.f = math.Round(f.f*factor) / factor
		env.vm.sp--
		return 1
	}, "FN ROUND FLOAT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_ROUND2_d() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		d := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)

		if d.dec.IsZero() {
			env.vm.sp--
			return 1
		}

		if r.i == 0 {
			d.dec = d.dec.Round(0)
			d.length = 0
			env.vm.sp--
			return 1
		}

		r.i = clampRounding(r.i)
		digit := int32(r.i)
		if digit < 0 {
			digit = 0
		}
		if digit > d.length {
			digit = d.length
		}
		rounded := d.dec.Round(int32(r.i))
		if rounded.IsZero() {
			d.dec = decimal.Zero
			d.length = 0
			env.vm.sp--
			return 1
		}
		env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalDecimalWithPrec(rounded, digit)
		env.vm.sp--
		return 1
	}, "FN ROUND DECIMAL(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_TRUNCATE_i() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		i := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)

		i.i = truncateSigned(i.i, r.i)
		env.vm.sp--
		return 1
	}, "FN TRUNCATE INT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_TRUNCATE_u() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		u := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)

		u.u = truncateUnsigned(u.u, r.i)
		env.vm.sp--
		return 1
	}, "FN TRUNCATE INT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_TRUNCATE_f() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		f := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if r.i == 0 {
			f.f = math.Trunc(f.f)
			env.vm.sp--
			return 1
		}

		r.i = clampRounding(r.i)
		factor := math.Pow(10, float64(r.i))
		if factor == 0.0 {
			f.f = 0.0
			env.vm.sp--
			return 1
		}
		f.f = math.Trunc(f.f*factor) / factor
		env.vm.sp--
		return 1
	}, "FN TRUNCATE FLOAT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_TRUNCATE_d() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		d := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)

		if d.dec.IsZero() {
			env.vm.sp--
			return 1
		}

		if r.i == 0 {
			d.dec = d.dec.Truncate(0)
			d.length = 0
			env.vm.sp--
			return 1
		}

		r.i = clampRounding(r.i)
		digit := int32(r.i)
		if digit < 0 {
			digit = 0
		}
		if digit > d.length {
			digit = d.length
		}
		rounded := d.dec.Truncate(int32(r.i))
		if rounded.IsZero() {
			d.dec = decimal.Zero
			d.length = 0
			env.vm.sp--
			return 1
		}
		env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalDecimalWithPrec(rounded, digit)
		env.vm.sp--
		return 1
	}, "FN TRUNCATE DECIMAL(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_CRC32() {
	asm.emit(func(env *ExpressionEnv) int {
		b := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalUint64(uint64(crc32.ChecksumIEEE(b.bytes)))
		return 1
	}, "FN CRC32 BINARY(SP-1)")
}

func (asm *assembler) Fn_CONV_hu(offset int, baseOffset int) {
	asm.emit(func(env *ExpressionEnv) int {
		base := env.vm.stack[env.vm.sp-baseOffset].(*evalInt64)

		// Even though the base is not used at all with a hex string literal,
		// we still need to check the base range to make sure it is valid.
		if base.i < -36 || (base.i > -2 && base.i < 2) || base.i > 36 {
			env.vm.stack[env.vm.sp-offset] = nil
			return 1
		}

		env.vm.stack[env.vm.sp-offset], _ = env.vm.stack[env.vm.sp-offset].(*evalBytes).toNumericHex()
		return 1
	}, "FN CONV VARBINARY(SP-%d), HEX", offset)
}

func (asm *assembler) Fn_CONV_bu(offset int, baseOffset int) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-offset].(*evalBytes)
		base := env.vm.stack[env.vm.sp-baseOffset].(*evalInt64)

		if base.i < -36 || (base.i > -2 && base.i < 2) || base.i > 36 {
			env.vm.stack[env.vm.sp-offset] = nil
			return 1
		}
		if base.i < 0 {
			base.i = -base.i
		}

		var u uint64
		i, err := fastparse.ParseInt64(arg.string(), int(base.i))
		u = uint64(i)
		if errors.Is(err, fastparse.ErrOverflow) {
			u, _ = fastparse.ParseUint64(arg.string(), int(base.i))
		}
		env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalUint64(u)
		return 1
	}, "FN CONV VARBINARY(SP-%d), INT64(SP-%d)", offset, baseOffset)
}

func (asm *assembler) Fn_CONV_uc(t sqltypes.Type, col collations.TypedCollation) {
	asm.adjustStack(-2)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-3] == nil {
			env.vm.sp -= 2
			return 1
		}
		u := env.vm.stack[env.vm.sp-3].(*evalUint64).u
		base := env.vm.stack[env.vm.sp-1].(*evalInt64)

		if base.i < -36 || (base.i > -2 && base.i < 2) || base.i > 36 {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		var out []byte
		if base.i < 0 {
			out = strconv.AppendInt(out, int64(u), -int(base.i))
		} else {
			out = strconv.AppendUint(out, u, int(base.i))
		}

		res := env.vm.arena.newEvalBytesEmpty()
		res.tt = int16(t)
		res.bytes = upcaseASCII(out)
		res.col = col

		env.vm.stack[env.vm.sp-3] = res
		env.vm.sp -= 2
		return 1
	}, "FN CONV VARCHAR(SP-3) INT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_COLLATION(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		v := evalCollation(env.vm.stack[env.vm.sp-1])
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalText([]byte(v.Collation.Get().Name()), col)
		return 1
	}, "FN COLLATION (SP-1)")
}

func (asm *assembler) Fn_FROM_BASE64(t sqltypes.Type) {
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-1].(*evalBytes)

		decoded, err := mysqlBase64Decode(str.bytes)
		if err != nil {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		str.tt = int16(t)
		str.bytes = decoded
		return 1
	}, "FN FROM_BASE64 VARCHAR(SP-1)")
}

func (asm *assembler) Fn_HEX_c(t sqltypes.Type, col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)
		encoded := env.vm.arena.newEvalText(hexEncodeBytes(arg.bytes), col)
		encoded.tt = int16(t)
		env.vm.stack[env.vm.sp-1] = encoded
		return 1
	}, "FN HEX VARCHAR(SP-1)")
}

func (asm *assembler) Fn_HEX_d(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(evalNumeric)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalText(hexEncodeUint(uint64(arg.toInt64().i)), col)
		return 1
	}, "FN HEX NUMERIC(SP-1)")
}

func (asm *assembler) Fn_UNHEX_i(tt sqltypes.Type) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if arg.toInt64().i < 0 {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalRaw(hexDecodeUint(uint64(arg.toInt64().i)), tt, collationBinary)
		return 1
	}, "FN UNHEX INT64(SP-1)")
}

func (asm *assembler) Fn_UNHEX_u(tt sqltypes.Type) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalUint64)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalRaw(hexDecodeUint(uint64(arg.u)), tt, collationBinary)
		return 1
	}, "FN UNHEX UINT64(SP-1)")
}

func (asm *assembler) Fn_UNHEX_f(tt sqltypes.Type) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalFloat)
		f := arg.f
		if f != float64(int64(f)) {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalRaw(hexDecodeUint(uint64(arg.f)), tt, collationBinary)
		return 1
	}, "FN UNHEX FLOAT64(SP-1)")
}

func (asm *assembler) Fn_UNHEX_b(tt sqltypes.Type) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)
		decoded := make([]byte, hexDecodedLen(arg.bytes))

		ok := hexDecodeBytes(decoded, arg.bytes)
		if !ok {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}

		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalRaw(decoded, tt, collationBinary)
		return 1
	}, "FN UNHEX VARBINARY(SP-1)")
}

func (asm *assembler) Fn_UNHEX_j(tt sqltypes.Type) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalJSON)
		decoded, ok := hexDecodeJSON(arg)
		if !ok {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalRaw(decoded, tt, collationBinary)
		return 1
	}, "FN UNHEX VARBINARY(SP-1)")
}

func (asm *assembler) Fn_JSON_ARRAY(args int) {
	asm.adjustStack(-(args - 1))
	asm.emit(func(env *ExpressionEnv) int {
		ary := make([]*json.Value, 0, args)
		for sp := env.vm.sp - args; sp < env.vm.sp; sp++ {
			ary = append(ary, env.vm.stack[sp].(*json.Value))
		}
		env.vm.stack[env.vm.sp-args] = json.NewArray(ary)
		env.vm.sp -= args - 1
		return 1
	}, "FN JSON_ARRAY (SP-%d)...(SP-1)", args)
}

func (asm *assembler) Fn_JSON_CONTAINS_PATH(match jsonMatch, paths []*json.Path) {
	switch match {
	case jsonMatchOne:
		asm.emit(func(env *ExpressionEnv) int {
			arg := env.vm.stack[env.vm.sp-1].(*evalJSON)
			matched := false
			for _, p := range paths {
				p.Match(arg, true, func(*json.Value) { matched = true })
				if matched {
					break
				}
			}
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(matched)
			return 1
		}, "FN JSON_CONTAINS_PATH, SP-1, 'one', [static]")
	case jsonMatchAll:
		asm.emit(func(env *ExpressionEnv) int {
			arg := env.vm.stack[env.vm.sp-1].(*evalJSON)
			matched := true
			for _, p := range paths {
				matched = false
				p.Match(arg, true, func(*json.Value) { matched = true })
				if !matched {
					break
				}
			}
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(matched)
			return 1
		}, "FN JSON_CONTAINS_PATH, SP-1, 'all', [static]")
	}
}

func (asm *assembler) Fn_JSON_EXTRACT0(jp []*json.Path) {
	multi := len(jp) > 1 || slices2.Any(jp, func(path *json.Path) bool { return path.ContainsWildcards() })

	if multi {
		asm.emit(func(env *ExpressionEnv) int {
			matches := make([]*json.Value, 0, 4)
			arg := env.vm.stack[env.vm.sp-1].(*evalJSON)
			for _, jp := range jp {
				jp.Match(arg, true, func(value *json.Value) {
					matches = append(matches, value)
				})
			}
			if len(matches) == 0 {
				env.vm.stack[env.vm.sp-1] = nil
			} else {
				env.vm.stack[env.vm.sp-1] = json.NewArray(matches)
			}
			return 1
		}, "FN JSON_EXTRACT, SP-1, [static]")
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			var match *json.Value
			arg := env.vm.stack[env.vm.sp-1].(*evalJSON)
			jp[0].Match(arg, true, func(value *json.Value) {
				match = value
			})
			if match == nil {
				env.vm.stack[env.vm.sp-1] = nil
			} else {
				env.vm.stack[env.vm.sp-1] = match
			}
			return 1
		}, "FN JSON_EXTRACT, SP-1, [static]")
	}
}

func (asm *assembler) Fn_JSON_KEYS(jp *json.Path) {
	if jp == nil {
		asm.emit(func(env *ExpressionEnv) int {
			doc := env.vm.stack[env.vm.sp-1]
			if doc == nil {
				return 1
			}
			j := doc.(*evalJSON)
			if obj, ok := j.Object(); ok {
				var keys []*json.Value
				obj.Visit(func(key string, _ *json.Value) {
					keys = append(keys, json.NewString(key))
				})
				env.vm.stack[env.vm.sp-1] = json.NewArray(keys)
			} else {
				env.vm.stack[env.vm.sp-1] = nil
			}
			return 1
		}, "FN JSON_KEYS (SP-1)")
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			doc := env.vm.stack[env.vm.sp-1]
			if doc == nil {
				return 1
			}
			var obj *json.Object
			jp.Match(doc.(*evalJSON), false, func(value *json.Value) {
				obj, _ = value.Object()
			})
			if obj != nil {
				var keys []*json.Value
				obj.Visit(func(key string, _ *json.Value) {
					keys = append(keys, json.NewString(key))
				})
				env.vm.stack[env.vm.sp-1] = json.NewArray(keys)
			} else {
				env.vm.stack[env.vm.sp-1] = nil
			}
			return 1
		}, "FN JSON_KEYS (SP-1), %q", jp.String())
	}
}

func (asm *assembler) Fn_JSON_OBJECT(args int) {
	asm.adjustStack(-(args - 1))
	asm.emit(func(env *ExpressionEnv) int {
		var obj json.Object
		for sp := env.vm.sp - args; sp < env.vm.sp; sp += 2 {
			key := env.vm.stack[sp]
			val := env.vm.stack[sp+1]

			if key == nil {
				env.vm.err = errJSONKeyIsNil
				return 0
			}

			obj.Set(key.(*evalBytes).string(), val.(*evalJSON), json.Set)
		}
		env.vm.stack[env.vm.sp-args] = json.NewObject(obj)
		env.vm.sp -= args - 1
		return 1
	}, "FN JSON_ARRAY (SP-%d)...(SP-1)", args)
}

func (asm *assembler) Fn_JSON_UNQUOTE() {
	asm.emit(func(env *ExpressionEnv) int {
		j := env.vm.stack[env.vm.sp-1].(*evalJSON)
		b := env.vm.arena.newEvalBytesEmpty()
		b.tt = int16(sqltypes.Blob)
		b.col = collationJSON
		if jbytes, ok := j.StringBytes(); ok {
			b.bytes = jbytes
		} else {
			b.bytes = j.MarshalTo(nil)
		}
		env.vm.stack[env.vm.sp-1] = b
		return 1
	}, "FN JSON_UNQUOTE (SP-1)")
}

func (asm *assembler) Fn_CHAR_LENGTH() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)

		if sqltypes.IsBinary(arg.SQLType()) {
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(len(arg.bytes)))
		} else {
			coll := arg.col.Collation.Get()
			count := charset.Length(coll.Charset(), arg.bytes)
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(count))
		}
		return 1
	}, "FN CHAR_LENGTH VARCHAR(SP-1)")
}

func (asm *assembler) Fn_LENGTH() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(len(arg.bytes)))
		return 1
	}, "FN LENGTH VARCHAR(SP-1)")
}

func (asm *assembler) Fn_BIT_LENGTH() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(len(arg.bytes) * 8))
		return 1
	}, "FN BIT_LENGTH VARCHAR(SP-1)")
}

func (asm *assembler) Fn_LUCASE(upcase bool) {
	if upcase {
		asm.emit(func(env *ExpressionEnv) int {
			str := env.vm.stack[env.vm.sp-1].(*evalBytes)

			coll := str.col.Collation.Get()
			csa, ok := coll.(collations.CaseAwareCollation)
			if !ok {
				env.vm.err = vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "not implemented")
			} else {
				str.bytes = csa.ToUpper(nil, str.bytes)
			}
			str.tt = int16(sqltypes.VarChar)
			return 1
		}, "FN UPPER VARCHAR(SP-1)")
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			str := env.vm.stack[env.vm.sp-1].(*evalBytes)

			coll := str.col.Collation.Get()
			csa, ok := coll.(collations.CaseAwareCollation)
			if !ok {
				env.vm.err = vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "not implemented")
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

	asm.emit(func(env *ExpressionEnv) int {
		x := env.vm.stack[env.vm.sp-args].ToRawBytes()
		for sp := env.vm.sp - args + 1; sp < env.vm.sp; sp++ {
			y := env.vm.stack[sp].ToRawBytes()
			if lessThan == (bytes.Compare(y, x) < 0) {
				x = y
			}
		}
		env.vm.stack[env.vm.sp-args] = env.vm.arena.newEvalBinary(x)
		env.vm.sp -= args - 1
		return 1
	}, "FN MULTICMP VARBINARY(SP-%d)...VARBINARY(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_c(args int, lessThan bool, tc collations.TypedCollation) {
	col := tc.Collation.Get()

	asm.adjustStack(-(args - 1))
	asm.emit(func(env *ExpressionEnv) int {
		x := env.vm.stack[env.vm.sp-args].ToRawBytes()
		for sp := env.vm.sp - args + 1; sp < env.vm.sp; sp++ {
			y := env.vm.stack[sp].ToRawBytes()
			if lessThan == (col.Collate(y, x, false) < 0) {
				x = y
			}
		}
		env.vm.stack[env.vm.sp-args] = env.vm.arena.newEvalText(x, tc)
		env.vm.sp -= args - 1
		return 1
	}, "FN MULTICMP FLOAT64(SP-%d)...FLOAT64(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_d(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(env *ExpressionEnv) int {
		x := env.vm.stack[env.vm.sp-args].(*evalDecimal)
		xprec := x.length

		for sp := env.vm.sp - args + 1; sp < env.vm.sp; sp++ {
			y := env.vm.stack[sp].(*evalDecimal)
			if lessThan == (y.dec.Cmp(x.dec) < 0) {
				x = y
			}
			if y.length > xprec {
				xprec = y.length
			}
		}
		env.vm.stack[env.vm.sp-args] = env.vm.arena.newEvalDecimalWithPrec(x.dec, xprec)
		env.vm.sp -= args - 1
		return 1
	}, "FN MULTICMP DECIMAL(SP-%d)...DECIMAL(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_f(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(env *ExpressionEnv) int {
		x := env.vm.stack[env.vm.sp-args].(*evalFloat)
		for sp := env.vm.sp - args + 1; sp < env.vm.sp; sp++ {
			y := env.vm.stack[sp].(*evalFloat)
			if lessThan == (y.f < x.f) {
				x = y
			}
		}
		env.vm.stack[env.vm.sp-args] = x
		env.vm.sp -= args - 1
		return 1
	}, "FN MULTICMP FLOAT64(SP-%d)...FLOAT64(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_i(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(env *ExpressionEnv) int {
		x := env.vm.stack[env.vm.sp-args].(*evalInt64)
		for sp := env.vm.sp - args + 1; sp < env.vm.sp; sp++ {
			y := env.vm.stack[sp].(*evalInt64)
			if lessThan == (y.i < x.i) {
				x = y
			}
		}
		env.vm.stack[env.vm.sp-args] = x
		env.vm.sp -= args - 1
		return 1
	}, "FN MULTICMP INT64(SP-%d)...INT64(SP-1)", args)
}

func (asm *assembler) Fn_MULTICMP_u(args int, lessThan bool) {
	asm.adjustStack(-(args - 1))

	asm.emit(func(env *ExpressionEnv) int {
		x := env.vm.stack[env.vm.sp-args].(*evalUint64)
		for sp := env.vm.sp - args + 1; sp < env.vm.sp; sp++ {
			y := env.vm.stack[sp].(*evalUint64)
			if lessThan == (y.u < x.u) {
				x = y
			}
		}
		env.vm.stack[env.vm.sp-args] = x
		env.vm.sp -= args - 1
		return 1
	}, "FN MULTICMP UINT64(SP-%d)...UINT64(SP-1)", args)
}

func (asm *assembler) Fn_REPEAT() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-2].(*evalBytes)
		repeat := env.vm.stack[env.vm.sp-1].(*evalInt64)

		if repeat.i < 0 {
			repeat.i = 0
		}

		if !validMaxLength(int64(len(str.bytes)), repeat.i) {
			env.vm.stack[env.vm.sp-2] = nil
			env.vm.sp--
			return 1
		}

		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.Repeat(str.bytes, int(repeat.i))
		env.vm.sp--
		return 1
	}, "FN REPEAT VARCHAR(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_LEFT(col collations.TypedCollation) {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-2].(*evalBytes)
		length := env.vm.stack[env.vm.sp-1].(*evalInt64)

		if length.i <= 0 {
			str.tt = int16(sqltypes.VarChar)
			str.bytes = nil
			str.col = col
			env.vm.sp--
			return 1
		}

		cs := col.Collation.Get().Charset()
		strLen := charset.Length(cs, str.bytes)

		str.tt = int16(sqltypes.VarChar)
		str.col = col
		if strLen <= int(length.i) {
			env.vm.sp--
			return 1
		}

		str.bytes = charset.Slice(cs, str.bytes, 0, int(length.i))
		env.vm.sp--
		return 1
	}, "FN LEFT VARCHAR(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_RIGHT(col collations.TypedCollation) {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-2].(*evalBytes)
		length := env.vm.stack[env.vm.sp-1].(*evalInt64)

		if length.i <= 0 {
			str.tt = int16(sqltypes.VarChar)
			str.bytes = nil
			str.col = col
			env.vm.sp--
			return 1
		}

		cs := col.Collation.Get().Charset()
		strLen := charset.Length(cs, str.bytes)

		str.tt = int16(sqltypes.VarChar)
		str.col = col

		if strLen <= int(length.i) {
			env.vm.sp--
			return 1
		}

		str.bytes = charset.Slice(cs, str.bytes, strLen-int(length.i), strLen)
		env.vm.sp--
		return 1
	}, "FN RIGHT VARCHAR(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_LPAD(col collations.TypedCollation) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-3].(*evalBytes)
		length := env.vm.stack[env.vm.sp-2].(*evalInt64)
		pad := env.vm.stack[env.vm.sp-1].(*evalBytes)

		if length.i < 0 {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		if !validMaxLength(int64(len(pad.bytes)), length.i) {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		cs := col.Collation.Get().Charset()
		strLen := charset.Length(cs, str.bytes)
		l := int(length.i)

		str.tt = int16(sqltypes.VarChar)
		str.col = col

		if strLen >= int(length.i) {
			str.bytes = charset.Slice(cs, str.bytes, 0, l)
			env.vm.sp -= 2
			return 1
		}

		runeLen := charset.Length(cs, pad.bytes)
		if runeLen == 0 {
			str.bytes = nil
			env.vm.sp -= 2
			return 1
		}

		repeat := (l - strLen) / runeLen
		remainder := (l - strLen) % runeLen

		res := bytes.Repeat(pad.bytes, repeat)
		if remainder > 0 {
			res = append(res, charset.Slice(cs, pad.bytes, 0, remainder)...)
		}
		str.bytes = append(res, str.bytes...)

		env.vm.sp -= 2
		return 1
	}, "FN LPAD VARCHAR(SP-3) INT64(SP-2) VARCHAR(SP-1)")
}

func (asm *assembler) Fn_RPAD(col collations.TypedCollation) {
	asm.adjustStack(-2)

	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-3].(*evalBytes)
		length := env.vm.stack[env.vm.sp-2].(*evalInt64)
		pad := env.vm.stack[env.vm.sp-1].(*evalBytes)

		if length.i < 0 {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		if !validMaxLength(int64(len(pad.bytes)), length.i) {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		cs := col.Collation.Get().Charset()
		strLen := charset.Length(cs, str.bytes)
		l := int(length.i)

		str.tt = int16(sqltypes.VarChar)
		str.col = col

		if strLen >= int(length.i) {
			str.bytes = charset.Slice(cs, str.bytes, 0, int(length.i))
			env.vm.sp -= 2
			return 1
		}

		runeLen := charset.Length(cs, pad.bytes)
		if runeLen == 0 {
			str.bytes = nil
			env.vm.sp -= 2
			return 1
		}

		repeat := (l - strLen) / runeLen
		remainder := (l - strLen) % runeLen

		str.bytes = append(str.bytes, bytes.Repeat(pad.bytes, repeat)...)
		if remainder > 0 {
			str.bytes = append(str.bytes, charset.Slice(cs, pad.bytes, 0, remainder)...)
		}

		env.vm.sp -= 2
		return 1
	}, "FN RPAD VARCHAR(SP-3) INT64(SP-2) VARCHAR(SP-1)")
}

func (asm *assembler) Fn_LTRIM1(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-1].(*evalBytes)
		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.TrimLeft(str.bytes, " ")
		str.col = col
		return 1
	}, "FN LTRIM VARCHAR(SP-1)")
}

func (asm *assembler) Fn_RTRIM1(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-1].(*evalBytes)
		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.TrimRight(str.bytes, " ")
		str.col = col
		return 1
	}, "FN RTRIM VARCHAR(SP-1)")
}

func (asm *assembler) Fn_TRIM1(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-1].(*evalBytes)
		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.Trim(str.bytes, " ")
		str.col = col
		return 1
	}, "FN TRIM VARCHAR(SP-1)")
}

func (asm *assembler) Fn_LTRIM2(col collations.TypedCollation) {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-2].(*evalBytes)
		pat := env.vm.stack[env.vm.sp-1].(*evalBytes)
		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.TrimPrefix(str.bytes, pat.bytes)
		str.col = col
		env.vm.sp--
		return 1
	}, "FN LTRIM VARCHAR(SP-2) VARCHAR(SP-1)")
}

func (asm *assembler) Fn_RTRIM2(col collations.TypedCollation) {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-2].(*evalBytes)
		pat := env.vm.stack[env.vm.sp-1].(*evalBytes)
		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.TrimSuffix(str.bytes, pat.bytes)
		str.col = col
		env.vm.sp--
		return 1
	}, "FN RTRIM VARCHAR(SP-2) VARCHAR(SP-1)")
}

func (asm *assembler) Fn_TRIM2(col collations.TypedCollation) {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-2].(*evalBytes)
		pat := env.vm.stack[env.vm.sp-1].(*evalBytes)
		str.tt = int16(sqltypes.VarChar)
		str.bytes = bytes.TrimPrefix(bytes.TrimSuffix(str.bytes, pat.bytes), pat.bytes)
		str.col = col
		env.vm.sp--
		return 1
	}, "FN TRIM VARCHAR(SP-2) VARCHAR(SP-1)")
}

func (asm *assembler) Fn_TO_BASE64(t sqltypes.Type, col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-1].(*evalBytes)
		encoded := mysqlBase64Encode(str.bytes)
		str.tt = int16(t)
		str.col = col
		str.bytes = encoded
		return 1
	}, "FN TO_BASE64 VARCHAR(SP-1)")
}

func (asm *assembler) Fn_WEIGHT_STRING_b(length int) {
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-1].(*evalBytes)
		w := collations.Binary.WeightString(make([]byte, 0, length), str.bytes, collations.PadToMax)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBinary(w)
		return 1
	}, "FN WEIGHT_STRING VARBINARY(SP-1)")
}

func (asm *assembler) Fn_WEIGHT_STRING_c(col collations.Collation, length int) {
	asm.emit(func(env *ExpressionEnv) int {
		str := env.vm.stack[env.vm.sp-1].(*evalBytes)
		w := col.WeightString(nil, str.bytes, length)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBinary(w)
		return 1
	}, "FN WEIGHT_STRING VARCHAR(SP-1)")
}

func (asm *assembler) In_table(not bool, table map[vthash.Hash]struct{}) {
	if not {
		asm.emit(func(env *ExpressionEnv) int {
			lhs := env.vm.stack[env.vm.sp-1]
			if lhs != nil {
				env.vm.hash.Reset()
				lhs.(hashable).Hash(&env.vm.hash)
				_, in := table[env.vm.hash.Sum128()]
				env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(!in)
			}
			return 1
		}, "NOT IN (SP-1), [static table]")
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			lhs := env.vm.stack[env.vm.sp-1]
			if lhs != nil {
				env.vm.hash.Reset()
				lhs.(hashable).Hash(&env.vm.hash)
				_, in := table[env.vm.hash.Sum128()]
				env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(in)
			}
			return 1
		}, "IN (SP-1), [static table]")
	}
}

func (asm *assembler) In_slow(not bool) {
	asm.adjustStack(-1)

	if not {
		asm.emit(func(env *ExpressionEnv) int {
			lhs := env.vm.stack[env.vm.sp-2]
			rhs := env.vm.stack[env.vm.sp-1].(*evalTuple)

			var in boolean
			in, env.vm.err = evalInExpr(lhs, rhs)

			env.vm.stack[env.vm.sp-2] = in.not().eval()
			env.vm.sp -= 1
			return 1
		}, "NOT IN (SP-2), TUPLE(SP-1)")
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			lhs := env.vm.stack[env.vm.sp-2]
			rhs := env.vm.stack[env.vm.sp-1].(*evalTuple)

			var in boolean
			in, env.vm.err = evalInExpr(lhs, rhs)

			env.vm.stack[env.vm.sp-2] = in.eval()
			env.vm.sp -= 1
			return 1
		}, "IN (SP-2), TUPLE(SP-1)")
	}
}

func (asm *assembler) Is(check func(eval) bool) {
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(check(env.vm.stack[env.vm.sp-1]))
		return 1
	}, "IS (SP-1), [static]")
}

func (asm *assembler) Not_i() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalInt64)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(arg.i == 0)
		return 1
	}, "NOT INT64(SP-1)")
}

func (asm *assembler) Not_u() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalUint64)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(arg.u == 0)
		return 1
	}, "NOT UINT64(SP-1)")
}

func (asm *assembler) Not_f() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalFloat)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(arg.f == 0.0)
		return 1
	}, "NOT FLOAT64(SP-1)")
}

func (asm *assembler) Not_d() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(arg.dec.IsZero())
		return 1
	}, "NOT DECIMAL(SP-1)")
}

func (asm *assembler) LogicalLeft(opname string) *jump {
	switch opname {
	case "AND":
		j := asm.jumpFrom()
		asm.emit(func(env *ExpressionEnv) int {
			left, ok := env.vm.stack[env.vm.sp-1].(*evalInt64)
			if ok && left.i == 0 {
				return j.offset()
			}
			return 1
		}, "AND CHECK INT64(SP-1)")
		return j
	case "OR":
		j := asm.jumpFrom()
		asm.emit(func(env *ExpressionEnv) int {
			left, ok := env.vm.stack[env.vm.sp-1].(*evalInt64)
			if ok && left.i != 0 {
				left.i = 1
				return j.offset()
			}
			return 1
		}, "OR CHECK INT64(SP-1)")
		return j
	case "XOR":
		j := asm.jumpFrom()
		asm.emit(func(env *ExpressionEnv) int {
			if env.vm.stack[env.vm.sp-1] == nil {
				return j.offset()
			}
			return 1
		}, "XOR CHECK INT64(SP-1)")
		return j
	}
	return nil
}

func (asm *assembler) LogicalRight(opname string) {
	asm.adjustStack(-1)
	switch opname {
	case "AND":
		asm.emit(func(env *ExpressionEnv) int {
			left, lok := env.vm.stack[env.vm.sp-2].(*evalInt64)
			right, rok := env.vm.stack[env.vm.sp-1].(*evalInt64)

			isLeft := lok && left.i != 0
			isRight := rok && right.i != 0

			if isLeft && isRight {
				left.i = 1
			} else if rok && !isRight {
				env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalBool(false)
			} else {
				env.vm.stack[env.vm.sp-2] = nil
			}
			env.vm.sp--
			return 1
		}, "AND INT64(SP-2), INT64(SP-1)")
	case "OR":
		asm.emit(func(env *ExpressionEnv) int {
			left, lok := env.vm.stack[env.vm.sp-2].(*evalInt64)
			right, rok := env.vm.stack[env.vm.sp-1].(*evalInt64)

			isLeft := lok && left.i != 0
			isRight := rok && right.i != 0

			switch {
			case !lok:
				if isRight {
					env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalBool(true)
				}
			case !rok:
				env.vm.stack[env.vm.sp-2] = nil
			default:
				if isLeft || isRight {
					left.i = 1
				} else {
					left.i = 0
				}
			}
			env.vm.sp--
			return 1
		}, "OR INT64(SP-2), INT64(SP-1)")
	case "XOR":
		asm.emit(func(env *ExpressionEnv) int {
			left := env.vm.stack[env.vm.sp-2].(*evalInt64)
			right, rok := env.vm.stack[env.vm.sp-1].(*evalInt64)

			isLeft := left.i != 0
			isRight := rok && right.i != 0

			switch {
			case !rok:
				env.vm.stack[env.vm.sp-2] = nil
			default:
				if isLeft != isRight {
					left.i = 1
				} else {
					left.i = 0
				}
			}
			env.vm.sp--
			return 1
		}, "XOR INT64(SP-2), INT64(SP-1)")
	}
}

func (asm *assembler) Like_coerce(expr *LikeExpr, coercion *compiledCoercion) {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.sp--

		var bl, br []byte
		bl, env.vm.err = coercion.left(nil, l.bytes)
		if env.vm.err != nil {
			return 0
		}
		br, env.vm.err = coercion.right(nil, r.bytes)
		if env.vm.err != nil {
			return 0
		}

		match := expr.matchWildcard(bl, br, coercion.col.ID())
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(match)
		return 1
	}, "LIKE VARCHAR(SP-2), VARCHAR(SP-1) COERCE AND COLLATE '%s'", coercion.col.Name())
}

func (asm *assembler) Like_collate(expr *LikeExpr, collation collations.Collation) {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.sp--

		match := expr.matchWildcard(l.bytes, r.bytes, collation.ID())
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBool(match)
		return 1
	}, "LIKE VARCHAR(SP-2), VARCHAR(SP-1) COLLATE '%s'", collation.Name())
}

func (asm *assembler) Strcmp(collation collations.TypedCollation) {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalBytes)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)
		env.vm.sp--

		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(strcmpCollate(l.bytes, r.bytes, collation.Collation))
		return 1
	}, "STRCMP VARCHAR(SP-2), VARCHAR(SP-1)")
}

func (asm *assembler) Mul_dd() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		mathMul_dd0(l, r)
		env.vm.sp--
		return 1
	}, "MUL DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Mul_ff() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalFloat)
		l.f *= r.f
		env.vm.sp--
		return 1
	}, "MUL FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) Mul_ii() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		l.i, env.vm.err = mathMul_ii0(l.i, r.i)
		env.vm.sp--
		return 1
	}, "MUL INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Mul_ui(swap bool) {
	asm.adjustStack(-1)

	if swap {
		asm.emit(func(env *ExpressionEnv) int {
			var u uint64
			l := env.vm.stack[env.vm.sp-1].(*evalUint64)
			r := env.vm.stack[env.vm.sp-2].(*evalInt64)
			u, env.vm.err = mathMul_ui0(l.u, r.i)
			env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalUint64(u)
			env.vm.sp--
			return 1
		}, "MUL UINT64(SP-1), INT64(SP-2)")
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			l := env.vm.stack[env.vm.sp-2].(*evalUint64)
			r := env.vm.stack[env.vm.sp-1].(*evalInt64)
			l.u, env.vm.err = mathMul_ui0(l.u, r.i)
			env.vm.sp--
			return 1
		}, "MUL UINT64(SP-2), INT64(SP-1)")
	}
}

func (asm *assembler) Mul_uu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u, env.vm.err = mathMul_uu0(l.u, r.u)
		env.vm.sp--
		return 1
	}, "MUL UINT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) Neg_d() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		arg.dec = arg.dec.Neg()
		return 1
	}, "NEG DECIMAL(SP-1)")
}

func (asm *assembler) Neg_f() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalFloat)
		arg.f = -arg.f
		return 1
	}, "NEG FLOAT64(SP-1)")
}

func (asm *assembler) Neg_hex() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalUint64)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalFloat(-float64(arg.u))
		return 1
	}, "NEG HEX(SP-1)")
}

func (asm *assembler) Neg_i() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if arg.i == math.MinInt64 {
			env.vm.err = errDeoptimize
		} else {
			arg.i = -arg.i
		}
		return 1
	}, "NEG INT64(SP-1)")
}

func (asm *assembler) Neg_u() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalUint64)
		if arg.u > math.MaxInt64+1 {
			env.vm.err = errDeoptimize
		} else {
			env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(-int64(arg.u))
		}
		return 1
	}, "NEG UINT64(SP-1)")
}

func (asm *assembler) NullCheck1(j *jump) {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1")
}

func (asm *assembler) NullCheck1r(j *jump) {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			env.vm.stack[env.vm.sp-2] = nil
			env.vm.sp--
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1 [rhs]")
}

func (asm *assembler) NullCheck2(j *jump) {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-2] == nil || env.vm.stack[env.vm.sp-1] == nil {
			env.vm.stack[env.vm.sp-2] = nil
			env.vm.sp--
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1, SP-2")
}

func (asm *assembler) NullCheck3(j *jump) {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-3] == nil || env.vm.stack[env.vm.sp-2] == nil || env.vm.stack[env.vm.sp-1] == nil {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return j.offset()
		}
		return 1
	}, "NULLCHECK SP-1, SP-2, SP-3")
}

func (asm *assembler) Cmp_nullsafe(j *jump) {
	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2]
		r := env.vm.stack[env.vm.sp-1]
		if l == nil || r == nil {
			if l == r {
				env.vm.flags.cmp = 0
			} else {
				env.vm.flags.cmp = 1
			}
			env.vm.sp -= 2
			return j.offset()
		}
		return 1
	}, "NULLCMP SP-1, SP-2")
}

func (asm *assembler) PackTuple(tlen int) {
	asm.adjustStack(-(tlen - 1))
	asm.emit(func(env *ExpressionEnv) int {
		tuple := make([]eval, tlen)
		copy(tuple, env.vm.stack[env.vm.sp-tlen:])
		env.vm.stack[env.vm.sp-tlen] = &evalTuple{tuple}
		env.vm.sp -= tlen - 1
		return 1
	}, "TUPLE (SP-%d)...(SP-1)", tlen)
}

func (asm *assembler) Parse_j(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		var p json.Parser
		arg := env.vm.stack[env.vm.sp-offset].(*evalBytes)
		env.vm.stack[env.vm.sp-offset], env.vm.err = p.ParseBytes(arg.bytes)
		return 1
	}, "PARSE_JSON VARCHAR(SP-%d)", offset)
}

func (asm *assembler) SetBool(offset int, b bool) {
	if b {
		asm.emit(func(env *ExpressionEnv) int {
			env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(true)
			return 1
		}, "SET (SP-%d), BOOL(true)", offset)
	} else {
		asm.emit(func(env *ExpressionEnv) int {
			env.vm.stack[env.vm.sp-offset] = env.vm.arena.newEvalBool(false)
			return 1
		}, "SET (SP-%d), BOOL(false)", offset)
	}
}

func (asm *assembler) SetNull(offset int) {
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp-offset] = nil
		return 1
	}, "SET (SP-%d), NULL", offset)
}

func (asm *assembler) Sub_dd() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalDecimal)
		r := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		mathSub_dd0(l, r)
		env.vm.sp--
		return 1
	}, "SUB DECIMAL(SP-2), DECIMAL(SP-1)")
}

func (asm *assembler) Sub_ff() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalFloat)
		r := env.vm.stack[env.vm.sp-1].(*evalFloat)
		l.f -= r.f
		env.vm.sp--
		return 1
	}, "SUB FLOAT64(SP-2), FLOAT64(SP-1)")
}

func (asm *assembler) Sub_ii() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		l.i, env.vm.err = mathSub_ii0(l.i, r.i)
		env.vm.sp--
		return 1
	}, "SUB INT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Sub_iu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalInt64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		r.u, env.vm.err = mathSub_iu0(l.i, r.u)
		env.vm.stack[env.vm.sp-2] = r
		env.vm.sp--
		return 1
	}, "SUB INT64(SP-2), UINT64(SP-1)")
}

func (asm *assembler) Sub_ui() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalInt64)
		l.u, env.vm.err = mathSub_ui0(l.u, r.i)
		env.vm.sp--
		return 1
	}, "SUB UINT64(SP-2), INT64(SP-1)")
}

func (asm *assembler) Sub_uu() {
	asm.adjustStack(-1)

	asm.emit(func(env *ExpressionEnv) int {
		l := env.vm.stack[env.vm.sp-2].(*evalUint64)
		r := env.vm.stack[env.vm.sp-1].(*evalUint64)
		l.u, env.vm.err = mathSub_uu0(l.u, r.u)
		env.vm.sp--
		return 1
	}, "SUB UINT64(SP-2), UINT64(SP-1)")
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

func (asm *assembler) Fn_Now(t querypb.Type, format *datetime.Strftime, prec uint8, utc bool) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		val := env.vm.arena.newEvalBytesEmpty()
		val.tt = int16(t)
		val.bytes = format.Format(env.time(utc), prec)
		env.vm.stack[env.vm.sp] = val
		env.vm.sp++
		return 1
	}, "FN NOW")
}

func (asm *assembler) Fn_Sysdate(prec uint8) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		val := env.vm.arena.newEvalBytesEmpty()
		val.tt = int16(sqltypes.Datetime)
		now := SystemTime()
		if tz := env.currentTimezone(); tz != nil {
			now = now.In(tz)
		}
		val.bytes = datetime.FromStdTime(now).Format(prec)
		env.vm.stack[env.vm.sp] = val
		env.vm.sp++
		return 1
	}, "FN SYSDATE")
}

func (asm *assembler) Fn_Curdate() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		val := env.vm.arena.newEvalBytesEmpty()
		val.tt = int16(sqltypes.Date)
		val.bytes = datetime.Date_YYYY_MM_DD.Format(env.time(false), 0)
		env.vm.stack[env.vm.sp] = val
		env.vm.sp++
		return 1
	}, "FN CURDATE")
}

func (asm *assembler) Fn_UtcDate() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		val := env.vm.arena.newEvalBytesEmpty()
		val.tt = int16(sqltypes.Date)
		val.bytes = datetime.Date_YYYY_MM_DD.Format(env.time(true), 0)
		env.vm.stack[env.vm.sp] = val
		env.vm.sp++
		return 1
	}, "FN UTC_DATE")
}

func (asm *assembler) Fn_User() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalText([]byte(env.currentUser()), collationUtf8mb3)
		env.vm.sp++
		return 1
	}, "FN USER")
}

func (asm *assembler) Fn_Database() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		db := env.currentDatabase()
		if db == "" {
			env.vm.stack[env.vm.sp] = nil
		} else {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalText([]byte(db), collationUtf8mb3)
		}
		env.vm.sp++
		return 1
	}, "FN DATABASE")
}

func (asm *assembler) Fn_Version() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalText([]byte(servenv.MySQLServerVersion()), collationUtf8mb3)
		env.vm.sp++
		return 1
	}, "FN VERSION")
}

func (asm *assembler) Fn_MD5(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)

		sum := md5.Sum(arg.bytes)
		buf := make([]byte, hex.EncodedLen(len(sum)))
		hex.Encode(buf, sum[:])

		arg.tt = int16(sqltypes.VarChar)
		arg.bytes = buf
		arg.col = col
		return 1
	}, "FN MD5 VARBINARY(SP-1)")
}

func (asm *assembler) Fn_SHA1(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalBytes)

		sum := sha1.Sum(arg.bytes)
		buf := make([]byte, hex.EncodedLen(len(sum)))
		hex.Encode(buf, sum[:])

		arg.tt = int16(sqltypes.VarChar)
		arg.bytes = buf
		arg.col = col
		return 1
	}, "FN SHA1 VARBINARY(SP-1)")
}

func (asm *assembler) Fn_SHA2(col collations.TypedCollation) {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-2].(*evalBytes)
		bits := env.vm.stack[env.vm.sp-1].(*evalInt64)

		var sum []byte
		switch bits.i {
		case 224:
			s := sha256.Sum224(arg.bytes)
			sum = s[:]
		case 0, 256:
			s := sha256.Sum256(arg.bytes)
			sum = s[:]
		case 384:
			s := sha512.Sum384(arg.bytes)
			sum = s[:]
		case 512:
			s := sha512.Sum512(arg.bytes)
			sum = s[:]
		default:
			env.vm.stack[env.vm.sp-2] = nil
			env.vm.sp--
			return 1
		}
		buf := make([]byte, hex.EncodedLen(len(sum)))
		hex.Encode(buf, sum[:])

		arg.tt = int16(sqltypes.VarChar)
		arg.bytes = buf
		arg.col = col
		env.vm.sp--
		return 1
	}, "FN SHA2 VARBINARY(SP-2), INT64(SP-1)")
}

func (asm *assembler) Fn_RandomBytes() {
	asm.emit(func(env *ExpressionEnv) int {
		size := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if size.i < 1 || size.i > 1024 {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		buf := make([]byte, size.i)
		_, env.vm.err = rand.Read(buf)
		if env.vm.err != nil {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}

		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalBinary(buf)
		return 1
	}, "FN RANDOM_BYTES INT64(SP-1)")
}

func (asm *assembler) Fn_DATE_FORMAT(col collations.TypedCollation) {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-2] == nil {
			env.vm.sp--
			return 1
		}
		l := env.vm.stack[env.vm.sp-2].(*evalTemporal)
		r := env.vm.stack[env.vm.sp-1].(*evalBytes)

		var d []byte
		d, env.vm.err = datetime.Format(r.string(), l.dt, l.prec)
		env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalText(d, col)
		env.vm.sp--
		return 1
	}, "FN DATE_FORMAT DATETIME(SP-2), VARBINARY(SP-1)")
}

func (asm *assembler) Fn_CONVERT_TZ() {
	asm.adjustStack(-2)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-3] == nil {
			env.vm.sp -= 2
			return 1
		}

		n := env.vm.stack[env.vm.sp-3].(*evalTemporal)
		f := env.vm.stack[env.vm.sp-2].(*evalBytes)
		t := env.vm.stack[env.vm.sp-1].(*evalBytes)

		fromTz, err := datetime.ParseTimeZone(f.string())
		if err != nil {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		toTz, err := datetime.ParseTimeZone(t.string())
		if err != nil {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		dt, ok := convertTz(n.dt, fromTz, toTz)
		if !ok {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}
		env.vm.stack[env.vm.sp-3] = env.vm.arena.newEvalDateTime(dt, int(n.prec))
		env.vm.sp -= 2
		return 1
	}, "FN CONVERT_TZ DATETIME(SP-3), VARBINARY(SP-2), VARBINARY(SP-1)")
}

func (asm *assembler) Fn_DAYOFMONTH() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Date.Day()))
		return 1
	}, "FN DAYOFMONTH DATE(SP-1)")
}

func (asm *assembler) Fn_DAYOFWEEK() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Date.Weekday() + 1))
		return 1
	}, "FN DAYOFWEEK DATE(SP-1)")
}

func (asm *assembler) Fn_DAYOFYEAR() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Date.Yearday()))
		return 1
	}, "FN DAYOFYEAR DATE(SP-1)")
}

func (asm *assembler) Fn_FROM_UNIXTIME_i() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if arg.i < 0 || arg.i > maxUnixtime {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		t := time.Unix(arg.i, 0)
		if tz := env.currentTimezone(); tz != nil {
			t = t.In(tz)
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalDateTime(datetime.FromStdTime(t), 0)
		return 1
	}, "FN FROM_UNIXTIME INT64(SP-1)")
}

func (asm *assembler) Fn_FROM_UNIXTIME_u() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalUint64)
		if arg.u > maxUnixtime {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		t := time.Unix(int64(arg.u), 0)
		if tz := env.currentTimezone(); tz != nil {
			t = t.In(tz)
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalDateTime(datetime.FromStdTime(t), 0)
		return 1
	}, "FN FROM_UNIXTIME UINT64(SP-1)")
}

func (asm *assembler) Fn_FROM_UNIXTIME_d() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalDecimal)
		if arg.dec.Sign() < 0 {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}

		sd, fd := arg.dec.QuoRem(decimal.New(1, 0), 0)
		sec, _ := sd.Int64()
		if sec > maxUnixtime {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		frac, _ := fd.Mul(decimal.New(1, 9)).Int64()
		t := time.Unix(sec, frac)
		if tz := env.currentTimezone(); tz != nil {
			t = t.In(tz)
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalDateTime(datetime.FromStdTime(t), int(arg.length))
		return 1
	}, "FN FROM_UNIXTIME DECIMAL(SP-1)")
}

func (asm *assembler) Fn_FROM_UNIXTIME_f() {
	asm.emit(func(env *ExpressionEnv) int {
		arg := env.vm.stack[env.vm.sp-1].(*evalFloat)
		if arg.f < 0 || arg.f > maxUnixtime {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}
		sec, frac := math.Modf(arg.f)
		t := time.Unix(int64(sec), int64(frac*1e9))
		if tz := env.currentTimezone(); tz != nil {
			t = t.In(tz)
		}
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalDateTime(datetime.FromStdTime(t), 6)
		return 1
	}, "FN FROM_UNIXTIME FLOAT(SP-1)")
}

func (asm *assembler) Fn_HOUR() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Time.Hour()))
		return 1
	}, "FN HOUR TIME(SP-1)")
}

func (asm *assembler) Fn_MAKEDATE() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		y := env.vm.stack[env.vm.sp-1].(*evalInt64)
		yd := env.vm.stack[env.vm.sp-2].(*evalInt64)

		t := yearDayToTime(y.i, yd.i)
		if t.IsZero() {
			env.vm.stack[env.vm.sp-2] = nil
		} else {
			env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalDate(datetime.FromStdTime(t).Date)
		}
		env.vm.sp--
		return 1
	}, "FN MAKEDATE INT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_MAKETIME_i() {
	asm.adjustStack(-2)
	asm.emit(func(env *ExpressionEnv) int {
		h := env.vm.stack[env.vm.sp-3].(*evalInt64)
		m := env.vm.stack[env.vm.sp-2].(*evalInt64)
		s := env.vm.stack[env.vm.sp-1].(*evalInt64)

		i, ok := makeTime_i(h.i, m.i, s.i)
		if !ok {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}
		t, ok := datetime.ParseTimeInt64(i)
		if !ok {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		env.vm.stack[env.vm.sp-3] = env.vm.arena.newEvalTime(t, 0)
		env.vm.sp -= 2
		return 1
	}, "FN MAKETIME INT64(SP-3) INT64(SP-2) INT64(SP-1)")
}

func (asm *assembler) Fn_MAKETIME_d() {
	asm.adjustStack(-2)
	asm.emit(func(env *ExpressionEnv) int {
		h := env.vm.stack[env.vm.sp-3].(*evalInt64)
		m := env.vm.stack[env.vm.sp-2].(*evalInt64)
		s := env.vm.stack[env.vm.sp-1].(*evalDecimal)

		d, ok := makeTime_d(h.i, m.i, s.dec)
		if !ok {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}
		t, l, ok := datetime.ParseTimeDecimal(d, s.length, -1)
		if !ok {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		env.vm.stack[env.vm.sp-3] = env.vm.arena.newEvalTime(t, l)
		env.vm.sp -= 2
		return 1
	}, "FN MAKETIME INT64(SP-3) INT64(SP-2) DECIMAL(SP-1)")
}

func (asm *assembler) Fn_MAKETIME_f() {
	asm.adjustStack(-2)
	asm.emit(func(env *ExpressionEnv) int {
		h := env.vm.stack[env.vm.sp-3].(*evalInt64)
		m := env.vm.stack[env.vm.sp-2].(*evalInt64)
		s := env.vm.stack[env.vm.sp-1].(*evalFloat)

		f, ok := makeTime_f(h.i, m.i, s.f)
		if !ok {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}
		t, l, ok := datetime.ParseTimeFloat(f, -1)
		if !ok {
			env.vm.stack[env.vm.sp-3] = nil
			env.vm.sp -= 2
			return 1
		}

		env.vm.stack[env.vm.sp-3] = env.vm.arena.newEvalTime(t, l)
		env.vm.sp -= 2
		return 1
	}, "FN MAKETIME INT64(SP-3) INT64(SP-2) FLOAT(SP-1)")
}

func (asm *assembler) Fn_MICROSECOND() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Time.Nanosecond() / 1000))
		return 1
	}, "FN MICROSECOND TIME(SP-1)")
}

func (asm *assembler) Fn_MINUTE() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Time.Minute()))
		return 1
	}, "FN MINUTE TIME(SP-1)")
}

func (asm *assembler) Fn_MONTH() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Date.Month()))
		return 1
	}, "FN MONTH DATE(SP-1)")
}

func (asm *assembler) Fn_MONTHNAME(col collations.TypedCollation) {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		m := arg.dt.Date.Month()
		if m < 1 || m > 12 {
			env.vm.stack[env.vm.sp-1] = nil
			return 1
		}

		mb := hack.StringBytes(time.Month(arg.dt.Date.Month()).String())
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalText(mb, col)
		return 1
	}, "FN MONTHNAME DATE(SP-1)")
}

func (asm *assembler) Fn_QUARTER() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Date.Quarter()))
		return 1
	}, "FN QUARTER DATE(SP-1)")
}

func (asm *assembler) Fn_SECOND() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Time.Second()))
		return 1
	}, "FN SECOND TIME(SP-1)")
}

func (asm *assembler) Fn_UNIX_TIMESTAMP0() {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		env.vm.stack[env.vm.sp] = env.vm.arena.newEvalInt64(env.now.Unix())
		env.vm.sp++
		return 1
	}, "FN UNIX_TIMESTAMP")
}

func (asm *assembler) Fn_UNIX_TIMESTAMP1() {
	asm.emit(func(env *ExpressionEnv) int {
		res := dateTimeUnixTimestamp(env, env.vm.stack[env.vm.sp-1])
		if _, ok := res.(*evalInt64); !ok {
			env.vm.err = errDeoptimize
		}
		env.vm.stack[env.vm.sp-1] = res
		return 1
	}, "FN UNIX_TIMESTAMP (SP-1)")
}

func (asm *assembler) Fn_WEEK0() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		week := arg.dt.Date.Week(0)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(week))
		return 1
	}, "FN WEEK0 DATE(SP-1)")
}

func (asm *assembler) Fn_WEEK() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-2] == nil {
			env.vm.sp--
			return 1
		}
		arg := env.vm.stack[env.vm.sp-2].(*evalTemporal)
		mode := env.vm.stack[env.vm.sp-1].(*evalInt64)
		week := arg.dt.Date.Week(int(mode.i))
		env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalInt64(int64(week))
		env.vm.sp--
		return 1
	}, "FN WEEK DATE(SP-1)")
}

func (asm *assembler) Fn_WEEKDAY() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Date.Weekday()+6) % 7)
		return 1
	}, "FN WEEKDAY DATE(SP-1)")
}

func (asm *assembler) Fn_WEEKOFYEAR() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		_, week := arg.dt.Date.ISOWeek()
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(week))
		return 1
	}, "FN WEEKOFYEAR DATE(SP-1)")
}

func (asm *assembler) Fn_YEAR() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(arg.dt.Date.Year()))
		return 1
	}, "FN YEAR DATE(SP-1)")
}

func (asm *assembler) Fn_YEARWEEK0() {
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return 1
		}
		arg := env.vm.stack[env.vm.sp-1].(*evalTemporal)
		yw := arg.dt.Date.YearWeek(0)
		env.vm.stack[env.vm.sp-1] = env.vm.arena.newEvalInt64(int64(yw))
		return 1
	}, "FN YEARWEEK0 DATE(SP-1)")
}

func (asm *assembler) Fn_YEARWEEK() {
	asm.adjustStack(-1)
	asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-2] == nil {
			env.vm.sp--
			return 1
		}
		arg := env.vm.stack[env.vm.sp-2].(*evalTemporal)
		mode := env.vm.stack[env.vm.sp-1].(*evalInt64)
		yw := arg.dt.Date.YearWeek(int(mode.i))
		env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalInt64(int64(yw))
		env.vm.sp--
		return 1
	}, "FN YEARWEEK DATE(SP-1)")
}
