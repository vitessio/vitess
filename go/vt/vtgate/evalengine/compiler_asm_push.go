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
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/mysql/json"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func push_null(env *ExpressionEnv) int {
	env.vm.stack[env.vm.sp] = nil
	env.vm.sp++
	return 1
}

func push_i(env *ExpressionEnv, raw []byte) int {
	var ival int64
	ival, env.vm.err = fastparse.ParseInt64(hack.String(raw), 10)
	env.vm.stack[env.vm.sp] = env.vm.arena.newEvalInt64(ival)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_i(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_i(env, col.Raw())
	}, "PUSH INT64(:%d)", offset)
}

func (asm *assembler) PushBVar_i(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_i(env, bvar.Value)
	}, "PUSH INT64(:%q)", key)
}

func push_bin(env *ExpressionEnv, raw []byte) int {
	env.vm.stack[env.vm.sp] = newEvalBinary(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_bin(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_bin(env, col.Raw())
	}, "PUSH VARBINARY(:%d)", offset)
}

func (asm *assembler) PushBVar_bin(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_bin(env, bvar.Value)
	}, "PUSH VARBINARY(:%q)", key)
}

func push_d(env *ExpressionEnv, raw []byte) int {
	var dec decimal.Decimal
	dec, env.vm.err = decimal.NewFromMySQL(raw)
	env.vm.stack[env.vm.sp] = env.vm.arena.newEvalDecimal(dec, 0, 0)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_d(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_d(env, col.Raw())
	}, "PUSH DECIMAL(:%d)", offset)
}

func (asm *assembler) PushBVar_d(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_d(env, bvar.Value)
	}, "PUSH DECIMAL(:%q)", key)
}

func push_f(env *ExpressionEnv, raw []byte) int {
	var fval float64
	fval, env.vm.err = fastparse.ParseFloat64(hack.String(raw))
	env.vm.stack[env.vm.sp] = env.vm.arena.newEvalFloat(fval)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_f(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_f(env, col.Raw())
	}, "PUSH FLOAT64(:%d)", offset)
}

func (asm *assembler) PushBVar_f(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_f(env, bvar.Value)
	}, "PUSH FLOAT64(:%q)", key)
}

func push_bitnum(env *ExpressionEnv, raw []byte) int {
	raw, env.vm.err = parseBitNum(raw)
	env.vm.stack[env.vm.sp] = newEvalBytesBit(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushBVar_bitnum(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_bitnum(env, bvar.Value)
	}, "PUSH BITNUM(:%q)", key)
}

func push_hexnum(env *ExpressionEnv, raw []byte) int {
	raw, env.vm.err = parseHexNumber(raw)
	env.vm.stack[env.vm.sp] = newEvalBytesHex(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_hexnum(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_hexnum(env, col.Raw())
	}, "PUSH HEXNUM(:%d)", offset)
}

func (asm *assembler) PushBVar_hexnum(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_hexnum(env, bvar.Value)
	}, "PUSH HEXNUM(:%q)", key)
}

func push_hexval(env *ExpressionEnv, raw []byte) int {
	raw, env.vm.err = parseHexLiteral(raw[2 : len(raw)-1])
	env.vm.stack[env.vm.sp] = newEvalBytesHex(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_hexval(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_hexval(env, col.Raw())
	}, "PUSH HEXVAL(:%d)", offset)
}

func (asm *assembler) PushBVar_hexval(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_hexval(env, bvar.Value)
	}, "PUSH HEXVAL(:%q)", key)
}

func push_json(env *ExpressionEnv, raw []byte) int {
	var parser json.Parser
	env.vm.stack[env.vm.sp], env.vm.err = parser.ParseBytes(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_json(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_json(env, col.Raw())
	}, "PUSH JSON(:%d)", offset)
}

func (asm *assembler) PushBVar_json(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_json(env, bvar.Value)
	}, "PUSH JSON(:%q)", key)
}

func push_datetime(env *ExpressionEnv, raw []byte) int {
	env.vm.stack[env.vm.sp], env.vm.err = parseDateTime(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_datetime(offset int) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_datetime(env, col.Raw())
	}, "PUSH DATETIME(:%d)", offset)
}

func (asm *assembler) PushBVar_datetime(key string) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_datetime(env, bvar.Value)
	}, "PUSH DATETIME(:%q)", key)
}

func push_date(env *ExpressionEnv, raw []byte) int {
	env.vm.stack[env.vm.sp], env.vm.err = parseDate(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_date(offset int) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_date(env, col.Raw())
	}, "PUSH DATE(:%d)", offset)
}

func (asm *assembler) PushBVar_date(key string) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_date(env, bvar.Value)
	}, "PUSH DATE(:%q)", key)
}

func push_time(env *ExpressionEnv, raw []byte) int {
	env.vm.stack[env.vm.sp], env.vm.err = parseTime(raw)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_time(offset int) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_time(env, col.Raw())
	}, "PUSH TIME(:%d)", offset)
}

func (asm *assembler) PushBVar_time(key string) {
	asm.adjustStack(1)
	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_time(env, bvar.Value)
	}, "PUSH TIME(:%q)", key)
}

func push_text(env *ExpressionEnv, raw []byte, col collations.TypedCollation) int {
	env.vm.stack[env.vm.sp] = newEvalText(raw, col)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_text(offset int, coll collations.TypedCollation) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_text(env, col.Raw(), coll)
	}, "PUSH VARCHAR(:%d) COLLATE %d", offset, coll.Collation)
}

func (asm *assembler) PushBVar_text(key string, col collations.TypedCollation) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_text(env, bvar.Value, col)
	}, "PUSH VARCHAR(:%q)", key)
}

func push_u(env *ExpressionEnv, raw []byte) int {
	var uval uint64
	uval, env.vm.err = fastparse.ParseUint64(hack.String(raw), 10)
	env.vm.stack[env.vm.sp] = env.vm.arena.newEvalUint64(uval)
	env.vm.sp++
	return 1
}

func (asm *assembler) PushColumn_u(offset int) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		col := env.Row[offset]
		if col.IsNull() {
			return push_null(env)
		}
		return push_u(env, col.Raw())
	}, "PUSH UINT64(:%d)", offset)
}

func (asm *assembler) PushBVar_u(key string) {
	asm.adjustStack(1)

	asm.emit(func(env *ExpressionEnv) int {
		var bvar *querypb.BindVariable
		bvar, env.vm.err = env.lookupBindVar(key)
		if env.vm.err != nil {
			return 0
		}
		return push_u(env, bvar.Value)
	}, "PUSH UINT64(:%q)", key)
}

func (asm *assembler) PushLiteral(lit eval) error {
	asm.adjustStack(1)

	switch lit := lit.(type) {
	case *evalInt64:
		asm.emit(func(env *ExpressionEnv) int {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalInt64(lit.i)
			env.vm.sp++
			return 1
		}, "PUSH INT64(%s)", lit.ToRawBytes())
	case *evalUint64:
		asm.emit(func(env *ExpressionEnv) int {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalUint64(lit.u)
			env.vm.sp++
			return 1
		}, "PUSH UINT64(%s)", lit.ToRawBytes())
	case *evalFloat:
		asm.emit(func(env *ExpressionEnv) int {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalFloat(lit.f)
			env.vm.sp++
			return 1
		}, "PUSH FLOAT64(%s)", lit.ToRawBytes())
	case *evalDecimal:
		asm.emit(func(env *ExpressionEnv) int {
			env.vm.stack[env.vm.sp] = env.vm.arena.newEvalDecimalWithPrec(lit.dec, lit.length)
			env.vm.sp++
			return 1
		}, "PUSH DECIMAL(%s)", lit.ToRawBytes())
	case *evalBytes:
		asm.emit(func(env *ExpressionEnv) int {
			b := env.vm.arena.newEvalBytesEmpty()
			*b = *lit
			env.vm.stack[env.vm.sp] = b
			env.vm.sp++
			return 1
		}, "PUSH VARCHAR(%q)", lit.ToRawBytes())
	case *evalTemporal:
		asm.emit(func(env *ExpressionEnv) int {
			env.vm.stack[env.vm.sp] = env.vm.arena.newTemporal(lit.t, lit.dt, lit.prec)
			env.vm.sp++
			return 1
		}, "PUSH TIME|DATETIME|DATE(%q)", lit.ToRawBytes())
	default:
		return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported literal kind '%T'", lit)
	}

	return nil
}

func (asm *assembler) PushNull() {
	asm.adjustStack(1)
	asm.emit(push_null, "PUSH NULL")
}
