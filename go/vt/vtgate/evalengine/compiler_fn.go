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
	"math"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func (c *compiler) compileFn(call callable) (ctype, error) {
	switch call := call.(type) {
	case *builtinMultiComparison:
		return c.compileFn_MULTICMP(call)
	case *builtinJSONExtract:
		return c.compileFn_JSON_EXTRACT(call)
	case *builtinJSONUnquote:
		return c.compileFn_JSON_UNQUOTE(call)
	case *builtinJSONContainsPath:
		return c.compileFn_JSON_CONTAINS_PATH(call)
	case *builtinJSONArray:
		return c.compileFn_JSON_ARRAY(call)
	case *builtinJSONObject:
		return c.compileFn_JSON_OBJECT(call)
	case *builtinJSONKeys:
		return c.compileFn_JSON_KEYS(call)
	case *builtinRepeat:
		return c.compileFn_REPEAT(call)
	case *builtinToBase64:
		return c.compileFn_TO_BASE64(call)
	case *builtinFromBase64:
		return c.compileFn_FROM_BASE64(call)
	case *builtinChangeCase:
		return c.compileFn_CCASE(call)
	case *builtinCharLength:
		return c.compileFn_length(call, c.asm.Fn_CHAR_LENGTH)
	case *builtinLength:
		return c.compileFn_length(call, c.asm.Fn_LENGTH)
	case *builtinBitLength:
		return c.compileFn_length(call, c.asm.Fn_BIT_LENGTH)
	case *builtinASCII:
		return c.compileFn_ASCII(call)
	case *builtinHex:
		return c.compileFn_HEX(call)
	case *builtinBitCount:
		return c.compileFn_BIT_COUNT(call)
	case *builtinCollation:
		return c.compileFn_COLLATION(call)
	case *builtinCeil:
		return c.compileFn_rounding(call, c.asm.Fn_CEIL_f, c.asm.Fn_CEIL_d)
	case *builtinFloor:
		return c.compileFn_rounding(call, c.asm.Fn_FLOOR_f, c.asm.Fn_FLOOR_d)
	case *builtinAbs:
		return c.compileFn_ABS(call)
	case *builtinPi:
		return c.compileFn_PI(call)
	case *builtinAcos:
		return c.compileFn_math1(call, c.asm.Fn_ACOS, flagNullable)
	case *builtinAsin:
		return c.compileFn_math1(call, c.asm.Fn_ASIN, flagNullable)
	case *builtinAtan:
		return c.compileFn_math1(call, c.asm.Fn_ATAN, 0)
	case *builtinAtan2:
		return c.compileFn_ATAN2(call)
	case *builtinCos:
		return c.compileFn_math1(call, c.asm.Fn_COS, 0)
	case *builtinCot:
		return c.compileFn_math1(call, c.asm.Fn_COT, 0)
	case *builtinSin:
		return c.compileFn_math1(call, c.asm.Fn_SIN, 0)
	case *builtinTan:
		return c.compileFn_math1(call, c.asm.Fn_TAN, 0)
	case *builtinDegrees:
		return c.compileFn_math1(call, c.asm.Fn_DEGREES, 0)
	case *builtinRadians:
		return c.compileFn_math1(call, c.asm.Fn_RADIANS, 0)
	case *builtinExp:
		return c.compileFn_math1(call, c.asm.Fn_EXP, flagNullable)
	case *builtinLn:
		return c.compileFn_math1(call, c.asm.Fn_LN, flagNullable)
	case *builtinLog:
		return c.compileFn_math1(call, c.asm.Fn_LOG, flagNullable)
	case *builtinLog10:
		return c.compileFn_math1(call, c.asm.Fn_LOG10, flagNullable)
	case *builtinLog2:
		return c.compileFn_math1(call, c.asm.Fn_LOG2, flagNullable)
	case *builtinPow:
		return c.compileFn_POW(call)
	case *builtinSign:
		return c.compileFn_SIGN(call)
	case *builtinSqrt:
		return c.compileFn_math1(call, c.asm.Fn_SQRT, flagNullable)
	case *builtinRound:
		return c.compileFn_ROUND(call)
	case *builtinTruncate:
		return c.compileFn_TRUNCATE(call)
	case *builtinCrc32:
		return c.compileFn_CRC32(call)
	case *builtinConv:
		return c.compileFn_CONV(call)
	case *builtinWeightString:
		return c.compileFn_WEIGHT_STRING(call)
	case *builtinNow:
		return c.compileFn_Now(call)
	case *builtinCurdate:
		return c.compileFn_Curdate(call)
	case *builtinUtcDate:
		return c.compileFn_UtcDate(call)
	case *builtinSysdate:
		return c.compileFn_Sysdate(call)
	case *builtinUser:
		return c.compileFn_User(call)
	case *builtinDatabase:
		return c.compileFn_Database(call)
	case *builtinVersion:
		return c.compileFn_Version(call)
	case *builtinMD5:
		return c.compileFn_MD5(call)
	case *builtinSHA1:
		return c.compileFn_SHA1(call)
	case *builtinSHA2:
		return c.compileFn_SHA2(call)
	case *builtinRandomBytes:
		return c.compileFn_RandomBytes(call)
	case *builtinDateFormat:
		return c.compileFn_DateFormat(call)
	default:
		return ctype{}, c.unsupported(call)
	}
}

func (c *compiler) compileFn_MULTICMP(call *builtinMultiComparison) (ctype, error) {
	var (
		signed   int
		unsigned int
		floats   int
		decimals int
		text     int
		binary   int
		args     []ctype
	)

	/*
		If any argument is NULL, the result is NULL. No comparison is needed.
		If all arguments are integer-valued, they are compared as integers.
		If at least one argument is double precision, they are compared as double-precision values. Otherwise, if at least one argument is a DECIMAL value, they are compared as DECIMAL values.
		If the arguments comprise a mix of numbers and strings, they are compared as strings.
		If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
		In all other cases, the arguments are compared as binary strings.
	*/

	for _, expr := range call.Arguments {
		tt, err := c.compileExpr(expr)
		if err != nil {
			return ctype{}, err
		}

		args = append(args, tt)

		switch tt.Type {
		case sqltypes.Int64:
			signed++
		case sqltypes.Uint64:
			unsigned++
		case sqltypes.Float64:
			floats++
		case sqltypes.Decimal:
			decimals++
		case sqltypes.Text, sqltypes.VarChar:
			text++
		case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
			binary++
		default:
			return ctype{}, c.unsupported(call)
		}
	}

	if signed+unsigned == len(args) {
		if signed == len(args) {
			c.asm.Fn_MULTICMP_i(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
		}
		if unsigned == len(args) {
			c.asm.Fn_MULTICMP_u(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Uint64, Col: collationNumeric}, nil
		}
		return c.compileFN_MULTICMP_d(args, call.cmp < 0)
	}
	if binary > 0 || text > 0 {
		if text > 0 {
			return c.compileFn_MULTICMP_c(args, call.cmp < 0)
		}
		c.asm.Fn_MULTICMP_b(len(args), call.cmp < 0)
		return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
	} else {
		if floats > 0 {
			for i, tt := range args {
				c.compileToFloat(tt, len(args)-i)
			}
			c.asm.Fn_MULTICMP_f(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
		}
		if decimals > 0 {
			return c.compileFN_MULTICMP_d(args, call.cmp < 0)
		}
	}
	return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected argument for GREATEST/LEAST")
}

func (c *compiler) compileFn_MULTICMP_c(args []ctype, lessThan bool) (ctype, error) {
	env := collations.Local()

	var ca collationAggregation
	for _, arg := range args {
		if err := ca.add(env, arg.Col); err != nil {
			return ctype{}, err
		}
	}

	tc := ca.result()
	c.asm.Fn_MULTICMP_c(len(args), lessThan, tc)
	return ctype{Type: sqltypes.VarChar, Col: tc}, nil
}

func (c *compiler) compileFN_MULTICMP_d(args []ctype, lessThan bool) (ctype, error) {
	for i, tt := range args {
		c.compileToDecimal(tt, len(args)-i)
	}
	c.asm.Fn_MULTICMP_d(len(args), lessThan)
	return ctype{Type: sqltypes.Decimal, Col: collationNumeric}, nil
}

func (c *compiler) compileFn_REPEAT(expr *builtinRepeat) (ctype, error) {
	str, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	repeat, err := c.compileExpr(expr.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(str, repeat)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(2, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}
	_ = c.compileToInt64(repeat, 1)

	c.asm.Fn_REPEAT(1)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: str.Col}, nil
}

func (c *compiler) compileFn_TO_BASE64(call *builtinToBase64) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	t := sqltypes.VarChar
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, t, 0, false)
	}

	col := collations.TypedCollation{
		Collation:    c.cfg.Collation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}

	c.asm.Fn_TO_BASE64(t, col)
	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: col}, nil
}

func (c *compiler) compileFn_FROM_BASE64(call *builtinFromBase64) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	t := sqltypes.VarBinary
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Blob
	}

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, t, 0, false)
	}

	c.asm.Fn_FROM_BASE64(t)
	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: collationBinary}, nil
}

func (c *compiler) compileFn_CCASE(call *builtinChangeCase) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}

	c.asm.Fn_LUCASE(call.upcase)
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Col: str.Col}, nil
}

func (c *compiler) compileFn_length(call callable, asm_ins func()) (ctype, error) {
	str, err := c.compileExpr(call.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}

	asm_ins()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

func (c *compiler) compileFn_ASCII(call *builtinASCII) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}

	c.asm.Fn_ASCII()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

func (c *compiler) compileFn_HEX(call *builtinHex) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)
	col := defaultCoercionCollation(c.cfg.Collation)
	t := sqltypes.VarChar
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case sqltypes.IsNumber(str.Type), sqltypes.IsDecimal(str.Type):
		c.asm.Fn_HEX_d(col)
	case str.isTextual():
		c.asm.Fn_HEX_c(t, col)
	default:
		c.asm.Convert_xc(1, t, c.cfg.Collation, 0, false)
		c.asm.Fn_HEX_c(t, col)
	}

	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: col}, nil
}

func (c *compiler) compileFn_COLLATION(expr *builtinCollation) (ctype, error) {
	_, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.asm.jumpFrom()

	c.asm.Fn_COLLATION(collationUtf8mb3)
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Col: collationUtf8mb3}, nil
}

func (c *compiler) compileFn_rounding(expr callable, asm_ins_f, asm_ins_d func()) (ctype, error) {
	arg, err := c.compileExpr(expr.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	if arg.Type == sqltypes.Int64 || arg.Type == sqltypes.Uint64 {
		// No-op for integers.
		return arg, nil
	}

	skip := c.compileNullCheck1(arg)

	convt := ctype{Type: arg.Type, Col: collationNumeric, Flag: arg.Flag}
	switch arg.Type {
	case sqltypes.Float64:
		asm_ins_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		convt.Type = sqltypes.Int64
		asm_ins_d()
	default:
		convt.Type = sqltypes.Float64
		c.asm.Convert_xf(1)
		asm_ins_f()
	}

	c.asm.jumpDestination(skip)
	return convt, nil
}

func (c *compiler) compileFn_ABS(expr *builtinAbs) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	if arg.Type == sqltypes.Uint64 {
		// No-op if it's unsigned since that's already positive.
		return arg, nil
	}

	skip := c.compileNullCheck1(arg)

	convt := ctype{Type: arg.Type, Col: collationNumeric, Flag: arg.Flag}
	switch arg.Type {
	case sqltypes.Int64:
		c.asm.Fn_ABS_i()
	case sqltypes.Float64:
		c.asm.Fn_ABS_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		c.asm.Fn_ABS_d()
	default:
		convt.Type = sqltypes.Float64
		c.asm.Convert_xf(1)
		c.asm.Fn_ABS_f()
	}

	c.asm.jumpDestination(skip)
	return convt, nil
}

func (c *compiler) compileFn_PI(_ *builtinPi) (ctype, error) {
	c.asm.Fn_PI()
	return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
}

func (c *compiler) compileFn_math1(expr callable, asm_ins func(), nullable typeFlag) (ctype, error) {
	arg, err := c.compileExpr(expr.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	c.compileToFloat(arg, 1)
	asm_ins()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag | nullable}, nil
}

func (c *compiler) compileFn_ATAN2(expr *builtinAtan2) (ctype, error) {
	arg1, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	arg2, err := c.compileExpr(expr.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(arg1, arg2)
	c.compileToFloat(arg1, 2)
	c.compileToFloat(arg2, 1)
	c.asm.Fn_ATAN2()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg1.Flag | arg2.Flag}, nil
}

func (c *compiler) compileFn_POW(expr *builtinPow) (ctype, error) {
	arg1, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	arg2, err := c.compileExpr(expr.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(arg1, arg2)
	c.compileToFloat(arg1, 2)
	c.compileToFloat(arg2, 1)
	c.asm.Fn_POW()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg1.Flag | arg2.Flag | flagNullable}, nil
}

func (c *compiler) compileFn_SIGN(expr callable) (ctype, error) {
	arg, err := c.compileExpr(expr.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Int64:
		c.asm.Fn_SIGN_i()
	case sqltypes.Uint64:
		c.asm.Fn_SIGN_u()
	case sqltypes.Float64:
		c.asm.Fn_SIGN_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		c.asm.Fn_SIGN_d()
	default:
		c.asm.Convert_xf(1)
		c.asm.Fn_SIGN_f()
	}

	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag}, nil
}

func (c *compiler) compileFn_ROUND(expr callable) (ctype, error) {
	args := expr.callable()

	arg, err := c.compileExpr(args[0])
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)
	var skip2 *jump

	if len(args) == 1 {
		switch arg.Type {
		case sqltypes.Int64:
			// No-op, already rounded
		case sqltypes.Uint64:
			// No-op, already rounded
		case sqltypes.Float64:
			c.asm.Fn_ROUND1_f()
		case sqltypes.Decimal:
			// We assume here the most common case here is that
			// the decimal fits into an integer.
			c.asm.Fn_ROUND1_d()
		default:
			c.asm.Convert_xf(1)
			c.asm.Fn_ROUND1_f()
		}
	} else {
		round, err := c.compileExpr(args[1])
		if err != nil {
			return ctype{}, err
		}

		skip2 = c.compileNullCheck1r(round)

		switch round.Type {
		case sqltypes.Int64:
			// No-op, already correct type
		case sqltypes.Uint64:
			c.asm.Clamp_u(1, math.MaxInt64)
			c.asm.Convert_ui(1)
		default:
			c.asm.Convert_xi(1)
		}

		switch arg.Type {
		case sqltypes.Int64:
			c.asm.Fn_ROUND2_i()
		case sqltypes.Uint64:
			c.asm.Fn_ROUND2_u()
		case sqltypes.Float64:
			c.asm.Fn_ROUND2_f()
		case sqltypes.Decimal:
			// We assume here the most common case here is that
			// the decimal fits into an integer.
			c.asm.Fn_ROUND2_d()
		default:
			c.asm.Convert_xf(2)
			c.asm.Fn_ROUND2_f()
		}
	}

	c.asm.jumpDestination(skip1, skip2)
	return arg, nil
}

func (c *compiler) compileFn_TRUNCATE(expr callable) (ctype, error) {
	args := expr.callable()

	arg, err := c.compileExpr(args[0])
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)

	round, err := c.compileExpr(args[1])
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(round)

	switch round.Type {
	case sqltypes.Int64:
		// No-op, already correct type
	case sqltypes.Uint64:
		c.asm.Clamp_u(1, math.MaxInt64)
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}

	switch arg.Type {
	case sqltypes.Int64:
		c.asm.Fn_TRUNCATE_i()
	case sqltypes.Uint64:
		c.asm.Fn_TRUNCATE_u()
	case sqltypes.Float64:
		c.asm.Fn_TRUNCATE_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		c.asm.Fn_TRUNCATE_d()
	default:
		c.asm.Convert_xf(2)
		c.asm.Fn_TRUNCATE_f()
	}

	c.asm.jumpDestination(skip1, skip2)
	return arg, nil
}

func (c *compiler) compileFn_CRC32(expr callable) (ctype, error) {
	arg, err := c.compileExpr(expr.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.Binary, 0, false)
	}

	c.asm.Fn_CRC32()
	c.asm.jumpDestination(skip)
	return arg, nil
}

func (c *compiler) compileFn_CONV(expr callable) (ctype, error) {
	n, err := c.compileExpr(expr.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	from, err := c.compileExpr(expr.callable()[1])
	if err != nil {
		return ctype{}, err
	}

	to, err := c.compileExpr(expr.callable()[2])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck3(n, from, to)

	_ = c.compileToInt64(from, 2)
	_ = c.compileToInt64(to, 1)

	t := sqltypes.VarChar
	if n.Type == sqltypes.Blob || n.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case n.isTextual():
	default:
		c.asm.Convert_xb(3, t, 0, false)
	}

	if n.isHexOrBitLiteral() {
		c.asm.Fn_CONV_hu(3, 2)
	} else {
		c.asm.Fn_CONV_bu(3, 2)
	}

	col := defaultCoercionCollation(n.Col.Collation)
	c.asm.Fn_CONV_uc(t, col)
	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: col, Flag: flagNullable}, nil
}

func (c *compiler) compileFn_WEIGHT_STRING(call *builtinWeightString) (ctype, error) {
	str, err := c.compileExpr(call.String)
	if err != nil {
		return ctype{}, err
	}

	switch str.Type {
	case sqltypes.Int64, sqltypes.Uint64:
		return ctype{}, c.unsupported(call)

	case sqltypes.VarChar, sqltypes.VarBinary:
		skip := c.compileNullCheck1(str)

		if call.Cast == "binary" {
			c.asm.Fn_WEIGHT_STRING_b(call.Len)
		} else {
			c.asm.Fn_WEIGHT_STRING_c(str.Col.Collation.Get(), call.Len)
		}
		c.asm.jumpDestination(skip)
		return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil

	default:
		c.asm.SetNull(1)
		return ctype{Type: sqltypes.VarBinary, Flag: flagNullable | flagNull, Col: collationBinary}, nil
	}
}

func (c *compiler) compileFn_Now(call *builtinNow) (ctype, error) {
	var format *datetime.Strftime
	var t sqltypes.Type

	if call.onlyTime {
		format = datetime.Time_hh_mm_ss
		t = sqltypes.Time
	} else {
		format = datetime.DateTime_YYYY_MM_DD_hh_mm_ss
		t = sqltypes.Datetime
	}
	c.asm.Fn_Now(t, format, call.prec, call.utc)
	return ctype{Type: t, Col: collationBinary}, nil
}

func (c *compiler) compileFn_Curdate(*builtinCurdate) (ctype, error) {
	c.asm.Fn_Curdate()
	return ctype{Type: sqltypes.Date, Col: collationBinary}, nil
}

func (c *compiler) compileFn_UtcDate(*builtinUtcDate) (ctype, error) {
	c.asm.Fn_UtcDate()
	return ctype{Type: sqltypes.Date, Col: collationBinary}, nil
}

func (c *compiler) compileFn_Sysdate(call *builtinSysdate) (ctype, error) {
	c.asm.Fn_Sysdate(call.prec)
	return ctype{Type: sqltypes.Datetime, Col: collationBinary}, nil
}

func (c *compiler) compileFn_User(_ *builtinUser) (ctype, error) {
	c.asm.Fn_User()
	return ctype{Type: sqltypes.VarChar, Col: collationUtf8mb3}, nil
}

func (c *compiler) compileFn_Database(_ *builtinDatabase) (ctype, error) {
	c.asm.Fn_Database()
	return ctype{Type: sqltypes.Datetime, Col: collationUtf8mb3}, nil
}

func (c *compiler) compileFn_Version(_ *builtinVersion) (ctype, error) {
	c.asm.Fn_Version()
	return ctype{Type: sqltypes.Datetime, Col: collationUtf8mb3}, nil
}

func (c *compiler) compileFn_MD5(call *builtinMD5) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.Binary, 0, false)
	}

	col := defaultCoercionCollation(c.cfg.Collation)
	c.asm.Fn_MD5(col)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: str.Flag}, nil
}

func (c *compiler) compileFn_SHA1(call *builtinSHA1) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.Binary, 0, false)
	}
	col := defaultCoercionCollation(c.cfg.Collation)
	c.asm.Fn_SHA1(col)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: str.Flag}, nil
}

func (c *compiler) compileFn_SHA2(call *builtinSHA2) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(str)

	bits, err := c.compileExpr(call.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(bits)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(2, sqltypes.Binary, 0, false)
	}

	switch bits.Type {
	case sqltypes.Int64:
		// No-op, already correct type
	case sqltypes.Uint64:
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}

	col := defaultCoercionCollation(c.cfg.Collation)
	c.asm.Fn_SHA2(col)
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: str.Flag | flagNullable}, nil
}

func (c *compiler) compileFn_RandomBytes(call *builtinRandomBytes) (ctype, error) {
	arg, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Int64:
		// No-op, already correct type
	case sqltypes.Uint64:
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}

	c.asm.Fn_RandomBytes()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarBinary, Col: collationBinary, Flag: arg.Flag | flagNullable}, nil
}

func (c *compiler) compileFn_DateFormat(call *builtinDateFormat) (ctype, error) {
	arg, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)

	format, err := c.compileExpr(call.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(format)

	switch arg.Type {
	case sqltypes.Datetime, sqltypes.Date, sqltypes.Time:
	default:
		c.asm.Convert_xDT(2)
	}

	switch arg.Type {
	case sqltypes.VarChar, sqltypes.VarBinary:
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	col := defaultCoercionCollation(c.cfg.Collation)
	c.asm.Fn_DATE_FORMAT(col)
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: arg.Flag | flagNullable}, nil
}
