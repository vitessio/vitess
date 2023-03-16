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
	"vitess.io/vitess/go/mysql/collations"
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
		return c.compileFn_xLENGTH(call, c.asm.Fn_CHAR_LENGTH)
	case *builtinLength:
		return c.compileFn_xLENGTH(call, c.asm.Fn_LENGTH)
	case *builtinBitLength:
		return c.compileFn_xLENGTH(call, c.asm.Fn_BIT_LENGTH)
	case *builtinASCII:
		return c.compileFn_ASCII(call)
	case *builtinHex:
		return c.compileFn_HEX(call)
	case *builtinBitCount:
		return c.compileFn_BIT_COUNT(call)
	case *builtinCollation:
		return c.compileFn_COLLATION(call)
	case *builtinCeil:
		return c.compileFn_CEIL(call)
	case *builtinFloor:
		return c.compileFn_FLOOR(call)
	case *builtinAbs:
		return c.compileFn_ABS(call)
	case *builtinPi:
		return c.compileFn_PI(call)
	case *builtinAcos:
		return c.compileFn_ACOS(call)
	case *builtinAsin:
		return c.compileFn_ASIN(call)
	case *builtinAtan:
		return c.compileFn_ATAN(call)
	case *builtinAtan2:
		return c.compileFn_ATAN2(call)
	case *builtinCos:
		return c.compileFn_COS(call)
	case *builtinCot:
		return c.compileFn_COT(call)
	case *builtinSin:
		return c.compileFn_SIN(call)
	case *builtinTan:
		return c.compileFn_TAN(call)
	case *builtinDegrees:
		return c.compileFn_DEGREES(call)
	case *builtinRadians:
		return c.compileFn_RADIANS(call)
	case *builtinWeightString:
		return c.compileFn_WEIGHT_STRING(call)
	default:
		return ctype{}, c.unsupported(call)
	}
}

func (c *compiler) compileFn_MULTICMP(call *builtinMultiComparison) (ctype, error) {
	var (
		integersI int
		integersU int
		floats    int
		decimals  int
		text      int
		binary    int
		args      []ctype
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
			integersI++
		case sqltypes.Uint64:
			integersU++
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

	if integersI+integersU == len(args) {
		if integersI == len(args) {
			c.asm.Fn_MULTICMP_i(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
		}
		if integersU == len(args) {
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
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.asm.Convert_xc(2, sqltypes.VarChar, c.defaultCollation, 0, false)
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
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.asm.Convert_xc(1, t, c.defaultCollation, 0, false)
	}

	col := collations.TypedCollation{
		Collation:    c.defaultCollation,
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

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.asm.Convert_xc(1, sqltypes.VarBinary, c.defaultCollation, 0, false)
	}

	c.asm.Fn_FROM_BASE64()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
}

func (c *compiler) compileFn_CCASE(call *builtinChangeCase) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.defaultCollation, 0, false)
	}

	c.asm.Fn_LUCASE(call.upcase)
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Col: str.Col}, nil
}

func (c *compiler) compileFn_xLENGTH(call callable, asm_ins func()) (ctype, error) {
	str, err := c.compileExpr(call.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.defaultCollation, 0, false)
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
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.defaultCollation, 0, false)
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

	col := collations.TypedCollation{
		Collation:    c.defaultCollation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}

	t := sqltypes.VarChar
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case sqltypes.IsNumber(str.Type), sqltypes.IsDecimal(str.Type):
		c.asm.Fn_HEX_d(col)
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
		c.asm.Fn_HEX_c(t, col)
	default:
		c.asm.Convert_xc(1, t, c.defaultCollation, 0, false)
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

	col := collations.TypedCollation{
		Collation:    collations.CollationUtf8ID,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}

	c.asm.Fn_COLLATION(col)
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Col: col}, nil
}

func (c *compiler) compileFn_CEIL(expr *builtinCeil) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	convt := ctype{Type: arg.Type, Col: collationNumeric, Flag: arg.Flag}
	switch {
	case sqltypes.IsIntegral(arg.Type):
		// No-op for integers.
	case sqltypes.IsFloat(arg.Type):
		c.asm.Fn_CEIL_f()
	case sqltypes.IsDecimal(arg.Type):
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		convt.Type = sqltypes.Int64
		c.asm.Fn_CEIL_d()
	default:
		convt.Type = sqltypes.Float64
		c.asm.Convert_xf(1)
		c.asm.Fn_CEIL_f()
	}

	c.asm.jumpDestination(skip)
	return convt, nil
}

func (c *compiler) compileFn_FLOOR(expr *builtinFloor) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	convt := ctype{Type: arg.Type, Col: collationNumeric, Flag: arg.Flag}
	switch {
	case sqltypes.IsIntegral(arg.Type):
		// No-op for integers.
	case sqltypes.IsFloat(arg.Type):
		c.asm.Fn_FLOOR_f()
	case sqltypes.IsDecimal(arg.Type):
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		convt.Type = sqltypes.Int64
		c.asm.Fn_FLOOR_d()
	default:
		convt.Type = sqltypes.Float64
		c.asm.Convert_xf(1)
		c.asm.Fn_FLOOR_f()
	}

	c.asm.jumpDestination(skip)
	return convt, nil
}

func (c *compiler) compileFn_ABS(expr *builtinAbs) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	convt := ctype{Type: arg.Type, Col: collationNumeric, Flag: arg.Flag}
	switch {
	case sqltypes.IsUnsigned(arg.Type):
		// No-op if it's unsigned since that's already positive.
	case sqltypes.IsSigned(arg.Type):
		c.asm.Fn_ABS_i()
	case sqltypes.IsFloat(arg.Type):
		c.asm.Fn_ABS_f()
	case sqltypes.IsDecimal(arg.Type):
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

func (c *compiler) compileFn_PI(expr *builtinPi) (ctype, error) {
	c.asm.Fn_PI()
	return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
}

func (c *compiler) compileFn_ACOS(expr *builtinAcos) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_ACOS()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (c *compiler) compileFn_ASIN(expr *builtinAsin) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_ASIN()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (c *compiler) compileFn_ATAN(expr *builtinAtan) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_ATAN()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag}, nil
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

	switch {
	case sqltypes.IsFloat(arg1.Type) && sqltypes.IsFloat(arg2.Type):
	case sqltypes.IsFloat(arg1.Type):
		c.asm.Convert_xf(1)
	case sqltypes.IsFloat(arg2.Type):
		c.asm.Convert_xf(2)
	default:
		c.asm.Convert_xf(2)
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_ATAN2()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg1.Flag | arg2.Flag}, nil
}

func (c *compiler) compileFn_COS(expr *builtinCos) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_COS()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag}, nil
}

func (c *compiler) compileFn_COT(expr *builtinCot) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_COT()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag}, nil
}

func (c *compiler) compileFn_SIN(expr *builtinSin) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_SIN()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag}, nil
}

func (c *compiler) compileFn_TAN(expr *builtinTan) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_TAN()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag}, nil
}

func (c *compiler) compileFn_DEGREES(expr *builtinDegrees) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_DEGREES()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag}, nil
}

func (c *compiler) compileFn_RADIANS(expr *builtinRadians) (ctype, error) {
	arg, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case sqltypes.IsFloat(arg.Type):
	default:
		c.asm.Convert_xf(1)
	}

	c.asm.Fn_RADIANS()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag}, nil
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
