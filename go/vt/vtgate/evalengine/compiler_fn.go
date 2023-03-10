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
		return c.compileFn_LENGTH(call, charLen)
	case *builtinLength:
		return c.compileFn_LENGTH(call, byteLen)
	case *builtinBitLength:
		return c.compileFn_LENGTH(call, bitLen)
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

type lengthOp int

const (
	charLen lengthOp = iota
	byteLen
	bitLen
)

func (c *compiler) compileFn_LENGTH(call callable, op lengthOp) (ctype, error) {
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

	c.asm.Fn_LENGTH(op)
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

	convt := ctype{Type: arg.Type, Col: collationNumeric}
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

	convt := ctype{Type: arg.Type, Col: collationNumeric}
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
