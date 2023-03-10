package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func (c *compiler) compileCollate(expr *CollateExpr) (ctype, error) {
	ct, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(ct)

	switch ct.Type {
	case sqltypes.VarChar:
		if err := collations.Local().EnsureCollate(ct.Col.Collation, expr.TypedCollation.Collation); err != nil {
			return ctype{}, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, err.Error())
		}
		fallthrough
	case sqltypes.VarBinary:
		c.asm.Collate(expr.TypedCollation.Collation)
	default:
		return ctype{}, c.unsupported(expr)
	}

	c.asm.jumpDestination(skip)

	ct.Col = expr.TypedCollation
	ct.Flag |= flagExplicitCollation | flagNullable
	return ct, nil
}

func (c *compiler) compileConvert(conv *ConvertExpr) (ctype, error) {
	arg, err := c.compileExpr(conv.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	var convt ctype

	switch conv.Type {
	case "BINARY":
		convt = ctype{Type: conv.convertToBinaryType(arg.Type), Col: collationBinary}
		c.asm.Convert_xb(1, convt.Type, conv.Length, conv.HasLength)

	case "CHAR", "NCHAR":
		convt = ctype{
			Type: conv.convertToCharType(arg.Type),
			Col:  collations.TypedCollation{Collation: conv.Collation},
		}
		c.asm.Convert_xc(1, convt.Type, convt.Col.Collation, conv.Length, conv.HasLength)

	case "DECIMAL":
		convt = ctype{Type: sqltypes.Decimal, Col: collationNumeric}
		m, d := conv.decimalPrecision()
		c.asm.Convert_xd(1, m, d)

	case "DOUBLE", "REAL":
		convt = c.compileToFloat(arg, 1)

	case "SIGNED", "SIGNED INTEGER":
		convt = c.compileToInt64(arg, 1)

	case "UNSIGNED", "UNSIGNED INTEGER":
		convt = c.compileToUint64(arg, 1)

	case "JSON":
		// TODO: what does NULL map to?
		convt, err = c.compileToJSON(arg, 1)
		if err != nil {
			return ctype{}, err
		}

	default:
		return ctype{}, c.unsupported(conv)
	}

	c.asm.jumpDestination(skip)
	convt.Flag = arg.Flag | flagNullable
	return convt, nil

}

func (c *compiler) compileConvertUsing(conv *ConvertUsingExpr) (ctype, error) {
	ct, err := c.compileExpr(conv.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(ct)
	c.asm.Convert_xc(1, sqltypes.VarChar, conv.Collation, 0, false)
	c.asm.jumpDestination(skip)

	col := collations.TypedCollation{
		Collation:    conv.Collation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}
	return ctype{Type: sqltypes.VarChar, Flag: flagNullable, Col: col}, nil
}
