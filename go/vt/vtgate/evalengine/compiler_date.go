package evalengine

import (
	"vitess.io/vitess/go/sqltypes"
)

func (c *compiler) compileToDate(doct ctype, offset int) ctype {
	switch doct.Type {
	case sqltypes.Date:
		return doct
	default:
		c.asm.Convert_xD(offset)
	}
	return ctype{Type: sqltypes.Date, Col: collationBinary, Flag: flagNullable}
}

func (c *compiler) compileToDateTime(doct ctype, offset, prec int) ctype {
	switch doct.Type {
	case sqltypes.Datetime:
		c.asm.Convert_tp(offset, prec)
		return doct
	default:
		c.asm.Convert_xDT(offset, prec)
	}
	return ctype{Type: sqltypes.Datetime, Col: collationBinary, Flag: flagNullable}
}

func (c *compiler) compileToTime(doct ctype, offset, prec int) ctype {
	switch doct.Type {
	case sqltypes.Time:
		c.asm.Convert_tp(offset, prec)
		return doct
	default:
		c.asm.Convert_xT(offset, prec)
	}
	return ctype{Type: sqltypes.Time, Col: collationBinary, Flag: flagNullable}
}
