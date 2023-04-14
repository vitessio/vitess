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

func (c *compiler) compileToDateTime(doct ctype, offset int) ctype {
	switch doct.Type {
	case sqltypes.Datetime:
		return doct
	default:
		c.asm.Convert_xDT(offset)
	}
	return ctype{Type: sqltypes.Datetime, Col: collationBinary, Flag: flagNullable}
}

func (c *compiler) compileToTime(doct ctype, offset int) ctype {
	switch doct.Type {
	case sqltypes.Time:
		return doct
	default:
		c.asm.Convert_xT(offset)
	}
	return ctype{Type: sqltypes.Time, Col: collationBinary, Flag: flagNullable}
}
