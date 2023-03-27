package evalengine

import (
	"fmt"

	"vitess.io/vitess/go/sqltypes"
)

func (c *compiler) compileFn_Now(call *builtinNow) (ctype, error) {
	var sub bool
	if len(call.Arguments) > 0 {
		ct, err := c.compileExpr(call.Arguments[0])
		if err != nil {
			return ctype{}, err
		}

		if ct.Type != sqltypes.Int64 {
			return ctype{}, fmt.Errorf("argument to now must be an integer: %v", ct.Type)
		}
		sub = true
	}

	format := "2006-01-02 15:04:05"
	t := sqltypes.Datetime
	if call.onlyTime {
		format = "15:04:05"
		t = sqltypes.Time
	}

	if sub {
		c.asm.Fn_NowSub(t, format, call.utc)
	} else {
		c.asm.Fn_Now(t, format, call.utc)
	}
	return ctype{Type: t, Col: collationBinary}, nil
}

func (c *compiler) compileFn_Curdate(call *builtinCurdate) (ctype, error) {
	c.asm.Fn_Curdate()
	return ctype{Type: sqltypes.Date, Col: collationBinary}, nil
}

func (c *compiler) compileFn_Sysdate(call *builtinSysdate) (ctype, error) {
	var sub bool
	if len(call.Arguments) > 0 {
		ct, err := c.compileExpr(call.Arguments[0])
		if err != nil {
			return ctype{}, err
		}

		if ct.Type != sqltypes.Int64 {
			return ctype{}, fmt.Errorf("argument to now must be an integer: %v", ct.Type)
		}
		sub = true
	}

	if sub {
		c.asm.Fn_SysdateSub()
	} else {
		c.asm.Fn_Sysdate()
	}
	return ctype{Type: sqltypes.Datetime, Col: collationBinary}, nil
}
