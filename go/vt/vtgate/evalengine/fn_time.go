package evalengine

import (
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	builtinNow struct {
		CallExpr
		utc      bool
		onlyTime bool
	}

	builtinSysdate struct {
		CallExpr
	}

	builtinCurdate struct {
		CallExpr
	}
)

var _ Expr = (*builtinNow)(nil)
var _ Expr = (*builtinSysdate)(nil)
var _ Expr = (*builtinCurdate)(nil)

func (call *builtinNow) eval(env *ExpressionEnv) (eval, error) {
	if env.now.IsZero() {
		env.now = time.Now()
	}

	now := env.now
	if call.utc {
		now = now.UTC()
	}

	sub := 0
	if len(call.Arguments) > 0 {
		arg, err := call.arg1(env)
		if err != nil {
			return nil, err
		}
		if arg, ok := arg.(*evalInt64); ok {
			sub = int(arg.i)
		}
	}

	format := "2006-01-02 15:04:05"
	t := sqltypes.Datetime
	if call.onlyTime {
		format = "15:04:05"
		t = sqltypes.Time
	}
	if sub > 0 {
		format = format + "." + strings.Repeat("9", sub)
	}

	return newEvalRaw(t, []byte(now.Format(format)), collationBinary), nil
}

func (call *builtinNow) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	if call.onlyTime {
		return sqltypes.Time, 0
	}
	return sqltypes.Datetime, 0
}

func (call *builtinSysdate) eval(env *ExpressionEnv) (eval, error) {
	sub := 0
	if len(call.Arguments) > 0 {
		arg, err := call.arg1(env)
		if err != nil {
			return nil, err
		}
		if arg, ok := arg.(*evalInt64); ok {
			sub = int(arg.i)
		}
	}

	format := "2006-01-02 15:04:05"
	if sub > 0 {
		format = format + "." + strings.Repeat("9", sub)
	}

	return newEvalRaw(sqltypes.Datetime, []byte(time.Now().Format(format)), collationBinary), nil
}

func (call *builtinSysdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Datetime, 0
}

func (call *builtinCurdate) eval(env *ExpressionEnv) (eval, error) {
	return newEvalRaw(sqltypes.Date, []byte(time.Now().Format("2006-01-02")), collationBinary), nil
}

func (call *builtinCurdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Date, 0
}
