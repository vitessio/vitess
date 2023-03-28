package evalengine

import (
	"time"

	"github.com/lestrrat-go/strftime"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	builtinNow struct {
		CallExpr
		utc      bool
		onlyTime bool
		prec     uint8
	}

	builtinSysdate struct {
		CallExpr
		prec uint8
	}

	builtinCurdate struct {
		CallExpr
	}
)

var _ Expr = (*builtinNow)(nil)
var _ Expr = (*builtinSysdate)(nil)
var _ Expr = (*builtinCurdate)(nil)

const formatBufferSize = 32

var (
	formatTime     [7]*strftime.Strftime
	formatDateTime [7]*strftime.Strftime
	formatDate     *strftime.Strftime
)

func init() {
	for i := 0; i < 7; i++ {
		formatTime[i], _ = strftime.New("%H:%M:%S")
		formatDateTime[i], _ = strftime.New("%Y-%m-%d %H:%M:%S")
	}
	formatDate, _ = strftime.New("%Y-%m-%d")
}

func (call *builtinNow) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(call.utc)
	buf := make([]byte, 0, formatBufferSize)
	if call.onlyTime {
		buf = formatTime[call.prec].FormatBuffer(buf, now)
		return newEvalRaw(sqltypes.Time, buf, collationBinary), nil
	} else {
		buf = formatDateTime[call.prec].FormatBuffer(buf, now)
		return newEvalRaw(sqltypes.Datetime, buf, collationBinary), nil
	}
}

func (call *builtinNow) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	if call.onlyTime {
		return sqltypes.Time, 0
	}
	return sqltypes.Datetime, 0
}

func (call *builtinSysdate) eval(env *ExpressionEnv) (eval, error) {
	buf := make([]byte, 0, formatBufferSize)
	return newEvalRaw(sqltypes.Datetime, formatDateTime[call.prec].FormatBuffer(buf, time.Now()), collationBinary), nil
}

func (call *builtinSysdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Datetime, 0
}

func (call *builtinCurdate) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(false)
	buf := make([]byte, 0, formatBufferSize)
	return newEvalRaw(sqltypes.Date, formatDate.FormatBuffer(buf, now), collationBinary), nil
}

func (call *builtinCurdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Date, 0
}
