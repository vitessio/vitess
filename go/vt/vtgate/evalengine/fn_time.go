package evalengine

import (
	"strconv"
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

func withSubSecondPrecision(prec int) strftime.Appender {
	return strftime.AppendFunc(func(b []byte, t time.Time) []byte {
		l := len(b)
		b = strconv.AppendUint(b, uint64(t.Nanosecond()), 10)
		for len(b)-l < prec {
			b = append(b, '0')
		}
		return b[:l+prec]
	})
}

func init() {
	formatTime[0], _ = strftime.New("%H:%M:%S")
	formatDateTime[0], _ = strftime.New("%Y-%m-%d %H:%M:%S")
	formatDate, _ = strftime.New("%Y-%m-%d")

	for i := 1; i <= 6; i++ {
		spec := strftime.WithSpecification('f', withSubSecondPrecision(i))
		formatTime[i], _ = strftime.New("%H:%M:%S.%f", spec)
		formatDateTime[i], _ = strftime.New("%Y-%m-%d %H:%M:%S.%f", spec)
	}
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
	now := time.Now()
	if env.Tz != nil {
		now = now.In(env.Tz)
	}
	return newEvalRaw(sqltypes.Datetime, formatDateTime[call.prec].FormatBuffer(buf, now), collationBinary), nil
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
