package evalengine

import (
	"math"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type buildinABS struct{}

func (buildinABS) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	toabs := &args[0]
	if toabs.null() {
		result.setNull()
		return
	}

	switch toabs.typeof() {
	case sqltypes.Int64:
		res := toabs.int64()
		if res >= 0 {
			result.setInt64(res)
		} else {
			if res == math.MinInt64 {
				//Same to mysql, return out of range error
				throwEvalError(vterrors.Errorf(vtrpcpb.Code_OUT_OF_RANGE, "BIGINT value is out of range in: %s", toabs.String()))
			} else {
				result.setInt64(-res)
			}
		}
	case sqltypes.Decimal:
		res := toabs.decimal()
		res.num.Abs(&res.num)
		result.setDecimal(res)
	case sqltypes.Float64:
		res := toabs.float64()
		res = math.Abs(res)
		result.setFloat(res)
	default:
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported ABS argument: %s", toabs.String()))
	}
}

func (buildinABS) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) < 1 {
		throwArgError("ABS")
	}

	tt, f := args[0].typeof(env)
	return tt, f
}
