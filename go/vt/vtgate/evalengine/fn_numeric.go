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
	"math"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type builtinCeil struct {
	CallExpr
}

var _ Expr = (*builtinCeil)(nil)

func (call *builtinCeil) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	switch num := arg.(type) {
	case *evalInt64, *evalUint64:
		return num, nil
	case *evalDecimal:
		dec := num.dec
		dec = dec.Ceil()
		intnum, isfit := dec.Int64()
		if isfit {
			return newEvalInt64(intnum), nil
		}
		return newEvalDecimalWithPrec(dec, 0), nil
	default:
		f, _ := evalToNumeric(num).toFloat()
		return newEvalFloat(math.Ceil(f.f)), nil
	}
}

func (call *builtinCeil) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t, f := call.Arguments[0].typeof(env, fields)
	if sqltypes.IsSigned(t) {
		return sqltypes.Int64, f
	} else if sqltypes.IsUnsigned(t) {
		return sqltypes.Uint64, f
	} else if sqltypes.Decimal == t {
		return sqltypes.Int64, f | flagAmbiguousType
	} else {
		return sqltypes.Float64, f
	}
}

type builtinFloor struct {
	CallExpr
}

var _ Expr = (*builtinFloor)(nil)

func (call *builtinFloor) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	switch num := arg.(type) {
	case *evalInt64, *evalUint64:
		return num, nil
	case *evalDecimal:
		dec := num.dec
		dec = dec.Floor()
		intnum, isfit := dec.Int64()
		if isfit {
			return newEvalInt64(intnum), nil
		}
		return newEvalDecimalWithPrec(dec, 0), nil
	default:
		f, _ := evalToNumeric(num).toFloat()
		return newEvalFloat(math.Floor(f.f)), nil
	}
}

func (call *builtinFloor) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t, f := call.Arguments[0].typeof(env, fields)
	if sqltypes.IsSigned(t) {
		return sqltypes.Int64, f
	} else if sqltypes.IsUnsigned(t) {
		return sqltypes.Uint64, f
	} else if sqltypes.Decimal == t {
		return sqltypes.Int64, f | flagAmbiguousType
	} else {
		return sqltypes.Float64, f
	}
}

type builtinAbs struct {
	CallExpr
}

var _ Expr = (*builtinAbs)(nil)

func (call *builtinAbs) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	switch num := arg.(type) {
	case *evalUint64:
		return num, nil
	case *evalInt64:
		if num.i < 0 {
			if num.i == math.MinInt64 {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "BIGINT value is out of range")
			}
			return newEvalInt64(-num.i), nil
		}
		return num, nil
	case *evalDecimal:
		return newEvalDecimalWithPrec(num.dec.Abs(), num.length), nil
	default:
		f, _ := evalToNumeric(num).toFloat()
		return newEvalFloat(math.Abs(f.f)), nil
	}
}

func (call *builtinAbs) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t, f := call.Arguments[0].typeof(env, fields)
	if sqltypes.IsNumber(t) {
		return t, f
	} else {
		return sqltypes.Float64, f
	}
}

type builtinPi struct {
	CallExpr
}

var _ Expr = (*builtinPi)(nil)

func (call *builtinPi) eval(env *ExpressionEnv) (eval, error) {
	return newEvalFloat(math.Pi), nil
}

func (call *builtinPi) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Float64, 0
}

func isFinite(f float64) bool {
	const mask = 0x7FF
	const shift = 64 - 11 - 1
	x := math.Float64bits(f)
	return uint32(x>>shift)&mask != mask
}

type builtinAcos struct {
	CallExpr
}

var _ Expr = (*builtinAcos)(nil)

func (call *builtinAcos) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	if f.f < -1 || f.f > 1 {
		return nil, nil
	}
	return newEvalFloat(math.Acos(f.f)), nil
}

func (call *builtinAcos) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinAsin struct {
	CallExpr
}

var _ Expr = (*builtinAsin)(nil)

func (call *builtinAsin) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	if f.f < -1 || f.f > 1 {
		return nil, nil
	}
	return newEvalFloat(math.Asin(f.f)), nil
}

func (call *builtinAsin) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinAtan struct {
	CallExpr
}

var _ Expr = (*builtinAtan)(nil)

func (call *builtinAtan) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Atan(f.f)), nil
}

func (call *builtinAtan) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinAtan2 struct {
	CallExpr
}

var _ Expr = (*builtinAtan2)(nil)

func (call *builtinAtan2) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	f1, _ := evalToNumeric(arg1).toFloat()
	f2, _ := evalToNumeric(arg2).toFloat()
	return newEvalFloat(math.Atan2(f1.f, f2.f)), nil
}

func (call *builtinAtan2) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinCos struct {
	CallExpr
}

var _ Expr = (*builtinCos)(nil)

func (call *builtinCos) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Cos(f.f)), nil
}

func (call *builtinCos) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinCot struct {
	CallExpr
}

var _ Expr = (*builtinCot)(nil)

func (call *builtinCot) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(1.0 / math.Tan(f.f)), nil
}

func (call *builtinCot) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinSin struct {
	CallExpr
}

var _ Expr = (*builtinSin)(nil)

func (call *builtinSin) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Sin(f.f)), nil
}

func (call *builtinSin) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinTan struct {
	CallExpr
}

var _ Expr = (*builtinTan)(nil)

func (call *builtinTan) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Tan(f.f)), nil
}

func (call *builtinTan) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinDegrees struct {
	CallExpr
}

var _ Expr = (*builtinDegrees)(nil)

func (call *builtinDegrees) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(f.f * (180 / math.Pi)), nil
}

func (call *builtinDegrees) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinRadians struct {
	CallExpr
}

var _ Expr = (*builtinRadians)(nil)

func (call *builtinRadians) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(f.f * (math.Pi / 180)), nil
}

func (call *builtinRadians) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinExp struct {
	CallExpr
}

var _ Expr = (*builtinExp)(nil)

func (call *builtinExp) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	a := math.Exp(f.f)
	if !isFinite(a) {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (call *builtinExp) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinLn struct {
	CallExpr
}

var _ Expr = (*builtinLn)(nil)

func (call *builtinLn) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	a, ok := math_log(f.f)
	if !ok {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (call *builtinLn) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinLog struct {
	CallExpr
}

var _ Expr = (*builtinLog)(nil)

func (call *builtinLog) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	f1, _ := evalToNumeric(arg1).toFloat()
	f2, _ := evalToNumeric(arg2).toFloat()

	a, ok := math_logN(f1.f, f2.f)
	if !ok {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (call *builtinLog) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinLog10 struct {
	CallExpr
}

var _ Expr = (*builtinLog10)(nil)

func (call *builtinLog10) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	a, ok := math_log10(f.f)
	if !ok {
		return nil, nil
	}

	return newEvalFloat(a), nil
}

func (call *builtinLog10) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinLog2 struct {
	CallExpr
}

var _ Expr = (*builtinLog2)(nil)

func (call *builtinLog2) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	a, ok := math_log2(f.f)
	if !ok {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (call *builtinLog2) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinPow struct {
	CallExpr
}

var _ Expr = (*builtinPow)(nil)

func (call *builtinPow) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	f1, _ := evalToNumeric(arg1).toFloat()
	f2, _ := evalToNumeric(arg2).toFloat()

	a := math.Pow(f1.f, f2.f)
	if !isFinite(a) {
		return nil, nil
	}

	return newEvalFloat(a), nil
}

func (call *builtinPow) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinSign struct {
	CallExpr
}

var _ Expr = (*builtinSign)(nil)

func (call *builtinSign) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	switch arg := arg.(type) {
	case *evalInt64:
		if arg.i < 0 {
			return newEvalInt64(-1), nil
		} else if arg.i > 0 {
			return newEvalInt64(1), nil
		} else {
			return newEvalInt64(0), nil
		}
	case *evalUint64:
		if arg.u > 0 {
			return newEvalInt64(1), nil
		} else {
			return newEvalInt64(0), nil
		}
	case *evalDecimal:
		return newEvalInt64(int64(arg.dec.Sign())), nil
	case *evalFloat:
		if arg.f < 0 {
			return newEvalInt64(-1), nil
		} else if arg.f > 0 {
			return newEvalInt64(1), nil
		} else {
			return newEvalInt64(0), nil
		}
	default:
		f, _ := evalToNumeric(arg).toFloat()
		if f.f < 0 {
			return newEvalInt64(-1), nil
		} else if f.f > 0 {
			return newEvalInt64(1), nil
		} else {
			return newEvalInt64(0), nil
		}
	}
}

func (call *builtinSign) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, t
}

type builtinSqrt struct {
	CallExpr
}

var _ Expr = (*builtinSqrt)(nil)

func (call *builtinSqrt) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	a := math.Sqrt(f.f)
	if !isFinite(a) {
		return nil, nil
	}

	return newEvalFloat(a), nil
}

func (call *builtinSqrt) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, t | flagNullable
}

// Math helpers extracted from `math` package

func math_log(x float64) (float64, bool) {
	const (
		Ln2Hi = 6.93147180369123816490e-01 /* 3fe62e42 fee00000 */
		Ln2Lo = 1.90821492927058770002e-10 /* 3dea39ef 35793c76 */
		L1    = 6.666666666666735130e-01   /* 3FE55555 55555593 */
		L2    = 3.999999999940941908e-01   /* 3FD99999 9997FA04 */
		L3    = 2.857142874366239149e-01   /* 3FD24924 94229359 */
		L4    = 2.222219843214978396e-01   /* 3FCC71C5 1D8E78AF */
		L5    = 1.818357216161805012e-01   /* 3FC74664 96CB03DE */
		L6    = 1.531383769920937332e-01   /* 3FC39A09 D078C69F */
		L7    = 1.479819860511658591e-01   /* 3FC2F112 DF3E5244 */
	)

	// special cases
	switch {
	case math.IsNaN(x) || math.IsInf(x, 1):
		return 0, false
	case x < 0:
		return 0, false
	case x == 0:
		return 0, false
	}

	// reduce
	f1, ki := math.Frexp(x)
	if f1 < math.Sqrt2/2 {
		f1 *= 2
		ki--
	}
	f := f1 - 1
	k := float64(ki)

	// compute
	s := f / (2 + f)
	s2 := s * s
	s4 := s2 * s2
	t1 := s2 * (L1 + s4*(L3+s4*(L5+s4*L7)))
	t2 := s4 * (L2 + s4*(L4+s4*L6))
	R := t1 + t2
	hfsq := 0.5 * f * f
	return k*Ln2Hi - ((hfsq - (s*(hfsq+R) + k*Ln2Lo)) - f), true
}

func math_logN(f1, f2 float64) (float64, bool) {
	a1, _ := math_log(f1)
	if a1 == 0 {
		return 0, false
	}
	a2, ok := math_log(f2)
	if !ok {
		return 0, false
	}
	return a2 / a1, true
}

func math_log10(f float64) (float64, bool) {
	if a, ok := math_log(f); ok {
		return a * (1 / math.Ln10), true
	}
	return 0, false
}

func math_log2(f float64) (float64, bool) {
	frac, exp := math.Frexp(f)
	// Make sure exact powers of two give an exact answer.
	// Don't depend on Log(0.5)*(1/Ln2)+exp being exactly exp-1.
	if frac == 0.5 {
		return float64(exp - 1), true
	}
	if a, ok := math_log(frac); ok {
		return a*(1/math.Ln2) + float64(exp), true
	}
	return 0, false
}
