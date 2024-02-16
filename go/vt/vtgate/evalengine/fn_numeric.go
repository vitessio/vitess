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
	"errors"
	"hash/crc32"
	"math"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type builtinCeil struct {
	CallExpr
}

var _ IR = (*builtinCeil)(nil)

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
		f, _ := evalToFloat(num)
		return newEvalFloat(math.Ceil(f.f)), nil
	}
}

func (call *builtinCeil) compile(c *compiler) (ctype, error) {
	return c.compileFn_rounding(call.Arguments[0], c.asm.Fn_CEIL_f, c.asm.Fn_CEIL_d)
}

type builtinFloor struct {
	CallExpr
}

var _ IR = (*builtinFloor)(nil)

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
		f, _ := evalToFloat(num)
		return newEvalFloat(math.Floor(f.f)), nil
	}
}

func (call *builtinFloor) compile(c *compiler) (ctype, error) {
	return c.compileFn_rounding(call.Arguments[0], c.asm.Fn_FLOOR_f, c.asm.Fn_FLOOR_d)
}

type builtinAbs struct {
	CallExpr
}

var _ IR = (*builtinAbs)(nil)

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
		f, _ := evalToFloat(num)
		return newEvalFloat(math.Abs(f.f)), nil
	}
}

func (expr *builtinAbs) compile(c *compiler) (ctype, error) {
	arg, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	if arg.Type == sqltypes.Uint64 {
		// No-op if it's unsigned since that's already positive.
		return arg, nil
	}

	skip := c.compileNullCheck1(arg)

	convt := ctype{Type: arg.Type, Col: collationNumeric, Flag: nullableFlags(arg.Flag)}
	switch arg.Type {
	case sqltypes.Int64:
		c.asm.Fn_ABS_i()
	case sqltypes.Float64:
		c.asm.Fn_ABS_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		c.asm.Fn_ABS_d()
	default:
		convt.Type = sqltypes.Float64
		c.asm.Convert_xf(1)
		c.asm.Fn_ABS_f()
	}

	c.asm.jumpDestination(skip)
	return convt, nil
}

type builtinPi struct {
	CallExpr
}

var _ IR = (*builtinPi)(nil)

func (call *builtinPi) eval(env *ExpressionEnv) (eval, error) {
	return newEvalFloat(math.Pi), nil
}

func (*builtinPi) compile(c *compiler) (ctype, error) {
	c.asm.Fn_PI()
	return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
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

var _ IR = (*builtinAcos)(nil)

func (call *builtinAcos) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	if f.f < -1 || f.f > 1 {
		return nil, nil
	}
	return newEvalFloat(math.Acos(f.f)), nil
}

func (call *builtinAcos) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_ACOS, flagNullable)
}

type builtinAsin struct {
	CallExpr
}

var _ IR = (*builtinAsin)(nil)

func (call *builtinAsin) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	if f.f < -1 || f.f > 1 {
		return nil, nil
	}
	return newEvalFloat(math.Asin(f.f)), nil
}

func (call *builtinAsin) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_ASIN, flagNullable)
}

type builtinAtan struct {
	CallExpr
}

var _ IR = (*builtinAtan)(nil)

func (call *builtinAtan) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	return newEvalFloat(math.Atan(f.f)), nil
}

func (call *builtinAtan) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_ATAN, 0)
}

type builtinAtan2 struct {
	CallExpr
}

var _ IR = (*builtinAtan2)(nil)

func (call *builtinAtan2) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	f1, _ := evalToFloat(arg1)
	f2, _ := evalToFloat(arg2)
	return newEvalFloat(math.Atan2(f1.f, f2.f)), nil
}

func (expr *builtinAtan2) compile(c *compiler) (ctype, error) {
	arg1, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	arg2, err := expr.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(arg1, arg2)
	c.compileToFloat(arg1, 2)
	c.compileToFloat(arg2, 1)
	c.asm.Fn_ATAN2()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: nullableFlags(arg1.Flag | arg2.Flag)}, nil
}

type builtinCos struct {
	CallExpr
}

var _ IR = (*builtinCos)(nil)

func (call *builtinCos) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	return newEvalFloat(math.Cos(f.f)), nil
}

func (call *builtinCos) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_COS, 0)
}

type builtinCot struct {
	CallExpr
}

var _ IR = (*builtinCot)(nil)

func (call *builtinCot) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	return newEvalFloat(1.0 / math.Tan(f.f)), nil
}

func (call *builtinCot) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_COT, 0)
}

type builtinSin struct {
	CallExpr
}

var _ IR = (*builtinSin)(nil)

func (call *builtinSin) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	return newEvalFloat(math.Sin(f.f)), nil
}

func (call *builtinSin) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_SIN, 0)
}

type builtinTan struct {
	CallExpr
}

var _ IR = (*builtinTan)(nil)

func (call *builtinTan) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	return newEvalFloat(math.Tan(f.f)), nil
}

func (call *builtinTan) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_TAN, 0)
}

type builtinDegrees struct {
	CallExpr
}

var _ IR = (*builtinDegrees)(nil)

func (call *builtinDegrees) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	return newEvalFloat(f.f * (180 / math.Pi)), nil
}

func (call *builtinDegrees) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_DEGREES, 0)
}

type builtinRadians struct {
	CallExpr
}

var _ IR = (*builtinRadians)(nil)

func (call *builtinRadians) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	return newEvalFloat(f.f * (math.Pi / 180)), nil
}

func (call *builtinRadians) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_RADIANS, 0)
}

type builtinExp struct {
	CallExpr
}

var _ IR = (*builtinExp)(nil)

func (call *builtinExp) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	a := math.Exp(f.f)
	if !isFinite(a) {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (call *builtinExp) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_EXP, flagNullable)
}

type builtinLn struct {
	CallExpr
}

var _ IR = (*builtinLn)(nil)

func (call *builtinLn) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	a, ok := math_log(f.f)
	if !ok {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (call *builtinLn) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_LN, flagNullable)
}

type builtinLog struct {
	CallExpr
}

var _ IR = (*builtinLog)(nil)

func (call *builtinLog) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	f1, _ := evalToFloat(arg1)
	f2, _ := evalToFloat(arg2)

	a, ok := math_logN(f1.f, f2.f)
	if !ok {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (expr *builtinLog) compile(c *compiler) (ctype, error) {
	arg1, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	arg2, err := expr.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(arg1, arg2)
	c.compileToFloat(arg1, 2)
	c.compileToFloat(arg2, 1)
	c.asm.Fn_LOG()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: nullableFlags(arg1.Flag | arg2.Flag)}, nil
}

type builtinLog10 struct {
	CallExpr
}

var _ IR = (*builtinLog10)(nil)

func (call *builtinLog10) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	a, ok := math_log10(f.f)
	if !ok {
		return nil, nil
	}

	return newEvalFloat(a), nil
}

func (call *builtinLog10) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_LOG10, flagNullable)
}

type builtinLog2 struct {
	CallExpr
}

var _ IR = (*builtinLog2)(nil)

func (call *builtinLog2) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	a, ok := math_log2(f.f)
	if !ok {
		return nil, nil
	}
	return newEvalFloat(a), nil
}

func (call *builtinLog2) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_LOG2, flagNullable)
}

type builtinPow struct {
	CallExpr
}

var _ IR = (*builtinPow)(nil)

func (call *builtinPow) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	f1, _ := evalToFloat(arg1)
	f2, _ := evalToFloat(arg2)

	a := math.Pow(f1.f, f2.f)
	if !isFinite(a) {
		return nil, nil
	}

	return newEvalFloat(a), nil
}

func (expr *builtinPow) compile(c *compiler) (ctype, error) {
	arg1, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	arg2, err := expr.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(arg1, arg2)
	c.compileToFloat(arg1, 2)
	c.compileToFloat(arg2, 1)
	c.asm.Fn_POW()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: nullableFlags(arg1.Flag | arg2.Flag)}, nil
}

type builtinSign struct {
	CallExpr
}

var _ IR = (*builtinSign)(nil)

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
		f, _ := evalToFloat(arg)
		if f.f < 0 {
			return newEvalInt64(-1), nil
		} else if f.f > 0 {
			return newEvalInt64(1), nil
		} else {
			return newEvalInt64(0), nil
		}
	}
}

func (expr *builtinSign) compile(c *compiler) (ctype, error) {
	arg, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Int64:
		c.asm.Fn_SIGN_i()
	case sqltypes.Uint64:
		c.asm.Fn_SIGN_u()
	case sqltypes.Float64:
		c.asm.Fn_SIGN_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		c.asm.Fn_SIGN_d()
	default:
		c.asm.Convert_xf(1)
		c.asm.Fn_SIGN_f()
	}

	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: nullableFlags(arg.Flag)}, nil
}

type builtinSqrt struct {
	CallExpr
}

var _ IR = (*builtinSqrt)(nil)

func (call *builtinSqrt) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToFloat(arg)
	a := math.Sqrt(f.f)
	if !isFinite(a) {
		return nil, nil
	}

	return newEvalFloat(a), nil
}

func (call *builtinSqrt) compile(c *compiler) (ctype, error) {
	return c.compileFn_math1(call.Arguments[0], c.asm.Fn_SQRT, flagNullable)
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

type builtinRound struct {
	CallExpr
}

var _ IR = (*builtinRound)(nil)

func clampRounding(round int64) int64 {
	// Use some reasonable lower limit to avoid too slow
	// iteration for very large numbers. We need to be able
	// to at least truncate math.MaxFloat64 to 0 for the largest
	// possible values.
	if round < -decimal.ExponentLimit {
		round = -decimal.ExponentLimit
	} else if round > 30 {
		round = 30
	}
	return round
}

func roundSigned(v int64, round int64) int64 {
	if round >= 0 {
		return v
	}
	round = clampRounding(round)

	if v == 0 {
		return 0
	}
	for i := round; i < -1 && v != 0; i++ {
		v /= 10
	}

	if v == 0 {
		return 0
	}
	if v%10 <= -5 {
		v -= 10
	} else if v%10 >= 5 {
		v += 10
	}

	v /= 10
	for i := round; i < 0; i++ {
		v *= 10
	}
	return v
}

func roundUnsigned(v uint64, round int64) uint64 {
	if round >= 0 {
		return v
	}
	round = clampRounding(round)

	if v == 0 {
		return 0
	}
	for i := round; i < -1 && v != 0; i++ {
		v /= 10
	}

	if v == 0 {
		return 0
	}

	if v%10 >= 5 {
		v += 10
	}

	v /= 10
	for i := round; i < 0; i++ {
		v *= 10
	}
	return v
}

func (call *builtinRound) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	round := int64(0)
	if len(call.Arguments) > 1 {
		d, err := call.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}
		if d == nil {
			return nil, nil
		}

		switch d := d.(type) {
		case *evalUint64:
			round = int64(d.u)
			if d.u > math.MaxInt64 {
				round = math.MaxInt64
			}
		default:
			round = evalToInt64(d).i
		}
	}

	switch arg := arg.(type) {
	case *evalInt64:
		return newEvalInt64(roundSigned(arg.i, round)), nil
	case *evalUint64:
		return newEvalUint64(roundUnsigned(arg.u, round)), nil
	case *evalDecimal:
		if arg.dec.IsZero() {
			return arg, nil
		}

		if round == 0 {
			return newEvalDecimalWithPrec(arg.dec.Round(0), 0), nil
		}

		round = clampRounding(round)
		digit := int32(round)
		if digit < 0 {
			digit = 0
		}
		if digit > arg.length {
			digit = arg.length
		}
		rounded := arg.dec.Round(int32(round))
		if rounded.IsZero() {
			return newEvalDecimalWithPrec(decimal.Zero, 0), nil
		}
		return newEvalDecimalWithPrec(rounded, digit), nil
	case *evalFloat:
		if arg.f == 0.0 {
			return arg, nil
		}
		if round == 0 {
			return newEvalFloat(math.Round(arg.f)), nil
		}

		round = clampRounding(round)
		f := math.Pow(10, float64(round))
		if f == 0 {
			return newEvalFloat(0), nil
		}
		return newEvalFloat(math.Round(arg.f*f) / f), nil
	default:
		v, _ := evalToFloat(arg)
		if v.f == 0.0 {
			return v, nil
		}

		if round == 0 {
			return newEvalFloat(math.Round(v.f)), nil
		}

		round = clampRounding(round)
		f := math.Pow(10, float64(round))
		if f == 0 {
			return newEvalFloat(0), nil
		}
		return newEvalFloat(math.Round(v.f*f) / f), nil
	}
}

func (expr *builtinRound) compile(c *compiler) (ctype, error) {
	arg, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)
	var skip2 *jump

	if len(expr.Arguments) == 1 {
		switch arg.Type {
		case sqltypes.Int64:
			// No-op, already rounded
		case sqltypes.Uint64:
			// No-op, already rounded
		case sqltypes.Float64:
			c.asm.Fn_ROUND1_f()
		case sqltypes.Decimal:
			// We assume here the most common case here is that
			// the decimal fits into an integer.
			c.asm.Fn_ROUND1_d()
		default:
			c.asm.Convert_xf(1)
			c.asm.Fn_ROUND1_f()
		}
	} else {
		round, err := expr.Arguments[1].compile(c)
		if err != nil {
			return ctype{}, err
		}

		skip2 = c.compileNullCheck1r(round)

		switch round.Type {
		case sqltypes.Int64:
			// No-op, already correct type
		case sqltypes.Uint64:
			c.asm.Clamp_u(1, math.MaxInt64)
			c.asm.Convert_ui(1)
		default:
			c.asm.Convert_xi(1)
		}

		switch arg.Type {
		case sqltypes.Int64:
			c.asm.Fn_ROUND2_i()
		case sqltypes.Uint64:
			c.asm.Fn_ROUND2_u()
		case sqltypes.Float64:
			c.asm.Fn_ROUND2_f()
		case sqltypes.Decimal:
			// We assume here the most common case here is that
			// the decimal fits into an integer.
			c.asm.Fn_ROUND2_d()
		default:
			c.asm.Convert_xf(2)
			c.asm.Fn_ROUND2_f()
		}
	}

	c.asm.jumpDestination(skip1, skip2)
	return arg, nil
}

type builtinTruncate struct {
	CallExpr
}

var _ IR = (*builtinRound)(nil)

func truncateSigned(v int64, round int64) int64 {
	if round >= 0 {
		return v
	}
	if v == 0 {
		return 0
	}
	round = clampRounding(round)
	for i := round; i < 0 && v != 0; i++ {
		v /= 10
	}

	if v == 0 {
		return 0
	}

	for i := round; i < 0; i++ {
		v *= 10
	}
	return v
}

func truncateUnsigned(v uint64, round int64) uint64 {
	if round >= 0 {
		return v
	}
	if v == 0 {
		return 0
	}
	round = clampRounding(round)
	for i := round; i < 0 && v != 0; i++ {
		v /= 10
	}

	if v == 0 {
		return 0
	}

	for i := round; i < 0; i++ {
		v *= 10
	}
	return v
}

func (call *builtinTruncate) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	round := int64(0)
	if len(call.Arguments) > 1 {
		d, err := call.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}
		if d == nil {
			return nil, nil
		}

		switch d := d.(type) {
		case *evalUint64:
			round = int64(d.u)
			if d.u > math.MaxInt64 {
				round = math.MaxInt64
			}
		default:
			round = evalToInt64(d).i
		}
	}

	switch arg := arg.(type) {
	case *evalInt64:
		return newEvalInt64(truncateSigned(arg.i, round)), nil
	case *evalUint64:
		return newEvalUint64(truncateUnsigned(arg.u, round)), nil
	case *evalDecimal:
		if arg.dec.IsZero() {
			return arg, nil
		}
		round = clampRounding(round)
		digit := int32(round)
		if digit < 0 {
			digit = 0
		}
		if digit > arg.length {
			digit = arg.length
		}

		truncated := arg.dec.Truncate(int32(round))
		if truncated.IsZero() {
			return newEvalDecimalWithPrec(decimal.Zero, 0), nil
		}
		return newEvalDecimalWithPrec(truncated, digit), nil
	case *evalFloat:
		if arg.f == 0.0 {
			return arg, nil
		}
		if round == 0 {
			return newEvalFloat(math.Trunc(arg.f)), nil
		}

		round = clampRounding(round)
		f := math.Pow(10, float64(round))
		if f == 0 {
			return newEvalFloat(0), nil
		}
		return newEvalFloat(math.Trunc(arg.f*f) / f), nil
	default:
		v, _ := evalToFloat(arg)
		if v.f == 0.0 {
			return v, nil
		}
		if round == 0 {
			return newEvalFloat(math.Trunc(v.f)), nil
		}

		round = clampRounding(round)
		f := math.Pow(10, float64(round))
		if f == 0 {
			return newEvalFloat(0), nil
		}
		return newEvalFloat(math.Trunc(v.f*f) / f), nil
	}
}

func (expr *builtinTruncate) compile(c *compiler) (ctype, error) {
	arg, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)

	round, err := expr.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(round)

	switch round.Type {
	case sqltypes.Int64:
		// No-op, already correct type
	case sqltypes.Uint64:
		c.asm.Clamp_u(1, math.MaxInt64)
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}

	switch arg.Type {
	case sqltypes.Int64:
		c.asm.Fn_TRUNCATE_i()
	case sqltypes.Uint64:
		c.asm.Fn_TRUNCATE_u()
	case sqltypes.Float64:
		c.asm.Fn_TRUNCATE_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		c.asm.Fn_TRUNCATE_d()
	default:
		c.asm.Convert_xf(2)
		c.asm.Fn_TRUNCATE_f()
	}

	c.asm.jumpDestination(skip1, skip2)
	return arg, nil
}

type builtinCrc32 struct {
	CallExpr
}

var _ IR = (*builtinCrc32)(nil)

func (call *builtinCrc32) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b := evalToBinary(arg)
	hash := crc32.ChecksumIEEE(b.bytes)
	return newEvalUint64(uint64(hash)), nil
}

func (expr *builtinCrc32) compile(c *compiler) (ctype, error) {
	arg, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.Binary, nil)
	}

	c.asm.Fn_CRC32()
	c.asm.jumpDestination(skip)
	return arg, nil
}

type builtinConv struct {
	CallExpr
	collate collations.ID
}

var _ IR = (*builtinConv)(nil)

func upcaseASCII(b []byte) []byte {
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return b
}

func (call *builtinConv) eval(env *ExpressionEnv) (eval, error) {
	n, err := call.Arguments[0].eval(env)
	if err != nil {
		return nil, err
	}
	from, err := call.Arguments[1].eval(env)
	if err != nil {
		return nil, err
	}
	to, err := call.Arguments[2].eval(env)
	if err != nil {
		return nil, err
	}

	if n == nil || from == nil || to == nil {
		return nil, nil
	}

	fromBase := evalToInt64(from).i
	toBase := evalToInt64(to).i

	if fromBase < -36 || (fromBase > -2 && fromBase < 2) || fromBase > 36 {
		return nil, nil
	}
	if fromBase < 0 {
		fromBase = -fromBase
	}

	if toBase < -36 || (toBase > -2 && toBase < 2) || toBase > 36 {
		return nil, nil
	}

	var u uint64
	if b, ok := n.(*evalBytes); ok && b.isHexOrBitLiteral() {
		nh, _ := b.toNumericHex()
		u = nh.u
	} else {
		nStr := evalToBinary(n)
		i, err := fastparse.ParseInt64(nStr.string(), int(fromBase))
		u = uint64(i)
		if errors.Is(err, fastparse.ErrOverflow) {
			u, _ = fastparse.ParseUint64WithNeg(nStr.string(), int(fromBase))
		}
	}

	var out []byte
	if toBase < 0 {
		out = strconv.AppendInt(out, int64(u), -int(toBase))
	} else {
		out = strconv.AppendUint(out, u, int(toBase))
	}
	return newEvalText(upcaseASCII(out), typedCoercionCollation(sqltypes.VarChar, call.collate)), nil
}

func (expr *builtinConv) compile(c *compiler) (ctype, error) {
	n, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	from, err := expr.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	to, err := expr.Arguments[2].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck3(n, from, to)

	_ = c.compileToInt64(from, 2)
	_ = c.compileToInt64(to, 1)

	t := sqltypes.VarChar
	if n.Type == sqltypes.Blob || n.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case n.isTextual():
	default:
		c.asm.Convert_xb(3, t, nil)
	}

	if n.isHexOrBitLiteral() {
		c.asm.Fn_CONV_hu(3, 2)
	} else {
		c.asm.Fn_CONV_bu(3, 2)
	}

	col := typedCoercionCollation(t, expr.collate)
	c.asm.Fn_CONV_uc(t, col)
	c.asm.jumpDestination(skip)

	return ctype{Type: t, Col: col, Flag: flagNullable}, nil
}
