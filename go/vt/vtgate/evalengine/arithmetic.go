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
	"strings"

	"golang.org/x/exp/constraints"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
)

func dataOutOfRangeError[N1, N2 constraints.Integer | constraints.Float](v1 N1, v2 N2, typ, sign string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in '(%v %s %v)'", typ, v1, sign, v2)
}

func dataOutOfRangeErrorDecimal(v1 decimal.Decimal, v2 decimal.Decimal, typ, sign string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in '(%v %s %v)'", typ, v1.String(), sign, v2.String())
}

func addNumericWithError(left, right eval) (eval, error) {
	v1, v2 := makeNumericAndPrioritize(left, right)
	switch v1 := v1.(type) {
	case *evalInt64:
		return mathAdd_ii(v1.i, v2.(*evalInt64).i)
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathAdd_ui(v1.u, v2.i)
		case *evalUint64:
			return mathAdd_uu(v1.u, v2.u)
		}
	case *evalDecimal:
		return mathAdd_dx(v1, v2), nil
	case *evalFloat:
		return mathAdd_fx(v1.f, v2)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", evalToSQLValue(v1), evalToSQLValue(v2))
}

func subtractNumericWithError(left, right eval) (eval, error) {
	v1 := evalToNumeric(left)
	v2 := evalToNumeric(right)
	switch v1 := v1.(type) {
	case *evalInt64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathSub_ii(v1.i, v2.i)
		case *evalUint64:
			return mathSub_iu(v1.i, v2.u)
		case *evalFloat:
			return mathSub_xf(v1, v2.f)
		case *evalDecimal:
			return mathSub_xd(v1, v2), nil
		}
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathSub_ui(v1.u, v2.i)
		case *evalUint64:
			return mathSub_uu(v1.u, v2.u)
		case *evalFloat:
			return mathSub_xf(v1, v2.f)
		case *evalDecimal:
			return mathSub_xd(v1, v2), nil
		}
	case *evalFloat:
		return mathSub_fx(v1.f, v2)
	case *evalDecimal:
		switch v2 := v2.(type) {
		case *evalFloat:
			return mathSub_xf(v1, v2.f)
		default:
			return mathSub_dx(v1, v2), nil
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", evalToSQLValue(v1), evalToSQLValue(v2))
}

func multiplyNumericWithError(left, right eval) (eval, error) {
	v1, v2 := makeNumericAndPrioritize(left, right)
	switch v1 := v1.(type) {
	case *evalInt64:
		return mathMul_ii(v1.i, v2.(*evalInt64).i)
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathMul_ui(v1.u, v2.i)
		case *evalUint64:
			return mathMul_uu(v1.u, v2.u)
		}
	case *evalFloat:
		return mathMul_fx(v1.f, v2)
	case *evalDecimal:
		return mathMul_dx(v1, v2), nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", evalToSQLValue(v1), evalToSQLValue(v2))
}

func divideNumericWithError(left, right eval, precise bool) (eval, error) {
	v1 := evalToNumeric(left)
	v2 := evalToNumeric(right)
	if v1, ok := v1.(*evalFloat); ok {
		return mathDiv_fx(v1.f, v2)
	}
	if v2, ok := v2.(*evalFloat); ok {
		v1f, ok := v1.toFloat()
		if !ok {
			return nil, errDecimalOutOfRange
		}
		return mathDiv_fx(v1f.f, v2)
	}
	return mathDiv_xx(v1, v2, divPrecisionIncrement)
}

func integerDivideNumericWithError(left, right eval, precise bool) (eval, error) {
	v1 := evalToNumeric(left)
	v2 := evalToNumeric(right)
	switch v1 := v1.(type) {
	case *evalInt64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathIntDiv_ii(v1, v2)
		case *evalUint64:
			return mathIntDiv_iu(v1, v2)
		case *evalFloat:
			return mathIntDiv_di(v1.toDecimal(0, 0), v2.toDecimal(0, 0))
		case *evalDecimal:
			return mathIntDiv_di(v1.toDecimal(0, 0), v2)
		}
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathIntDiv_ui(v1, v2)
		case *evalUint64:
			return mathIntDiv_uu(v1, v2)
		case *evalFloat:
			return mathIntDiv_du(v1.toDecimal(0, 0), v2.toDecimal(0, 0))
		case *evalDecimal:
			return mathIntDiv_du(v1.toDecimal(0, 0), v2)
		}
	case *evalFloat:
		switch v2 := v2.(type) {
		case *evalUint64:
			return mathIntDiv_du(v1.toDecimal(0, 0), v2.toDecimal(0, 0))
		default:
			return mathIntDiv_di(v1.toDecimal(0, 0), v2.toDecimal(0, 0))
		}
	case *evalDecimal:
		switch v2 := v2.(type) {
		case *evalUint64:
			return mathIntDiv_du(v1, v2.toDecimal(0, 0))
		default:
			return mathIntDiv_di(v1, v2.toDecimal(0, 0))
		}
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", evalToSQLValue(v1), evalToSQLValue(v2))
}

func modNumericWithError(left, right eval, precise bool) (eval, error) {
	v1 := evalToNumeric(left)
	v2 := evalToNumeric(right)

	switch v1 := v1.(type) {
	case *evalInt64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathMod_ii(v1, v2)
		case *evalUint64:
			return mathMod_iu(v1, v2)
		case *evalFloat:
			v1f, ok := v1.toFloat()
			if !ok {
				return nil, errDecimalOutOfRange
			}
			return mathMod_ff(v1f, v2)
		case *evalDecimal:
			return mathMod_dd(v1.toDecimal(0, 0), v2)
		}
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return mathMod_ui(v1, v2)
		case *evalUint64:
			return mathMod_uu(v1, v2)
		case *evalFloat:
			v1f, ok := v1.toFloat()
			if !ok {
				return nil, errDecimalOutOfRange
			}
			return mathMod_ff(v1f, v2)
		case *evalDecimal:
			return mathMod_dd(v1.toDecimal(0, 0), v2)
		}
	case *evalDecimal:
		switch v2 := v2.(type) {
		case *evalFloat:
			v1f, ok := v1.toFloat()
			if !ok {
				return nil, errDecimalOutOfRange
			}
			return mathMod_ff(v1f, v2)
		default:
			return mathMod_dd(v1, v2.toDecimal(0, 0))
		}
	case *evalFloat:
		v2f, ok := v2.toFloat()
		if !ok {
			return nil, errDecimalOutOfRange
		}
		return mathMod_ff(v1, v2f)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", evalToSQLValue(v1), evalToSQLValue(v2))
}

// makeNumericAndPrioritize reorders the input parameters
// to be Float64, Decimal, Uint64, Int64.
func makeNumericAndPrioritize(left, right eval) (evalNumeric, evalNumeric) {
	i1 := evalToNumeric(left)
	i2 := evalToNumeric(right)
	switch i1.SQLType() {
	case sqltypes.Int64:
		if i2.SQLType() == sqltypes.Uint64 || i2.SQLType() == sqltypes.Float64 || i2.SQLType() == sqltypes.Decimal {
			return i2, i1
		}
	case sqltypes.Uint64:
		if i2.SQLType() == sqltypes.Float64 || i2.SQLType() == sqltypes.Decimal {
			return i2, i1
		}
	case sqltypes.Decimal:
		if i2.SQLType() == sqltypes.Float64 {
			return i2, i1
		}
	}
	return i1, i2
}

func mathAdd_ii(v1, v2 int64) (eval, error) {
	result, err := mathAdd_ii0(v1, v2)
	return newEvalInt64(result), err
}

func mathAdd_ii0(v1, v2 int64) (int64, error) {
	result := v1 + v2
	if (result > v1) != (v2 > 0) {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT", "+")
	}
	return result, nil
}

func mathAdd_ui(v1 uint64, v2 int64) (*evalUint64, error) {
	result, err := mathAdd_ui0(v1, v2)
	return newEvalUint64(result), err
}

func mathAdd_ui0(v1 uint64, v2 int64) (uint64, error) {
	result := v1 + uint64(v2)
	if v2 < 0 && v1 < uint64(-v2) || v2 > 0 && (result < v1 || result < uint64(v2)) {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "+")
	}
	return result, nil
}

func mathAdd_uu(v1, v2 uint64) (*evalUint64, error) {
	result, err := mathAdd_uu0(v1, v2)
	return newEvalUint64(result), err
}

func mathAdd_uu0(v1, v2 uint64) (uint64, error) {
	result := v1 + v2
	if result < v1 || result < v2 {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "+")
	}
	return result, nil
}

var errDecimalOutOfRange = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")

func mathAdd_fx(v1 float64, v2 evalNumeric) (*evalFloat, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return mathAdd_ff(v1, v2f.f), nil
}

func mathAdd_ff(v1, v2 float64) *evalFloat {
	return newEvalFloat(v1 + v2)
}

func mathAdd_dx(v1 *evalDecimal, v2 evalNumeric) *evalDecimal {
	return mathAdd_dd(v1, v2.toDecimal(0, 0))
}

func mathAdd_dd(v1, v2 *evalDecimal) *evalDecimal {
	return newEvalDecimalWithPrec(v1.dec.Add(v2.dec), maxprec(v1.length, v2.length))
}

func mathAdd_dd0(v1, v2 *evalDecimal) {
	v1.dec = v1.dec.Add(v2.dec)
	v1.length = maxprec(v1.length, v2.length)
}

func mathSub_ii(v1, v2 int64) (*evalInt64, error) {
	result, err := mathSub_ii0(v1, v2)
	return newEvalInt64(result), err
}

func mathSub_ii0(v1, v2 int64) (int64, error) {
	result := v1 - v2
	if (result < v1) != (v2 > 0) {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT", "-")
	}
	return result, nil
}

func mathSub_iu(v1 int64, v2 uint64) (*evalUint64, error) {
	result, err := mathSub_iu0(v1, v2)
	return newEvalUint64(result), err
}

func mathSub_iu0(v1 int64, v2 uint64) (uint64, error) {
	if v1 < 0 {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	return mathSub_uu0(uint64(v1), v2)
}

func mathSub_ui(v1 uint64, v2 int64) (*evalUint64, error) {
	result, err := mathSub_ui0(v1, v2)
	return newEvalUint64(result), err
}

func mathSub_ui0(v1 uint64, v2 int64) (uint64, error) {
	if v2 > 0 && v1 < uint64(v2) {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	// uint - (- int) = uint + int
	if v2 < 0 {
		return mathAdd_uu0(v1, uint64(-v2))
	}
	return mathSub_uu0(v1, uint64(v2))
}

func mathSub_uu(v1, v2 uint64) (*evalUint64, error) {
	result, err := mathSub_uu0(v1, v2)
	return newEvalUint64(result), err
}

func mathSub_uu0(v1, v2 uint64) (uint64, error) {
	result := v1 - v2
	if v2 > v1 {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	return result, nil
}

func mathSub_fx(v1 float64, v2 evalNumeric) (*evalFloat, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return mathSub_ff(v1, v2f.f), nil
}

func mathSub_xf(v1 evalNumeric, v2 float64) (*evalFloat, error) {
	v1f, ok := v1.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return mathSub_ff(v1f.f, v2), nil
}

func mathSub_ff(v1, v2 float64) *evalFloat {
	return newEvalFloat(v1 - v2)
}

func mathSub_dx(v1 *evalDecimal, v2 evalNumeric) *evalDecimal {
	return mathSub_dd(v1, v2.toDecimal(0, 0))
}

func mathSub_xd(v1 evalNumeric, v2 *evalDecimal) *evalDecimal {
	return mathSub_dd(v1.toDecimal(0, 0), v2)
}

func mathSub_dd(v1, v2 *evalDecimal) *evalDecimal {
	return newEvalDecimalWithPrec(v1.dec.Sub(v2.dec), maxprec(v1.length, v2.length))
}

func mathSub_dd0(v1, v2 *evalDecimal) {
	v1.dec = v1.dec.Sub(v2.dec)
	v1.length = maxprec(v1.length, v2.length)
}

func mathMul_ii(v1, v2 int64) (*evalInt64, error) {
	result, err := mathMul_ii0(v1, v2)
	return newEvalInt64(result), err
}

func mathMul_ii0(v1, v2 int64) (int64, error) {
	result := v1 * v2
	if v1 != 0 && result/v1 != v2 {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT", "*")
	}
	return result, nil
}

func mathMul_ui(v1 uint64, v2 int64) (*evalUint64, error) {
	result, err := mathMul_ui0(v1, v2)
	return newEvalUint64(result), err
}

func mathMul_ui0(v1 uint64, v2 int64) (uint64, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}
	if v2 < 0 {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "*")
	}
	return mathMul_uu0(v1, uint64(v2))
}

func mathMul_uu(v1, v2 uint64) (*evalUint64, error) {
	result, err := mathMul_uu0(v1, v2)
	return newEvalUint64(result), err
}

func mathMul_uu0(v1, v2 uint64) (uint64, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}
	result := v1 * v2
	if result < v2 || result < v1 {
		return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "*")
	}
	return result, nil
}

func mathMul_fx(v1 float64, v2 evalNumeric) (eval, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return mathMul_ff(v1, v2f.f), nil
}

func mathMul_ff(v1, v2 float64) *evalFloat {
	return newEvalFloat(v1 * v2)
}

func maxprec(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func mathMul_dx(v1 *evalDecimal, v2 evalNumeric) *evalDecimal {
	return mathMul_dd(v1, v2.toDecimal(0, 0))
}

func mathMul_dd(v1, v2 *evalDecimal) *evalDecimal {
	return newEvalDecimalWithPrec(v1.dec.Mul(v2.dec), v1.length+v2.length)
}

func mathMul_dd0(v1, v2 *evalDecimal) {
	v1.dec = v1.dec.Mul(v2.dec)
	v1.length = v1.length + v2.length
}

const divPrecisionIncrement = 4

func mathDiv_xx(v1, v2 evalNumeric, incrPrecision int32) (eval, error) {
	return mathDiv_dd(v1.toDecimal(0, 0), v2.toDecimal(0, 0), incrPrecision)
}

func mathDiv_dd(v1, v2 *evalDecimal, incrPrecision int32) (eval, error) {
	if v2.dec.IsZero() {
		return nil, nil
	}
	return newEvalDecimalWithPrec(v1.dec.Div(v2.dec, incrPrecision), v1.length+incrPrecision), nil
}

func mathDiv_dd0(v1, v2 *evalDecimal, incrPrecision int32) {
	v1.dec = v1.dec.Div(v2.dec, incrPrecision)
	v1.length = v1.length + incrPrecision
}

func mathDiv_fx(v1 float64, v2 evalNumeric) (eval, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return mathDiv_ff(v1, v2f.f)
}

func mathDiv_ff(v1, v2 float64) (eval, error) {
	if v2 == 0.0 {
		return nil, nil
	}
	result, err := mathDiv_ff0(v1, v2)
	return newEvalFloat(result), err
}

func mathDiv_ff0(v1, v2 float64) (float64, error) {
	result := v1 / v2

	if math.IsInf(result, 1) || math.IsInf(result, -1) {
		return 0, dataOutOfRangeError(v1, v2, "DOUBLE", "/")
	}
	return result, nil
}

func mathIntDiv_ii(v1, v2 *evalInt64) (eval, error) {
	if v2.i == 0 {
		return nil, nil
	}
	result := v1.i / v2.i
	return newEvalInt64(result), nil
}

func mathIntDiv_iu(v1 *evalInt64, v2 *evalUint64) (eval, error) {
	if v2.u == 0 {
		return nil, nil
	}
	result, err := mathIntDiv_iu0(v1.i, v2.u)
	return newEvalUint64(result), err
}

func mathIntDiv_iu0(v1 int64, v2 uint64) (uint64, error) {
	if v1 < 0 {
		if v2 >= math.MaxInt64 {
			// We know here that v2 is always so large the result
			// must be 0.
			return 0, nil
		}
		result := v1 / int64(v2)
		if result < 0 {
			return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "DIV")
		}
		return uint64(result), nil

	}
	return uint64(v1) / v2, nil
}

func mathIntDiv_ui(v1 *evalUint64, v2 *evalInt64) (eval, error) {
	if v2.i == 0 {
		return nil, nil
	}
	result, err := mathIntDiv_ui0(v1.u, v2.i)
	return newEvalUint64(result), err
}

func mathIntDiv_ui0(v1 uint64, v2 int64) (uint64, error) {
	if v2 < 0 {
		if v1 >= math.MaxInt64 {
			// We know that v1 is always large here and with v2, the result
			// must be at least -1 so we can't store this in the available range.
			return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "DIV")
		}
		// Safe to cast since we know it fits in int64 when we get here.
		result := int64(v1) / v2
		if result < 0 {
			return 0, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "DIV")
		}
		return uint64(result), nil
	}
	return v1 / uint64(v2), nil
}

func mathIntDiv_uu(v1, v2 *evalUint64) (eval, error) {
	if v2.u == 0 {
		return nil, nil
	}
	return newEvalUint64(v1.u / v2.u), nil
}

func mathIntDiv_di(v1, v2 *evalDecimal) (eval, error) {
	if v2.dec.IsZero() {
		return nil, nil
	}
	result, err := mathIntDiv_di0(v1, v2)
	return newEvalInt64(result), err
}

func mathIntDiv_di0(v1, v2 *evalDecimal) (int64, error) {
	div, _ := v1.dec.QuoRem(v2.dec, 0)
	result, ok := div.Int64()
	if !ok {
		return 0, dataOutOfRangeErrorDecimal(v1.dec, v2.dec, "BIGINT", "DIV")
	}
	return result, nil
}

func mathIntDiv_du(v1, v2 *evalDecimal) (eval, error) {
	if v2.dec.IsZero() {
		return nil, nil
	}
	result, err := mathIntDiv_du0(v1, v2)
	return newEvalUint64(result), err
}

func mathIntDiv_du0(v1, v2 *evalDecimal) (uint64, error) {
	div, _ := v1.dec.QuoRem(v2.dec, 0)
	result, ok := div.Uint64()
	if !ok {
		return 0, dataOutOfRangeErrorDecimal(v1.dec, v2.dec, "BIGINT UNSIGNED", "DIV")
	}
	return result, nil
}

func mathMod_ii(v1, v2 *evalInt64) (eval, error) {
	if v2.i == 0 {
		return nil, nil
	}
	return newEvalInt64(v1.i % v2.i), nil
}

func mathMod_iu(v1 *evalInt64, v2 *evalUint64) (eval, error) {
	if v2.u == 0 {
		return nil, nil
	}
	return newEvalInt64(mathMod_iu0(v1.i, v2.u)), nil
}

func mathMod_iu0(v1 int64, v2 uint64) int64 {
	if v1 == math.MinInt64 && v2 == math.MaxInt64+1 {
		return 0
	}
	if v2 > math.MaxInt64 {
		return v1
	}
	return v1 % int64(v2)
}

func mathMod_ui(v1 *evalUint64, v2 *evalInt64) (eval, error) {
	if v2.i == 0 {
		return nil, nil
	}
	result, err := mathMod_ui0(v1.u, v2.i)
	return newEvalUint64(result), err
}

func mathMod_ui0(v1 uint64, v2 int64) (uint64, error) {
	if v2 < 0 {
		return v1 % uint64(-v2), nil
	}
	return v1 % uint64(v2), nil
}

func mathMod_uu(v1, v2 *evalUint64) (eval, error) {
	if v2.u == 0 {
		return nil, nil
	}
	return newEvalUint64(v1.u % v2.u), nil
}

func mathMod_ff(v1, v2 *evalFloat) (eval, error) {
	if v2.f == 0.0 {
		return nil, nil
	}
	return newEvalFloat(math.Mod(v1.f, v2.f)), nil
}

func mathMod_dd(v1, v2 *evalDecimal) (eval, error) {
	if v2.dec.IsZero() {
		return nil, nil
	}

	dec, prec := mathMod_dd0(v1, v2)
	return newEvalDecimalWithPrec(dec, prec), nil
}

func mathMod_dd0(v1, v2 *evalDecimal) (decimal.Decimal, int32) {
	length := v1.length
	if v2.length > length {
		length = v2.length
	}
	_, rem := v1.dec.QuoRem(v2.dec, 0)
	return rem, length
}

func parseStringToFloat(str string) float64 {
	str = strings.TrimSpace(str)

	// We only care to parse as many of the initial float characters of the
	// string as possible. This functionality is implemented in the `strconv` package
	// of the standard library, but not exposed, so we hook into it.
	val, _, err := hack.ParseFloatPrefix(str, 64)
	if err != nil {
		return 0.0
	}
	return val
}
