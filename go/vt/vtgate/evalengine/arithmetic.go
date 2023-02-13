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
	"strings"

	"golang.org/x/exp/constraints"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func dataOutOfRangeError[N1, N2 constraints.Integer | constraints.Float](v1 N1, v2 N2, typ, sign string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in '(%v %s %v)'", typ, v1, sign, v2)
}

func addNumericWithError(left, right eval) (eval, error) {
	v1, v2 := makeNumericAndPrioritize(left, right)
	switch v1 := v1.(type) {
	case *evalInt64:
		return intPlusIntWithError(v1.i, v2.(*evalInt64).i)
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return uintPlusIntWithError(v1.u, v2.i)
		case *evalUint64:
			return uintPlusUintWithError(v1.u, v2.u)
		}
	case *evalDecimal:
		return decimalPlusAny(v1, v2)
	case *evalFloat:
		return floatPlusAny(v1.f, v2)
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
			return intMinusIntWithError(v1.i, v2.i)
		case *evalUint64:
			return intMinusUintWithError(v1.i, v2.u)
		case *evalFloat:
			return anyMinusFloat(v1, v2.f)
		case *evalDecimal:
			return anyMinusDecimal(v1, v2)
		}
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return uintMinusIntWithError(v1.u, v2.i)
		case *evalUint64:
			return uintMinusUintWithError(v1.u, v2.u)
		case *evalFloat:
			return anyMinusFloat(v1, v2.f)
		case *evalDecimal:
			return anyMinusDecimal(v1, v2)
		}
	case *evalFloat:
		return floatMinusAny(v1.f, v2)
	case *evalDecimal:
		switch v2 := v2.(type) {
		case *evalFloat:
			return anyMinusFloat(v1, v2.f)
		default:
			return decimalMinusAny(v1, v2)
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", evalToSQLValue(v1), evalToSQLValue(v2))
}

func multiplyNumericWithError(left, right eval) (eval, error) {
	v1, v2 := makeNumericAndPrioritize(left, right)
	switch v1 := v1.(type) {
	case *evalInt64:
		return intTimesIntWithError(v1.i, v2.(*evalInt64).i)
	case *evalUint64:
		switch v2 := v2.(type) {
		case *evalInt64:
			return uintTimesIntWithError(v1.u, v2.i)
		case *evalUint64:
			return uintTimesUintWithError(v1.u, v2.u)
		}
	case *evalFloat:
		return floatTimesAny(v1.f, v2)
	case *evalDecimal:
		return decimalTimesAny(v1, v2)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", evalToSQLValue(v1), evalToSQLValue(v2))
}

func divideNumericWithError(left, right eval, precise bool) (eval, error) {
	v1 := evalToNumeric(left)
	v2 := evalToNumeric(right)
	if !precise && v1.sqlType() != sqltypes.Decimal && v2.sqlType() != sqltypes.Decimal {
		switch v1 := v1.(type) {
		case *evalInt64:
			return floatDivideAnyWithError(float64(v1.i), v2)
		case *evalUint64:
			return floatDivideAnyWithError(float64(v1.u), v2)
		case *evalFloat:
			return floatDivideAnyWithError(v1.f, v2)
		}
	}
	if v1, ok := v1.(*evalFloat); ok {
		return floatDivideAnyWithError(v1.f, v2)
	}
	if v2, ok := v2.(*evalFloat); ok {
		v1f, ok := v1.toFloat()
		if !ok {
			return nil, errDecimalOutOfRange
		}
		return floatDivideAnyWithError(v1f.f, v2)
	}
	return decimalDivide(v1, v2, divPrecisionIncrement)
}

// makeNumericAndPrioritize reorders the input parameters
// to be Float64, Decimal, Uint64, Int64.
func makeNumericAndPrioritize(left, right eval) (evalNumeric, evalNumeric) {
	i1 := evalToNumeric(left)
	i2 := evalToNumeric(right)
	switch i1.sqlType() {
	case sqltypes.Int64:
		if i2.sqlType() == sqltypes.Uint64 || i2.sqlType() == sqltypes.Float64 || i2.sqlType() == sqltypes.Decimal {
			return i2, i1
		}
	case sqltypes.Uint64:
		if i2.sqlType() == sqltypes.Float64 || i2.sqlType() == sqltypes.Decimal {
			return i2, i1
		}
	case sqltypes.Decimal:
		if i2.sqlType() == sqltypes.Float64 {
			return i2, i1
		}
	}
	return i1, i2
}

func intPlusIntWithError(v1, v2 int64) (eval, error) {
	result := v1 + v2
	if (result > v1) != (v2 > 0) {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT", "+")
	}
	return &evalInt64{result}, nil
}

func intMinusIntWithError(v1, v2 int64) (eval, error) {
	result := v1 - v2
	if (result < v1) != (v2 > 0) {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT", "-")
	}
	return &evalInt64{result}, nil
}

func intTimesIntWithError(v1, v2 int64) (eval, error) {
	result := v1 * v2
	if v1 != 0 && result/v1 != v2 {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT", "*")
	}
	return &evalInt64{result}, nil

}

func intMinusUintWithError(v1 int64, v2 uint64) (eval, error) {
	if v1 < 0 || v1 < int64(v2) {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	return uintMinusUintWithError(uint64(v1), v2)
}

func uintPlusIntWithError(v1 uint64, v2 int64) (eval, error) {
	result := v1 + uint64(v2)
	if v2 < 0 && v1 < uint64(-v2) || v2 > 0 && (result < v1 || result < uint64(v2)) {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "+")
	}
	// convert to int -> uint is because for numeric operators (such as + or -)
	// where one of the operands is an unsigned integer, the result is unsigned by default.
	return newEvalUint64(result), nil
}

func uintMinusIntWithError(v1 uint64, v2 int64) (eval, error) {
	if int64(v1) < v2 && v2 > 0 {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	// uint - (- int) = uint + int
	if v2 < 0 {
		return uintPlusIntWithError(v1, -v2)
	}
	return uintMinusUintWithError(v1, uint64(v2))
}

func uintTimesIntWithError(v1 uint64, v2 int64) (eval, error) {
	if v1 == 0 || v2 == 0 {
		return newEvalUint64(0), nil
	}
	if v2 < 0 || int64(v1) < 0 {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "*")
	}
	return uintTimesUintWithError(v1, uint64(v2))
}

func uintPlusUintWithError(v1, v2 uint64) (eval, error) {
	result := v1 + v2
	if result < v1 || result < v2 {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "+")
	}
	return newEvalUint64(result), nil
}

func uintMinusUintWithError(v1, v2 uint64) (eval, error) {
	result := v1 - v2
	if v2 > v1 {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	return newEvalUint64(result), nil
}

func uintTimesUintWithError(v1, v2 uint64) (eval, error) {
	if v1 == 0 || v2 == 0 {
		return newEvalUint64(0), nil
	}
	result := v1 * v2
	if result < v2 || result < v1 {
		return nil, dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "*")
	}
	return newEvalUint64(result), nil
}

var errDecimalOutOfRange = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")

func floatPlusAny(v1 float64, v2 evalNumeric) (eval, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return &evalFloat{v1 + v2f.f}, nil
}

func floatMinusAny(v1 float64, v2 evalNumeric) (eval, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return &evalFloat{v1 - v2f.f}, nil
}

func floatTimesAny(v1 float64, v2 evalNumeric) (eval, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return &evalFloat{v1 * v2f.f}, nil
}

func maxprec(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func decimalPlusAny(v1 *evalDecimal, v2 evalNumeric) (eval, error) {
	v2d := v2.toDecimal(0, 0)
	return &evalDecimal{
		dec:    v1.dec.Add(v2d.dec),
		length: maxprec(v1.length, v2d.length),
	}, nil
}

func decimalMinusAny(v1 *evalDecimal, v2 evalNumeric) (eval, error) {
	v2d := v2.toDecimal(0, 0)
	return &evalDecimal{
		dec:    v1.dec.Sub(v2d.dec),
		length: maxprec(v1.length, v2d.length),
	}, nil
}

func anyMinusDecimal(v1 evalNumeric, v2 *evalDecimal) (eval, error) {
	v1d := v1.toDecimal(0, 0)
	return &evalDecimal{
		dec:    v1d.dec.Sub(v2.dec),
		length: maxprec(v1d.length, v2.length),
	}, nil
}

func decimalTimesAny(v1 *evalDecimal, v2 evalNumeric) (eval, error) {
	v2d := v2.toDecimal(0, 0)
	return &evalDecimal{
		dec:    v1.dec.Mul(v2d.dec),
		length: maxprec(v1.length, v2d.length),
	}, nil
}

const divPrecisionIncrement = 4

func decimalDivide(v1, v2 evalNumeric, incrPrecision int32) (eval, error) {
	v1d := v1.toDecimal(0, 0)
	v2d := v2.toDecimal(0, 0)
	if v2d.dec.IsZero() {
		return nil, nil
	}
	return &evalDecimal{
		dec:    v1d.dec.Div(v2d.dec, incrPrecision),
		length: v1d.length + incrPrecision,
	}, nil
}

func floatDivideAnyWithError(v1 float64, v2 evalNumeric) (eval, error) {
	v2f, ok := v2.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	if v2f.f == 0.0 {
		return nil, nil
	}

	result := v1 / v2f.f
	divisorLessThanOne := v2f.f < 1
	resultMismatch := v2f.f*result != v1

	if divisorLessThanOne && resultMismatch {
		return nil, dataOutOfRangeError(v1, v2f.f, "BIGINT", "/")
	}
	return &evalFloat{result}, nil
}

func anyMinusFloat(v1 evalNumeric, v2 float64) (eval, error) {
	v1f, ok := v1.toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}
	return &evalFloat{v1f.f - v2}, nil
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
