/*
Copyright 2019 The Vitess Authors.

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
	"bytes"
	"strconv"
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
)

// evalengine represents a numeric value extracted from
// a Value, used for arithmetic operations.
var zeroBytes = []byte("0")

func dataOutOfRangeError(v1, v2 any, typ, sign string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in '(%v %s %v)'", typ, v1, sign, v2)
}

// FormatFloat formats a float64 as a byte string in a similar way to what MySQL does
func FormatFloat(typ sqltypes.Type, f float64) []byte {
	return AppendFloat(nil, typ, f)
}

func AppendFloat(buf []byte, typ sqltypes.Type, f float64) []byte {
	format := byte('g')
	if typ == sqltypes.Decimal {
		format = 'f'
	}

	// the float printer in MySQL does not add a positive sign before
	// the exponent for positive exponents, but the Golang printer does
	// do that, and there's no way to customize it, so we must strip the
	// redundant positive sign manually
	// e.g. 1.234E+56789 -> 1.234E56789
	fstr := strconv.AppendFloat(buf, f, format, -1, 64)
	if idx := bytes.IndexByte(fstr, 'e'); idx >= 0 {
		if fstr[idx+1] == '+' {
			fstr = append(fstr[:idx+1], fstr[idx+2:]...)
		}
	}

	return fstr
}

// Add adds two values together
// if v1 or v2 is null, then it returns null
func Add(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	var lv1, lv2, out EvalResult
	if err := lv1.setValue(v1, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}

	err := addNumericWithError(&lv1, &lv2, &out)
	if err != nil {
		return sqltypes.NULL, err
	}
	return out.Value(), nil
}

// Subtract takes two values and subtracts them
func Subtract(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	var lv1, lv2, out EvalResult
	if err := lv1.setValue(v1, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}

	err := subtractNumericWithError(&lv1, &lv2, &out)
	if err != nil {
		return sqltypes.NULL, err
	}

	return out.Value(), nil
}

// Multiply takes two values and multiplies it together
func Multiply(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	var lv1, lv2, out EvalResult
	if err := lv1.setValue(v1, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}

	err := multiplyNumericWithError(&lv1, &lv2, &out)
	if err != nil {
		return sqltypes.NULL, err
	}

	return out.Value(), nil
}

// Divide (Float) for MySQL. Replicates behavior of "/" operator
func Divide(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	var lv1, lv2, out EvalResult
	if err := lv1.setValue(v1, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}

	err := divideNumericWithError(&lv1, &lv2, true, &out)
	if err != nil {
		return sqltypes.NULL, err
	}

	return out.Value(), nil
}

// NullSafeAdd adds two Values in a null-safe manner. A null value
// is treated as 0. If both values are null, then a null is returned.
// If both values are not null, a numeric value is built
// from each input: Signed->int64, Unsigned->uint64, Float->float64.
// Otherwise the 'best type fit' is chosen for the number: int64 or float64.
// OpAddition is performed by upgrading types as needed, or in case
// of overflow: int64->uint64, int64->float64, uint64->float64.
// Unsigned ints can only be added to positive ints. After the
// addition, if one of the input types was Decimal, then
// a Decimal is built. Otherwise, the final type of the
// result is preserved.
func NullSafeAdd(v1, v2 sqltypes.Value, resultType sqltypes.Type) (sqltypes.Value, error) {
	if v1.IsNull() {
		v1 = sqltypes.MakeTrusted(resultType, zeroBytes)
	}
	if v2.IsNull() {
		v2 = sqltypes.MakeTrusted(resultType, zeroBytes)
	}

	var lv1, lv2, out EvalResult
	if err := lv1.setValue(v1, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2, collationNumeric); err != nil {
		return sqltypes.NULL, err
	}

	err := addNumericWithError(&lv1, &lv2, &out)
	if err != nil {
		return sqltypes.NULL, err
	}
	return out.toSQLValue(resultType), nil
}

func addNumericWithError(v1, v2, out *EvalResult) error {
	v1, v2 = makeNumericAndPrioritize(v1, v2)
	switch v1.typeof() {
	case sqltypes.Int64:
		return intPlusIntWithError(v1.uint64(), v2.uint64(), out)
	case sqltypes.Uint64:
		switch v2.typeof() {
		case sqltypes.Int64:
			return uintPlusIntWithError(v1.uint64(), v2.uint64(), out)
		case sqltypes.Uint64:
			return uintPlusUintWithError(v1.uint64(), v2.uint64(), out)
		}
	case sqltypes.Decimal:
		decimalPlusAny(v1.decimal(), v1.length_, v2, out)
		return nil
	case sqltypes.Float64:
		return floatPlusAny(v1.float64(), v2, out)
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", v1.value().String(), v2.value().String())
}

func subtractNumericWithError(v1, v2, out *EvalResult) error {
	v1.makeNumeric()
	v2.makeNumeric()
	switch v1.typeof() {
	case sqltypes.Int64:
		switch v2.typeof() {
		case sqltypes.Int64:
			return intMinusIntWithError(v1.uint64(), v2.uint64(), out)
		case sqltypes.Uint64:
			return intMinusUintWithError(v1.uint64(), v2.uint64(), out)
		case sqltypes.Float64:
			return anyMinusFloat(v1, v2.float64(), out)
		case sqltypes.Decimal:
			anyMinusDecimal(v1, v2.decimal(), v2.length_, out)
			return nil
		}
	case sqltypes.Uint64:
		switch v2.typeof() {
		case sqltypes.Int64:
			return uintMinusIntWithError(v1.uint64(), v2.uint64(), out)
		case sqltypes.Uint64:
			return uintMinusUintWithError(v1.uint64(), v2.uint64(), out)
		case sqltypes.Float64:
			return anyMinusFloat(v1, v2.float64(), out)
		case sqltypes.Decimal:
			anyMinusDecimal(v1, v2.decimal(), v2.length_, out)
			return nil
		}
	case sqltypes.Float64:
		return floatMinusAny(v1.float64(), v2, out)
	case sqltypes.Decimal:
		switch v2.typeof() {
		case sqltypes.Float64:
			return anyMinusFloat(v1, v2.float64(), out)
		default:
			decimalMinusAny(v1.decimal(), v1.length_, v2, out)
			return nil
		}
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", v1.value().String(), v2.value().String())
}

func multiplyNumericWithError(v1, v2, out *EvalResult) error {
	v1, v2 = makeNumericAndPrioritize(v1, v2)
	switch v1.typeof() {
	case sqltypes.Int64:
		return intTimesIntWithError(v1.uint64(), v2.uint64(), out)
	case sqltypes.Uint64:
		switch v2.typeof() {
		case sqltypes.Int64:
			return uintTimesIntWithError(v1.uint64(), v2.uint64(), out)
		case sqltypes.Uint64:
			return uintTimesUintWithError(v1.uint64(), v2.uint64(), out)
		}
	case sqltypes.Float64:
		return floatTimesAny(v1.float64(), v2, out)
	case sqltypes.Decimal:
		decimalTimesAny(v1.decimal(), v1.length_, v2, out)
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", v1.value().String(), v2.value().String())

}

func divideNumericWithError(v1, v2 *EvalResult, precise bool, out *EvalResult) error {
	v1.makeNumeric()
	v2.makeNumeric()
	if !precise && v1.typeof() != sqltypes.Decimal && v2.typeof() != sqltypes.Decimal {
		switch v1.typeof() {
		case sqltypes.Int64:
			return floatDivideAnyWithError(float64(v1.int64()), v2, out)

		case sqltypes.Uint64:
			return floatDivideAnyWithError(float64(v1.uint64()), v2, out)

		case sqltypes.Float64:
			return floatDivideAnyWithError(v1.float64(), v2, out)
		}
	}
	switch {
	case v1.typeof() == sqltypes.Float64:
		return floatDivideAnyWithError(v1.float64(), v2, out)
	case v2.typeof() == sqltypes.Float64:
		v1f, err := v1.coerceToFloat()
		if err != nil {
			return err
		}
		return floatDivideAnyWithError(v1f, v2, out)
	default:
		decimalDivide(v1, v2, divPrecisionIncrement, out)
		return nil
	}
}

// makeNumericAndPrioritize reorders the input parameters
// to be Float64, Decimal, Uint64, Int64.
func makeNumericAndPrioritize(i1, i2 *EvalResult) (*EvalResult, *EvalResult) {
	i1.makeNumeric()
	i2.makeNumeric()
	switch i1.typeof() {
	case sqltypes.Int64:
		if i2.typeof() == sqltypes.Uint64 || i2.typeof() == sqltypes.Float64 || i2.typeof() == sqltypes.Decimal {
			return i2, i1
		}
	case sqltypes.Uint64:
		if i2.typeof() == sqltypes.Float64 || i2.typeof() == sqltypes.Decimal {
			return i2, i1
		}
	case sqltypes.Decimal:
		if i2.typeof() == sqltypes.Float64 {
			return i2, i1
		}
	}
	return i1, i2
}

func intPlusIntWithError(v1u, v2u uint64, out *EvalResult) error {
	v1, v2 := int64(v1u), int64(v2u)
	result := v1 + v2
	if (result > v1) != (v2 > 0) {
		return dataOutOfRangeError(v1, v2, "BIGINT", "+")
	}
	out.setInt64(result)
	return nil
}

func intMinusIntWithError(v1u, v2u uint64, out *EvalResult) error {
	v1, v2 := int64(v1u), int64(v2u)
	result := v1 - v2

	if (result < v1) != (v2 > 0) {
		return dataOutOfRangeError(v1, v2, "BIGINT", "-")
	}
	out.setInt64(result)
	return nil
}

func intTimesIntWithError(v1u, v2u uint64, out *EvalResult) error {
	v1, v2 := int64(v1u), int64(v2u)
	result := v1 * v2
	if v1 != 0 && result/v1 != v2 {
		return dataOutOfRangeError(v1, v2, "BIGINT", "*")
	}
	out.setInt64(result)
	return nil

}

func intMinusUintWithError(v1u uint64, v2 uint64, out *EvalResult) error {
	v1 := int64(v1u)
	if v1 < 0 || v1 < int64(v2) {
		return dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	return uintMinusUintWithError(v1u, v2, out)
}

func uintPlusIntWithError(v1 uint64, v2u uint64, out *EvalResult) error {
	v2 := int64(v2u)
	result := v1 + uint64(v2)
	if v2 < 0 && v1 < uint64(-v2) || v2 > 0 && (result < v1 || result < uint64(v2)) {
		return dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "+")
	}
	// convert to int -> uint is because for numeric operators (such as + or -)
	// where one of the operands is an unsigned integer, the result is unsigned by default.
	out.setUint64(result)
	return nil
}

func uintMinusIntWithError(v1 uint64, v2u uint64, out *EvalResult) error {
	v2 := int64(v2u)
	if int64(v1) < v2 && v2 > 0 {
		return dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	// uint - (- int) = uint + int
	if v2 < 0 {
		return uintPlusIntWithError(v1, uint64(-v2), out)
	}
	return uintMinusUintWithError(v1, uint64(v2), out)
}

func uintTimesIntWithError(v1 uint64, v2u uint64, out *EvalResult) error {
	v2 := int64(v2u)
	if v1 == 0 || v2 == 0 {
		out.setUint64(0)
		return nil
	}
	if v2 < 0 || int64(v1) < 0 {
		return dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "*")
	}
	return uintTimesUintWithError(v1, uint64(v2), out)
}

func uintPlusUintWithError(v1, v2 uint64, out *EvalResult) error {
	result := v1 + v2
	if result < v1 || result < v2 {
		return dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "+")
	}
	out.setUint64(result)
	return nil
}

func uintMinusUintWithError(v1, v2 uint64, out *EvalResult) error {
	result := v1 - v2
	if v2 > v1 {
		return dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "-")
	}
	out.setUint64(result)
	return nil
}

func uintTimesUintWithError(v1, v2 uint64, out *EvalResult) error {
	if v1 == 0 || v2 == 0 {
		out.setUint64(0)
		return nil
	}
	result := v1 * v2
	if result < v2 || result < v1 {
		return dataOutOfRangeError(v1, v2, "BIGINT UNSIGNED", "*")
	}
	out.setUint64(result)
	return nil
}

func floatPlusAny(v1 float64, v2 *EvalResult, out *EvalResult) error {
	v2f, err := v2.coerceToFloat()
	if err != nil {
		return err
	}
	add := v1 + v2f
	out.setFloat(add)
	return nil
}

func floatMinusAny(v1 float64, v2 *EvalResult, out *EvalResult) error {
	v2f, err := v2.coerceToFloat()
	if err != nil {
		return err
	}
	out.setFloat(v1 - v2f)
	return nil
}

func floatTimesAny(v1 float64, v2 *EvalResult, out *EvalResult) error {
	v2f, err := v2.coerceToFloat()
	if err != nil {
		return err
	}
	out.setFloat(v1 * v2f)
	return nil
}

func maxprec(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func decimalPlusAny(v1 decimal.Decimal, f1 int32, v2 *EvalResult, out *EvalResult) {
	v2d := v2.coerceToDecimal()
	out.setDecimal(v1.Add(v2d), maxprec(f1, v2.length_))
}

func decimalMinusAny(v1 decimal.Decimal, f1 int32, v2 *EvalResult, out *EvalResult) {
	v2d := v2.coerceToDecimal()
	out.setDecimal(v1.Sub(v2d), maxprec(f1, v2.length_))
}

func anyMinusDecimal(v1 *EvalResult, v2 decimal.Decimal, f2 int32, out *EvalResult) {
	v1d := v1.coerceToDecimal()
	out.setDecimal(v1d.Sub(v2), maxprec(v1.length_, f2))
}

func decimalTimesAny(v1 decimal.Decimal, f1 int32, v2 *EvalResult, out *EvalResult) {
	v2d := v2.coerceToDecimal()
	out.setDecimal(v1.Mul(v2d), maxprec(f1, v2.length_))
}

const divPrecisionIncrement = 4

func decimalDivide(v1, v2 *EvalResult, incrPrecision int32, out *EvalResult) {
	v1d := v1.coerceToDecimal()
	v2d := v2.coerceToDecimal()
	if v2d.IsZero() {
		out.setNull()
		return
	}
	out.setDecimal(v1d.Div(v2d, incrPrecision), v1.length_+incrPrecision)
}

func floatDivideAnyWithError(v1 float64, v2 *EvalResult, out *EvalResult) error {
	v2f, err := v2.coerceToFloat()
	if err != nil {
		return err
	}
	if v2f == 0.0 {
		out.setNull()
		return nil
	}

	result := v1 / v2f
	divisorLessThanOne := v2f < 1
	resultMismatch := v2f*result != v1

	if divisorLessThanOne && resultMismatch {
		return dataOutOfRangeError(v1, v2f, "BIGINT", "/")
	}

	out.setFloat(result)
	return nil
}

func anyMinusFloat(v1 *EvalResult, v2 float64, out *EvalResult) error {
	v1f, err := v1.coerceToFloat()
	if err != nil {
		return err
	}
	out.setFloat(v1f - v2)
	return nil
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
