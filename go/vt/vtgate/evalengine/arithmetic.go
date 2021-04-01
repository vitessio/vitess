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
	"fmt"
	"math"

	"vitess.io/vitess/go/sqltypes"

	"strconv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// evalengine represents a numeric value extracted from
// a Value, used for arithmetic operations.
var zeroBytes = []byte("0")

// UnsupportedComparisonError represents the error where the comparison between the two types is unsupported on vitess
type UnsupportedComparisonError struct {
	Type1 querypb.Type
	Type2 querypb.Type
}

// Error function implements the error interface
func (err UnsupportedComparisonError) Error() string {
	return fmt.Sprintf("types are not comparable: %v vs %v", err.Type1, err.Type2)
}

// Add adds two values together
// if v1 or v2 is null, then it returns null
func Add(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	lv1, err := newEvalResult(v1)
	if err != nil {
		return sqltypes.NULL, err
	}

	lv2, err := newEvalResult(v2)
	if err != nil {
		return sqltypes.NULL, err
	}

	lresult, err := addNumericWithError(lv1, lv2)
	if err != nil {
		return sqltypes.NULL, err
	}

	return lresult.toSQLValue(lresult.typ), nil
}

// Subtract takes two values and subtracts them
func Subtract(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	lv1, err := newEvalResult(v1)
	if err != nil {
		return sqltypes.NULL, err
	}

	lv2, err := newEvalResult(v2)
	if err != nil {
		return sqltypes.NULL, err
	}

	lresult, err := subtractNumericWithError(lv1, lv2)
	if err != nil {
		return sqltypes.NULL, err
	}

	return lresult.toSQLValue(lresult.typ), nil
}

// Multiply takes two values and multiplies it together
func Multiply(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	lv1, err := newEvalResult(v1)
	if err != nil {
		return sqltypes.NULL, err
	}
	lv2, err := newEvalResult(v2)
	if err != nil {
		return sqltypes.NULL, err
	}
	lresult, err := multiplyNumericWithError(lv1, lv2)
	if err != nil {
		return sqltypes.NULL, err
	}

	return lresult.toSQLValue(lresult.typ), nil
}

// Divide (Float) for MySQL. Replicates behavior of "/" operator
func Divide(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	lv2AsFloat, err := ToFloat64(v2)
	divisorIsZero := lv2AsFloat == 0

	if divisorIsZero || err != nil {
		return sqltypes.NULL, err
	}

	lv1, err := newEvalResult(v1)
	if err != nil {
		return sqltypes.NULL, err
	}

	lv2, err := newEvalResult(v2)
	if err != nil {
		return sqltypes.NULL, err
	}

	lresult, err := divideNumericWithError(lv1, lv2)
	if err != nil {
		return sqltypes.NULL, err
	}

	return lresult.toSQLValue(lresult.typ), nil
}

// NullsafeAdd adds two Values in a null-safe manner. A null value
// is treated as 0. If both values are null, then a null is returned.
// If both values are not null, a numeric value is built
// from each input: Signed->int64, Unsigned->uint64, Float->float64.
// Otherwise the 'best type fit' is chosen for the number: int64 or float64.
// Addition is performed by upgrading types as needed, or in case
// of overflow: int64->uint64, int64->float64, uint64->float64.
// Unsigned ints can only be added to positive ints. After the
// addition, if one of the input types was Decimal, then
// a Decimal is built. Otherwise, the final type of the
// result is preserved.
func NullsafeAdd(v1, v2 sqltypes.Value, resultType querypb.Type) sqltypes.Value {
	if v1.IsNull() {
		v1 = sqltypes.MakeTrusted(resultType, zeroBytes)
	}
	if v2.IsNull() {
		v2 = sqltypes.MakeTrusted(resultType, zeroBytes)
	}

	lv1, err := newEvalResult(v1)
	if err != nil {
		return sqltypes.NULL
	}
	lv2, err := newEvalResult(v2)
	if err != nil {
		return sqltypes.NULL
	}
	lresult := addNumeric(lv1, lv2)

	return lresult.toSQLValue(resultType)
}

// NullsafeCompare returns 0 if v1==v2, -1 if v1<v2, and 1 if v1>v2.
// NULL is the lowest value. If any value is
// numeric, then a numeric comparison is performed after
// necessary conversions. If none are numeric, then it's
// a simple binary comparison. Uncomparable values return an error.
func NullsafeCompare(v1, v2 sqltypes.Value) (int, error) {
	// Based on the categorization defined for the types,
	// we're going to allow comparison of the following:
	// Null, isNumber, IsBinary. This will exclude IsQuoted
	// types that are not Binary, and Expression.
	if v1.IsNull() {
		if v2.IsNull() {
			return 0, nil
		}
		return -1, nil
	}
	if v2.IsNull() {
		return 1, nil
	}
	if sqltypes.IsNumber(v1.Type()) || sqltypes.IsNumber(v2.Type()) {
		lv1, err := newEvalResult(v1)
		if err != nil {
			return 0, err
		}
		lv2, err := newEvalResult(v2)
		if err != nil {
			return 0, err
		}
		return compareNumeric(lv1, lv2)
	}
	if isByteComparable(v1) && isByteComparable(v2) {
		return bytes.Compare(v1.ToBytes(), v2.ToBytes()), nil
	}
	return 0, UnsupportedComparisonError{
		Type1: v1.Type(),
		Type2: v2.Type(),
	}
}

// NullsafeHashcode returns an int64 hashcode that is guaranteed to be the same
// for two values that are considered equal by `NullsafeCompare`.
// TODO: should be extended to support all possible types
func NullsafeHashcode(v sqltypes.Value) (int64, error) {
	if v.IsNull() {
		return math.MaxInt64, nil
	}

	if sqltypes.IsNumber(v.Type()) {
		result, err := newEvalResult(v)
		if err != nil {
			return 0, err
		}
		return hashCode(result), nil
	}

	return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v", v.Type())
}

// isByteComparable returns true if the type is binary or date/time.
func isByteComparable(v sqltypes.Value) bool {
	if v.IsBinary() {
		return true
	}
	switch v.Type() {
	case sqltypes.Timestamp, sqltypes.Date, sqltypes.Time, sqltypes.Datetime, sqltypes.Enum, sqltypes.Set, sqltypes.TypeJSON:
		return true
	}
	return false
}

// Min returns the minimum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Min(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	return minmax(v1, v2, true)
}

// Max returns the maximum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Max(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	return minmax(v1, v2, false)
}

func minmax(v1, v2 sqltypes.Value, min bool) (sqltypes.Value, error) {
	if v1.IsNull() {
		return v2, nil
	}
	if v2.IsNull() {
		return v1, nil
	}

	n, err := NullsafeCompare(v1, v2)
	if err != nil {
		return sqltypes.NULL, err
	}

	// XNOR construct. See tests.
	v1isSmaller := n < 0
	if min == v1isSmaller {
		return v1, nil
	}
	return v2, nil
}

func addNumeric(v1, v2 EvalResult) EvalResult {
	v1, v2 = makeNumericAndprioritize(v1, v2)
	switch v1.typ {
	case sqltypes.Int64:
		return intPlusInt(v1.ival, v2.ival)
	case sqltypes.Uint64:
		switch v2.typ {
		case sqltypes.Int64:
			return uintPlusInt(v1.uval, v2.ival)
		case sqltypes.Uint64:
			return uintPlusUint(v1.uval, v2.uval)
		}
	case sqltypes.Float64:
		return floatPlusAny(v1.fval, v2)
	}
	panic("unreachable")
}

func addNumericWithError(v1, v2 EvalResult) (EvalResult, error) {
	v1, v2 = makeNumericAndprioritize(v1, v2)
	switch v1.typ {
	case sqltypes.Int64:
		return intPlusIntWithError(v1.ival, v2.ival)
	case sqltypes.Uint64:
		switch v2.typ {
		case sqltypes.Int64:
			return uintPlusIntWithError(v1.uval, v2.ival)
		case sqltypes.Uint64:
			return uintPlusUintWithError(v1.uval, v2.uval)
		}
	case sqltypes.Float64:
		return floatPlusAny(v1.fval, v2), nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", v1.Value().String(), v2.Value().String())

}

func subtractNumericWithError(i1, i2 EvalResult) (EvalResult, error) {
	v1 := makeNumeric(i1)
	v2 := makeNumeric(i2)
	switch v1.typ {
	case sqltypes.Int64:
		switch v2.typ {
		case sqltypes.Int64:
			return intMinusIntWithError(v1.ival, v2.ival)
		case sqltypes.Uint64:
			return intMinusUintWithError(v1.ival, v2.uval)
		case sqltypes.Float64:
			return anyMinusFloat(v1, v2.fval), nil
		}
	case sqltypes.Uint64:
		switch v2.typ {
		case sqltypes.Int64:
			return uintMinusIntWithError(v1.uval, v2.ival)
		case sqltypes.Uint64:
			return uintMinusUintWithError(v1.uval, v2.uval)
		case sqltypes.Float64:
			return anyMinusFloat(v1, v2.fval), nil
		}
	case sqltypes.Float64:
		return floatMinusAny(v1.fval, v2), nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", v1.Value().String(), v2.Value().String())
}

func multiplyNumericWithError(v1, v2 EvalResult) (EvalResult, error) {
	v1, v2 = makeNumericAndprioritize(v1, v2)
	switch v1.typ {
	case sqltypes.Int64:
		return intTimesIntWithError(v1.ival, v2.ival)
	case sqltypes.Uint64:
		switch v2.typ {
		case sqltypes.Int64:
			return uintTimesIntWithError(v1.uval, v2.ival)
		case sqltypes.Uint64:
			return uintTimesUintWithError(v1.uval, v2.uval)
		}
	case sqltypes.Float64:
		return floatTimesAny(v1.fval, v2), nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", v1.Value().String(), v2.Value().String())

}

func divideNumericWithError(i1, i2 EvalResult) (EvalResult, error) {
	v1 := makeNumeric(i1)
	v2 := makeNumeric(i2)
	switch v1.typ {
	case sqltypes.Int64:
		return floatDivideAnyWithError(float64(v1.ival), v2)

	case sqltypes.Uint64:
		return floatDivideAnyWithError(float64(v1.uval), v2)

	case sqltypes.Float64:
		return floatDivideAnyWithError(v1.fval, v2)
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid arithmetic between: %s %s", v1.Value().String(), v2.Value().String())
}

// makeNumericAndprioritize reorders the input parameters
// to be Float64, Uint64, Int64.
func makeNumericAndprioritize(i1, i2 EvalResult) (EvalResult, EvalResult) {
	v1 := makeNumeric(i1)
	v2 := makeNumeric(i2)
	switch v1.typ {
	case sqltypes.Int64:
		if v2.typ == sqltypes.Uint64 || v2.typ == sqltypes.Float64 {
			return v2, v1
		}
	case sqltypes.Uint64:
		if v2.typ == sqltypes.Float64 {
			return v2, v1
		}
	}
	return v1, v2
}

func makeNumeric(v EvalResult) EvalResult {
	if sqltypes.IsNumber(v.typ) {
		return v
	}
	if ival, err := strconv.ParseInt(string(v.bytes), 10, 64); err == nil {
		return EvalResult{ival: ival, typ: sqltypes.Int64}
	}
	if fval, err := strconv.ParseFloat(string(v.bytes), 64); err == nil {
		return EvalResult{fval: fval, typ: sqltypes.Float64}
	}
	return EvalResult{ival: 0, typ: sqltypes.Int64}
}

func intPlusInt(v1, v2 int64) EvalResult {
	result := v1 + v2
	if v1 > 0 && v2 > 0 && result < 0 {
		goto overflow
	}
	if v1 < 0 && v2 < 0 && result > 0 {
		goto overflow
	}
	return EvalResult{typ: sqltypes.Int64, ival: result}

overflow:
	return EvalResult{typ: sqltypes.Float64, fval: float64(v1) + float64(v2)}
}

func intPlusIntWithError(v1, v2 int64) (EvalResult, error) {
	result := v1 + v2
	if (result > v1) != (v2 > 0) {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v + %v", "BIGINT", v1, v2)
	}
	return EvalResult{typ: sqltypes.Int64, ival: result}, nil
}

func intMinusIntWithError(v1, v2 int64) (EvalResult, error) {
	result := v1 - v2

	if (result < v1) != (v2 > 0) {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v - %v", "BIGINT", v1, v2)
	}
	return EvalResult{typ: sqltypes.Int64, ival: result}, nil
}

func intTimesIntWithError(v1, v2 int64) (EvalResult, error) {
	result := v1 * v2
	if v1 != 0 && result/v1 != v2 {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v * %v", "BIGINT", v1, v2)
	}
	return EvalResult{typ: sqltypes.Int64, ival: result}, nil

}

func intMinusUintWithError(v1 int64, v2 uint64) (EvalResult, error) {
	if v1 < 0 || v1 < int64(v2) {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v - %v", "BIGINT UNSIGNED", v1, v2)
	}
	return uintMinusUintWithError(uint64(v1), v2)
}

func uintPlusInt(v1 uint64, v2 int64) EvalResult {
	return uintPlusUint(v1, uint64(v2))
}

func uintPlusIntWithError(v1 uint64, v2 int64) (EvalResult, error) {
	if v2 < 0 && v1 < uint64(v2) {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v + %v", "BIGINT UNSIGNED", v1, v2)
	}
	// convert to int -> uint is because for numeric operators (such as + or -)
	// where one of the operands is an unsigned integer, the result is unsigned by default.
	return uintPlusUintWithError(v1, uint64(v2))
}

func uintMinusIntWithError(v1 uint64, v2 int64) (EvalResult, error) {
	if int64(v1) < v2 && v2 > 0 {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v - %v", "BIGINT UNSIGNED", v1, v2)
	}
	// uint - (- int) = uint + int
	if v2 < 0 {
		return uintPlusIntWithError(v1, -v2)
	}
	return uintMinusUintWithError(v1, uint64(v2))
}

func uintTimesIntWithError(v1 uint64, v2 int64) (EvalResult, error) {
	if v2 < 0 || int64(v1) < 0 {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v * %v", "BIGINT UNSIGNED", v1, v2)
	}
	return uintTimesUintWithError(v1, uint64(v2))
}

func uintPlusUint(v1, v2 uint64) EvalResult {
	result := v1 + v2
	if result < v2 {
		return EvalResult{typ: sqltypes.Float64, fval: float64(v1) + float64(v2)}
	}
	return EvalResult{typ: sqltypes.Uint64, uval: result}
}

func uintPlusUintWithError(v1, v2 uint64) (EvalResult, error) {
	result := v1 + v2
	if result < v2 {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v + %v", "BIGINT UNSIGNED", v1, v2)
	}
	return EvalResult{typ: sqltypes.Uint64, uval: result}, nil
}

func uintMinusUintWithError(v1, v2 uint64) (EvalResult, error) {
	result := v1 - v2
	if v2 > v1 {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v - %v", "BIGINT UNSIGNED", v1, v2)
	}

	return EvalResult{typ: sqltypes.Uint64, uval: result}, nil
}

func uintTimesUintWithError(v1, v2 uint64) (EvalResult, error) {
	result := v1 * v2
	if result < v2 || result < v1 {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v * %v", "BIGINT UNSIGNED", v1, v2)
	}
	return EvalResult{typ: sqltypes.Uint64, uval: result}, nil
}

func floatPlusAny(v1 float64, v2 EvalResult) EvalResult {
	switch v2.typ {
	case sqltypes.Int64:
		v2.fval = float64(v2.ival)
	case sqltypes.Uint64:
		v2.fval = float64(v2.uval)
	}
	return EvalResult{typ: sqltypes.Float64, fval: v1 + v2.fval}
}

func floatMinusAny(v1 float64, v2 EvalResult) EvalResult {
	switch v2.typ {
	case sqltypes.Int64:
		v2.fval = float64(v2.ival)
	case sqltypes.Uint64:
		v2.fval = float64(v2.uval)
	}
	return EvalResult{typ: sqltypes.Float64, fval: v1 - v2.fval}
}

func floatTimesAny(v1 float64, v2 EvalResult) EvalResult {
	switch v2.typ {
	case sqltypes.Int64:
		v2.fval = float64(v2.ival)
	case sqltypes.Uint64:
		v2.fval = float64(v2.uval)
	}
	return EvalResult{typ: sqltypes.Float64, fval: v1 * v2.fval}
}

func floatDivideAnyWithError(v1 float64, v2 EvalResult) (EvalResult, error) {
	switch v2.typ {
	case sqltypes.Int64:
		v2.fval = float64(v2.ival)
	case sqltypes.Uint64:
		v2.fval = float64(v2.uval)
	}
	result := v1 / v2.fval
	divisorLessThanOne := v2.fval < 1
	resultMismatch := v2.fval*result != v1

	if divisorLessThanOne && resultMismatch {
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in %v / %v", "BIGINT", v1, v2.fval)
	}

	return EvalResult{typ: sqltypes.Float64, fval: v1 / v2.fval}, nil
}

func anyMinusFloat(v1 EvalResult, v2 float64) EvalResult {
	switch v1.typ {
	case sqltypes.Int64:
		v1.fval = float64(v1.ival)
	case sqltypes.Uint64:
		v1.fval = float64(v1.uval)
	}
	return EvalResult{typ: sqltypes.Float64, fval: v1.fval - v2}
}
