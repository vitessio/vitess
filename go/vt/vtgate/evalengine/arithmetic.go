package evalengine

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal"
)

// evalengine represents a numeric value extracted from
// a Value, used for arithmetic operations.
var zeroBytes = []byte("0")

// UnsupportedComparisonError represents the error where the comparison between the two types is unsupported on vitess
type UnsupportedComparisonError struct {
	Type1 sqltypes.Type
	Type2 sqltypes.Type
}

// Error function implements the error interface
func (err UnsupportedComparisonError) Error() string {
	return fmt.Sprintf("types are not comparable: %v vs %v", err.Type1, err.Type2)
}

// UnsupportedCollationError represents the error where the comparison using provided collation is unsupported on vitess
type UnsupportedCollationError struct {
	ID collations.ID
}

// Error function implements the error interface
func (err UnsupportedCollationError) Error() string {
	return fmt.Sprintf("cannot compare strings, collation is unknown or unsupported (collation ID: %d)", err.ID)
}

func dataOutOfRangeError(v1, v2 interface{}, typ, sign string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "%s value is out of range in '(%v %s %v)'", typ, v1, sign, v2)
}

// Add adds two values together
// if v1 or v2 is null, then it returns null
func Add(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}

	var lv1, lv2, out EvalResult
	if err := lv1.setValue(v1); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2); err != nil {
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
	if err := lv1.setValue(v1); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2); err != nil {
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
	if err := lv1.setValue(v1); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2); err != nil {
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
	if err := lv1.setValue(v1); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2); err != nil {
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
	if err := lv1.setValue(v1); err != nil {
		return sqltypes.NULL, err
	}
	if err := lv2.setValue(v2); err != nil {
		return sqltypes.NULL, err
	}

	err := addNumericWithError(&lv1, &lv2, &out)
	if err != nil {
		return sqltypes.NULL, err
	}
	return out.toSQLValue(resultType), nil
}

// NullsafeCompare returns 0 if v1==v2, -1 if v1<v2, and 1 if v1>v2.
// NULL is the lowest value. If any value is
// numeric, then a numeric comparison is performed after
// necessary conversions. If none are numeric, then it's
// a simple binary comparison. Uncomparable values return an error.
func NullsafeCompare(v1, v2 sqltypes.Value, collationID collations.ID) (int, error) {
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

	if isByteComparable(v1.Type()) && isByteComparable(v2.Type()) {
		v1Bytes, err1 := v1.ToBytes()
		if err1 != nil {
			return 0, err1
		}
		v2Bytes, err2 := v2.ToBytes()
		if err2 != nil {
			return 0, err2
		}
		return bytes.Compare(v1Bytes, v2Bytes), nil
	}

	typ, err := CoerceTo(v1.Type(), v2.Type()) // TODO systay we should add a method where this decision is done at plantime
	if err != nil {
		return 0, err
	}
	var v1cast, v2cast EvalResult
	if err := v1cast.setValueCast(v1, typ); err != nil {
		return 0, err
	}
	if err := v2cast.setValueCast(v2, typ); err != nil {
		return 0, err
	}

	if sqltypes.IsNumber(typ) {
		return compareNumeric(&v1cast, &v2cast)
	}
	if sqltypes.IsText(typ) || sqltypes.IsBinary(typ) {
		if collationID == collations.Unknown {
			return 0, UnsupportedCollationError{
				ID: collationID,
			}
		}
		collation := collations.Local().LookupByID(collationID)
		if collation == nil {
			return 0, UnsupportedCollationError{
				ID: collationID,
			}
		}
		v1Bytes, err1 := v1.ToBytes()
		if err1 != nil {
			return 0, err1
		}
		v2Bytes, err2 := v2.ToBytes()
		if err2 != nil {
			return 0, err2
		}
		switch result := collation.Collate(v1Bytes, v2Bytes, false); {
		case result < 0:
			return -1, nil
		case result > 0:
			return 1, nil
		default:
			return 0, nil
		}
	}
	return 0, UnsupportedComparisonError{
		Type1: v1.Type(),
		Type2: v2.Type(),
	}
}

// isByteComparable returns true if the type is binary or date/time.
func isByteComparable(typ sqltypes.Type) bool {
	if sqltypes.IsBinary(typ) {
		return true
	}
	switch typ {
	case sqltypes.Timestamp, sqltypes.Date, sqltypes.Time, sqltypes.Datetime, sqltypes.Enum, sqltypes.Set, sqltypes.TypeJSON, sqltypes.Bit:
		return true
	}
	return false
}

// Min returns the minimum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Min(v1, v2 sqltypes.Value, collation collations.ID) (sqltypes.Value, error) {
	return minmax(v1, v2, true, collation)
}

// Max returns the maximum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Max(v1, v2 sqltypes.Value, collation collations.ID) (sqltypes.Value, error) {
	return minmax(v1, v2, false, collation)
}

func minmax(v1, v2 sqltypes.Value, min bool, collation collations.ID) (sqltypes.Value, error) {
	if v1.IsNull() {
		return v2, nil
	}
	if v2.IsNull() {
		return v1, nil
	}

	n, err := NullsafeCompare(v1, v2, collation)
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
		decimalPlusAny(v1.decimal(), v2, out)
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
			anyMinusDecimal(v1, v2.decimal(), out)
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
			anyMinusDecimal(v1, v2.decimal(), out)
			return nil
		}
	case sqltypes.Float64:
		return floatMinusAny(v1.float64(), v2, out)
	case sqltypes.Decimal:
		switch v2.typeof() {
		case sqltypes.Float64:
			return anyMinusFloat(v1, v2.float64(), out)
		default:
			decimalMinusAny(v1.decimal(), v2, out)
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
		decimalTimesAny(v1.decimal(), v2, out)
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
		return decimalDivide(v1, v2, divPrecisionIncrement, out)
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
	fmt.Fprintf(os.Stderr, "%f (%v) + %f (%v) = %f (%v)\n", v1, math.Signbit(v1), v2f, math.Signbit(v2f), add, math.Signbit(add))
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

const roundingModeArithmetic = decimal.ToZero
const roundingModeFormat = decimal.ToNearestEven

var decimalContextSQL = decimal.Context{
	MaxScale:      30,
	MinScale:      0,
	Precision:     65,
	Traps:         ^(decimal.Inexact | decimal.Rounded | decimal.Subnormal),
	RoundingMode:  roundingModeArithmetic,
	OperatingMode: decimal.GDA,
}

func newDecimalUint64(x uint64) *decimalResult {
	var result decimalResult
	result.num.Context = decimalContextSQL
	result.num.SetUint64(x)
	return &result
}

func newDecimalString(x string) (*decimalResult, error) {
	var result decimalResult
	result.num.Context = decimalContextSQL
	result.num.SetString(x)
	if result.num.Context.Conditions != 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", result.num.Context.Conditions)
	}
	result.frac = result.num.Scale()
	return &result, nil
}

func newDecimalInt64(x int64) *decimalResult {
	var result decimalResult
	result.num.Context = decimalContextSQL
	result.num.SetMantScale(x, 0)
	return &result
}

func newDecimalFromOp(left, right *decimalResult, op func(r, x, y *decimal.Big)) *decimalResult {
	var result decimalResult
	result.num.Context = decimalContextSQL
	op(&result.num, &left.num, &right.num)
	if left.frac > right.frac {
		result.frac = left.frac
	} else {
		result.frac = right.frac
	}
	return &result
}

func decimalPlusAny(v1 *decimalResult, v2 *EvalResult, out *EvalResult) {
	v2d := v2.coerceToDecimal()
	result := newDecimalFromOp(v1, v2d, func(r, x, y *decimal.Big) { r.Add(x, y) })
	out.setDecimal(result)
}

func decimalMinusAny(v1 *decimalResult, v2 *EvalResult, out *EvalResult) {
	v2d := v2.coerceToDecimal()
	result := newDecimalFromOp(v1, v2d, func(r, x, y *decimal.Big) { r.Sub(x, y) })
	out.setDecimal(result)
}

func anyMinusDecimal(v1 *EvalResult, v2 *decimalResult, out *EvalResult) {
	v1d := v1.coerceToDecimal()
	result := newDecimalFromOp(v1d, v2, func(r, x, y *decimal.Big) { r.Sub(x, y) })
	out.setDecimal(result)
}

func decimalTimesAny(v1 *decimalResult, v2 *EvalResult, out *EvalResult) {
	v2d := v2.coerceToDecimal()
	result := newDecimalFromOp(v1, v2d, func(r, x, y *decimal.Big) { r.Mul(x, y) })
	out.setDecimal(result)
}

const divPrecisionIncrement = 4

func decimalDivide(v1, v2 *EvalResult, incrPrecision int, out *EvalResult) error {
	left := v1.coerceToDecimal()
	right := v2.coerceToDecimal()

	var result decimalResult
	result.num.Context = decimalContextSQL
	result.frac = left.frac + incrPrecision
	result.num.Div(&left.num, &right.num, incrPrecision)
	if result.num.Context.Conditions&(decimal.DivisionByZero|decimal.DivisionUndefined) != 0 {
		out.setNull()
		return nil
	}
	out.setDecimal(&result)
	return nil
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
