/*
Copyright 2020 The Vitess Authors.

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
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
)

// Cast converts a Value to the target type.
func Cast(v sqltypes.Value, typ sqltypes.Type) (sqltypes.Value, error) {
	if v.Type() == typ || v.IsNull() {
		return v, nil
	}
	vBytes, err := v.ToBytes()
	if err != nil {
		return v, err
	}
	if sqltypes.IsSigned(typ) && v.IsSigned() {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}
	if sqltypes.IsUnsigned(typ) && v.IsUnsigned() {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}
	if (sqltypes.IsFloat(typ) || typ == sqltypes.Decimal) && (v.IsIntegral() || v.IsFloat() || v.Type() == sqltypes.Decimal) {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}
	if sqltypes.IsQuoted(typ) && (v.IsIntegral() || v.IsFloat() || v.Type() == sqltypes.Decimal || v.IsQuoted()) {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}

	// Explicitly disallow Expression.
	if v.Type() == sqltypes.Expression {
		return sqltypes.NULL, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v cannot be cast to %v", v, typ)
	}

	// If the above fast-paths were not possible,
	// go through full validation.
	return sqltypes.NewValue(typ, vBytes)
}

// ToUint64 converts Value to uint64.
func ToUint64(v sqltypes.Value) (uint64, error) {
	var num EvalResult
	if err := num.setValueIntegralNumeric(v); err != nil {
		return 0, err
	}
	switch num.typeof() {
	case sqltypes.Int64:
		if num.uint64() > math.MaxInt64 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "negative number cannot be converted to unsigned: %d", num.int64())
		}
		return num.uint64(), nil
	case sqltypes.Uint64:
		return num.uint64(), nil
	}
	panic("unreachable")
}

// ToInt64 converts Value to int64.
func ToInt64(v sqltypes.Value) (int64, error) {
	var num EvalResult
	if err := num.setValueIntegralNumeric(v); err != nil {
		return 0, err
	}
	switch num.typeof() {
	case sqltypes.Int64:
		return num.int64(), nil
	case sqltypes.Uint64:
		ival := num.int64()
		if ival < 0 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsigned number overflows int64 value: %d", num.uint64())
		}
		return ival, nil
	}
	panic("unreachable")
}

// ToFloat64 converts Value to float64.
func ToFloat64(v sqltypes.Value) (float64, error) {
	var num EvalResult
	if err := num.setValue(v, collationNumeric); err != nil {
		return 0, err
	}
	num.makeFloat()
	return num.float64(), nil
}

func LiteralToValue(literal *sqlparser.Literal) (sqltypes.Value, error) {
	lit, err := translateLiteral(literal, nil)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return lit.Val.Value(), nil
}

// ToNative converts Value to a native go type.
// Decimal is returned as []byte.
func ToNative(v sqltypes.Value) (any, error) {
	var out any
	var err error
	switch {
	case v.Type() == sqltypes.Null:
		// no-op
	case v.IsSigned():
		return ToInt64(v)
	case v.IsUnsigned():
		return ToUint64(v)
	case v.IsFloat():
		return ToFloat64(v)
	case v.IsQuoted() || v.Type() == sqltypes.Bit || v.Type() == sqltypes.Decimal:
		out, err = v.ToBytes()
	case v.Type() == sqltypes.Expression:
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v cannot be converted to a go type", v)
	}
	return out, err
}

func compareNumeric(v1, v2 *EvalResult) (int, error) {
	// upcast all <64 bit numeric types to 64 bit, e.g. int8 -> int64, uint8 -> uint64, float32 -> float64
	// so we don't have to consider integer types which aren't 64 bit
	v1.upcastNumeric()
	v2.upcastNumeric()

	// Equalize the types the same way MySQL does
	// https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
	switch v1.typeof() {
	case sqltypes.Int64:
		switch v2.typeof() {
		case sqltypes.Uint64:
			if v1.uint64() > math.MaxInt64 {
				return -1, nil
			}
			v1.setUint64(v1.uint64())
		case sqltypes.Float64:
			v1.setFloat(float64(v1.int64()))
		case sqltypes.Decimal:
			v1.setDecimal(decimal.NewFromInt(v1.int64()), 0)
		}
	case sqltypes.Uint64:
		switch v2.typeof() {
		case sqltypes.Int64:
			if v2.uint64() > math.MaxInt64 {
				return 1, nil
			}
			v2.setUint64(v2.uint64())
		case sqltypes.Float64:
			v1.setFloat(float64(v1.uint64()))
		case sqltypes.Decimal:
			v1.setDecimal(decimal.NewFromUint(v1.uint64()), 0)
		}
	case sqltypes.Float64:
		switch v2.typeof() {
		case sqltypes.Int64:
			v2.setFloat(float64(v2.int64()))
		case sqltypes.Uint64:
			if v1.float64() < 0 {
				return -1, nil
			}
			v2.setFloat(float64(v2.uint64()))
		case sqltypes.Decimal:
			f, ok := v2.decimal().Float64()
			if !ok {
				return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
			}
			v2.setFloat(f)
		}
	case sqltypes.Decimal:
		switch v2.typeof() {
		case sqltypes.Int64:
			v2.setDecimal(decimal.NewFromInt(v2.int64()), 0)
		case sqltypes.Uint64:
			v2.setDecimal(decimal.NewFromUint(v2.uint64()), 0)
		case sqltypes.Float64:
			f, ok := v1.decimal().Float64()
			if !ok {
				return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
			}
			v1.setFloat(f)
		}
	}

	// Both values are of the same type.
	switch v1.typeof() {
	case sqltypes.Int64:
		v1v, v2v := v1.int64(), v2.int64()
		switch {
		case v1v == v2v:
			return 0, nil
		case v1v < v2v:
			return -1, nil
		}
	case sqltypes.Uint64:
		switch {
		case v1.uint64() == v2.uint64():
			return 0, nil
		case v1.uint64() < v2.uint64():
			return -1, nil
		}
	case sqltypes.Float64:
		v1v, v2v := v1.float64(), v2.float64()
		switch {
		case v1v == v2v:
			return 0, nil
		case v1v < v2v:
			return -1, nil
		}
	case sqltypes.Decimal:
		return v1.decimal().Cmp(v2.decimal()), nil
	}
	return 1, nil
}

func parseDate(expr *EvalResult) (t time.Time, err error) {
	switch expr.typeof() {
	case sqltypes.Date:
		t, err = sqlparser.ParseDate(expr.string())
	case sqltypes.Timestamp, sqltypes.Datetime:
		t, err = sqlparser.ParseDateTime(expr.string())
	case sqltypes.Time:
		t, err = sqlparser.ParseTime(expr.string())
	}
	return
}

// matchExprWithAnyDateFormat formats the given expr (usually a string) to a date using the first format
// that does not return an error.
func matchExprWithAnyDateFormat(expr *EvalResult) (t time.Time, err error) {
	t, err = sqlparser.ParseDate(expr.string())
	if err == nil {
		return
	}
	t, err = sqlparser.ParseDateTime(expr.string())
	if err == nil {
		return
	}
	t, err = sqlparser.ParseTime(expr.string())
	return
}

// Date comparison based on:
//   - https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
//   - https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-conversion.html
func compareDates(l, r *EvalResult) (int, error) {
	lTime, err := parseDate(l)
	if err != nil {
		return 0, err
	}
	rTime, err := parseDate(r)
	if err != nil {
		return 0, err
	}

	return compareGoTimes(lTime, rTime)
}

func compareDateAndString(l, r *EvalResult) (int, error) {
	var lTime, rTime time.Time
	var err error
	switch {
	case sqltypes.IsDate(l.typeof()):
		lTime, err = parseDate(l)
		if err != nil {
			return 0, err
		}
		rTime, err = matchExprWithAnyDateFormat(r)
		if err != nil {
			return 0, err
		}
	case l.isTextual():
		rTime, err = parseDate(r)
		if err != nil {
			return 0, err
		}
		lTime, err = matchExprWithAnyDateFormat(l)
		if err != nil {
			return 0, err
		}
	}
	return compareGoTimes(lTime, rTime)
}

func compareGoTimes(lTime, rTime time.Time) (int, error) {
	if lTime.Before(rTime) {
		return -1, nil
	}
	if lTime.After(rTime) {
		return 1, nil
	}
	return 0, nil
}

// More on string collations coercibility on MySQL documentation:
//   - https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
func compareStrings(l, r *EvalResult) int {
	coll, err := mergeCollations(l, r)
	if err != nil {
		throwEvalError(err)
	}
	collation := collations.Local().LookupByID(coll)
	if collation == nil {
		panic("unknown collation after coercion")
	}
	return collation.Collate(l.bytes(), r.bytes(), false)
}
