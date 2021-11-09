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
	"time"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"

	"strconv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Cast converts a Value to the target type.
func Cast(v sqltypes.Value, typ querypb.Type) (sqltypes.Value, error) {
	if v.Type() == typ || v.IsNull() {
		return v, nil
	}
	if sqltypes.IsSigned(typ) && v.IsSigned() {
		return sqltypes.MakeTrusted(typ, v.ToBytes()), nil
	}
	if sqltypes.IsUnsigned(typ) && v.IsUnsigned() {
		return sqltypes.MakeTrusted(typ, v.ToBytes()), nil
	}
	if (sqltypes.IsFloat(typ) || typ == sqltypes.Decimal) && (v.IsIntegral() || v.IsFloat() || v.Type() == sqltypes.Decimal) {
		return sqltypes.MakeTrusted(typ, v.ToBytes()), nil
	}
	if sqltypes.IsQuoted(typ) && (v.IsIntegral() || v.IsFloat() || v.Type() == sqltypes.Decimal || v.IsQuoted()) {
		return sqltypes.MakeTrusted(typ, v.ToBytes()), nil
	}

	// Explicitly disallow Expression.
	if v.Type() == sqltypes.Expression {
		return sqltypes.NULL, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v cannot be cast to %v", v, typ)
	}

	// If the above fast-paths were not possible,
	// go through full validation.
	return sqltypes.NewValue(typ, v.ToBytes())
}

// ToUint64 converts Value to uint64.
func ToUint64(v sqltypes.Value) (uint64, error) {
	num, err := newIntegralNumeric(v)
	if err != nil {
		return 0, err
	}
	switch num.typ {
	case sqltypes.Int64:
		if num.ival < 0 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "negative number cannot be converted to unsigned: %d", num.ival)
		}
		return uint64(num.ival), nil
	case sqltypes.Uint64:
		return num.uval, nil
	}
	panic("unreachable")
}

// ToInt64 converts Value to int64.
func ToInt64(v sqltypes.Value) (int64, error) {
	num, err := newIntegralNumeric(v)
	if err != nil {
		return 0, err
	}
	switch num.typ {
	case sqltypes.Int64:
		return num.ival, nil
	case sqltypes.Uint64:
		ival := int64(num.uval)
		if ival < 0 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsigned number overflows int64 value: %d", num.uval)
		}
		return ival, nil
	}
	panic("unreachable")
}

// ToFloat64 converts Value to float64.
func ToFloat64(v sqltypes.Value) (float64, error) {
	num, err := newEvalResult(v)
	if err != nil {
		return 0, err
	}
	switch num.typ {
	case sqltypes.Int64:
		return float64(num.ival), nil
	case sqltypes.Uint64:
		return float64(num.uval), nil
	case sqltypes.Float64:
		return num.fval, nil
	}

	if sqltypes.IsText(num.typ) || sqltypes.IsBinary(num.typ) {
		fval, err := strconv.ParseFloat(string(v.Raw()), 64)
		if err != nil {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return fval, nil
	}

	return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot convert to float: %s", v.String())
}

// ToNative converts Value to a native go type.
// Decimal is returned as []byte.
func ToNative(v sqltypes.Value) (interface{}, error) {
	var out interface{}
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
		out = v.ToBytes()
	case v.Type() == sqltypes.Expression:
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v cannot be converted to a go type", v)
	}
	return out, err
}

// newEvalResult parses a value and produces an EvalResult containing the value
func newEvalResult(v sqltypes.Value) (EvalResult, error) {
	raw := v.Raw()
	switch {
	case v.IsBinary() || v.IsText():
		return EvalResult{bytes: raw, typ: sqltypes.VarBinary}, nil
	case v.IsSigned():
		ival, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return EvalResult{ival: ival, typ: sqltypes.Int64}, nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return EvalResult{uval: uval, typ: sqltypes.Uint64}, nil
	case v.IsFloat() || v.Type() == sqltypes.Decimal:
		fval, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		typ := sqltypes.Float64
		if v.Type() == sqltypes.Decimal {
			typ = sqltypes.Decimal
		}
		return EvalResult{fval: fval, typ: typ}, nil
	default:
		return EvalResult{typ: v.Type(), bytes: raw}, nil
	}
}

// newIntegralNumeric parses a value and produces an Int64 or Uint64.
func newIntegralNumeric(v sqltypes.Value) (EvalResult, error) {
	str := v.ToString()
	switch {
	case v.IsSigned():
		ival, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return EvalResult{ival: ival, typ: sqltypes.Int64}, nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return EvalResult{uval: uval, typ: sqltypes.Uint64}, nil
	}

	// For other types, do best effort.
	if ival, err := strconv.ParseInt(str, 10, 64); err == nil {
		return EvalResult{ival: ival, typ: sqltypes.Int64}, nil
	}
	if uval, err := strconv.ParseUint(str, 10, 64); err == nil {
		return EvalResult{uval: uval, typ: sqltypes.Uint64}, nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: '%s'", str)
}

func (v EvalResult) toSQLValue(resultType querypb.Type) sqltypes.Value {
	switch {
	case sqltypes.IsSigned(resultType):
		switch v.typ {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.ival), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.uval), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.fval), 10))
		}
	case sqltypes.IsUnsigned(resultType):
		switch v.typ {
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.uval), 10))
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.ival), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.fval), 10))
		}
	case sqltypes.IsFloat(resultType) || resultType == sqltypes.Decimal:
		switch v.typ {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.ival), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.uval), 10))
		case sqltypes.Float64, sqltypes.Float32:
			format := byte('g')
			if resultType == sqltypes.Decimal {
				format = 'f'
			}
			return sqltypes.MakeTrusted(resultType, strconv.AppendFloat(nil, float64(v.fval), format, -1, 64))
		}
	default:
		return sqltypes.MakeTrusted(resultType, v.bytes)
	}
	return sqltypes.NULL
}

func hashCode(v EvalResult) int64 {
	// we cast all numerical values to float64 and return the hashcode for that
	var val float64
	switch v.typ {
	case sqltypes.Int64:
		val = float64(v.ival)
	case sqltypes.Uint64:
		val = float64(v.uval)
	case sqltypes.Float64:
		val = v.fval
	}

	// this will not work for ±0, NaN and ±Inf,
	// so one must still check using `compareNumeric` which will not be fooled
	return int64(val)
}

func compareNumeric(v1, v2 EvalResult) (int, error) {
	// Equalize the types the same way MySQL does
	// https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
	switch v1.typ {
	case sqltypes.Int64:
		switch v2.typ {
		case sqltypes.Uint64:
			if v1.ival < 0 {
				return -1, nil
			}
			v1 = EvalResult{typ: sqltypes.Uint64, uval: uint64(v1.ival)}
		case sqltypes.Float64, sqltypes.Decimal:
			v1 = EvalResult{typ: v2.typ, fval: float64(v1.ival)}
		}
	case sqltypes.Uint64:
		switch v2.typ {
		case sqltypes.Int64:
			if v2.ival < 0 {
				return 1, nil
			}
			v2 = EvalResult{typ: sqltypes.Uint64, uval: uint64(v2.ival)}
		case sqltypes.Float64, sqltypes.Decimal:
			v1 = EvalResult{typ: v2.typ, fval: float64(v1.uval)}
		}
	case sqltypes.Float64:
		switch v2.typ {
		case sqltypes.Int64:
			v2 = EvalResult{typ: sqltypes.Float64, fval: float64(v2.ival)}
		case sqltypes.Uint64:
			if v1.fval < 0 {
				return -1, nil
			}
			v2 = EvalResult{typ: sqltypes.Float64, fval: float64(v2.uval)}
		case sqltypes.Decimal:
			v2.typ = sqltypes.Float64
		}
	case sqltypes.Decimal:
		switch v2.typ {
		case sqltypes.Int64:
			v2 = EvalResult{typ: sqltypes.Decimal, fval: float64(v2.ival)}
		case sqltypes.Uint64:
			if v1.fval < 0 {
				return -1, nil
			}
			v2 = EvalResult{typ: sqltypes.Decimal, fval: float64(v2.uval)}
		case sqltypes.Float64:
			v1.typ = sqltypes.Float64
		}
	}

	// Both values are of the same type.
	switch v1.typ {
	case sqltypes.Int64:
		switch {
		case v1.ival == v2.ival:
			return 0, nil
		case v1.ival < v2.ival:
			return -1, nil
		}
	case sqltypes.Uint64:
		switch {
		case v1.uval == v2.uval:
			return 0, nil
		case v1.uval < v2.uval:
			return -1, nil
		}
	case sqltypes.Float64, sqltypes.Decimal:
		switch {
		case v1.fval == v2.fval:
			return 0, nil
		case v1.fval < v2.fval:
			return -1, nil
		}
	}

	// v1>v2
	return 1, nil
}

func parseDate(expr EvalResult) (t time.Time, err error) {
	switch expr.typ {
	case sqltypes.Date:
		t, err = time.Parse("2006-01-02", string(expr.bytes))
	case sqltypes.Timestamp, sqltypes.Datetime:
		t, err = time.Parse("2006-01-02 15:04:05", string(expr.bytes))
	case sqltypes.Time:
		t, err = time.Parse("15:04:05", string(expr.bytes))
		if err == nil {
			now := time.Now()
			// setting the date to today's date, because we use AddDate on t
			// which is "0000-01-01 xx:xx:xx", we do minus one on the month
			// and day to take into account the 01 in both month and day of t
			t = t.AddDate(now.Year(), int(now.Month()-1), now.Day()-1)
		}
	}
	return
}

// matchExprWithAnyDateFormat formats the given expr (usually a string) to a date using the first format
// that does not return an error.
func matchExprWithAnyDateFormat(expr EvalResult) (t time.Time, err error) {
	layouts := []string{"2006-01-02", "2006-01-02 15:04:05", "15:04:05"}
	for _, layout := range layouts {
		t, err = time.Parse(layout, string(expr.bytes))
		if err == nil {
			if layout == "15:04:05" {
				now := time.Now()
				// setting the date to today's date, because we use AddDate on t
				// which is "0000-01-01 xx:xx:xx", we do minus one on the month
				// and day to take into account the 01 in both month and day of t
				t = t.AddDate(now.Year(), int(now.Month()-1), now.Day()-1)
			}
			return
		}
	}
	return
}

// Date comparison based on:
// 		- https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
// 		- https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-conversion.html
func compareDates(l, r EvalResult) (int, error) {
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

func compareDateAndString(l, r EvalResult) (int, error) {
	var lTime, rTime time.Time
	var err error
	switch {
	case sqltypes.IsDate(l.typ):
		lTime, err = parseDate(l)
		if err != nil {
			return 0, err
		}
		rTime, err = matchExprWithAnyDateFormat(r)
		if err != nil {
			return 0, err
		}
	case sqltypes.IsText(l.typ) || sqltypes.IsBinary(l.typ):
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
// 		- https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
func compareStrings(l, r EvalResult) (int, error) {
	// If one of the strings has an unknown collation we fail, though such error should
	// already be handled before the execution by the planner.
	if l.collation == collations.Unknown || r.collation == collations.Unknown {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot compare strings with an unknown collation")
	}

	// We cannot compare different collations for now, so we fail
	// TODO: support multiple collations comparison
	if r.collation != l.collation {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot compare strings with different collations")
	}

	collation := collations.Default().LookupByID(l.collation)
	return collation.Collate(l.bytes, r.bytes, false), nil
}
