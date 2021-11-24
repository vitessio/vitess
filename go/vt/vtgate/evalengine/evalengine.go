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
	"strconv"
	"time"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Cast converts a Value to the target type.
func Cast(v sqltypes.Value, typ querypb.Type) (sqltypes.Value, error) {
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
	num, err := newIntegralNumeric(v)
	if err != nil {
		return 0, err
	}
	switch num.typ {
	case sqltypes.Int64:
		if num.numval > math.MaxInt64 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "negative number cannot be converted to unsigned: %d", int64(num.numval))
		}
		return num.numval, nil
	case sqltypes.Uint64:
		return num.numval, nil
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
		return int64(num.numval), nil
	case sqltypes.Uint64:
		ival := int64(num.numval)
		if ival < 0 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsigned number overflows int64 value: %d", num.numval)
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
		return float64(int64(num.numval)), nil
	case sqltypes.Uint64:
		return float64(num.numval), nil
	case sqltypes.Float64:
		return math.Float64frombits(num.numval), nil
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
		out, err = v.ToBytes()
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
		// TODO: collation
		return EvalResult{bytes: raw, typ: sqltypes.VarBinary}, nil
	case v.IsSigned():
		ival, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalInt64(ival), nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalUint64(uval), nil
	case v.IsFloat() || v.Type() == sqltypes.Decimal:
		fval, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		if v.Type() == sqltypes.Decimal {
			return EvalResult{
				typ:       sqltypes.Decimal,
				numval:    math.Float64bits(fval),
				collation: collationNumeric,
			}, nil
		}
		return newEvalFloat(fval), nil
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
		return EvalResult{numval: uint64(ival), typ: sqltypes.Int64}, nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return EvalResult{numval: uval, typ: sqltypes.Uint64}, nil
	}

	// For other types, do best effort.
	if ival, err := strconv.ParseInt(str, 10, 64); err == nil {
		return EvalResult{numval: uint64(ival), typ: sqltypes.Int64}, nil
	}
	if uval, err := strconv.ParseUint(str, 10, 64); err == nil {
		return EvalResult{numval: uval, typ: sqltypes.Uint64}, nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: '%s'", str)
}

func (v EvalResult) toSQLValue(resultType querypb.Type) sqltypes.Value {
	switch {
	case sqltypes.IsSigned(resultType):
		switch v.typ {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.numval), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.numval), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(math.Float64frombits(v.numval)), 10))
		}
	case sqltypes.IsUnsigned(resultType):
		switch v.typ {
		case sqltypes.Uint64, sqltypes.Uint32, sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.numval), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(math.Float64frombits(v.numval)), 10))
		}
	case sqltypes.IsFloat(resultType) || resultType == sqltypes.Decimal:
		switch v.typ {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.numval), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.numval), 10))
		case sqltypes.Float64, sqltypes.Float32:
			format := byte('g')
			if resultType == sqltypes.Decimal {
				format = 'f'
			}
			return sqltypes.MakeTrusted(resultType, strconv.AppendFloat(nil, math.Float64frombits(v.numval), format, -1, 64))
		}
	default:
		return sqltypes.MakeTrusted(resultType, v.bytes)
	}
	return sqltypes.NULL
}

func numericalHashCode(v EvalResult) HashCode {
	return HashCode(v.numval)
}

func compareNumeric(v1, v2 EvalResult) (int, error) {
	// Equalize the types the same way MySQL does
	// https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
	switch v1.typ {
	case sqltypes.Int64:
		switch v2.typ {
		case sqltypes.Uint64:
			if v1.numval > math.MaxInt64 {
				return -1, nil
			}
			v1 = EvalResult{typ: sqltypes.Uint64, numval: uint64(v1.numval)}
		case sqltypes.Float64, sqltypes.Decimal:
			v1 = EvalResult{typ: v2.typ, numval: math.Float64bits(float64(int64(v1.numval)))}
		}
	case sqltypes.Uint64:
		switch v2.typ {
		case sqltypes.Int64:
			if v2.numval > math.MaxInt64 {
				return 1, nil
			}
			v2 = EvalResult{typ: sqltypes.Uint64, numval: uint64(v2.numval)}
		case sqltypes.Float64, sqltypes.Decimal:
			v1 = EvalResult{typ: v2.typ, numval: math.Float64bits(float64(v1.numval))}
		}
	case sqltypes.Float64:
		switch v2.typ {
		case sqltypes.Int64:
			v2 = EvalResult{typ: sqltypes.Float64, numval: math.Float64bits(float64(int64(v2.numval)))}
		case sqltypes.Uint64:
			if math.Float64frombits(v1.numval) < 0 {
				return -1, nil
			}
			v2 = EvalResult{typ: sqltypes.Float64, numval: math.Float64bits(float64(v2.numval))}
		case sqltypes.Decimal:
			v2.typ = sqltypes.Float64
		}
	case sqltypes.Decimal:
		switch v2.typ {
		case sqltypes.Int64:
			v2 = EvalResult{typ: sqltypes.Decimal, numval: math.Float64bits(float64(int64(v2.numval)))}
		case sqltypes.Uint64:
			if math.Float64frombits(v1.numval) < 0 {
				return -1, nil
			}
			v2 = EvalResult{typ: sqltypes.Decimal, numval: math.Float64bits(float64(v2.numval))}
		case sqltypes.Float64:
			v1.typ = sqltypes.Float64
		}
	}

	// Both values are of the same type.
	switch v1.typ {
	case sqltypes.Int64:
		v1v, v2v := int64(v1.numval), int64(v2.numval)
		switch {
		case v1v == v2v:
			return 0, nil
		case v1v < v2v:
			return -1, nil
		}
	case sqltypes.Uint64:
		switch {
		case v1.numval == v2.numval:
			return 0, nil
		case v1.numval < v2.numval:
			return -1, nil
		}
	case sqltypes.Float64, sqltypes.Decimal:
		v1v, v2v := math.Float64frombits(v1.numval), math.Float64frombits(v2.numval)
		switch {
		case v1v == v2v:
			return 0, nil
		case v1v < v2v:
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

func mergeCollations(left, right EvalResult) (EvalResult, EvalResult, error) {
	if !sqltypes.IsText(left.typ) || !sqltypes.IsText(right.typ) {
		return left, right, nil
	}
	env := collations.Local()
	tc, coerceLeft, coerceRight, err := env.MergeCollations(left.collation, right.collation, collations.CoercionOptions{
		ConvertToSuperset:   true,
		ConvertWithCoercion: true,
	})
	if err != nil {
		return EvalResult{}, EvalResult{}, err
	}
	if coerceLeft != nil {
		left.bytes, _ = coerceLeft(nil, left.bytes)
	}
	if coerceRight != nil {
		right.bytes, _ = coerceRight(nil, right.bytes)
	}
	left.collation = tc
	right.collation = tc
	return left, right, nil
}

func compareTuples(lVal EvalResult, rVal EvalResult) (int, bool, error) {
	if len(*lVal.tuple) != len(*rVal.tuple) {
		return 0, false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", len(*lVal.tuple))
	}
	hasSeenNull := false
	for idx, lResult := range *lVal.tuple {
		rResult := (*rVal.tuple)[idx]
		res, isNull, err := nullSafeCoerceAndCompare(lResult, rResult)
		if isNull {
			hasSeenNull = true
		}
		if res != 0 || err != nil {
			return res, false, err
		}
	}
	return 0, hasSeenNull, nil
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
func compareStrings(l, r EvalResult) int {
	if l.collation.Collation != r.collation.Collation {
		panic("compareStrings: did not coerce")
	}
	collation := collations.Local().LookupByID(l.collation.Collation)
	if collation == nil {
		panic("unknown collation after coercion")
	}
	return collation.Collate(l.bytes, r.bytes, false)
}
