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
	"bytes"
	"time"

	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func compareNumeric(left, right eval) (int, error) {
	// Equalize the types the same way MySQL does
	// https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
	switch l := left.(type) {
	case *evalInt64:
		switch right.(type) {
		case *evalUint64:
			if l.i < 0 {
				return -1, nil
			}
			left = newEvalUint64(uint64(l.i))
		case *evalFloat:
			left = newEvalFloat(float64(l.i))
		case *evalDecimal:
			left = newEvalDecimalWithPrec(decimal.NewFromInt(l.i), 0)
		}
	case *evalUint64:
		switch r := right.(type) {
		case *evalInt64:
			if r.i < 0 {
				return 1, nil
			}
			right = newEvalUint64(uint64(r.i))
		case *evalFloat:
			left = newEvalFloat(float64(l.u))
		case *evalDecimal:
			left = newEvalDecimalWithPrec(decimal.NewFromUint(l.u), 0)
		}
	case *evalFloat:
		switch r := right.(type) {
		case *evalInt64:
			right = newEvalFloat(float64(r.i))
		case *evalUint64:
			if l.f < 0 {
				return -1, nil
			}
			right = newEvalFloat(float64(r.u))
		case *evalDecimal:
			f, ok := r.dec.Float64()
			if !ok {
				return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
			}
			right = newEvalFloat(f)
		}
	case *evalDecimal:
		switch r := right.(type) {
		case *evalInt64:
			right = newEvalDecimalWithPrec(decimal.NewFromInt(r.i), 0)
		case *evalUint64:
			right = newEvalDecimalWithPrec(decimal.NewFromUint(r.u), 0)
		case *evalFloat:
			f, ok := l.dec.Float64()
			if !ok {
				return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
			}
			left = newEvalFloat(f)
		}
	}

	// Both values are of the same type.
	switch l := left.(type) {
	case *evalInt64:
		i1, i2 := l.i, right.(*evalInt64).i
		switch {
		case i1 == i2:
			return 0, nil
		case i1 < i2:
			return -1, nil
		}
	case *evalUint64:
		u1, u2 := l.u, right.(*evalUint64).u
		switch {
		case u1 == u2:
			return 0, nil
		case u1 < u2:
			return -1, nil
		}
	case *evalFloat:
		f1, f2 := l.f, right.(*evalFloat).f
		switch {
		case f1 == f2:
			return 0, nil
		case f1 < f2:
			return -1, nil
		}
	case *evalDecimal:
		return l.dec.Cmp(right.(*evalDecimal).dec), nil
	}
	return 1, nil
}

// matchExprWithAnyDateFormat formats the given expr (usually a string) to a date using the first format
// that does not return an error.
func matchExprWithAnyDateFormat(e eval) (t time.Time, err error) {
	expr := e.(*evalBytes)
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
func compareDates(l, r eval) (int, error) {
	lTime, err := l.(*evalBytes).parseDate()
	if err != nil {
		return 0, err
	}
	rTime, err := r.(*evalBytes).parseDate()
	if err != nil {
		return 0, err
	}
	return compareGoTimes(lTime, rTime)
}

func compareDateAndString(l, r eval) (int, error) {
	lb := l.(*evalBytes)
	rb := r.(*evalBytes)

	var lTime, rTime time.Time
	var err error
	switch {
	case sqltypes.IsDate(lb.SQLType()):
		lTime, err = lb.parseDate()
		if err != nil {
			return 0, err
		}
		rTime, err = matchExprWithAnyDateFormat(r)
		if err != nil {
			return 0, err
		}
	case typeIsTextual(lb.SQLType()):
		lTime, err = matchExprWithAnyDateFormat(l)
		if err != nil {
			return 0, err
		}
		rTime, err = rb.parseDate()
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
func compareStrings(l, r eval) (int, error) {
	l, r, col, err := mergeCollations(l, r)
	if err != nil {
		return 0, err
	}
	collation := col.Get()
	if collation == nil {
		panic("unknown collation after coercion")
	}
	return collation.Collate(l.(*evalBytes).bytes, r.(*evalBytes).bytes, false), nil
}

func compareJSON(l, r eval) (int, error) {
	lj, err := argToJSON(l)
	if err != nil {
		return 0, err
	}

	rj, err := argToJSON(r)
	if err != nil {
		return 0, err
	}

	return compareJSONValue(lj, rj)
}

// compareJSONValue compares two JSON values.
// See https://dev.mysql.com/doc/refman/8.0/en/json.html#json-comparison for all the rules.
func compareJSONValue(lj, rj *json.Value) (int, error) {
	cmp := int(lj.Type()) - int(rj.Type())
	if cmp != 0 {
		return cmp, nil
	}

	switch lj.Type() {
	case json.TypeNull:
		return 0, nil
	case json.TypeNumber:
		ld, ok := lj.Decimal()
		if !ok {
			return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
		}
		rd, ok := rj.Decimal()
		if !ok {
			return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
		}
		return ld.Cmp(rd), nil
	case json.TypeString:
		return collationJSON.Collation.Get().Collate(lj.ToRawBytes(), rj.ToRawBytes(), false), nil
	case json.TypeBlob, json.TypeBit, json.TypeOpaque:
		return bytes.Compare(lj.ToUnencodedBytes(), rj.ToUnencodedBytes()), nil
	case json.TypeBoolean:
		if lj == rj {
			return 0, nil
		}
		if lj == json.ValueFalse {
			return -1, nil
		}
		return 1, nil
	case json.TypeDate:
		ld, _ := lj.Date()
		rd, _ := rj.Date()
		return ld.Compare(rd), nil
	case json.TypeDateTime:
		ld, _ := lj.DateTime()
		rd, _ := rj.DateTime()
		return ld.Compare(rd), nil
	case json.TypeTime:
		ld, _ := lj.Time()
		rd, _ := rj.Time()
		return ld.Compare(rd), nil
	case json.TypeArray:
		la, _ := lj.Array()
		ra, _ := rj.Array()
		until := len(la)
		if len(la) > len(ra) {
			until = len(ra)
		}
		for i := 0; i < until; i++ {
			cmp, err := compareJSONValue(la[i], ra[i])
			if err != nil {
				return 0, err
			}
			if cmp != 0 {
				return cmp, nil
			}
		}
		return len(la) - len(ra), nil
	case json.TypeObject:
		// These rules are not documented but this is the so far
		// best effort reverse engineered implementation based on
		// what MySQL returns in our tests.
		lo, _ := lj.Object()
		ro, _ := rj.Object()

		if lo.Len() != ro.Len() {
			return lo.Len() - ro.Len(), nil
		}

		rks := ro.Keys()
		lks := lo.Keys()

		for i := 0; i < len(lks); i++ {
			if lks[i] < rks[i] {
				return -1, nil
			}
			if lks[i] > rks[i] {
				return 1, nil
			}
		}

		for i := 0; i < len(lks); i++ {
			cmp, err := compareJSONValue(lo.Get(lks[i]), ro.Get(rks[i]))
			if err != nil {
				return 0, err
			}
			if cmp != 0 {
				return cmp, nil
			}
		}
		return 0, nil
	}

	return cmp, nil
}
