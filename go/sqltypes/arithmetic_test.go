/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestSubtraction(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    Value
		err    error
	}{{

		//All Nulls
		v1:  NULL,
		v2:  NULL,
		out: NULL,
	}, {

		// First value null.
		v1:  NewInt32(1),
		v2:  NULL,
		out: NULL,
	}, {

		// Second value null.
		v1:  NULL,
		v2:  NewInt32(1),
		out: NULL,
	}, {

		// case with negative value
		v1:  NewInt64(-1),
		v2:  NewInt64(-2),
		out: NewInt64(1),
	}, {

		// testing for int64 overflow with min negative value
		v1:  NewInt64(-9223372036854775808),
		v2:  NewInt64(1),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "BIGINT value is out of range in -9223372036854775808 - 1"),
	}, {

		v1:  NewUint64(4),
		v2:  NewInt64(5),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "BIGINT UNSIGNED value is out of range in 4 - 5"),
	}, {

		v1:  NewUint64(7),
		v2:  NewInt64(5),
		out: NewUint64(2),
	}, {

		v1:  NewUint64(18446744073709551615),
		v2:  NewInt64(0),
		out: NewUint64(18446744073709551615),
	}, {

		// testing for int64 overflow
		v1:  NewInt64(-9223372036854775808),
		v2:  NewUint64(0),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "BIGINT UNSIGNED value is out of range in 0 - -9223372036854775808"),
	}}

	for _, tcase := range tcases {

		got, err := Subtraction(tcase.v1, tcase.v2)

		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("Subtraction(%v, %v) error: %v, want %v", printValue(tcase.v1), printValue(tcase.v2), vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("Subtraction(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), printValue(got), printValue(tcase.out))
		}
	}

}

func TestAddition(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    Value
		err    error
	}{{

		//All Nulls
		v1:  NULL,
		v2:  NULL,
		out: NULL,
	}, {
		// First value null.
		v1:  NewInt32(1),
		v2:  NULL,
		out: NULL,
	}, {
		// Second value null.
		v1:  NULL,
		v2:  NewInt32(1),
		out: NULL,
	}, {

		// case with negatives
		v1:  NewInt64(-1),
		v2:  NewInt64(-2),
		out: NewInt64(-3),
	}, {

		// testing for overflow int64
		v1:  NewInt64(9223372036854775807),
		v2:  NewUint64(2),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "BIGINT value is out of range in 2 + 9223372036854775807"),
	}, {

		v1: NewInt64(-2),
		v2: NewUint64(1),
		//out: NewInt64(-1),
		out: NewUint64(18446744073709551615),
	}, {

		v1:  NewInt64(9223372036854775807),
		v2:  NewInt64(-2),
		out: NewInt64(9223372036854775805),
	}, {
		//Normal case
		v1:  NewUint64(1),
		v2:  NewUint64(2),
		out: NewUint64(3),
	}, {
		//testing for overflow uint64
		v1:  NewUint64(18446744073709551615),
		v2:  NewUint64(2),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "BIGINT UNSIGNED value is out of range in 18446744073709551615 + 2"),
	}, {

		//int64 underflow
		v1:  NewInt64(-9223372036854775807),
		v2:  NewInt64(-2),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "BIGINT value is out of range in -9223372036854775807 + -2"),
	}, {

		//checking int64 max value can be returned
		v1:  NewInt64(9223372036854775807),
		v2:  NewUint64(0),
		out: NewUint64(9223372036854775807),
	}, {

		// testing whether uint64 max value can be returned
		v1:  NewUint64(18446744073709551615),
		v2:  NewInt64(0),
		out: NewUint64(18446744073709551615),
	}, {

		v1:  NewInt64(-3),
		v2:  NewUint64(1),
		out: NewUint64(18446744073709551614),
	}, {

		//how is this okay? Because v1 is greater than max int64 value
		v1:  NewUint64(9223372036854775808),
		v2:  NewInt64(1),
		out: NewUint64(9223372036854775809),
	}}

	for _, tcase := range tcases {

		got, err := Addition(tcase.v1, tcase.v2)

		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("Addition(%v, %v) error: %v, want %v", printValue(tcase.v1), printValue(tcase.v2), vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("Addition(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), printValue(got), printValue(tcase.out))
		}
	}

}

func TestAdd(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    Value
		err    error
	}{{
		// All nulls.
		v1:  NULL,
		v2:  NULL,
		out: NewInt64(0),
	}, {
		// First value null.
		v1:  NewInt32(1),
		v2:  NULL,
		out: NewInt64(1),
	}, {
		// Second value null.
		v1:  NULL,
		v2:  NewInt32(1),
		out: NewInt64(1),
	}, {
		// Normal case.
		v1:  NewInt64(1),
		v2:  NewInt64(2),
		out: NewInt64(3),
	}, {
		// Make sure underlying error is returned for LHS.
		v1:  TestValue(Int64, "1.2"),
		v2:  NewInt64(2),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  NewInt64(2),
		v2:  TestValue(Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Make sure underlying error is returned while adding.
		v1:  NewInt64(-1),
		v2:  NewUint64(2),
		out: NewInt64(-9223372036854775808),
	}, {
		// Make sure underlying error is returned while converting.
		v1:  NewFloat64(1),
		v2:  NewFloat64(2),
		out: NewInt64(3),
	}}
	for _, tcase := range tcases {
		got := NullsafeAdd(tcase.v1, tcase.v2, Int64)

		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("Add(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), printValue(got), printValue(tcase.out))
		}
	}
}

func TestNullsafeCompare(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    int
		err    error
	}{{
		// All nulls.
		v1:  NULL,
		v2:  NULL,
		out: 0,
	}, {
		// LHS null.
		v1:  NULL,
		v2:  NewInt64(1),
		out: -1,
	}, {
		// RHS null.
		v1:  NewInt64(1),
		v2:  NULL,
		out: 1,
	}, {
		// LHS Text
		v1:  TestValue(VarChar, "abcd"),
		v2:  TestValue(VarChar, "abcd"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, "types are not comparable: VARCHAR vs VARCHAR"),
	}, {
		// Make sure underlying error is returned for LHS.
		v1:  TestValue(Int64, "1.2"),
		v2:  NewInt64(2),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  NewInt64(2),
		v2:  TestValue(Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Numeric equal.
		v1:  NewInt64(1),
		v2:  NewUint64(1),
		out: 0,
	}, {
		// Numeric unequal.
		v1:  NewInt64(1),
		v2:  NewUint64(2),
		out: -1,
	}, {
		// Non-numeric equal
		v1:  TestValue(VarBinary, "abcd"),
		v2:  TestValue(Binary, "abcd"),
		out: 0,
	}, {
		// Non-numeric unequal
		v1:  TestValue(VarBinary, "abcd"),
		v2:  TestValue(Binary, "bcde"),
		out: -1,
	}, {
		// Date/Time types
		v1:  TestValue(Datetime, "1000-01-01 00:00:00"),
		v2:  TestValue(Binary, "1000-01-01 00:00:00"),
		out: 0,
	}, {
		// Date/Time types
		v1:  TestValue(Datetime, "2000-01-01 00:00:00"),
		v2:  TestValue(Binary, "1000-01-01 00:00:00"),
		out: 1,
	}, {
		// Date/Time types
		v1:  TestValue(Datetime, "1000-01-01 00:00:00"),
		v2:  TestValue(Binary, "2000-01-01 00:00:00"),
		out: -1,
	}}
	for _, tcase := range tcases {
		got, err := NullsafeCompare(tcase.v1, tcase.v2)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("NullsafeCompare(%v, %v) error: %v, want %v", printValue(tcase.v1), printValue(tcase.v2), vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("NullsafeCompare(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), got, tcase.out)
		}
	}
}

func TestCast(t *testing.T) {
	tcases := []struct {
		typ querypb.Type
		v   Value
		out Value
		err error
	}{{
		typ: VarChar,
		v:   NULL,
		out: NULL,
	}, {
		typ: VarChar,
		v:   TestValue(VarChar, "exact types"),
		out: TestValue(VarChar, "exact types"),
	}, {
		typ: Int64,
		v:   TestValue(Int32, "32"),
		out: TestValue(Int64, "32"),
	}, {
		typ: Int24,
		v:   TestValue(Uint64, "64"),
		out: TestValue(Int24, "64"),
	}, {
		typ: Int24,
		v:   TestValue(VarChar, "bad int"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `strconv.ParseInt: parsing "bad int": invalid syntax`),
	}, {
		typ: Uint64,
		v:   TestValue(Uint32, "32"),
		out: TestValue(Uint64, "32"),
	}, {
		typ: Uint24,
		v:   TestValue(Int64, "64"),
		out: TestValue(Uint24, "64"),
	}, {
		typ: Uint24,
		v:   TestValue(Int64, "-1"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `strconv.ParseUint: parsing "-1": invalid syntax`),
	}, {
		typ: Float64,
		v:   TestValue(Int64, "64"),
		out: TestValue(Float64, "64"),
	}, {
		typ: Float32,
		v:   TestValue(Float64, "64"),
		out: TestValue(Float32, "64"),
	}, {
		typ: Float32,
		v:   TestValue(Decimal, "1.24"),
		out: TestValue(Float32, "1.24"),
	}, {
		typ: Float64,
		v:   TestValue(VarChar, "1.25"),
		out: TestValue(Float64, "1.25"),
	}, {
		typ: Float64,
		v:   TestValue(VarChar, "bad float"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `strconv.ParseFloat: parsing "bad float": invalid syntax`),
	}, {
		typ: VarChar,
		v:   TestValue(Int64, "64"),
		out: TestValue(VarChar, "64"),
	}, {
		typ: VarBinary,
		v:   TestValue(Float64, "64"),
		out: TestValue(VarBinary, "64"),
	}, {
		typ: VarBinary,
		v:   TestValue(Decimal, "1.24"),
		out: TestValue(VarBinary, "1.24"),
	}, {
		typ: VarBinary,
		v:   TestValue(VarChar, "1.25"),
		out: TestValue(VarBinary, "1.25"),
	}, {
		typ: VarChar,
		v:   TestValue(VarBinary, "valid string"),
		out: TestValue(VarChar, "valid string"),
	}, {
		typ: VarChar,
		v:   TestValue(Expression, "bad string"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "EXPRESSION(bad string) cannot be cast to VARCHAR"),
	}}
	for _, tcase := range tcases {
		got, err := Cast(tcase.v, tcase.typ)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("Cast(%v) error: %v, want %v", tcase.v, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("Cast(%v): %v, want %v", tcase.v, got, tcase.out)
		}
	}
}

func TestToUint64(t *testing.T) {
	tcases := []struct {
		v   Value
		out uint64
		err error
	}{{
		v:   TestValue(VarChar, "abcd"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: 'abcd'"),
	}, {
		v:   NewInt64(-1),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "negative number cannot be converted to unsigned: -1"),
	}, {
		v:   NewInt64(1),
		out: 1,
	}, {
		v:   NewUint64(1),
		out: 1,
	}}
	for _, tcase := range tcases {
		got, err := ToUint64(tcase.v)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("ToUint64(%v) error: %v, want %v", tcase.v, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("ToUint64(%v): %v, want %v", tcase.v, got, tcase.out)
		}
	}
}

func TestToInt64(t *testing.T) {
	tcases := []struct {
		v   Value
		out int64
		err error
	}{{
		v:   TestValue(VarChar, "abcd"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: 'abcd'"),
	}, {
		v:   NewUint64(18446744073709551615),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsigned number overflows int64 value: 18446744073709551615"),
	}, {
		v:   NewInt64(1),
		out: 1,
	}, {
		v:   NewUint64(1),
		out: 1,
	}}
	for _, tcase := range tcases {
		got, err := ToInt64(tcase.v)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("ToInt64(%v) error: %v, want %v", tcase.v, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("ToInt64(%v): %v, want %v", tcase.v, got, tcase.out)
		}
	}
}

func TestToFloat64(t *testing.T) {
	tcases := []struct {
		v   Value
		out float64
		err error
	}{{
		v:   TestValue(VarChar, "abcd"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: 'abcd'"),
	}, {
		v:   NewInt64(1),
		out: 1,
	}, {
		v:   NewUint64(1),
		out: 1,
	}, {
		v:   NewFloat64(1.2),
		out: 1.2,
	}}
	for _, tcase := range tcases {
		got, err := ToFloat64(tcase.v)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("ToFloat64(%v) error: %v, want %v", tcase.v, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("ToFloat64(%v): %v, want %v", tcase.v, got, tcase.out)
		}
	}
}

func TestToNative(t *testing.T) {
	testcases := []struct {
		in  Value
		out interface{}
	}{{
		in:  NULL,
		out: nil,
	}, {
		in:  TestValue(Int8, "1"),
		out: int64(1),
	}, {
		in:  TestValue(Int16, "1"),
		out: int64(1),
	}, {
		in:  TestValue(Int24, "1"),
		out: int64(1),
	}, {
		in:  TestValue(Int32, "1"),
		out: int64(1),
	}, {
		in:  TestValue(Int64, "1"),
		out: int64(1),
	}, {
		in:  TestValue(Uint8, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(Uint16, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(Uint24, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(Uint32, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(Uint64, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(Float32, "1"),
		out: float64(1),
	}, {
		in:  TestValue(Float64, "1"),
		out: float64(1),
	}, {
		in:  TestValue(Timestamp, "2012-02-24 23:19:43"),
		out: []byte("2012-02-24 23:19:43"),
	}, {
		in:  TestValue(Date, "2012-02-24"),
		out: []byte("2012-02-24"),
	}, {
		in:  TestValue(Time, "23:19:43"),
		out: []byte("23:19:43"),
	}, {
		in:  TestValue(Datetime, "2012-02-24 23:19:43"),
		out: []byte("2012-02-24 23:19:43"),
	}, {
		in:  TestValue(Year, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(Decimal, "1"),
		out: []byte("1"),
	}, {
		in:  TestValue(Text, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(Blob, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(VarChar, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(VarBinary, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(Char, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(Binary, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(Bit, "1"),
		out: []byte("1"),
	}, {
		in:  TestValue(Enum, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(Set, "a"),
		out: []byte("a"),
	}}
	for _, tcase := range testcases {
		v, err := ToNative(tcase.in)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(v, tcase.out) {
			t.Errorf("%v.ToNative = %#v, want %#v", tcase.in, v, tcase.out)
		}
	}

	// Test Expression failure.
	_, err := ToNative(TestValue(Expression, "aa"))
	want := vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "EXPRESSION(aa) cannot be converted to a go type")
	if !vterrors.Equals(err, want) {
		t.Errorf("ToNative(EXPRESSION): %v, want %v", vterrors.Print(err), vterrors.Print(want))
	}
}

func TestNewNumeric(t *testing.T) {
	tcases := []struct {
		v   Value
		out numeric
		err error
	}{{
		v:   NewInt64(1),
		out: numeric{typ: Int64, ival: 1},
	}, {
		v:   NewUint64(1),
		out: numeric{typ: Uint64, uval: 1},
	}, {
		v:   NewFloat64(1),
		out: numeric{typ: Float64, fval: 1},
	}, {
		// For non-number type, Int64 is the default.
		v:   TestValue(VarChar, "1"),
		out: numeric{typ: Int64, ival: 1},
	}, {
		// If Int64 can't work, we use Float64.
		v:   TestValue(VarChar, "1.2"),
		out: numeric{typ: Float64, fval: 1.2},
	}, {
		// Only valid Int64 allowed if type is Int64.
		v:   TestValue(Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Only valid Uint64 allowed if type is Uint64.
		v:   TestValue(Uint64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseUint: parsing \"1.2\": invalid syntax"),
	}, {
		// Only valid Float64 allowed if type is Float64.
		v:   TestValue(Float64, "abcd"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseFloat: parsing \"abcd\": invalid syntax"),
	}, {
		v:   TestValue(VarChar, "abcd"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: 'abcd'"),
	}}
	for _, tcase := range tcases {
		got, err := newNumeric(tcase.v)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("newNumeric(%s) error: %v, want %v", printValue(tcase.v), vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err == nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("newNumeric(%s): %v, want %v", printValue(tcase.v), got, tcase.out)
		}
	}
}

func TestNewIntegralNumeric(t *testing.T) {
	tcases := []struct {
		v   Value
		out numeric
		err error
	}{{
		v:   NewInt64(1),
		out: numeric{typ: Int64, ival: 1},
	}, {
		v:   NewUint64(1),
		out: numeric{typ: Uint64, uval: 1},
	}, {
		v:   NewFloat64(1),
		out: numeric{typ: Int64, ival: 1},
	}, {
		// For non-number type, Int64 is the default.
		v:   TestValue(VarChar, "1"),
		out: numeric{typ: Int64, ival: 1},
	}, {
		// If Int64 can't work, we use Uint64.
		v:   TestValue(VarChar, "18446744073709551615"),
		out: numeric{typ: Uint64, uval: 18446744073709551615},
	}, {
		// Only valid Int64 allowed if type is Int64.
		v:   TestValue(Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Only valid Uint64 allowed if type is Uint64.
		v:   TestValue(Uint64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseUint: parsing \"1.2\": invalid syntax"),
	}, {
		v:   TestValue(VarChar, "abcd"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: 'abcd'"),
	}}
	for _, tcase := range tcases {
		got, err := newIntegralNumeric(tcase.v)
		if err != nil && !vterrors.Equals(err, tcase.err) {
			t.Errorf("newIntegralNumeric(%s) error: %v, want %v", printValue(tcase.v), vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err == nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("newIntegralNumeric(%s): %v, want %v", printValue(tcase.v), got, tcase.out)
		}
	}
}

func TestAddNumeric(t *testing.T) {
	tcases := []struct {
		v1, v2 numeric
		out    numeric
		err    error
	}{{
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: numeric{typ: Int64, ival: 3},
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Uint64, uval: 3},
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: numeric{typ: Float64, fval: 3},
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Uint64, uval: 3},
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: numeric{typ: Float64, fval: 3},
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: numeric{typ: Float64, fval: 3},
	}, {
		// Int64 overflow.
		v1:  numeric{typ: Int64, ival: 9223372036854775807},
		v2:  numeric{typ: Int64, ival: 2},
		out: numeric{typ: Float64, fval: 9223372036854775809},
	}, {
		// Int64 underflow.
		v1:  numeric{typ: Int64, ival: -9223372036854775807},
		v2:  numeric{typ: Int64, ival: -2},
		out: numeric{typ: Float64, fval: -9223372036854775809},
	}, {
		v1:  numeric{typ: Int64, ival: -1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Float64, fval: 18446744073709551617},
		//err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "cannot add a negative number to an unsigned integer: 2, -1"),
	}, {
		// Uint64 overflow.
		v1:  numeric{typ: Uint64, uval: 18446744073709551615},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Float64, fval: 18446744073709551617},
	}}
	for _, tcase := range tcases {
		got := addNumeric(tcase.v1, tcase.v2)
		/*
			if !vterrors.Equals(err, tcase.err) {
				t.Errorf("addNumeric(%v, %v) error: %v, want %v", tcase.v1, tcase.v2, vterrors.Print(err), vterrors.Print(tcase.err))
			}
			if tcase.err != nil {
				continue
			}
		*/

		if got != tcase.out {
			t.Errorf("addNumeric(%v, %v): %v, want %v", tcase.v1, tcase.v2, got, tcase.out)
		}
	}
}

func TestPrioritize(t *testing.T) {
	ival := numeric{typ: Int64}
	uval := numeric{typ: Uint64}
	fval := numeric{typ: Float64}

	tcases := []struct {
		v1, v2     numeric
		out1, out2 numeric
	}{{
		v1:   ival,
		v2:   uval,
		out1: uval,
		out2: ival,
	}, {
		v1:   ival,
		v2:   fval,
		out1: fval,
		out2: ival,
	}, {
		v1:   uval,
		v2:   ival,
		out1: uval,
		out2: ival,
	}, {
		v1:   uval,
		v2:   fval,
		out1: fval,
		out2: uval,
	}, {
		v1:   fval,
		v2:   ival,
		out1: fval,
		out2: ival,
	}, {
		v1:   fval,
		v2:   uval,
		out1: fval,
		out2: uval,
	}}
	for _, tcase := range tcases {
		got1, got2 := prioritize(tcase.v1, tcase.v2)
		if got1 != tcase.out1 || got2 != tcase.out2 {
			t.Errorf("prioritize(%v, %v): (%v, %v) , want (%v, %v)", tcase.v1.typ, tcase.v2.typ, got1.typ, got2.typ, tcase.out1.typ, tcase.out2.typ)
		}
	}
}

func TestCastFromNumeric(t *testing.T) {
	tcases := []struct {
		typ querypb.Type
		v   numeric
		out Value
		err error
	}{{
		typ: Int64,
		v:   numeric{typ: Int64, ival: 1},
		out: NewInt64(1),
	}, {
		typ: Int64,
		v:   numeric{typ: Uint64, uval: 1},
		out: NewInt64(1),
		//err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected type conversion: UINT64 to INT64"),
	}, {
		typ: Int64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: NewInt64(0),
		//err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected type conversion: FLOAT64 to INT64"),
	}, {
		typ: Uint64,
		v:   numeric{typ: Int64, ival: 1},
		out: NewUint64(1),
		//err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected type conversion: INT64 to UINT64"),
	}, {
		typ: Uint64,
		v:   numeric{typ: Uint64, uval: 1},
		out: NewUint64(1),
	}, {
		typ: Uint64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: NewUint64(0),
		//err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected type conversion: FLOAT64 to UINT64"),
	}, {
		typ: Float64,
		v:   numeric{typ: Int64, ival: 1},
		out: TestValue(Float64, "1"),
	}, {
		typ: Float64,
		v:   numeric{typ: Uint64, uval: 1},
		out: TestValue(Float64, "1"),
	}, {
		typ: Float64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: TestValue(Float64, "1.2e-16"),
	}, {
		typ: Decimal,
		v:   numeric{typ: Int64, ival: 1},
		out: TestValue(Decimal, "1"),
	}, {
		typ: Decimal,
		v:   numeric{typ: Uint64, uval: 1},
		out: TestValue(Decimal, "1"),
	}, {
		// For float, we should not use scientific notation.
		typ: Decimal,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: TestValue(Decimal, "0.00000000000000012"),
	}, {
		typ: VarBinary,
		v:   numeric{typ: Int64, ival: 1},
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected type conversion to non-numeric: VARBINARY"),
	}}
	for _, tcase := range tcases {
		got := castFromNumeric(tcase.v, tcase.typ)
		/*
			if !vterrors.Equals(err, tcase.err) {
				t.Errorf("castFromNumeric(%v, %v) error: %v, want %v", tcase.v, tcase.typ, vterrors.Print(err), vterrors.Print(tcase.err))
			}
			if tcase.err != nil {
				continue
			}
		*/

		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("castFromNumeric(%v, %v): %v, want %v", tcase.v, tcase.typ, printValue(got), printValue(tcase.out))
		}
	}
}

func TestCompareNumeric(t *testing.T) {
	tcases := []struct {
		v1, v2 numeric
		out    int
	}{{
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Int64, ival: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 2},
		v2:  numeric{typ: Int64, ival: 1},
		out: 1,
	}, {
		// Special case.
		v1:  numeric{typ: Int64, ival: -1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 2},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Float64, fval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 2},
		v2:  numeric{typ: Float64, fval: 1},
		out: 1,
	}, {
		// Special case.
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Int64, ival: -1},
		out: 1,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Int64, ival: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Uint64, uval: 2},
		v2:  numeric{typ: Int64, ival: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Uint64, uval: 2},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Float64, fval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Uint64, uval: 2},
		v2:  numeric{typ: Float64, fval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Int64, ival: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Float64, fval: 2},
		v2:  numeric{typ: Int64, ival: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Float64, fval: 2},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Float64, fval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Float64, fval: 2},
		v2:  numeric{typ: Float64, fval: 1},
		out: 1,
	}}
	for _, tcase := range tcases {
		got := compareNumeric(tcase.v1, tcase.v2)
		if got != tcase.out {
			t.Errorf("equalNumeric(%v, %v): %v, want %v", tcase.v1, tcase.v2, got, tcase.out)
		}
	}
}

func TestMin(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		min    Value
		err    error
	}{{
		v1:  NULL,
		v2:  NULL,
		min: NULL,
	}, {
		v1:  NewInt64(1),
		v2:  NULL,
		min: NewInt64(1),
	}, {
		v1:  NULL,
		v2:  NewInt64(1),
		min: NewInt64(1),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(2),
		min: NewInt64(1),
	}, {
		v1:  NewInt64(2),
		v2:  NewInt64(1),
		min: NewInt64(1),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(1),
		min: NewInt64(1),
	}, {
		v1:  TestValue(VarChar, "aa"),
		v2:  TestValue(VarChar, "aa"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, "types are not comparable: VARCHAR vs VARCHAR"),
	}}
	for _, tcase := range tcases {
		v, err := Min(tcase.v1, tcase.v2)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("Min error: %v, want %v", vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if !reflect.DeepEqual(v, tcase.min) {
			t.Errorf("Min(%v, %v): %v, want %v", tcase.v1, tcase.v2, v, tcase.min)
		}
	}
}

func TestMax(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		max    Value
		err    error
	}{{
		v1:  NULL,
		v2:  NULL,
		max: NULL,
	}, {
		v1:  NewInt64(1),
		v2:  NULL,
		max: NewInt64(1),
	}, {
		v1:  NULL,
		v2:  NewInt64(1),
		max: NewInt64(1),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(2),
		max: NewInt64(2),
	}, {
		v1:  NewInt64(2),
		v2:  NewInt64(1),
		max: NewInt64(2),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(1),
		max: NewInt64(1),
	}, {
		v1:  TestValue(VarChar, "aa"),
		v2:  TestValue(VarChar, "aa"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, "types are not comparable: VARCHAR vs VARCHAR"),
	}}
	for _, tcase := range tcases {
		v, err := Max(tcase.v1, tcase.v2)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("Max error: %v, want %v", vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if !reflect.DeepEqual(v, tcase.max) {
			t.Errorf("Max(%v, %v): %v, want %v", tcase.v1, tcase.v2, v, tcase.max)
		}
	}
}

func printValue(v Value) string {
	return fmt.Sprintf("%v:%q", v.typ, v.val)
}

// These benchmarks show that using existing ASCII representations
// for numbers is about 6x slower than using native representations.
// However, 229ns is still a negligible time compared to the cost of
// other operations. The additional complexity of introducing native
// types is currently not worth it. So, we'll stay with the existing
// ASCII representation for now. Using interfaces is more expensive
// than native representation of values. This is probably because
// interfaces also allocate memory, and also perform type assertions.
// Actual benchmark is based on NoNative. So, the numbers are similar.
// Date: 6/4/17
// Version: go1.8
// BenchmarkAddActual-8            10000000               263 ns/op
// BenchmarkAddNoNative-8          10000000               228 ns/op
// BenchmarkAddNative-8            50000000                40.0 ns/op
// BenchmarkAddGoInterface-8       30000000                52.4 ns/op
// BenchmarkAddGoNonInterface-8    2000000000               1.00 ns/op
// BenchmarkAddGo-8                2000000000               1.00 ns/op
func BenchmarkAddActual(b *testing.B) {
	v1 := MakeTrusted(Int64, []byte("1"))
	v2 := MakeTrusted(Int64, []byte("12"))
	for i := 0; i < b.N; i++ {
		v1 = NullsafeAdd(v1, v2, Int64)
	}
}

func BenchmarkAddNoNative(b *testing.B) {
	v1 := MakeTrusted(Int64, []byte("1"))
	v2 := MakeTrusted(Int64, []byte("12"))
	for i := 0; i < b.N; i++ {
		iv1, _ := ToInt64(v1)
		iv2, _ := ToInt64(v2)
		v1 = MakeTrusted(Int64, strconv.AppendInt(nil, iv1+iv2, 10))
	}
}

func BenchmarkAddNative(b *testing.B) {
	v1 := makeNativeInt64(1)
	v2 := makeNativeInt64(12)
	for i := 0; i < b.N; i++ {
		iv1 := int64(binary.BigEndian.Uint64(v1.Raw()))
		iv2 := int64(binary.BigEndian.Uint64(v2.Raw()))
		v1 = makeNativeInt64(iv1 + iv2)
	}
}

func makeNativeInt64(v int64) Value {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return MakeTrusted(Int64, buf)
}

func BenchmarkAddGoInterface(b *testing.B) {
	var v1, v2 interface{}
	v1 = int64(1)
	v2 = int64(2)
	for i := 0; i < b.N; i++ {
		v1 = v1.(int64) + v2.(int64)
	}
}

func BenchmarkAddGoNonInterface(b *testing.B) {
	v1 := numeric{typ: Int64, ival: 1}
	v2 := numeric{typ: Int64, ival: 12}
	for i := 0; i < b.N; i++ {
		if v1.typ != Int64 {
			b.Error("type assertion failed")
		}
		if v2.typ != Int64 {
			b.Error("type assertion failed")
		}
		v1 = numeric{typ: Int64, ival: v1.ival + v2.ival}
	}
}

func BenchmarkAddGo(b *testing.B) {
	v1 := int64(1)
	v2 := int64(2)
	for i := 0; i < b.N; i++ {
		v1 += v2
	}
}
