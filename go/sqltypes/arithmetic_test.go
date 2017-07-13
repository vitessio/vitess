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

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestAdd(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    Value
		err    string
	}{{
		// All nulls.
		v1:  NULL,
		v2:  NULL,
		out: NULL,
	}, {
		// First value null.
		v1:  makeInt(1),
		v2:  NULL,
		out: makeInt(1),
	}, {
		// Second value null.
		v1:  NULL,
		v2:  makeInt(1),
		out: makeInt(1),
	}, {
		// Normal case.
		v1:  makeInt(1),
		v2:  makeInt(2),
		out: makeInt(3),
	}, {
		// Make sure underlying error is returned for LHS.
		v1:  makeAny(Int64, "1.2"),
		v2:  makeInt(2),
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  makeInt(2),
		v2:  makeAny(Int64, "1.2"),
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned while adding.
		v1:  makeInt(-1),
		v2:  makeUint(2),
		err: "cannot add a negative number to an unsigned integer: 2, -1",
	}, {
		// Make sure underlying error is returned while converting.
		v1:  makeFloat(1),
		v2:  makeFloat(2),
		err: "unexpected type conversion: FLOAT64 to INT64",
	}}
	for _, tcase := range tcases {
		got, err := NullsafeAdd(tcase.v1, tcase.v2, Int64)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("Add(%v, %v) error: %v, want %v", printValue(tcase.v1), printValue(tcase.v2), err, tcase.err)
		}
		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("Add(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), printValue(got), printValue(tcase.out))
		}
	}
}

func TestNullsafeCompare(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    int
		err    string
	}{{
		// All nulls.
		v1:  NULL,
		v2:  NULL,
		out: 0,
	}, {
		// LHS null.
		v1:  NULL,
		v2:  makeInt(1),
		out: -1,
	}, {
		// RHS null.
		v1:  makeInt(1),
		v2:  NULL,
		out: 1,
	}, {
		// LHS Text
		v1:  makeAny(VarChar, "abcd"),
		v2:  makeInt(1),
		err: "text fields cannot be compared",
	}, {
		// Make sure underlying error is returned for LHS.
		v1:  makeAny(Int64, "1.2"),
		v2:  makeInt(2),
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  makeInt(2),
		v2:  makeAny(Int64, "1.2"),
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Numeric equal.
		v1:  makeInt(1),
		v2:  makeUint(1),
		out: 0,
	}, {
		// Numeric unequal.
		v1:  makeInt(1),
		v2:  makeUint(2),
		out: -1,
	}, {
		// Non-numeric equal
		v1:  makeAny(VarBinary, "abcd"),
		v2:  makeAny(Binary, "abcd"),
		out: 0,
	}, {
		// Non-numeric unequal
		v1:  makeAny(VarBinary, "abcd"),
		v2:  makeAny(Binary, "bcde"),
		out: -1,
	}}
	for _, tcase := range tcases {
		got, err := NullsafeCompare(tcase.v1, tcase.v2)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("NullsafeLess(%v, %v) error: %v, want %v", printValue(tcase.v1), printValue(tcase.v2), err, tcase.err)
		}
		if got != tcase.out {
			t.Errorf("NullsafeLess(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), got, tcase.out)
		}
	}
}

func TestConvertToUint64(t *testing.T) {
	tcases := []struct {
		v   interface{}
		out uint64
		err string
	}{{
		v:   int(1),
		out: 1,
	}, {
		v:   int8(1),
		out: 1,
	}, {
		v:   int16(1),
		out: 1,
	}, {
		v:   int32(1),
		out: 1,
	}, {
		v:   int64(1),
		out: 1,
	}, {
		v:   int64(1),
		out: 1,
	}, {
		v:   int64(-1),
		err: "getNumber: negative number cannot be converted to unsigned: -1",
	}, {
		v:   uint(1),
		out: 1,
	}, {
		v:   uint8(1),
		out: 1,
	}, {
		v:   uint16(1),
		out: 1,
	}, {
		v:   uint32(1),
		out: 1,
	}, {
		v:   uint64(1),
		out: 1,
	}, {
		v:   []byte("1"),
		out: 1,
	}, {
		v:   "1",
		out: 1,
	}, {
		v:   makeInt(1),
		out: 1,
	}, {
		v:   &querypb.BindVariable{Type: Int64, Value: []byte("1")},
		out: 1,
	}, {
		v:   nil,
		err: "getNumber: unexpected type for <nil>: <nil>",
	}, {
		v:   makeAny(VarChar, "abcd"),
		err: "could not parse value: abcd",
	}, {
		v:   makeInt(-1),
		err: "getNumber: negative number cannot be converted to unsigned: -1",
	}, {
		v:   makeUint(1),
		out: 1,
	}}
	for _, tcase := range tcases {
		got, err := ConvertToUint64(tcase.v)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("ConvertToUint64(%v) error: %v, want %v", tcase.v, err, tcase.err)
		}
		if got != tcase.out {
			t.Errorf("ConvertToUint64(%v): %v, want %v", tcase.v, got, tcase.out)
		}
	}
}

func TestNewNumeric(t *testing.T) {
	tcases := []struct {
		v   Value
		out numeric
		err string
	}{{
		v:   makeInt(1),
		out: numeric{typ: Int64, ival: 1},
	}, {
		v:   makeUint(1),
		out: numeric{typ: Uint64, uval: 1},
	}, {
		v:   makeFloat(1),
		out: numeric{typ: Float64, fval: 1},
	}, {
		// For non-number type, Int64 is the default.
		v:   makeAny(VarChar, "1"),
		out: numeric{typ: Int64, ival: 1},
	}, {
		// If Int64 can't work, we use Float64.
		v:   makeAny(VarChar, "1.2"),
		out: numeric{typ: Float64, fval: 1.2},
	}, {
		// Only valid Int64 allowed if type is Int64.
		v:   makeAny(Int64, "1.2"),
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Only valid Uint64 allowed if type is Uint64.
		v:   makeAny(Uint64, "1.2"),
		err: "strconv.ParseUint: parsing \"1.2\": invalid syntax",
	}, {
		// Only valid Float64 allowed if type is Float64.
		v:   makeAny(Float64, "abcd"),
		err: "strconv.ParseFloat: parsing \"abcd\": invalid syntax",
	}, {
		v:   makeAny(VarChar, "abcd"),
		err: "could not parse value: abcd",
	}}
	for _, tcase := range tcases {
		got, err := newNumeric(tcase.v)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("newNumeric(%s) error: %v, want %v", printValue(tcase.v), err, tcase.err)
		}
		if tcase.err == "" && got != tcase.out {
			t.Errorf("newNumeric(%s): %v, want %v", printValue(tcase.v), got, tcase.out)
		}
	}
}

func TestNewIntegralNumeric(t *testing.T) {
	tcases := []struct {
		v   Value
		out numeric
		err string
	}{{
		v:   makeInt(1),
		out: numeric{typ: Int64, ival: 1},
	}, {
		v:   makeUint(1),
		out: numeric{typ: Uint64, uval: 1},
	}, {
		v:   makeFloat(1),
		out: numeric{typ: Int64, ival: 1},
	}, {
		// For non-number type, Int64 is the default.
		v:   makeAny(VarChar, "1"),
		out: numeric{typ: Int64, ival: 1},
	}, {
		// If Int64 can't work, we use Uint64.
		v:   makeAny(VarChar, "18446744073709551615"),
		out: numeric{typ: Uint64, uval: 18446744073709551615},
	}, {
		// Only valid Int64 allowed if type is Int64.
		v:   makeAny(Int64, "1.2"),
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Only valid Uint64 allowed if type is Uint64.
		v:   makeAny(Uint64, "1.2"),
		err: "strconv.ParseUint: parsing \"1.2\": invalid syntax",
	}, {
		v:   makeAny(VarChar, "abcd"),
		err: "could not parse value: abcd",
	}}
	for _, tcase := range tcases {
		got, err := newIntegralNumeric(tcase.v)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("newIntegralNumeric(%s) error: %v, want %v", printValue(tcase.v), err, tcase.err)
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
		err    string
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
		err: "cannot add a negative number to an unsigned integer: 2, -1",
	}, {
		// Uint64 overflow.
		v1:  numeric{typ: Uint64, uval: 18446744073709551615},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Float64, fval: 18446744073709551617},
	}}
	for _, tcase := range tcases {
		got, err := addNumeric(tcase.v1, tcase.v2)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("addNumeric(%v, %v) error: %v, want %v", tcase.v1, tcase.v2, err, tcase.err)
		}
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
		err string
	}{{
		typ: Int64,
		v:   numeric{typ: Int64, ival: 1},
		out: makeInt(1),
	}, {
		typ: Int64,
		v:   numeric{typ: Uint64, uval: 1},
		err: "unexpected type conversion: UINT64 to INT64",
	}, {
		typ: Int64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		err: "unexpected type conversion: FLOAT64 to INT64",
	}, {
		typ: Uint64,
		v:   numeric{typ: Int64, ival: 1},
		err: "unexpected type conversion: INT64 to UINT64",
	}, {
		typ: Uint64,
		v:   numeric{typ: Uint64, uval: 1},
		out: makeUint(1),
	}, {
		typ: Uint64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		err: "unexpected type conversion: FLOAT64 to UINT64",
	}, {
		typ: Float64,
		v:   numeric{typ: Int64, ival: 1},
		out: makeAny(Float64, "1"),
	}, {
		typ: Float64,
		v:   numeric{typ: Uint64, uval: 1},
		out: makeAny(Float64, "1"),
	}, {
		typ: Float64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: makeAny(Float64, "1.2e-16"),
	}, {
		typ: Decimal,
		v:   numeric{typ: Int64, ival: 1},
		out: makeAny(Decimal, "1"),
	}, {
		typ: Decimal,
		v:   numeric{typ: Uint64, uval: 1},
		out: makeAny(Decimal, "1"),
	}, {
		// For float, we should not use scientific notation.
		typ: Decimal,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: makeAny(Decimal, "0.00000000000000012"),
	}, {
		typ: VarBinary,
		v:   numeric{typ: Int64, ival: 1},
		err: "unexpected type conversion to non-numeric: VARBINARY",
	}}
	for _, tcase := range tcases {
		got, err := castFromNumeric(tcase.v, tcase.typ)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("castFromNumeric(%v, %v) error: %v, want %v", tcase.v, tcase.typ, err, tcase.err)
		}
		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("castFromNumeric(%v, %v): %s, want %s", tcase.v, tcase.typ, printValue(got), printValue(tcase.out))
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
		err    string
	}{{
		v1:  NULL,
		v2:  NULL,
		min: NULL,
	}, {
		v1:  makeInt(1),
		v2:  NULL,
		min: makeInt(1),
	}, {
		v1:  NULL,
		v2:  makeInt(1),
		min: makeInt(1),
	}, {
		v1:  makeInt(1),
		v2:  makeInt(2),
		min: makeInt(1),
	}, {
		v1:  makeInt(2),
		v2:  makeInt(1),
		min: makeInt(1),
	}, {
		v1:  makeInt(1),
		v2:  makeInt(1),
		min: makeInt(1),
	}, {
		v1:  makeAny(VarChar, "aa"),
		v2:  makeInt(1),
		err: "text fields cannot be compared",
	}}
	for _, tcase := range tcases {
		v, err := Min(tcase.v1, tcase.v2)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("Min error: %v, want %s", err, tcase.err)
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
		err    string
	}{{
		v1:  NULL,
		v2:  NULL,
		max: NULL,
	}, {
		v1:  makeInt(1),
		v2:  NULL,
		max: makeInt(1),
	}, {
		v1:  NULL,
		v2:  makeInt(1),
		max: makeInt(1),
	}, {
		v1:  makeInt(1),
		v2:  makeInt(2),
		max: makeInt(2),
	}, {
		v1:  makeInt(2),
		v2:  makeInt(1),
		max: makeInt(2),
	}, {
		v1:  makeInt(1),
		v2:  makeInt(1),
		max: makeInt(1),
	}, {
		v1:  makeAny(VarChar, "aa"),
		v2:  makeInt(1),
		err: "text fields cannot be compared",
	}}
	for _, tcase := range tcases {
		v, err := Max(tcase.v1, tcase.v2)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != tcase.err {
			t.Errorf("Max error: %v, want %s", err, tcase.err)
		}
		if !reflect.DeepEqual(v, tcase.max) {
			t.Errorf("Max(%v, %v): %v, want %v", tcase.v1, tcase.v2, v, tcase.max)
		}
	}
}

func makeInt(v int64) Value {
	return MakeTrusted(Int64, strconv.AppendInt(nil, v, 10))
}

func makeUint(v uint64) Value {
	return MakeTrusted(Uint64, strconv.AppendUint(nil, v, 10))
}

func makeFloat(v float64) Value {
	return MakeTrusted(Float64, strconv.AppendFloat(nil, v, 'g', -1, 64))
}

func makeAny(typ querypb.Type, v string) Value {
	return MakeTrusted(typ, []byte(v))
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
		v1, _ = NullsafeAdd(v1, v2, Int64)
	}
}

func BenchmarkAddNoNative(b *testing.B) {
	v1 := MakeTrusted(Int64, []byte("1"))
	v2 := MakeTrusted(Int64, []byte("12"))
	for i := 0; i < b.N; i++ {
		iv1, _ := v1.ParseInt64()
		iv2, _ := v2.ParseInt64()
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
