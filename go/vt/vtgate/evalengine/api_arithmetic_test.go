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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	NULL       = sqltypes.NULL
	NewInt32   = sqltypes.NewInt32
	NewInt64   = sqltypes.NewInt64
	NewUint64  = sqltypes.NewUint64
	NewFloat64 = sqltypes.NewFloat64
	TestValue  = sqltypes.TestValue
	NewDecimal = sqltypes.NewDecimal

	maxUint64 uint64 = math.MaxUint64
)

func TestArithmetics(t *testing.T) {
	type tcase struct {
		v1, v2, out sqltypes.Value
		err         string
	}

	tests := []struct {
		operator string
		f        func(a, b sqltypes.Value) (sqltypes.Value, error)
		cases    []tcase
	}{{
		operator: "-",
		f:        Subtract,
		cases: []tcase{{
			// All Nulls
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
			v1:  NewInt64(math.MinInt64),
			v2:  NewInt64(1),
			err: dataOutOfRangeError(math.MinInt64, 1, "BIGINT", "-").Error(),
		}, {
			v1:  NewUint64(4),
			v2:  NewInt64(5),
			err: dataOutOfRangeError(4, 5, "BIGINT UNSIGNED", "-").Error(),
		}, {
			// testing uint - int
			v1:  NewUint64(7),
			v2:  NewInt64(5),
			out: NewUint64(2),
		}, {
			v1:  NewUint64(math.MaxUint64),
			v2:  NewInt64(0),
			out: NewUint64(math.MaxUint64),
		}, {
			// testing for int64 overflow
			v1:  NewInt64(math.MinInt64),
			v2:  NewUint64(0),
			err: dataOutOfRangeError(math.MinInt64, 0, "BIGINT UNSIGNED", "-").Error(),
		}, {
			v1:  TestValue(sqltypes.VarChar, "c"),
			v2:  NewInt64(1),
			out: NewFloat64(-1),
		}, {
			v1:  NewUint64(1),
			v2:  TestValue(sqltypes.VarChar, "c"),
			out: NewFloat64(1),
		}, {
			// testing for error for parsing float value to uint64
			v1:  TestValue(sqltypes.Uint64, "1.2"),
			v2:  NewInt64(2),
			err: "strconv.ParseUint: parsing \"1.2\": invalid syntax",
		}, {
			// testing for error for parsing float value to uint64
			v1:  NewUint64(2),
			v2:  TestValue(sqltypes.Uint64, "1.2"),
			err: "strconv.ParseUint: parsing \"1.2\": invalid syntax",
		}, {
			// uint64 - uint64
			v1:  NewUint64(8),
			v2:  NewUint64(4),
			out: NewUint64(4),
		}, {
			// testing for float subtraction: float - int
			v1:  NewFloat64(1.2),
			v2:  NewInt64(2),
			out: NewFloat64(-0.8),
		}, {
			// testing for float subtraction: float - uint
			v1:  NewFloat64(1.2),
			v2:  NewUint64(2),
			out: NewFloat64(-0.8),
		}, {
			v1:  NewInt64(-1),
			v2:  NewUint64(2),
			err: dataOutOfRangeError(-1, 2, "BIGINT UNSIGNED", "-").Error(),
		}, {
			v1:  NewInt64(2),
			v2:  NewUint64(1),
			out: NewUint64(1),
		}, {
			// testing int64 - float64 method
			v1:  NewInt64(-2),
			v2:  NewFloat64(1.0),
			out: NewFloat64(-3.0),
		}, {
			// testing uint64 - float64 method
			v1:  NewUint64(1),
			v2:  NewFloat64(-2.0),
			out: NewFloat64(3.0),
		}, {
			// testing uint - int to return uintplusint
			v1:  NewUint64(1),
			v2:  NewInt64(-2),
			out: NewUint64(3),
		}, {
			// testing for float - float
			v1:  NewFloat64(1.2),
			v2:  NewFloat64(3.2),
			out: NewFloat64(-2),
		}, {
			// testing uint - uint if v2 > v1
			v1:  NewUint64(2),
			v2:  NewUint64(4),
			err: dataOutOfRangeError(2, 4, "BIGINT UNSIGNED", "-").Error(),
		}, {
			// testing uint - (- int)
			v1:  NewUint64(1),
			v2:  NewInt64(-2),
			out: NewUint64(3),
		}},
	}, {
		operator: "+",
		f:        Add,
		cases: []tcase{{
			// All Nulls
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
			// testing for overflow int64, result will be unsigned int
			v1:  NewInt64(math.MaxInt64),
			v2:  NewUint64(2),
			out: NewUint64(9223372036854775809),
		}, {
			v1:  NewInt64(-2),
			v2:  NewUint64(1),
			err: dataOutOfRangeError(1, -2, "BIGINT UNSIGNED", "+").Error(),
		}, {
			v1:  NewInt64(math.MaxInt64),
			v2:  NewInt64(-2),
			out: NewInt64(9223372036854775805),
		}, {
			// Normal case
			v1:  NewUint64(1),
			v2:  NewUint64(2),
			out: NewUint64(3),
		}, {
			// testing for overflow uint64
			v1:  NewUint64(maxUint64),
			v2:  NewUint64(2),
			err: dataOutOfRangeError(maxUint64, 2, "BIGINT UNSIGNED", "+").Error(),
		}, {
			// int64 underflow
			v1:  NewInt64(math.MinInt64),
			v2:  NewInt64(-2),
			err: dataOutOfRangeError(math.MinInt64, -2, "BIGINT", "+").Error(),
		}, {
			// checking int64 max value can be returned
			v1:  NewInt64(math.MaxInt64),
			v2:  NewUint64(0),
			out: NewUint64(9223372036854775807),
		}, {
			// testing whether uint64 max value can be returned
			v1:  NewUint64(math.MaxUint64),
			v2:  NewInt64(0),
			out: NewUint64(math.MaxUint64),
		}, {
			v1:  NewUint64(math.MaxInt64),
			v2:  NewInt64(1),
			out: NewUint64(9223372036854775808),
		}, {
			v1:  NewUint64(1),
			v2:  TestValue(sqltypes.VarChar, "c"),
			out: NewFloat64(1),
		}, {
			v1:  NewUint64(1),
			v2:  TestValue(sqltypes.VarChar, "1.2"),
			out: NewFloat64(2.2),
		}, {
			v1:  TestValue(sqltypes.Int64, "1.2"),
			v2:  NewInt64(2),
			err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
		}, {
			v1:  NewInt64(2),
			v2:  TestValue(sqltypes.Int64, "1.2"),
			err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
		}, {
			// testing for uint64 overflow with max uint64 + int value
			v1:  NewUint64(maxUint64),
			v2:  NewInt64(2),
			err: dataOutOfRangeError(maxUint64, 2, "BIGINT UNSIGNED", "+").Error(),
		}, {
			v1:  sqltypes.NewHexNum([]byte("0x9")),
			v2:  NewInt64(1),
			out: NewUint64(10),
		}},
	}, {
		operator: "/",
		f:        Divide,
		cases: []tcase{{
			// All Nulls
			v1:  NULL,
			v2:  NULL,
			out: NULL,
		}, {
			// First value null.
			v1:  NULL,
			v2:  NewInt32(1),
			out: NULL,
		}, {
			// Second value null.
			v1:  NewInt32(1),
			v2:  NULL,
			out: NULL,
		}, {
			// Second arg 0
			v1:  NewInt32(5),
			v2:  NewInt32(0),
			out: NULL,
		}, {
			// Both arguments zero
			v1:  NewInt32(0),
			v2:  NewInt32(0),
			out: NULL,
		}, {
			// case with negative value
			v1:  NewInt64(-1),
			v2:  NewInt64(-2),
			out: NewDecimal("0.5000"),
		}, {
			// float64 division by zero
			v1:  NewFloat64(2),
			v2:  NewFloat64(0),
			out: NULL,
		}, {
			// Lower bound for int64
			v1:  NewInt64(math.MinInt64),
			v2:  NewInt64(1),
			out: NewDecimal(strconv.Itoa(math.MinInt64) + ".0000"),
		}, {
			// upper bound for uint64
			v1:  NewUint64(math.MaxUint64),
			v2:  NewUint64(1),
			out: NewDecimal(strconv.FormatUint(math.MaxUint64, 10) + ".0000"),
		}, {
			// testing for error in types
			v1:  TestValue(sqltypes.Int64, "1.2"),
			v2:  NewInt64(2),
			err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
		}, {
			// testing for error in types
			v1:  NewInt64(2),
			v2:  TestValue(sqltypes.Int64, "1.2"),
			err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
		}, {
			// testing for uint/int
			v1:  NewUint64(4),
			v2:  NewInt64(5),
			out: NewDecimal("0.8000"),
		}, {
			// testing for uint/uint
			v1:  NewUint64(1),
			v2:  NewUint64(2),
			out: NewDecimal("0.5000"),
		}, {
			// testing for float64/int64
			v1:  TestValue(sqltypes.Float64, "1.2"),
			v2:  NewInt64(-2),
			out: NewFloat64(-0.6),
		}, {
			// testing for float64/uint64
			v1:  TestValue(sqltypes.Float64, "1.2"),
			v2:  NewUint64(2),
			out: NewFloat64(0.6),
		}, {
			// testing for overflow of float64
			v1:  NewFloat64(math.MaxFloat64),
			v2:  NewFloat64(0.5),
			err: dataOutOfRangeError(math.MaxFloat64, 0.5, "BIGINT", "/").Error(),
		}},
	}, {
		operator: "*",
		f:        Multiply,
		cases: []tcase{{
			// All Nulls
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
			out: NewInt64(2),
		}, {
			// testing for int64 overflow with min negative value
			v1:  NewInt64(math.MinInt64),
			v2:  NewInt64(1),
			out: NewInt64(math.MinInt64),
		}, {
			// testing for error in types
			v1:  TestValue(sqltypes.Int64, "1.2"),
			v2:  NewInt64(2),
			err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
		}, {
			// testing for error in types
			v1:  NewInt64(2),
			v2:  TestValue(sqltypes.Int64, "1.2"),
			err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
		}, {
			// testing for uint*int
			v1:  NewUint64(4),
			v2:  NewInt64(5),
			out: NewUint64(20),
		}, {
			// testing for uint*uint
			v1:  NewUint64(1),
			v2:  NewUint64(2),
			out: NewUint64(2),
		}, {
			// testing for float64*int64
			v1:  TestValue(sqltypes.Float64, "1.2"),
			v2:  NewInt64(-2),
			out: NewFloat64(-2.4),
		}, {
			// testing for float64*uint64
			v1:  TestValue(sqltypes.Float64, "1.2"),
			v2:  NewUint64(2),
			out: NewFloat64(2.4),
		}, {
			// testing for overflow of int64
			v1:  NewInt64(math.MaxInt64),
			v2:  NewInt64(2),
			err: dataOutOfRangeError(math.MaxInt64, 2, "BIGINT", "*").Error(),
		}, {
			// testing for underflow of uint64*max.uint64
			v1:  NewInt64(2),
			v2:  NewUint64(maxUint64),
			err: dataOutOfRangeError(maxUint64, 2, "BIGINT UNSIGNED", "*").Error(),
		}, {
			v1:  NewUint64(math.MaxUint64),
			v2:  NewUint64(1),
			out: NewUint64(math.MaxUint64),
		}, {
			// Checking whether maxInt value can be passed as uint value
			v1:  NewUint64(math.MaxInt64),
			v2:  NewInt64(3),
			err: dataOutOfRangeError(math.MaxInt64, 3, "BIGINT UNSIGNED", "*").Error(),
		}},
	}}

	for _, test := range tests {
		t.Run(test.operator, func(t *testing.T) {
			for _, tcase := range test.cases {
				name := fmt.Sprintf("%s%s%s", tcase.v1.String(), test.operator, tcase.v2.String())
				t.Run(name, func(t *testing.T) {
					got, err := test.f(tcase.v1, tcase.v2)
					if tcase.err == "" {
						require.NoError(t, err)
						require.Equal(t, tcase.out, got)
					} else {
						require.EqualError(t, err, tcase.err)
					}
				})
			}
		})
	}
}

func TestNullSafeAdd(t *testing.T) {
	tcases := []struct {
		v1, v2 sqltypes.Value
		out    sqltypes.Value
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
		v1:  TestValue(sqltypes.Int64, "1.2"),
		v2:  NewInt64(2),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  NewInt64(2),
		v2:  TestValue(sqltypes.Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Make sure underlying error is returned while adding.
		v1:  NewInt64(-1),
		v2:  NewUint64(2),
		out: NewInt64(1),
	}, {
		v1:  NewInt64(-100),
		v2:  NewUint64(10),
		err: dataOutOfRangeError(10, -100, "BIGINT UNSIGNED", "+"),
	}, {
		// Make sure underlying error is returned while converting.
		v1:  NewFloat64(1),
		v2:  NewFloat64(2),
		out: NewInt64(3),
	}}
	for _, tcase := range tcases {
		got, err := NullSafeAdd(tcase.v1, tcase.v2, sqltypes.Int64)

		if tcase.err == nil {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, tcase.err.Error())
		}

		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("NullSafeAdd(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), printValue(got), printValue(tcase.out))
		}
	}
}

func TestCast(t *testing.T) {
	tcases := []struct {
		typ sqltypes.Type
		v   sqltypes.Value
		out sqltypes.Value
		err error
	}{{
		typ: sqltypes.VarChar,
		v:   NULL,
		out: NULL,
	}, {
		typ: sqltypes.VarChar,
		v:   TestValue(sqltypes.VarChar, "exact types"),
		out: TestValue(sqltypes.VarChar, "exact types"),
	}, {
		typ: sqltypes.Int64,
		v:   TestValue(sqltypes.Int32, "32"),
		out: TestValue(sqltypes.Int64, "32"),
	}, {
		typ: sqltypes.Int24,
		v:   TestValue(sqltypes.Uint64, "64"),
		out: TestValue(sqltypes.Int24, "64"),
	}, {
		typ: sqltypes.Int24,
		v:   TestValue(sqltypes.VarChar, "bad int"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `strconv.ParseInt: parsing "bad int": invalid syntax`),
	}, {
		typ: sqltypes.Uint64,
		v:   TestValue(sqltypes.Uint32, "32"),
		out: TestValue(sqltypes.Uint64, "32"),
	}, {
		typ: sqltypes.Uint24,
		v:   TestValue(sqltypes.Int64, "64"),
		out: TestValue(sqltypes.Uint24, "64"),
	}, {
		typ: sqltypes.Uint24,
		v:   TestValue(sqltypes.Int64, "-1"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `strconv.ParseUint: parsing "-1": invalid syntax`),
	}, {
		typ: sqltypes.Float64,
		v:   TestValue(sqltypes.Int64, "64"),
		out: TestValue(sqltypes.Float64, "64"),
	}, {
		typ: sqltypes.Float32,
		v:   TestValue(sqltypes.Float64, "64"),
		out: TestValue(sqltypes.Float32, "64"),
	}, {
		typ: sqltypes.Float32,
		v:   TestValue(sqltypes.Decimal, "1.24"),
		out: TestValue(sqltypes.Float32, "1.24"),
	}, {
		typ: sqltypes.Float64,
		v:   TestValue(sqltypes.VarChar, "1.25"),
		out: TestValue(sqltypes.Float64, "1.25"),
	}, {
		typ: sqltypes.Float64,
		v:   TestValue(sqltypes.VarChar, "bad float"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `strconv.ParseFloat: parsing "bad float": invalid syntax`),
	}, {
		typ: sqltypes.VarChar,
		v:   TestValue(sqltypes.Int64, "64"),
		out: TestValue(sqltypes.VarChar, "64"),
	}, {
		typ: sqltypes.VarBinary,
		v:   TestValue(sqltypes.Float64, "64"),
		out: TestValue(sqltypes.VarBinary, "64"),
	}, {
		typ: sqltypes.VarBinary,
		v:   TestValue(sqltypes.Decimal, "1.24"),
		out: TestValue(sqltypes.VarBinary, "1.24"),
	}, {
		typ: sqltypes.VarBinary,
		v:   TestValue(sqltypes.VarChar, "1.25"),
		out: TestValue(sqltypes.VarBinary, "1.25"),
	}, {
		typ: sqltypes.VarChar,
		v:   TestValue(sqltypes.VarBinary, "valid string"),
		out: TestValue(sqltypes.VarChar, "valid string"),
	}, {
		typ: sqltypes.VarChar,
		v:   TestValue(sqltypes.Expression, "bad string"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "expression cannot be converted to bytes"),
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
		v   sqltypes.Value
		out uint64
		err error
	}{{
		v:   TestValue(sqltypes.VarChar, "abcd"),
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
		v   sqltypes.Value
		out int64
		err error
	}{{
		v:   TestValue(sqltypes.VarChar, "abcd"),
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
		v   sqltypes.Value
		out float64
		err error
	}{{
		v:   TestValue(sqltypes.VarChar, "abcd"),
		out: 0,
	}, {
		v:   TestValue(sqltypes.VarChar, "1.2"),
		out: 1.2,
	}, {
		v:   NewInt64(1),
		out: 1,
	}, {
		v:   NewUint64(1),
		out: 1,
	}, {
		v:   NewFloat64(1.2),
		out: 1.2,
	}, {
		v:   TestValue(sqltypes.Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}}
	for _, tcase := range tcases {
		t.Run(tcase.v.String(), func(t *testing.T) {
			got, err := ToFloat64(tcase.v)
			if tcase.err != nil {
				require.EqualError(t, err, tcase.err.Error())
			} else {
				require.Equal(t, tcase.out, got)
			}
		})
	}
}

func TestToNative(t *testing.T) {
	testcases := []struct {
		in  sqltypes.Value
		out any
	}{{
		in:  NULL,
		out: nil,
	}, {
		in:  TestValue(sqltypes.Int8, "1"),
		out: int64(1),
	}, {
		in:  TestValue(sqltypes.Int16, "1"),
		out: int64(1),
	}, {
		in:  TestValue(sqltypes.Int24, "1"),
		out: int64(1),
	}, {
		in:  TestValue(sqltypes.Int32, "1"),
		out: int64(1),
	}, {
		in:  TestValue(sqltypes.Int64, "1"),
		out: int64(1),
	}, {
		in:  TestValue(sqltypes.Uint8, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(sqltypes.Uint16, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(sqltypes.Uint24, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(sqltypes.Uint32, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(sqltypes.Uint64, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(sqltypes.Float32, "1"),
		out: float64(1),
	}, {
		in:  TestValue(sqltypes.Float64, "1"),
		out: float64(1),
	}, {
		in:  TestValue(sqltypes.Timestamp, "2012-02-24 23:19:43"),
		out: []byte("2012-02-24 23:19:43"),
	}, {
		in:  TestValue(sqltypes.Date, "2012-02-24"),
		out: []byte("2012-02-24"),
	}, {
		in:  TestValue(sqltypes.Time, "23:19:43"),
		out: []byte("23:19:43"),
	}, {
		in:  TestValue(sqltypes.Datetime, "2012-02-24 23:19:43"),
		out: []byte("2012-02-24 23:19:43"),
	}, {
		in:  TestValue(sqltypes.Year, "1"),
		out: uint64(1),
	}, {
		in:  TestValue(sqltypes.Decimal, "1"),
		out: []byte("1"),
	}, {
		in:  TestValue(sqltypes.Text, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(sqltypes.Blob, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(sqltypes.VarChar, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(sqltypes.VarBinary, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(sqltypes.Char, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(sqltypes.Binary, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(sqltypes.Bit, "1"),
		out: []byte("1"),
	}, {
		in:  TestValue(sqltypes.Enum, "a"),
		out: []byte("a"),
	}, {
		in:  TestValue(sqltypes.Set, "a"),
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
	_, err := ToNative(TestValue(sqltypes.Expression, "aa"))
	want := vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "EXPRESSION(aa) cannot be converted to a go type")
	if !vterrors.Equals(err, want) {
		t.Errorf("ToNative(EXPRESSION): %v, want %v", vterrors.Print(err), vterrors.Print(want))
	}
}

func TestNewIntegralNumeric(t *testing.T) {
	tcases := []struct {
		v   sqltypes.Value
		out eval
		err error
	}{{
		v:   NewInt64(1),
		out: newEvalInt64(1),
	}, {
		v:   NewUint64(1),
		out: newEvalUint64(1),
	}, {
		v:   NewFloat64(1),
		out: newEvalInt64(1),
	}, {
		// For non-number type, Int64 is the default.
		v:   TestValue(sqltypes.VarChar, "1"),
		out: newEvalInt64(1),
	}, {
		// If Int64 can't work, we use Uint64.
		v:   TestValue(sqltypes.VarChar, "18446744073709551615"),
		out: newEvalUint64(18446744073709551615),
	}, {
		// Only valid Int64 allowed if type is Int64.
		v:   TestValue(sqltypes.Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseInt: parsing \"1.2\": invalid syntax"),
	}, {
		// Only valid Uint64 allowed if type is Uint64.
		v:   TestValue(sqltypes.Uint64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "strconv.ParseUint: parsing \"1.2\": invalid syntax"),
	}, {
		v:   TestValue(sqltypes.VarChar, "abcd"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: 'abcd'"),
	}}
	for _, tcase := range tcases {
		got, err := valueToEvalNumeric(tcase.v)
		if err != nil && !vterrors.Equals(err, tcase.err) {
			t.Errorf("newIntegralNumeric(%s) error: %v, want %v", printValue(tcase.v), vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err == nil {
			continue
		}

		utils.MustMatch(t, tcase.out, got, "newIntegralNumeric")
	}
}

func TestAddNumeric(t *testing.T) {
	tcases := []struct {
		v1, v2 eval
		out    eval
		err    error
	}{{
		v1:  newEvalInt64(1),
		v2:  newEvalInt64(2),
		out: newEvalInt64(3),
	}, {
		v1:  newEvalInt64(1),
		v2:  newEvalUint64(2),
		out: newEvalUint64(3),
	}, {
		v1:  newEvalInt64(1),
		v2:  newEvalFloat(2),
		out: newEvalFloat(3),
	}, {
		v1:  newEvalUint64(1),
		v2:  newEvalUint64(2),
		out: newEvalUint64(3),
	}, {
		v1:  newEvalUint64(1),
		v2:  newEvalFloat(2),
		out: newEvalFloat(3),
	}, {
		v1:  newEvalFloat(1),
		v2:  newEvalFloat(2),
		out: newEvalFloat(3),
	}, {
		// Int64 overflow.
		v1:  newEvalInt64(9223372036854775807),
		v2:  newEvalInt64(2),
		err: dataOutOfRangeError(9223372036854775807, 2, "BIGINT", "+"),
	}, {
		// Int64 underflow.
		v1:  newEvalInt64(-9223372036854775807),
		v2:  newEvalInt64(-2),
		err: dataOutOfRangeError(-9223372036854775807, -2, "BIGINT", "+"),
	}, {
		v1:  newEvalInt64(-1),
		v2:  newEvalUint64(2),
		out: newEvalUint64(1),
	}, {
		// Uint64 overflow.
		v1:  newEvalUint64(18446744073709551615),
		v2:  newEvalUint64(2),
		err: dataOutOfRangeError(uint64(18446744073709551615), 2, "BIGINT UNSIGNED", "+"),
	}}
	for _, tcase := range tcases {
		got, err := addNumericWithError(tcase.v1, tcase.v2)
		if err != nil {
			if tcase.err == nil {
				t.Fatal(err)
			}
			if err.Error() != tcase.err.Error() {
				t.Fatalf("bad error message: got %q want %q", err, tcase.err)
			}
			continue
		}
		utils.MustMatch(t, tcase.out, got, "addNumeric")
	}
}

func TestPrioritize(t *testing.T) {
	ival := newEvalInt64(-1)
	uval := newEvalUint64(1)
	fval := newEvalFloat(1.2)
	textIntval := newEvalRaw(sqltypes.VarBinary, []byte("-1"), collationNumeric)
	textFloatval := newEvalRaw(sqltypes.VarBinary, []byte("1.2"), collationNumeric)

	tcases := []struct {
		v1, v2     eval
		out1, out2 eval
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
	}, {
		v1:   textIntval,
		v2:   ival,
		out1: newEvalFloat(-1.0),
		out2: ival,
	}, {
		v1:   ival,
		v2:   textFloatval,
		out1: fval,
		out2: ival,
	}}
	for _, tcase := range tcases {
		t.Run(fmt.Sprintf("%s - %s", evalToSQLValue(tcase.v1), evalToSQLValue(tcase.v2)), func(t *testing.T) {
			got1, got2 := makeNumericAndPrioritize(tcase.v1, tcase.v2)
			utils.MustMatch(t, evalToSQLValue(tcase.out1), evalToSQLValue(got1), "makeNumericAndPrioritize")
			utils.MustMatch(t, evalToSQLValue(tcase.out2), evalToSQLValue(got2), "makeNumericAndPrioritize")
		})
	}
}

func TestToSqlValue(t *testing.T) {
	tcases := []struct {
		typ sqltypes.Type
		v   eval
		out sqltypes.Value
		err error
	}{{
		typ: sqltypes.Int64,
		v:   newEvalInt64(1),
		out: NewInt64(1),
	}, {
		typ: sqltypes.Int64,
		v:   newEvalUint64(1),
		out: NewInt64(1),
	}, {
		typ: sqltypes.Int64,
		v:   newEvalFloat(1.2e-16),
		out: NewInt64(0),
	}, {
		typ: sqltypes.Uint64,
		v:   newEvalInt64(1),
		out: NewUint64(1),
	}, {
		typ: sqltypes.Uint64,
		v:   newEvalUint64(1),
		out: NewUint64(1),
	}, {
		typ: sqltypes.Uint64,
		v:   newEvalFloat(1.2e-16),
		out: NewUint64(0),
	}, {
		typ: sqltypes.Float64,
		v:   newEvalInt64(1),
		out: TestValue(sqltypes.Float64, "1"),
	}, {
		typ: sqltypes.Float64,
		v:   newEvalUint64(1),
		out: TestValue(sqltypes.Float64, "1"),
	}, {
		typ: sqltypes.Float64,
		v:   newEvalFloat(1.2e-16),
		out: TestValue(sqltypes.Float64, "1.2e-16"),
	}, {
		typ: sqltypes.Decimal,
		v:   newEvalInt64(1),
		out: TestValue(sqltypes.Decimal, "1"),
	}, {
		typ: sqltypes.Decimal,
		v:   newEvalUint64(1),
		out: TestValue(sqltypes.Decimal, "1"),
	}, {
		// For float, we should not use scientific notation.
		typ: sqltypes.Decimal,
		v:   newEvalFloat(1.2e-16),
		out: TestValue(sqltypes.Decimal, "0.00000000000000012"),
	}}
	for _, tcase := range tcases {
		got := evalToSQLValueWithType(tcase.v, tcase.typ)
		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("toSQLValue(%v, %v): %v, want %v", tcase.v, tcase.typ, printValue(got), printValue(tcase.out))
		}
	}
}

func TestCompareNumeric(t *testing.T) {
	values := []eval{
		newEvalInt64(1),
		newEvalInt64(-1),
		newEvalInt64(0),
		newEvalInt64(2),
		newEvalUint64(1),
		newEvalUint64(0),
		newEvalUint64(2),
		newEvalFloat(1.0),
		newEvalFloat(-1.0),
		newEvalFloat(0.0),
		newEvalFloat(2.0),
	}

	// cmpResults is a 2D array with the comparison expectations if we compare all values with each other
	cmpResults := [][]int{
		// LHS ->  1  -1   0  2  u1  u0 u2 1.0 -1.0 0.0  2.0
		/*RHS 1*/ {0, 1, 1, -1, 0, 1, -1, 0, 1, 1, -1},
		/*   -1*/ {-1, 0, -1, -1, -1, -1, -1, -1, 0, -1, -1},
		/*    0*/ {-1, 1, 0, -1, -1, 0, -1, -1, 1, 0, -1},
		/*    2*/ {1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0},
		/*   u1*/ {0, 1, 1, -1, 0, 1, -1, 0, 1, 1, -1},
		/*   u0*/ {-1, 1, 0, -1, -1, 0, -1, -1, 1, 0, -1},
		/*   u2*/ {1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0},
		/*  1.0*/ {0, 1, 1, -1, 0, 1, -1, 0, 1, 1, -1},
		/* -1.0*/ {-1, 0, -1, -1, -1, -1, -1, -1, 0, -1, -1},
		/*  0.0*/ {-1, 1, 0, -1, -1, 0, -1, -1, 1, 0, -1},
		/*  2.0*/ {1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0},
	}

	for aIdx, aVal := range values {
		for bIdx, bVal := range values {
			t.Run(fmt.Sprintf("[%d/%d] %s %s", aIdx, bIdx, evalToSQLValue(aVal), evalToSQLValue(bVal)), func(t *testing.T) {
				result, err := compareNumeric(aVal, bVal)
				require.NoError(t, err)
				assert.Equal(t, cmpResults[aIdx][bIdx], result)

				// if two values are considered equal, they must also produce the same hashcode
				if result == 0 {
					if aVal.sqlType() == bVal.sqlType() {
						// hash codes can only be compared if they are coerced to the same type first
						aHash, _ := aVal.hash()
						bHash, _ := bVal.hash()
						assert.Equal(t, aHash, bHash, "hash code does not match")
					}
				}
			})
		}
	}
}

func TestMin(t *testing.T) {
	tcases := []struct {
		v1, v2 sqltypes.Value
		min    sqltypes.Value
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
		v1:  TestValue(sqltypes.VarChar, "aa"),
		v2:  TestValue(sqltypes.VarChar, "aa"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, "cannot compare strings, collation is unknown or unsupported (collation ID: 0)"),
	}}
	for _, tcase := range tcases {
		v, err := Min(tcase.v1, tcase.v2, collations.Unknown)
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

func TestMinCollate(t *testing.T) {
	tcases := []struct {
		v1, v2    string
		collation collations.ID
		out       string
		err       error
	}{
		{
			// accent insensitive
			v1:        "ǍḄÇ",
			v2:        "ÁḆĈ",
			out:       "ǍḄÇ",
			collation: getCollationID("utf8mb4_0900_as_ci"),
		},
		{
			// kana sensitive
			v1:        "\xE3\x81\xAB\xE3\x81\xBB\xE3\x82\x93\xE3\x81\x94",
			v2:        "\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4",
			out:       "\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4",
			collation: getCollationID("utf8mb4_ja_0900_as_cs_ks"),
		},
		{
			// non breaking space
			v1:        "abc ",
			v2:        "abc\u00a0",
			out:       "abc\u00a0",
			collation: getCollationID("utf8mb4_0900_as_cs"),
		},
		{
			// "cs" counts as a separate letter, where c < cs < d
			v1:        "c",
			v2:        "cs",
			out:       "cs",
			collation: getCollationID("utf8mb4_hu_0900_ai_ci"),
		},
		{
			// "cs" counts as a separate letter, where c < cs < d
			v1:        "cukor",
			v2:        "csak",
			out:       "csak",
			collation: getCollationID("utf8mb4_hu_0900_ai_ci"),
		},
	}
	for _, tcase := range tcases {
		got, err := Min(TestValue(sqltypes.VarChar, tcase.v1), TestValue(sqltypes.VarChar, tcase.v2), tcase.collation)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("NullsafeCompare(%v, %v) error: %v, want %v", tcase.v1, tcase.v2, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if got.ToString() == tcase.out {
			t.Errorf("NullsafeCompare(%v, %v): %v, want %v", tcase.v1, tcase.v2, got, tcase.out)
		}
	}
}

func TestMax(t *testing.T) {
	tcases := []struct {
		v1, v2 sqltypes.Value
		max    sqltypes.Value
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
		v1:  TestValue(sqltypes.VarChar, "aa"),
		v2:  TestValue(sqltypes.VarChar, "aa"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, "cannot compare strings, collation is unknown or unsupported (collation ID: 0)"),
	}}
	for _, tcase := range tcases {
		v, err := Max(tcase.v1, tcase.v2, collations.Unknown)
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

func TestMaxCollate(t *testing.T) {
	tcases := []struct {
		v1, v2    string
		collation collations.ID
		out       string
		err       error
	}{
		{
			// accent insensitive
			v1:        "ǍḄÇ",
			v2:        "ÁḆĈ",
			out:       "ǍḄÇ",
			collation: getCollationID("utf8mb4_0900_as_ci"),
		},
		{
			// kana sensitive
			v1:        "\xE3\x81\xAB\xE3\x81\xBB\xE3\x82\x93\xE3\x81\x94",
			v2:        "\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4",
			out:       "\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4",
			collation: getCollationID("utf8mb4_ja_0900_as_cs_ks"),
		},
		{
			// non breaking space
			v1:        "abc ",
			v2:        "abc\u00a0",
			out:       "abc\u00a0",
			collation: getCollationID("utf8mb4_0900_as_cs"),
		},
		{
			// "cs" counts as a separate letter, where c < cs < d
			v1:        "c",
			v2:        "cs",
			out:       "cs",
			collation: getCollationID("utf8mb4_hu_0900_ai_ci"),
		},
		{
			// "cs" counts as a separate letter, where c < cs < d
			v1:        "cukor",
			v2:        "csak",
			out:       "csak",
			collation: getCollationID("utf8mb4_hu_0900_ai_ci"),
		},
	}
	for _, tcase := range tcases {
		got, err := Max(TestValue(sqltypes.VarChar, tcase.v1), TestValue(sqltypes.VarChar, tcase.v2), tcase.collation)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("NullsafeCompare(%v, %v) error: %v, want %v", tcase.v1, tcase.v2, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if got.ToString() != tcase.out {
			t.Errorf("NullsafeCompare(%v, %v): %v, want %v", tcase.v1, tcase.v2, got, tcase.out)
		}
	}
}

func printValue(v sqltypes.Value) string {
	vBytes, _ := v.ToBytes()
	return fmt.Sprintf("%v:%q", v.Type(), vBytes)
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
	v1 := sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))
	v2 := sqltypes.MakeTrusted(sqltypes.Int64, []byte("12"))
	for i := 0; i < b.N; i++ {
		v1, _ = NullSafeAdd(v1, v2, sqltypes.Int64)
	}
}

func BenchmarkAddNoNative(b *testing.B) {
	v1 := sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))
	v2 := sqltypes.MakeTrusted(sqltypes.Int64, []byte("12"))
	for i := 0; i < b.N; i++ {
		iv1, _ := ToInt64(v1)
		iv2, _ := ToInt64(v2)
		v1 = sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, iv1+iv2, 10))
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

func makeNativeInt64(v int64) sqltypes.Value {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return sqltypes.MakeTrusted(sqltypes.Int64, buf)
}

func BenchmarkAddGoInterface(b *testing.B) {
	var v1, v2 any
	v1 = int64(1)
	v2 = int64(2)
	for i := 0; i < b.N; i++ {
		v1 = v1.(int64) + v2.(int64)
	}
}

func BenchmarkAddGo(b *testing.B) {
	v1 := int64(1)
	v2 := int64(2)
	for i := 0; i < b.N; i++ {
		v1 += v2
	}
}

func TestParseStringToFloat(t *testing.T) {
	tcs := []struct {
		str string
		val float64
	}{
		{str: ""},
		{str: " "},
		{str: "1", val: 1},
		{str: "1.10", val: 1.10},
		{str: "    6.87", val: 6.87},
		{str: "93.66  ", val: 93.66},
		{str: "\t 42.10 \n ", val: 42.10},
		{str: "1.10aa", val: 1.10},
		{str: ".", val: 0.00},
		{str: ".99", val: 0.99},
		{str: "..99", val: 0},
		{str: "1.", val: 1},
		{str: "0.1.99", val: 0.1},
		{str: "0.", val: 0},
		{str: "8794354", val: 8794354},
		{str: "    10  ", val: 10},
		{str: "2266951196291479516", val: 2266951196291479516},
		{str: "abcd123", val: 0},
	}

	for _, tc := range tcs {
		t.Run(tc.str, func(t *testing.T) {
			got := parseStringToFloat(tc.str)
			require.EqualValues(t, tc.val, got)
		})
	}
}
