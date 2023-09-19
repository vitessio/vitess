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

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/vthash"

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
			err: "unparsed tail left after parsing uint64 from \"1.2\": \".2\"",
		}, {
			// testing for error for parsing float value to uint64
			v1:  NewUint64(2),
			v2:  TestValue(sqltypes.Uint64, "1.2"),
			err: "unparsed tail left after parsing uint64 from \"1.2\": \".2\"",
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
			err: "unparsed tail left after parsing int64 from \"1.2\": \".2\"",
		}, {
			v1:  NewInt64(2),
			v2:  TestValue(sqltypes.Int64, "1.2"),
			err: "unparsed tail left after parsing int64 from \"1.2\": \".2\"",
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
			err: "unparsed tail left after parsing int64 from \"1.2\": \".2\"",
		}, {
			// testing for error in types
			v1:  NewInt64(2),
			v2:  TestValue(sqltypes.Int64, "1.2"),
			err: "unparsed tail left after parsing int64 from \"1.2\": \".2\"",
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
			err: dataOutOfRangeError(math.MaxFloat64, 0.5, "DOUBLE", "/").Error(),
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
			err: "unparsed tail left after parsing int64 from \"1.2\": \".2\"",
		}, {
			// testing for error in types
			v1:  NewInt64(2),
			v2:  TestValue(sqltypes.Int64, "1.2"),
			err: "unparsed tail left after parsing int64 from \"1.2\": \".2\"",
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
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unparsed tail left after parsing int64 from \"1.2\": \".2\""),
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  NewInt64(2),
		v2:  TestValue(sqltypes.Int64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unparsed tail left after parsing int64 from \"1.2\": \".2\""),
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
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unparsed tail left after parsing int64 from \"1.2\": \".2\""),
	}, {
		// Only valid Uint64 allowed if type is Uint64.
		v:   TestValue(sqltypes.Uint64, "1.2"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unparsed tail left after parsing uint64 from \"1.2\": \".2\""),
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
					if aVal.SQLType() == bVal.SQLType() {
						hash1 := vthash.New()
						hash2 := vthash.New()

						// hash codes can only be compared if they are coerced to the same type first
						aVal.(hashable).Hash(&hash1)
						bVal.(hashable).Hash(&hash2)
						assert.Equal(t, hash1.Sum128(), hash2.Sum128(), "hash code does not match")
					}
				}
			})
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
		iv1, _ := v1.ToInt64()
		iv2, _ := v2.ToInt64()
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
