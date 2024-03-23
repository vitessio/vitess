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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/vthash"
)

var (
	NULL       = sqltypes.NULL
	NewInt64   = sqltypes.NewInt64
	NewUint64  = sqltypes.NewUint64
	NewFloat64 = sqltypes.NewFloat64
	TestValue  = sqltypes.TestValue
)

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
		err: dataOutOfRangeError(int64(9223372036854775807), int64(2), "BIGINT", "+"),
	}, {
		// Int64 underflow.
		v1:  newEvalInt64(-9223372036854775807),
		v2:  newEvalInt64(-2),
		err: dataOutOfRangeError(int64(-9223372036854775807), int64(-2), "BIGINT", "+"),
	}, {
		v1:  newEvalInt64(-1),
		v2:  newEvalUint64(2),
		out: newEvalUint64(1),
	}, {
		// Uint64 overflow.
		v1:  newEvalUint64(18446744073709551615),
		v2:  newEvalUint64(2),
		err: dataOutOfRangeError(uint64(18446744073709551615), uint64(2), "BIGINT UNSIGNED", "+"),
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
	nt := func(t sqltypes.Type) Type {
		return NewType(t, collations.CollationBinaryID)
	}

	tcases := []struct {
		typ Type
		v   eval
		out sqltypes.Value
		err error
	}{{
		typ: nt(sqltypes.Int64),
		v:   newEvalInt64(1),
		out: NewInt64(1),
	}, {
		typ: nt(sqltypes.Int64),
		v:   newEvalUint64(1),
		out: NewInt64(1),
	}, {
		typ: nt(sqltypes.Int64),
		v:   newEvalFloat(1.2e-16),
		out: NewInt64(0),
	}, {
		typ: nt(sqltypes.Uint64),
		v:   newEvalInt64(1),
		out: NewUint64(1),
	}, {
		typ: nt(sqltypes.Uint64),
		v:   newEvalUint64(1),
		out: NewUint64(1),
	}, {
		typ: nt(sqltypes.Uint64),
		v:   newEvalFloat(1.2e-16),
		out: NewUint64(0),
	}, {
		typ: nt(sqltypes.Float64),
		v:   newEvalInt64(1),
		out: TestValue(sqltypes.Float64, "1"),
	}, {
		typ: nt(sqltypes.Float64),
		v:   newEvalUint64(1),
		out: TestValue(sqltypes.Float64, "1"),
	}, {
		typ: nt(sqltypes.Float64),
		v:   newEvalFloat(1.2e-16),
		out: TestValue(sqltypes.Float64, "1.2e-16"),
	}, {
		typ: nt(sqltypes.Decimal),
		v:   newEvalInt64(1),
		out: TestValue(sqltypes.Decimal, "1"),
	}, {
		typ: nt(sqltypes.Decimal),
		v:   newEvalUint64(1),
		out: TestValue(sqltypes.Decimal, "1"),
	}, {
		// For float, we should not use scientific notation.
		typ: nt(sqltypes.Decimal),
		v:   newEvalFloat(1.2e-16),
		out: TestValue(sqltypes.Decimal, "0.00000000000000012"),
	}, {
		// null in should return null out no matter what type
		typ: nt(sqltypes.Int64),
		v:   nil,
		out: sqltypes.NULL,
	}, {
		typ: nt(sqltypes.Uint64),
		v:   nil,
		out: sqltypes.NULL,
	}, {
		typ: nt(sqltypes.Float64),
		v:   nil,
		out: sqltypes.NULL,
	}, {
		typ: nt(sqltypes.VarChar),
		v:   nil,
		out: sqltypes.NULL,
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
