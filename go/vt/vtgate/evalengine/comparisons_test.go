/*
Copyright 2021 The Vitess Authors.

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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

type testCase struct {
	name   string
	v1, v2 Expr
	out    *bool
	err    string
	op     sqlparser.ComparisonExprOperator
	bv     map[string]*querypb.BindVariable
	row    []sqltypes.Value
}

var (
	T            = true
	F            = false
	collationEnv *collations.Environment
)

func init() {
	// We require MySQL 8.0 collations for the comparisons in the tests
	mySQLVersion := "8.0.0"
	servenv.SetMySQLServerVersionForTest(mySQLVersion)
	collationEnv = collations.NewEnvironment(mySQLVersion)
}

func defaultCollation() collations.TypedCollation {
	return collations.TypedCollation{
		Collation:    collationEnv.LookupByName("utf8mb4_bin").ID(),
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireASCII,
	}
}

func (tc testCase) run(t *testing.T) {
	if tc.bv == nil {
		tc.bv = map[string]*querypb.BindVariable{}
	}
	fields := make([]*querypb.Field, len(tc.row))
	for i, value := range tc.row {
		fields[i] = &querypb.Field{Type: value.Type()}
	}
	env := &ExpressionEnv{
		BindVars: tc.bv,
		Row:      tc.row,
		Fields:   fields,
	}
	cmp, err := translateComparisonExpr2(tc.op, tc.v1, tc.v2)
	if err != nil {
		t.Fatalf("failed to convert: %v", err)
	}

	got, err := env.Evaluate(cmp)
	if tc.err == "" {
		require.NoError(t, err)
		if tc.out != nil && *tc.out {
			require.EqualValues(t, uint64(1), got.uint64())
		} else if tc.out != nil && !*tc.out {
			require.EqualValues(t, uint64(0), got.uint64())
		} else {
			require.EqualValues(t, sqltypes.Null, got.typeof())
		}
	} else {
		require.EqualError(t, err, tc.err)
	}
}

// This test tests the comparison of two integers
func TestCompareIntegers(t *testing.T) {
	tests := []testCase{
		{
			name: "integers are equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewInt64(18)},
		},
		{
			name: "integers are equal (2)",
			v1:   NewLiteralInt(56), v2: NewLiteralInt(56),
			out: &T, op: sqlparser.EqualOp,
		},
		{
			name: "integers are not equal (1)",
			v1:   NewLiteralInt(56), v2: NewLiteralInt(10),
			out: &F, op: sqlparser.EqualOp,
		},
		{
			name: "integers are not equal (2)",
			v1:   NewLiteralInt(56), v2: NewLiteralInt(10),
			out: &T, op: sqlparser.NotEqualOp,
		},
		{
			name: "integers are not equal (3)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewInt64(18), sqltypes.NewInt64(98)},
		},
		{
			name: "unsigned integers are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewUint64(18)},
		},
		{
			name: "unsigned integer and integer are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewUint64(18), sqltypes.NewInt64(18)},
		},
		{
			name: "unsigned integer and integer are not equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewUint64(18), sqltypes.NewInt64(42)},
		},
		{
			name: "integer is less than integer",
			v1:   NewLiteralInt(3549), v2: NewLiteralInt(8072),
			out: &T, op: sqlparser.LessThanOp,
		},
		{
			name: "integer is not less than integer",
			v1:   NewLiteralInt(3549), v2: NewLiteralInt(21),
			out: &F, op: sqlparser.LessThanOp,
		},
		{
			name: "integer is less-equal to integer (1)",
			v1:   NewLiteralInt(3549), v2: NewLiteralInt(9863),
			out: &T, op: sqlparser.LessEqualOp,
		},
		{
			name: "integer is less-equal to integer (2)",
			v1:   NewLiteralInt(3549), v2: NewLiteralInt(3549),
			out: &T, op: sqlparser.LessEqualOp,
		},
		{
			name: "integer is greater than integer",
			v1:   NewLiteralInt(9809), v2: NewLiteralInt(9800),
			out: &T, op: sqlparser.GreaterThanOp,
		},
		{
			name: "integer is not greater than integer",
			v1:   NewLiteralInt(549), v2: NewLiteralInt(21579),
			out: &F, op: sqlparser.GreaterThanOp,
		},
		{
			name: "integer is greater-equal to integer (1)",
			v1:   NewLiteralInt(987), v2: NewLiteralInt(15),
			out: &T, op: sqlparser.GreaterEqualOp,
		},
		{
			name: "integer is greater-equal to integer (2)",
			v1:   NewLiteralInt(3549), v2: NewLiteralInt(3549),
			out: &T, op: sqlparser.GreaterEqualOp,
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of two floats
func TestCompareFloats(t *testing.T) {
	tests := []testCase{
		{
			name: "floats are equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewFloat64(18)},
		},
		{
			name: "floats are equal (2)",
			v1:   NewLiteralFloat(3549.9), v2: NewLiteralFloat(3549.9),
			out: &T, op: sqlparser.EqualOp,
		},
		{
			name: "floats are not equal (1)",
			v1:   NewLiteralFloat(7858.016), v2: NewLiteralFloat(8943298.56),
			out: &F, op: sqlparser.EqualOp,
		},
		{
			name: "floats are not equal (2)",
			v1:   NewLiteralFloat(351049.65), v2: NewLiteralFloat(62508.99),
			out: &T, op: sqlparser.NotEqualOp,
		},
		{
			name: "floats are not equal (3)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewFloat64(16516.84), sqltypes.NewFloat64(219541.01)},
		},
		{
			name: "float is less than float",
			v1:   NewLiteralFloat(3549.9), v2: NewLiteralFloat(8072),
			out: &T, op: sqlparser.LessThanOp,
		},
		{
			name: "float is not less than float",
			v1:   NewLiteralFloat(3549.9), v2: NewLiteralFloat(21.564),
			out: &F, op: sqlparser.LessThanOp,
		},
		{
			name: "float is less-equal to float (1)",
			v1:   NewLiteralFloat(3549.9), v2: NewLiteralFloat(9863),
			out: &T, op: sqlparser.LessEqualOp,
		},
		{
			name: "float is less-equal to float (2)",
			v1:   NewLiteralFloat(3549.9), v2: NewLiteralFloat(3549.9),
			out: &T, op: sqlparser.LessEqualOp,
		},
		{
			name: "float is greater than float",
			v1:   NewLiteralFloat(9808.549), v2: NewLiteralFloat(9808.540),
			out: &T, op: sqlparser.GreaterThanOp,
		},
		{
			name: "float is not greater than float",
			v1:   NewLiteralFloat(549.02), v2: NewLiteralFloat(21579.64),
			out: &F, op: sqlparser.GreaterThanOp,
		},
		{
			name: "float is greater-equal to float (1)",
			v1:   NewLiteralFloat(987.30), v2: NewLiteralFloat(15.5),
			out: &T, op: sqlparser.GreaterEqualOp,
		},
		{
			name: "float is greater-equal to float (2)",
			v1:   NewLiteralFloat(3549.9), v2: NewLiteralFloat(3549.9),
			out: &T, op: sqlparser.GreaterEqualOp,
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of two decimals
func TestCompareDecimals(t *testing.T) {
	tests := []testCase{
		{
			name: "decimals are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("12.9019")},
		},
		{
			name: "decimals are not equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("12.9019"), sqltypes.NewDecimal("489.156849")},
		},
		{
			name: "decimal is greater than decimal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewDecimal("192.128")},
		},
		{
			name: "decimal is not greater than decimal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("192.128"), sqltypes.NewDecimal("192.129")},
		},
		{
			name: "decimal is less than decimal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("192.128"), sqltypes.NewDecimal("192.129")},
		},
		{
			name: "decimal is not less than decimal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewDecimal("192.128")},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of numerical values (float, decimal, integer)
func TestCompareNumerics(t *testing.T) {
	tests := []testCase{
		{
			name: "decimal and float are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewFloat64(189.6), sqltypes.NewDecimal("189.6")},
		},
		{
			name: "decimal and float with negative values are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewFloat64(-98.1839), sqltypes.NewDecimal("-98.1839")},
		},
		{
			name: "decimal and float with negative values are not equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewFloat64(-98.9381), sqltypes.NewDecimal("-98.1839")},
		},
		{
			name: "decimal and float with negative values are not equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewFloat64(-98.9381), sqltypes.NewDecimal("-98.1839")},
		},
		{
			name: "decimal and integer are equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewInt64(8979), sqltypes.NewDecimal("8979")},
		},
		{
			name: "decimal and integer are equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("8979.0000"), sqltypes.NewInt64(8979)},
		},
		{
			name: "decimal and unsigned integer are equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewUint64(901), sqltypes.NewDecimal("901")},
		},
		{
			name: "decimal and unsigned integer are equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("901.00"), sqltypes.NewUint64(901)},
		},
		{
			name: "decimal and unsigned integer are not equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewUint64(192)},
		},
		{
			name: "decimal and unsigned integer are not equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewUint64(192)},
		},
		{
			name: "decimal is greater than integer",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("1.01"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is greater-equal to integer",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("1.00"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is less than integer",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal(".99"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is less-equal to integer",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("1.00"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is greater than float",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("849.896"), sqltypes.NewFloat64(86.568)},
		},
		{
			name: "decimal is not greater than float",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("15.23"), sqltypes.NewFloat64(8689.5)},
		},
		{
			name: "decimal is greater-equal to float (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("65"), sqltypes.NewFloat64(65)},
		},
		{
			name: "decimal is greater-equal to float (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("65"), sqltypes.NewFloat64(60)},
		},
		{
			name: "decimal is less than float",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("0.998"), sqltypes.NewFloat64(0.999)},
		},
		{
			name: "decimal is less-equal to float",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewDecimal("1.000101"), sqltypes.NewFloat64(1.00101)},
		},
		{
			name: "different int types are equal for 8 bit",
			v1:   NewColumn(0, defaultCollation()), v2: NewLiteralInt(0),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewInt8(0)},
		},
		{
			name: "different int types are equal for 32 bit",
			v1:   NewColumn(0, defaultCollation()), v2: NewLiteralInt(0),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewInt32(0)},
		},
		{
			name: "different int types are equal for float32 bit",
			v1:   NewColumn(0, defaultCollation()), v2: NewLiteralFloat(1.0),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Float32, []byte("1.0"))},
		},
		{
			name: "different unsigned int types are equal for 8 bit",
			v1:   NewColumn(0, defaultCollation()), v2: NewLiteralInt(0),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Uint8, []byte("0"))},
		},
		{
			name: "different unsigned int types are equal for 32 bit",
			v1:   NewColumn(0, defaultCollation()), v2: NewLiteralInt(0),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewUint32(0)},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of two datetimes
func TestCompareDatetime(t *testing.T) {
	tests := []testCase{
		{
			name: "datetimes are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-22 12:00:00")},
		},
		{
			name: "datetimes are not equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-22 12:00:00"), sqltypes.NewDatetime("2020-10-22 12:00:00")},
		},
		{
			name: "datetimes are not equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-22 12:00:00"), sqltypes.NewDatetime("2021-10-22 10:23:56")},
		},
		{
			name: "datetimes are not equal (3)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-01 00:00:00"), sqltypes.NewDatetime("2021-02-01 00:00:00")},
		},
		{
			name: "datetime is greater than datetime",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-30 10:42:50"), sqltypes.NewDatetime("2021-10-01 13:10:02")},
		},
		{
			name: "datetime is not greater than datetime",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-01 13:10:02"), sqltypes.NewDatetime("2021-10-30 10:42:50")},
		},
		{
			name: "datetime is less than datetime",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-01 13:10:02"), sqltypes.NewDatetime("2021-10-30 10:42:50")},
		},
		{
			name: "datetime is not less than datetime",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-30 10:42:50"), sqltypes.NewDatetime("2021-10-01 13:10:02")},
		},
		{
			name: "datetime is greater-equal to datetime (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-30 10:42:50"), sqltypes.NewDatetime("2021-10-30 10:42:50")},
		},
		{
			name: "datetime is greater-equal to datetime (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-30 10:42:50"), sqltypes.NewDatetime("2021-10-01 13:10:02")},
		},
		{
			name: "datetime is less-equal to datetime (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-30 10:42:50"), sqltypes.NewDatetime("2021-10-30 10:42:50")},
		},
		{
			name: "datetime is less-equal to datetime (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewDatetime("2021-10-01 13:10:02"), sqltypes.NewDatetime("2021-10-30 10:42:50")},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of two timestamps
func TestCompareTimestamp(t *testing.T) {
	tests := []testCase{
		{
			name: "timestamps are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-22 12:00:00")},
		},
		{
			name: "timestamps are not equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-22 12:00:00"), sqltypes.NewTimestamp("2020-10-22 12:00:00")},
		},
		{
			name: "timestamps are not equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-22 12:00:00"), sqltypes.NewTimestamp("2021-10-22 10:23:56")},
		},
		{
			name: "timestamps are not equal (3)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-01 00:00:00"), sqltypes.NewTimestamp("2021-02-01 00:00:00")},
		},
		{
			name: "timestamp is greater than timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-30 10:42:50"), sqltypes.NewTimestamp("2021-10-01 13:10:02")},
		},
		{
			name: "timestamp is not greater than timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-01 13:10:02"), sqltypes.NewTimestamp("2021-10-30 10:42:50")},
		},
		{
			name: "timestamp is less than timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-01 13:10:02"), sqltypes.NewTimestamp("2021-10-30 10:42:50")},
		},
		{
			name: "timestamp is not less than timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-30 10:42:50"), sqltypes.NewTimestamp("2021-10-01 13:10:02")},
		},
		{
			name: "timestamp is greater-equal to timestamp (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-30 10:42:50"), sqltypes.NewTimestamp("2021-10-30 10:42:50")},
		},
		{
			name: "timestamp is greater-equal to timestamp (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-30 10:42:50"), sqltypes.NewTimestamp("2021-10-01 13:10:02")},
		},
		{
			name: "timestamp is less-equal to timestamp (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-30 10:42:50"), sqltypes.NewTimestamp("2021-10-30 10:42:50")},
		},
		{
			name: "timestamp is less-equal to timestamp (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewTimestamp("2021-10-01 13:10:02"), sqltypes.NewTimestamp("2021-10-30 10:42:50")},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of two dates
func TestCompareDate(t *testing.T) {
	tests := []testCase{
		{
			name: "dates are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-22")},
		},
		{
			name: "dates are not equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-22"), sqltypes.NewDate("2020-10-21")},
		},
		{
			name: "dates are not equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-01"), sqltypes.NewDate("2021-02-01")},
		},
		{
			name: "date is greater than date",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-30"), sqltypes.NewDate("2021-10-01")},
		},
		{
			name: "date is not greater than date",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-01"), sqltypes.NewDate("2021-10-30")},
		},
		{
			name: "date is less than date",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-01"), sqltypes.NewDate("2021-10-30")},
		},
		{
			name: "date is not less than date",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-30"), sqltypes.NewDate("2021-10-01")},
		},
		{
			name: "date is greater-equal to date (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-30"), sqltypes.NewDate("2021-10-30")},
		},
		{
			name: "date is greater-equal to date (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-30"), sqltypes.NewDate("2021-10-01")},
		},
		{
			name: "date is less-equal to date (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-30"), sqltypes.NewDate("2021-10-30")},
		},
		{
			name: "date is less-equal to date (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-01"), sqltypes.NewDate("2021-10-30")},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of two times
func TestCompareTime(t *testing.T) {
	tests := []testCase{
		{
			name: "times are equal",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(0, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewTime("12:00:00")},
		},
		{
			name: "times are not equal (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewTime("12:00:00"), sqltypes.NewTime("10:23:56")},
		},
		{
			name: "times are not equal (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewTime("00:00:00"), sqltypes.NewTime("10:15:00")},
		},
		{
			name: "time is greater than time",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewTime("18:14:35"), sqltypes.NewTime("13:01:38")},
		},
		{
			name: "time is not greater than time",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.GreaterThanOp,
			row: []sqltypes.Value{sqltypes.NewTime("02:46:02"), sqltypes.NewTime("10:42:50")},
		},
		{
			name: "time is less than time",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewTime("04:30:00"), sqltypes.NewTime("09:23:48")},
		},
		{
			name: "time is not less than time",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &F, op: sqlparser.LessThanOp,
			row: []sqltypes.Value{sqltypes.NewTime("15:21:00"), sqltypes.NewTime("10:00:00")},
		},
		{
			name: "time is greater-equal to time (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewTime("10:42:50"), sqltypes.NewTime("10:42:50")},
		},
		{
			name: "time is greater-equal to time (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.GreaterEqualOp,
			row: []sqltypes.Value{sqltypes.NewTime("19:42:50"), sqltypes.NewTime("13:10:02")},
		},
		{
			name: "time is less-equal to time (1)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewTime("10:42:50"), sqltypes.NewTime("10:42:50")},
		},
		{
			name: "time is less-equal to time (2)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.LessEqualOp,
			row: []sqltypes.Value{sqltypes.NewTime("10:10:02"), sqltypes.NewTime("10:42:50")},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of two dates (datetime, date, timestamp, time)
func TestCompareDates(t *testing.T) {
	tests := []testCase{
		{
			name: "date equal datetime",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-22"), sqltypes.NewDatetime("2021-10-22 00:00:00")},
		},
		{
			name: "date equal datetime through bind variables",
			v1:   NewBindVar("k1", defaultCollation()), v2: NewBindVar("k2", defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			bv: map[string]*querypb.BindVariable{
				"k1": {Type: sqltypes.Date, Value: []byte("2021-10-22")},
				"k2": {Type: sqltypes.Datetime, Value: []byte("2021-10-22 00:00:00")},
			},
		},
		{
			name: "date not equal datetime through bind variables",
			v1:   NewBindVar("k1", defaultCollation()), v2: NewBindVar("k2", defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			bv: map[string]*querypb.BindVariable{
				"k1": {Type: sqltypes.Date, Value: []byte("2021-02-20")},
				"k2": {Type: sqltypes.Datetime, Value: []byte("2021-10-22 00:00:00")},
			},
		},
		{
			name: "date not equal datetime",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-22"), sqltypes.NewDatetime("2021-10-20 00:06:00")},
		},
		{
			name: "date equal timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-22"), sqltypes.NewTimestamp("2021-10-22 00:00:00")},
		},
		{
			name: "date not equal timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-10-22"), sqltypes.NewTimestamp("2021-10-22 16:00:00")},
		},
		{
			name: "date equal time",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewDate(time.Now().Format("2006-01-02")), sqltypes.NewTime("00:00:00")},
		},
		{
			name: "date not equal time",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate(time.Now().Format("2006-01-02")), sqltypes.NewTime("12:00:00")},
		},
		{
			name: "string equal datetime",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("2021-10-22"), sqltypes.NewDatetime("2021-10-22 00:00:00")},
		},
		{
			name: "string equal timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("2021-10-22 00:00:00"), sqltypes.NewTimestamp("2021-10-22 00:00:00")},
		},
		{
			name: "string not equal timestamp",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("2021-10-22 06:00:30"), sqltypes.NewTimestamp("2021-10-20 15:02:10")},
		},
		{
			name: "string equal time",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("00:05:12"), sqltypes.NewTime("00:05:12")},
		},
		{
			name: "string equal date",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("2021-02-22"), sqltypes.NewDate("2021-02-22")},
		},
		{
			name: "string not equal date (1, date on the RHS)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("2021-02-20"), sqltypes.NewDate("2021-03-30")},
		},
		{
			name: "string not equal date (2, date on the LHS)",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.NotEqualOp,
			row: []sqltypes.Value{sqltypes.NewDate("2021-03-30"), sqltypes.NewVarChar("2021-02-20")},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// This test tests the comparison of strings
func TestCompareStrings(t *testing.T) {
	tests := []testCase{
		{
			name: "string equal string",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("toto"), sqltypes.NewVarChar("toto")},
		},
		{
			name: "string equal number",
			v1:   NewColumn(0, defaultCollation()), v2: NewColumn(1, defaultCollation()),
			out: &T, op: sqlparser.EqualOp,
			row: []sqltypes.Value{sqltypes.NewVarChar("1"), sqltypes.NewInt64(1)},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// TestInOp tests the In operator comparisons
func TestInOp(t *testing.T) {
	tests := []testCase{
		{
			name: "integer In tuple",
			v1:   NewLiteralInt(52), v2: TupleExpr{NewLiteralInt(52), NewLiteralInt(54)},
			out: &T,
			op:  sqlparser.InOp,
		}, {
			name: "integer not In tuple",
			v1:   NewLiteralInt(51), v2: TupleExpr{NewLiteralInt(52), NewLiteralInt(54)},
			out: &F,
			op:  sqlparser.InOp,
		}, {
			name: "integer In tuple - single value",
			v1:   NewLiteralInt(52), v2: TupleExpr{NewLiteralInt(52)},
			out: &T,
			op:  sqlparser.InOp,
		}, {
			name: "integer not In tuple - single value",
			v1:   NewLiteralInt(51), v2: TupleExpr{NewLiteralInt(52)},
			out: &F,
			op:  sqlparser.InOp,
		}, {
			name: "integer not In tuple - no value",
			v1:   NewLiteralInt(51), v2: TupleExpr{},
			out: &F,
			op:  sqlparser.InOp,
		}, {
			name: "integer not In tuple - null value",
			v1:   NewLiteralInt(51), v2: TupleExpr{NullExpr},
			out: nil,
			op:  sqlparser.InOp,
		}, {
			name: "integer not In tuple but with NullExpr inside",
			v1:   NewLiteralInt(52), v2: TupleExpr{NullExpr, NewLiteralInt(51), NewLiteralInt(54), NullExpr},
			out: nil,
			op:  sqlparser.InOp,
		}, {
			name: "integer In tuple with null inside",
			v1:   NewLiteralInt(52), v2: TupleExpr{NullExpr, NewLiteralInt(52), NewLiteralInt(54)},
			out: &T,
			op:  sqlparser.InOp,
		}, {
			name: "NullExpr In tuple",
			v1:   NullExpr, v2: TupleExpr{NullExpr, NewLiteralInt(52), NewLiteralInt(54)},
			out: nil,
			op:  sqlparser.InOp,
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// TestNotInOp tests the NotIn operator comparisons
func TestNotInOp(t *testing.T) {
	tests := []testCase{
		{
			name: "integer In tuple",
			v1:   NewLiteralInt(52), v2: TupleExpr{NewLiteralInt(52), NewLiteralInt(54)},
			out: &F,
			op:  sqlparser.NotInOp,
		}, {
			name: "integer not In tuple",
			v1:   NewLiteralInt(51), v2: TupleExpr{NewLiteralInt(52), NewLiteralInt(54)},
			out: &T,
			op:  sqlparser.NotInOp,
		}, {
			name: "integer In tuple - single value",
			v1:   NewLiteralInt(52), v2: TupleExpr{NewLiteralInt(52)},
			out: &F,
			op:  sqlparser.NotInOp,
		}, {
			name: "integer not In tuple - single value",
			v1:   NewLiteralInt(51), v2: TupleExpr{NewLiteralInt(52)},
			out: &T,
			op:  sqlparser.NotInOp,
		}, {
			name: "integer not In tuple - no value",
			v1:   NewLiteralInt(51), v2: TupleExpr{},
			out: &T,
			op:  sqlparser.NotInOp,
		}, {
			name: "integer not In tuple - null value",
			v1:   NewLiteralInt(51), v2: TupleExpr{NullExpr},
			out: nil,
			op:  sqlparser.NotInOp,
		}, {
			name: "integer not In tuple but with NullExpr inside",
			v1:   NewLiteralInt(52), v2: TupleExpr{NullExpr, NewLiteralInt(51), NewLiteralInt(54), NullExpr},
			out: nil,
			op:  sqlparser.NotInOp,
		}, {
			name: "integer In tuple with null inside",
			v1:   NewLiteralInt(52), v2: TupleExpr{NullExpr, NewLiteralInt(52), NewLiteralInt(54)},
			out: &F,
			op:  sqlparser.NotInOp,
		}, {
			name: "NullExpr In tuple",
			v1:   NullExpr, v2: TupleExpr{NullExpr, NewLiteralInt(52), NewLiteralInt(54)},
			out: nil,
			op:  sqlparser.NotInOp,
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

// TestNotInOp tests the NotIn operator comparisons
func TestNullComparisons(t *testing.T) {
	tests := []testCase{
		{
			name: "null not like string",
			v1:   NullExpr, v2: NewLiteralString([]byte("foo"), defaultCollation()),
			op: sqlparser.NotLikeOp,
		},
		{
			name: "null equal integer",
			v1:   NullExpr, v2: NewLiteralInt(10),
			op: sqlparser.EqualOp,
		},
		{
			name: "null null-safe-equal null",
			v1:   NullExpr, v2: NullExpr,
			out: &T,
			op:  sqlparser.NullSafeEqualOp,
		},
		{
			name: "0 null-safe-equal null",
			v1:   NewLiteralInt(0), v2: NullExpr,
			out: &F,
			op:  sqlparser.NullSafeEqualOp,
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}

func TestNullsafeCompare(t *testing.T) {
	collation := collationEnv.LookupByName("utf8mb4_general_ci").ID()
	tcases := []struct {
		v1, v2 sqltypes.Value
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
		v1:  TestValue(sqltypes.VarChar, "abcd"),
		v2:  TestValue(sqltypes.VarChar, "abcd"),
		out: 0,
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
		v1:  TestValue(sqltypes.VarBinary, "abcd"),
		v2:  TestValue(sqltypes.Binary, "abcd"),
		out: 0,
	}, {
		// Non-numeric unequal
		v1:  TestValue(sqltypes.VarBinary, "abcd"),
		v2:  TestValue(sqltypes.Binary, "bcde"),
		out: -1,
	}, {
		// Date/Time types
		v1:  TestValue(sqltypes.Datetime, "1000-01-01 00:00:00"),
		v2:  TestValue(sqltypes.Binary, "1000-01-01 00:00:00"),
		out: 0,
	}, {
		// Date/Time types
		v1:  TestValue(sqltypes.Datetime, "2000-01-01 00:00:00"),
		v2:  TestValue(sqltypes.Binary, "1000-01-01 00:00:00"),
		out: 1,
	}, {
		// Date/Time types
		v1:  TestValue(sqltypes.Datetime, "1000-01-01 00:00:00"),
		v2:  TestValue(sqltypes.Binary, "2000-01-01 00:00:00"),
		out: -1,
	}, {
		// Date/Time types
		v1:  TestValue(sqltypes.Bit, "101"),
		v2:  TestValue(sqltypes.Bit, "101"),
		out: 0,
	}, {
		// Date/Time types
		v1:  TestValue(sqltypes.Bit, "1"),
		v2:  TestValue(sqltypes.Bit, "0"),
		out: 1,
	}, {
		// Date/Time types
		v1:  TestValue(sqltypes.Bit, "0"),
		v2:  TestValue(sqltypes.Bit, "1"),
		out: -1,
	}}
	for _, tcase := range tcases {
		got, err := NullsafeCompare(tcase.v1, tcase.v2, collation)
		if tcase.err != nil {
			require.EqualError(t, err, tcase.err.Error())
		}
		if tcase.err != nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("NullsafeCompare(%v, %v): %v, want %v", printValue(tcase.v1), printValue(tcase.v2), got, tcase.out)
		}
	}
}

func getCollationID(collation string) collations.ID {
	id, _ := collationEnv.LookupID(collation)
	return id
}

func TestNullsafeCompareCollate(t *testing.T) {
	tcases := []struct {
		v1, v2    string
		collation collations.ID
		out       int
		err       error
	}{
		{
			// case insensitive
			v1:        "abCd",
			v2:        "aBcd",
			out:       0,
			collation: getCollationID("utf8mb4_0900_as_ci"),
		},
		{
			// accent sensitive
			v1:        "ǍḄÇ",
			v2:        "ÁḆĈ",
			out:       1,
			collation: getCollationID("utf8mb4_0900_as_ci"),
		},
		{
			// hangul decomposition
			v1:        "\uAC00",
			v2:        "\u326E",
			out:       0,
			collation: getCollationID("utf8mb4_0900_as_ci"),
		},
		{
			// kana sensitive
			v1:        "\xE3\x81\xAB\xE3\x81\xBB\xE3\x82\x93\xE3\x81\x94",
			v2:        "\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4",
			out:       -1,
			collation: getCollationID("utf8mb4_ja_0900_as_cs_ks"),
		},
		{
			// non breaking space
			v1:        "abc ",
			v2:        "abc\u00a0",
			out:       -1,
			collation: getCollationID("utf8mb4_0900_as_cs"),
		},
		{
			// "cs" counts as a separate letter, where c < cs < d
			v1:        "c",
			v2:        "cs",
			out:       -1,
			collation: getCollationID("utf8mb4_hu_0900_ai_ci"),
		},
		{
			v1:        "abcd",
			v2:        "abcd",
			collation: 0,
			err:       vterrors.New(vtrpcpb.Code_UNKNOWN, "cannot compare strings, collation is unknown or unsupported (collation ID: 0)"),
		},
		{
			v1:        "abcd",
			v2:        "abcd",
			collation: 1111,
			err:       vterrors.New(vtrpcpb.Code_UNKNOWN, "cannot compare strings, collation is unknown or unsupported (collation ID: 1111)"),
		},
		{
			v1: "abcd",
			v2: "abcd",
			// unsupported collation gb18030_bin
			collation: 249,
			err:       vterrors.New(vtrpcpb.Code_UNKNOWN, "cannot compare strings, collation is unknown or unsupported (collation ID: 249)"),
		},
	}
	for _, tcase := range tcases {
		got, err := NullsafeCompare(TestValue(sqltypes.VarChar, tcase.v1), TestValue(sqltypes.VarChar, tcase.v2), tcase.collation)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("NullsafeCompare(%v, %v) error: %v, want %v", tcase.v1, tcase.v2, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if got != tcase.out {
			t.Errorf("NullsafeCompare(%v, %v): %v, want %v", tcase.v1, tcase.v2, got, tcase.out)
		}
	}
}

func BenchmarkNullSafeComparison(b *testing.B) {
	var collnames = []string{
		"utf8mb4_0900_ai_ci",
		"utf8mb4_0900_as_cs",
		"utf8mb4_general_ci",
		"utf8mb4_0900_bin",
	}

	var lengths = []int{1, 16}

	for _, collation := range collnames {
		for _, length := range lengths {
			b.Run(fmt.Sprintf("Strings/%s/%d", collation, length), func(b *testing.B) {
				var collid = getCollationID(collation)
				var long = func(in string) string {
					return strings.Repeat(in, length)
				}
				var inputs = []sqltypes.Value{
					TestValue(sqltypes.VarChar, long("abCd")),
					TestValue(sqltypes.VarChar, long("aBcd")),
					TestValue(sqltypes.VarChar, long("ǍḄÇ")),
					TestValue(sqltypes.VarChar, long("ÁḆĈ")),
					TestValue(sqltypes.VarChar, long("\xE3\x81\xAB\xE3\x81\xBB\xE3\x82\x93\xE3\x81\x94")),
					TestValue(sqltypes.VarChar, long("\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4")),
				}
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					for _, lhs := range inputs {
						for _, rhs := range inputs {
							_, _ = NullsafeCompare(lhs, rhs, collid)
						}
					}
				}
			})
		}
	}

	b.Run("Numeric", func(b *testing.B) {
		var inputs = []sqltypes.Value{
			TestValue(sqltypes.Int64, "123456789"),
			TestValue(sqltypes.Uint64, "123456789"),
			TestValue(sqltypes.Int64, "-123456789"),
			TestValue(sqltypes.Int64, "0"),
			TestValue(sqltypes.Uint64, "0"),
			TestValue(sqltypes.Float64, "1.0"),
			TestValue(sqltypes.Float64, "0.0"),
			TestValue(sqltypes.Decimal, "1.0000"),
			TestValue(sqltypes.Decimal, "0.0000"),
			TestValue(sqltypes.VarChar, "12345"),
			TestValue(sqltypes.VarChar, "0"),
			TestValue(sqltypes.VarChar, "12392874.0potato"),
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for _, lhs := range inputs {
				for _, rhs := range inputs {
					_, _ = NullsafeCompare(lhs, rhs, collations.CollationUtf8mb4ID)
				}
			}
		}
	})
}
