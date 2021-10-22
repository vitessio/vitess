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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

type testCase struct {
	name   string
	v1, v2 Expr
	out    *bool
	err    string
	op     ComparisonOp
	bv     map[string]*querypb.BindVariable
	row    []sqltypes.Value
}

var (
	T = true
	F = false
)

func (tc testCase) run(t *testing.T) {
	if tc.bv == nil {
		tc.bv = map[string]*querypb.BindVariable{}
	}
	env := ExpressionEnv{
		BindVars: tc.bv,
		Row:      tc.row,
	}
	cmp := ComparisonExpr{
		Op:    tc.op,
		Left:  tc.v1,
		Right: tc.v2,
	}
	got, err := cmp.Evaluate(env)
	if tc.err == "" {
		require.NoError(t, err)
		if tc.out != nil && *tc.out {
			require.EqualValues(t, 1, got.ival)
		} else if tc.out != nil && !*tc.out {
			require.EqualValues(t, 0, got.ival)
		} else {
			require.EqualValues(t, 0, got.ival)
			require.EqualValues(t, sqltypes.Null, got.typ)
		}
	} else {
		require.EqualError(t, err, tc.err)
	}
}

func TestCompareDecimals(t *testing.T) {
	tests := []testCase{
		{
			name: "decimals are equal",
			v1:   NewColumn(0), v2: NewColumn(0),
			out: &T, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("12.9019")},
		},
		{
			name: "decimals are not equal",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &NotEqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("12.9019"), sqltypes.NewDecimal("489.156849")},
		},
		{
			name: "decimal and float are equal",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewFloat64(189.6), sqltypes.NewDecimal("189.6")},
		},
		{
			name: "decimal and float with negative values are equal",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewFloat64(-98.1839), sqltypes.NewDecimal("-98.1839")},
		},
		{
			name: "decimal and float with negative values are not equal (1)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &F, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewFloat64(-98.9381), sqltypes.NewDecimal("-98.1839")},
		},
		{
			name: "decimal and float with negative values are not equal (2)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &NotEqualOp{},
			row: []sqltypes.Value{sqltypes.NewFloat64(-98.9381), sqltypes.NewDecimal("-98.1839")},
		},
		{
			name: "decimal and integer are equal (1)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewInt64(8979), sqltypes.NewDecimal("8979")},
		},
		{
			name: "decimal and integer are equal (2)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("8979.0000"), sqltypes.NewInt64(8979)},
		},
		{
			name: "decimal and unsigned integer are equal (1)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewUint64(901), sqltypes.NewDecimal("901")},
		},
		{
			name: "decimal and unsigned integer are equal (2)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("901.00"), sqltypes.NewUint64(901)},
		},
		{
			name: "decimal and unsigned integer are not equal (1)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &NotEqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewUint64(192)},
		},
		{
			name: "decimal and unsigned integer are not equal (2)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &F, op: &EqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewUint64(192)},
		},
		{
			name: "decimal is greater than decimal",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &GreaterThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewDecimal("192.128")},
		},
		{
			name: "decimal is not greater than decimal",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &F, op: &GreaterThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("192.128"), sqltypes.NewDecimal("192.129")},
		},
		{
			name: "decimal is greater than integer",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &GreaterThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("1.01"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is greater-equal to integer",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &GreaterEqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("1.00"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is less than decimal",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &LessThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("192.128"), sqltypes.NewDecimal("192.129")},
		},
		{
			name: "decimal is not less than decimal",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &F, op: &LessThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("192.129"), sqltypes.NewDecimal("192.128")},
		},
		{
			name: "decimal is less than integer",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &LessThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal(".99"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is less-equal to integer",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &LessEqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("1.00"), sqltypes.NewInt64(1)},
		},
		{
			name: "decimal is greater than float",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &GreaterThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("849.896"), sqltypes.NewFloat64(86.568)},
		},
		{
			name: "decimal is not greater than float",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &F, op: &GreaterThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("15.23"), sqltypes.NewFloat64(8689.5)},
		},
		{
			name: "decimal is greater-equal to float (1)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &GreaterEqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("65"), sqltypes.NewFloat64(65)},
		},
		{
			name: "decimal is greater-equal to float (2)",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &GreaterEqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("65"), sqltypes.NewFloat64(60)},
		},
		{
			name: "decimal is less than float",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &LessThanOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("0.998"), sqltypes.NewFloat64(0.999)},
		},
		{
			name: "decimal is less-equal to float",
			v1:   NewColumn(0), v2: NewColumn(1),
			out: &T, op: &LessEqualOp{},
			row: []sqltypes.Value{sqltypes.NewDecimal("1.000101"), sqltypes.NewFloat64(1.00101)},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			tcase.run(t)
		})
	}
}
