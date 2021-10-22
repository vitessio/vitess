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
	bv     map[string]*querypb.BindVariable
	row    []sqltypes.Value
}

var (
	T = true
	F = false
)

func (tc testCase) run(t *testing.T, i int, cmpOp ComparisonOp) {
	if tc.bv == nil {
		tc.bv = map[string]*querypb.BindVariable{}
	}
	env := ExpressionEnv{
		BindVars: tc.bv,
		Row:      tc.row,
	}
	cmp := ComparisonExpr{
		Op:    cmpOp,
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

func TestComparisonEquality(t *testing.T) {
	tests := []testCase{
		{
			out: nil,
		}, {
			name: "Second value null.",
			v1:   NewLiteralInt(1),
			v2:   &Null{},
			out:  nil,
		}, {
			name: "First value null.",
			v1:   &Null{},
			v2:   NewLiteralInt(1),
			out:  nil,
		}, {
			name: "int with int",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralInt(1),
			out:  &T,
		}, {
			name: "wrong int",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralInt(2),
			out:  &F,
		}, {
			name: "int with string",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralString([]byte("1")),
			out:  &T,
		}, {
			name: "varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("1")),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewVarBinary("1")},
		}, {
			name: "int column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("1")),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewInt32(1)},
		}, {
			name: "wrong varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("42")),
			out:  &F,
			row:  []sqltypes.Value{sqltypes.NewVarBinary("1")},
		}, {
			name: "string with int",
			v1:   NewLiteralString([]byte("1")),
			v2:   NewLiteralInt(1),
			out:  &T,
		}, {
			name: "wrong int with string",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralString([]byte("42")),
			out:  &F,
		}, {
			name: "wrong string with int",
			v1:   NewLiteralString([]byte("42")),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float with float",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralFloat(1.7),
			out:  &T,
		}, {
			name: "wrong float with float",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralFloat(9.1),
			out:  &F,
		}, {
			name: "wrong float with int",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float with int",
			v1:   NewLiteralFloat(1.00),
			v2:   NewLiteralInt(1),
			out:  &T,
		}, {
			name: "float with float column",
			v1:   NewLiteralFloat(42.21),
			v2:   NewColumn(0),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewFloat64(42.21)},
		}, {
			name: "string with string",
			v1:   NewLiteralString([]byte(";")),
			v2:   NewLiteralString([]byte(";")),
			out:  &T,
		}, {
			name: "column date with column date",
			v1:   NewColumn(0),
			v2:   NewColumn(0),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewDate("2012-01-01")},
		}, {
			name: "column datetime with column datetime",
			v1:   NewColumn(0),
			v2:   NewColumn(0),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewDatetime("2012-01-01 12:00:00")},
		}, {
			name: "column timestamp with column timestamp",
			v1:   NewColumn(0),
			v2:   NewColumn(0),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewTimestamp("2012-01-01 12:00:00")},
		}, {
			name: "column time with column time",
			v1:   NewColumn(0),
			v2:   NewColumn(0),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewTime("12:00:00")},
		}, {
			name: "column decimal with column decimal",
			v1:   NewColumn(0),
			v2:   NewColumn(1),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewDecimal("12.9012"), sqltypes.NewDecimal("12.9012")},
		},
	}

	for i, tcase := range tests {
		t.Run(fmt.Sprintf("%d %s", i, tcase.name), func(t *testing.T) {
			cmpOp := &EqualOp{}
			tcase.run(t, i+1, cmpOp)
		})
	}
}

func TestComparisonLess(t *testing.T) {
	testsLessDifferentValues := []testCase{
		{
			name: "All Nulls",
			v1:   &Null{},
			v2:   &Null{},
			out:  nil,
		}, {
			name: "Second value null.",
			v1:   NewLiteralInt(1),
			v2:   &Null{},
			out:  nil,
		}, {
			name: "First value null.",
			v1:   &Null{},
			v2:   NewLiteralInt(1),
			out:  nil,
		}, {
			name: "int with int",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralInt(2),
			out:  &T,
		}, {
			name: "wrong int",
			v1:   NewLiteralInt(2),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "int with string",
			v1:   NewLiteralInt(40),
			v2:   NewLiteralString([]byte("80")),
			out:  &T,
		}, {
			name: "string with string",
			v1:   NewLiteralString([]byte("10")),
			v2:   NewLiteralString([]byte("11")),
			out:  &T,
		}, {
			name: "varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("10")),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewVarBinary("1")},
		}, {
			name: "int column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("9")),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewInt32(8)},
		}, {
			name: "wrong varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("4")),
			out:  &F,
			row:  []sqltypes.Value{sqltypes.NewVarBinary("84")},
		}, {
			name: "string with int",
			v1:   NewLiteralString([]byte("700")),
			v2:   NewLiteralInt(900),
			out:  &T,
		}, {
			name: "wrong int with string",
			v1:   NewLiteralInt(99),
			v2:   NewLiteralString([]byte("7")),
			out:  &F,
		}, {
			name: "wrong string with int",
			v1:   NewLiteralString([]byte("42")),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float with float",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralFloat(1.8),
			out:  &T,
		}, {
			name: "wrong float with float",
			v1:   NewLiteralFloat(3.1),
			v2:   NewLiteralFloat(1.8),
			out:  &F,
		}, {
			name: "wrong float with int",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float with float column",
			v1:   NewLiteralFloat(21.84),
			v2:   NewColumn(0),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewFloat64(42.21)},
		},
	}

	testsLessSameValues := []testCase{
		{
			name: "equal ints",
			v1:   NewLiteralInt(42),
			v2:   NewLiteralInt(42),
			out:  &F,
		}, {
			name: "string equal to string",
			v1:   NewLiteralString([]byte("11")),
			v2:   NewLiteralString([]byte("11")),
			out:  &F,
		}, {
			name: "float equal to float",
			v1:   NewLiteralFloat(4.2),
			v2:   NewLiteralFloat(4.2),
			out:  &F,
		}, {
			name: "float equal to int",
			v1:   NewLiteralFloat(1.00),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float equal to float column",
			v1:   NewLiteralFloat(42.21),
			v2:   NewColumn(0),
			out:  &F,
			row:  []sqltypes.Value{sqltypes.NewFloat64(42.21)},
		},
	}

	t.Run("LessThanOp", func(t *testing.T) {
		t.Run("non-equal values", func(t *testing.T) {
			for i, tcase := range testsLessDifferentValues {
				tcase.run(t, i+1, &LessThanOp{})
			}
		})
		t.Run("equal values", func(t *testing.T) {
			for i, tcase := range testsLessSameValues {
				tcase.run(t, i+1, &LessThanOp{})
			}
		})
	})

	t.Run("LessEqualOp", func(t *testing.T) {
		t.Run("non-equal values", func(t *testing.T) {
			for i, tcase := range testsLessDifferentValues {
				tcase.run(t, i+1, &LessEqualOp{})
			}
		})
		t.Run("equal values", func(t *testing.T) {
			for i, tcase := range testsLessSameValues {
				tcase.out = &T
				tcase.run(t, i+1, &LessEqualOp{})
			}
		})
	})
}

func TestComparisonGreater(t *testing.T) {
	testsGreaterDifferentValues := []testCase{
		{
			name: "All Nulls",
			v1:   &Null{},
			v2:   &Null{},
			out:  nil,
		}, {
			name: "Second value null.",
			v1:   NewLiteralInt(1),
			v2:   &Null{},
			out:  nil,
		}, {
			name: "First value null.",
			v1:   &Null{},
			v2:   NewLiteralInt(1),
			out:  nil,
		}, {
			name: "int with int",
			v1:   NewLiteralInt(2),
			v2:   NewLiteralInt(1),
			out:  &T,
		}, {
			name: "wrong int",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralInt(2),
			out:  &F,
		}, {
			name: "int with string",
			v1:   NewLiteralInt(80),
			v2:   NewLiteralString([]byte("40")),
			out:  &T,
		}, {
			name: "string with string",
			v1:   NewLiteralString([]byte("11")),
			v2:   NewLiteralString([]byte("10")),
			out:  &T,
		}, {
			name: "varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("1")),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewVarBinary("10")},
		}, {
			name: "int column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("8")),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewInt32(9)},
		}, {
			name: "wrong varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("84")),
			out:  &F,
			row:  []sqltypes.Value{sqltypes.NewVarBinary("4")},
		}, {
			name: "string with int",
			v1:   NewLiteralString([]byte("900")),
			v2:   NewLiteralInt(700),
			out:  &T,
		}, {
			name: "wrong int with string",
			v1:   NewLiteralInt(7),
			v2:   NewLiteralString([]byte("99")),
			out:  &F,
		}, {
			name: "wrong string with int",
			v1:   NewLiteralString([]byte("1")),
			v2:   NewLiteralInt(42),
			out:  &F,
		}, {
			name: "float with float",
			v1:   NewLiteralFloat(1.8),
			v2:   NewLiteralFloat(1.7),
			out:  &T,
		}, {
			name: "wrong float with float",
			v1:   NewLiteralFloat(.1),
			v2:   NewLiteralFloat(.8),
			out:  &F,
		}, {
			name: "wrong float with int",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralFloat(1.7),
			out:  &F,
		}, {
			name: "float with float column",
			v1:   NewLiteralFloat(42.21),
			v2:   NewColumn(0),
			out:  &T,
			row:  []sqltypes.Value{sqltypes.NewFloat64(21.42)},
		},
	}

	testsGreaterSameValues := []testCase{
		{
			name: "equal ints",
			v1:   NewLiteralInt(42),
			v2:   NewLiteralInt(42),
			out:  &F,
		}, {
			name: "string equal to string",
			v1:   NewLiteralString([]byte("11")),
			v2:   NewLiteralString([]byte("11")),
			out:  &F,
		}, {
			name: "float equal to float",
			v1:   NewLiteralFloat(4.2),
			v2:   NewLiteralFloat(4.2),
			out:  &F,
		}, {
			name: "float equal to int",
			v1:   NewLiteralFloat(1.00),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float equal to float column",
			v1:   NewLiteralFloat(42.21),
			v2:   NewColumn(0),
			out:  &F,
			row:  []sqltypes.Value{sqltypes.NewFloat64(42.21)},
		},
	}

	t.Run("GreaterThanOp", func(t *testing.T) {
		t.Run("non-equal values", func(t *testing.T) {
			for i, tcase := range testsGreaterDifferentValues {
				tcase.run(t, i+1, &GreaterThanOp{})
			}
		})
		t.Run("equal values", func(t *testing.T) {
			for i, tcase := range testsGreaterSameValues {
				tcase.run(t, i+1, &GreaterThanOp{})
			}
		})
	})

	t.Run("GreaterEqualOp", func(t *testing.T) {
		t.Run("non-equal values", func(t *testing.T) {
			for i, tcase := range testsGreaterDifferentValues {
				tcase.run(t, i+1, &GreaterEqualOp{})
			}
		})
		t.Run("equal values", func(t *testing.T) {
			for i, tcase := range testsGreaterSameValues {
				tcase.out = &T
				tcase.run(t, i+1, &GreaterEqualOp{})
			}
		})
	})
}
