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
	name := fmt.Sprintf("%d_%s_%s%s%s", i, tc.name, tc.v1.String(), cmpOp.String(), tc.v2.String())
	if tc.bv == nil {
		tc.bv = map[string]*querypb.BindVariable{}
	}
	t.Run(name, func(t *testing.T) {
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
	})
}

func TestComparisonEquality(t *testing.T) {
	tests := []testCase{
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
		},
	}

	t.Run("EqualOp", func(t *testing.T) {
		for i, tcase := range tests {
			tcase.run(t, i+1, &EqualOp{})
		}
	})

	t.Run("NotEqualOp", func(t *testing.T) {
		for i, tcase := range tests {
			// transforming the expected output to its opposite so we can test NotEqualOp
			switch tcase.out {
			case &T:
				tcase.out = &F
			case &F:
				tcase.out = &T
			}
			tcase.run(t, i+1, &NotEqualOp{})
		}
	})
}
