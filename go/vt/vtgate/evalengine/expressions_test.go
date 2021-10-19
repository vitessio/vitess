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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// more tests in go/sqlparser/expressions_test.go

func TestBinaryOpTypes(t *testing.T) {
	type testcase struct {
		l, r, e querypb.Type
	}
	type ops struct {
		op        BinaryExpr
		testcases []testcase
	}

	tests := []ops{
		{
			op: &Addition{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		}, {
			op: &Subtraction{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		}, {
			op: &Multiplication{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		}, {
			op: &Division{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Float64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Float64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		},
	}

	for _, op := range tests {
		for _, tc := range op.testcases {
			name := fmt.Sprintf("%s %s %s", tc.l.String(), reflect.TypeOf(op.op).String(), tc.r.String())
			t.Run(name, func(t *testing.T) {
				result := op.op.Type(tc.l)
				assert.Equal(t, tc.e, result)
			})
		}
	}
}

func TestEquals(t *testing.T) {
	type tcase struct {
		v1, v2 Expr
		out    *bool
		err    string
	}

	T := true
	F := false
	tests := []struct {
		cases []tcase
	}{{
		cases: []tcase{{
			// All Nulls
			v1:  &Null{},
			v2:  &Null{},
			out: nil,
		}, {
			// Second value null.
			v1:  NewLiteralInt(1),
			v2:  &Null{},
			out: nil,
		}, {
			// First value null.
			v1:  &Null{},
			v2:  NewLiteralInt(1),
			out: nil,
		}, {
			v1:  NewLiteralInt(1),
			v2:  NewLiteralInt(1),
			out: &T,
		}, {
			v1:  NewLiteralInt(1),
			v2:  NewLiteralInt(2),
			out: &F,
		}},
	}}

	for _, test := range tests {
		for _, tcase := range test.cases {
			name := fmt.Sprintf("%s%s%s", tcase.v1.String(), "=", tcase.v2.String())
			t.Run(name, func(t *testing.T) {
				eq := &Equals{
					Left:  tcase.v1,
					Right: tcase.v2,
				}

				env := ExpressionEnv{
					BindVars: map[string]*querypb.BindVariable{},
				}
				got, err := eq.Evaluate(env)
				if tcase.err == "" {
					require.NoError(t, err)
					if tcase.out != nil && *tcase.out {
						require.EqualValues(t, 1, got.ival)
					} else if tcase.out != nil && !*tcase.out {
						require.EqualValues(t, 0, got.ival)
					} else {
						require.EqualValues(t, 0, got.ival)
						require.EqualValues(t, sqltypes.Null, got.typ)
					}
				} else {
					require.EqualError(t, err, tcase.err)
				}
			})
		}
	}
}
