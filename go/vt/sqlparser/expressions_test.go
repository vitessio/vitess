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

package sqlparser

import (
	"testing"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

/*
These tests should in theory live in the sqltypes package but they live here so we can
exercise both expression conversion and evaluation in the same test file
*/

func TestEvaluate(t *testing.T) {
	type testCase struct {
		expression string
		expected   sqltypes.Value
	}

	tests := []testCase{{
		expression: "42",
		expected:   sqltypes.NewInt64(42),
	}, {
		expression: "42.42",
		expected:   sqltypes.NewFloat64(42.42),
	}, {
		expression: "40+2",
		expected:   sqltypes.NewInt64(42),
	}, {
		expression: "40-2",
		expected:   sqltypes.NewInt64(38),
	}, {
		expression: "40*2",
		expected:   sqltypes.NewInt64(80),
	}, {
		expression: "40/2",
		expected:   sqltypes.NewFloat64(20),
	}, {
		expression: ":exp",
		expected:   sqltypes.NewInt64(66),
	}, {
		expression: ":uint64_bind_variable",
		expected:   sqltypes.NewUint64(22),
	}, {
		expression: ":string_bind_variable",
		expected:   sqltypes.NewVarBinary("bar"),
	}, {
		expression: ":float_bind_variable",
		expected:   sqltypes.NewFloat64(2.2),
	}}

	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			// Given
			stmt, err := Parse("select " + test.expression)
			require.NoError(t, err)
			astExpr := stmt.(*Select).SelectExprs[0].(*AliasedExpr).Expr
			sqltypesExpr, err := Convert(astExpr)
			require.Nil(t, err)
			require.NotNil(t, sqltypesExpr)
			env := evalengine.ExpressionEnv{
				BindVars: map[string]*querypb.BindVariable{
					"exp":                  sqltypes.Int64BindVariable(66),
					"string_bind_variable": sqltypes.StringBindVariable("bar"),
					"uint64_bind_variable": sqltypes.Uint64BindVariable(22),
					"float_bind_variable":  sqltypes.Float64BindVariable(2.2),
				},
				Row: nil,
			}

			// When
			r, err := sqltypesExpr.Evaluate(env)

			// Then
			require.NoError(t, err)
			assert.Equal(t, test.expected, r.Value(), "expected %s", test.expected.String())
		})
	}
}
