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

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestEvaluate(t *testing.T) {
	type testCase struct {
		name     string
		e        Expr
		bindvars map[string]*querypb.BindVariable
		expected Value
	}

	tests := []testCase{
		{
			name:     "42",
			e:        i("42"),
			expected: NewInt64(42),
		},
		{
			name:     "40+2",
			e:        &add{i("40"), i("2")},
			expected: NewInt64(42),
		},
		{
			name:     "40-2",
			e:        &subtract{i("40"), i("2")},
			expected: NewInt64(38),
		},
		{
			name:     "Bind Variable",
			e:        b(":exp"),
			expected: NewInt64(66),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := ExpressionEnv{
				BindVars: map[string]*querypb.BindVariable{
					":exp": Int64BindVariable(66),
				},
				Row: nil,
			}
			r, err := test.e.Evaluate(env)
			require.NoError(t, err)
			result := castFromNumeric(r, r.typ)
			assert.Equal(t, test.expected, result, "expected %s but got %s", test.expected.String(), result.String())
		})
	}

}

func i(in string) *SQLVal {
	return &SQLVal{
		Type: IntVal,
		Val:  []byte(in),
	}
}

func b(in string) *SQLVal {
	return &SQLVal{
		Type: ValArg,
		Val:  []byte(in),
	}
}
