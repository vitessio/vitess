/*
Copyright 2026 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestJSONBindVariableEvaluation tests that JSON bind parameters are evaluated
// with string semantics rather than parsed as JSON documents during expression
// evaluation, matching MySQL prepared statement parameter behavior.
func TestJSONBindVariableEvaluation(t *testing.T) {
	testCases := []struct {
		name       string
		expression string
		bindVars   map[string]*querypb.BindVariable
		expected   string
	}{
		{
			name:       "json parameter equals varchar with quotes",
			expression: ":j = :s",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte(`"abc"`)},
				"s": sqltypes.StringBindVariable(`"abc"`),
			},
			expected: "INT64(1)",
		},
		{
			name:       "json parameter not equals unquoted varchar",
			expression: ":j = :s",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte(`"abc"`)},
				"s": sqltypes.StringBindVariable("abc"),
			},
			expected: "INT64(0)",
		},
		{
			name:       "json parameter equals matching numeric string representation",
			expression: ":j = :s",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte("1.50")},
				"s": sqltypes.StringBindVariable("1.50"),
			},
			expected: "INT64(1)",
		},
		{
			name:       "json parameter not equals differing numeric string representation",
			expression: ":j = :s",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte("1.50")},
				"s": sqltypes.StringBindVariable("1.5"),
			},
			expected: "INT64(0)",
		},
		{
			name:       "json parameter equals literal with quotes",
			expression: `:j = '"abc"'`,
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte(`"abc"`)},
			},
			expected: "INT64(1)",
		},
		{
			name:       "json parameter not equals literal without quotes",
			expression: ":j = 'abc'",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte(`"abc"`)},
			},
			expected: "INT64(0)",
		},
		{
			name:       "json parameter equals literal numeric string representation",
			expression: ":j = '1.50'",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte("1.50")},
			},
			expected: "INT64(1)",
		},
		{
			name:       "json parameter not equals differing literal numeric string representation",
			expression: ":j = '1.5'",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte("1.50")},
			},
			expected: "INT64(0)",
		},
		{
			name:       "two json parameters compare as strings preserving exact bytes",
			expression: ":j1 = :j2",
			bindVars: map[string]*querypb.BindVariable{
				"j1": {Type: querypb.Type_JSON, Value: []byte("1.50")},
				"j2": {Type: querypb.Type_JSON, Value: []byte("1.5")},
			},
			expected: "INT64(0)",
		},
		{
			name:       "two json parameters with identical content match",
			expression: ":j1 = :j2",
			bindVars: map[string]*querypb.BindVariable{
				"j1": {Type: querypb.Type_JSON, Value: []byte(`{"a": 1}`)},
				"j2": {Type: querypb.Type_JSON, Value: []byte(`{"a": 1}`)},
			},
			expected: "INT64(1)",
		},
		{
			name:       "json parameter parsed as json document within json function",
			expression: "json_extract(:j, '$.key')",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte(`{"key": "value"}`)},
			},
			expected: `JSON("\"value\"")`,
		},
		{
			name:       "json parameter unquoted within json_unquote",
			expression: "json_unquote(:j)",
			bindVars: map[string]*querypb.BindVariable{
				"j": {Type: querypb.Type_JSON, Value: []byte(`"hello"`)},
			},
			expected: `BLOB("hello")`,
		},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			cfg := &Config{
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			}

			translated, err := Translate(expr, cfg)
			require.NoError(t, err)

			env := EmptyExpressionEnv(venv)
			env.BindVars = tc.bindVars

			astResult, err := env.EvaluateAST(translated)
			require.NoError(t, err)
			require.Equal(t, tc.expected, astResult.String(), "AST evaluation mismatch")

			vmResult, err := env.Evaluate(translated)
			require.NoError(t, err)
			require.Equal(t, tc.expected, vmResult.String(), "VM evaluation mismatch")
		})
	}
}

// TestJSONBindVariableTypeOf verifies that BindVariable.typeof returns VarChar
// for JSON bind parameters.
func TestJSONBindVariableTypeOf(t *testing.T) {
	venv := vtenv.NewTestEnv()
	env := EmptyExpressionEnv(venv)
	env.BindVars = map[string]*querypb.BindVariable{
		"j": {Type: querypb.Type_JSON, Value: []byte(`"abc"`)},
	}

	bv := &BindVariable{
		Key:       "j",
		Type:      sqltypes.Unknown,
		Collation: collations.CollationUtf8mb4ID,
	}

	ct, err := bv.typeof(env)
	require.NoError(t, err)
	require.Equal(t, sqltypes.VarChar, ct.Type)

	typedBv := NewBindVar("j", NewType(sqltypes.TypeJSON, collations.CollationUtf8mb4ID))
	require.Equal(t, sqltypes.VarChar, typedBv.Type)

	ctTyped, err := typedBv.typeof(env)
	require.NoError(t, err)
	require.Equal(t, sqltypes.VarChar, ctTyped.Type)
}
