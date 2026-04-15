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

package json

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/sqltypes"
)

func TestMarshalSQLTo(t *testing.T) {
	testcases := []struct {
		input    string
		expected string
	}{
		{
			input:    "null",
			expected: "CAST(_utf8mb4'null' as JSON)",
		},
		{
			input:    `{}`,
			expected: `JSON_OBJECT()`,
		},
		{
			input:    `{"a": 1}`,
			expected: `JSON_OBJECT(_utf8mb4'a', 1)`,
		},
		{
			input:    `{"key with ' in it": []}`,
			expected: `JSON_OBJECT(_utf8mb4'key with \' in it', JSON_ARRAY())`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			var p Parser

			v, err := p.Parse(tc.input)
			require.NoError(t, err)
			buf := v.MarshalSQLTo(nil)
			require.Equal(t, tc.expected, string(buf))
		})
	}
}

// TestMarshalSQLValuePreservesControlByte verifies that JSON control bytes
// are preserved when marshaled into SQL.
func TestMarshalSQLValuePreservesControlByte(t *testing.T) {
	raw := "Foo Bar" + string([]byte{26}) + "a"

	val := NewString(raw)

	got, err := MarshalSQLValue(val.MarshalTo(nil))
	require.NoError(t, err)

	expected := "CAST(JSON_QUOTE(_utf8mb4" + sqltypes.EncodeStringSQL(raw) + ") as JSON)"
	require.Equal(t, expected, string(got.Raw()))
}

// TestMarshalSQLValueNormalizesInvalidUTF8 verifies that JSON string
// marshaling normalizes invalid UTF-8 before producing SQL.
func TestMarshalSQLValueNormalizesInvalidUTF8(t *testing.T) {
	raw := string([]byte{0xff, 0xfe, 'A'})
	val := NewString(raw)

	got, err := MarshalSQLValue(val.MarshalTo(nil))
	require.NoError(t, err)

	normalized := string([]rune(raw))
	expected := "CAST(JSON_QUOTE(_utf8mb4" + sqltypes.EncodeStringSQL(normalized) + ") as JSON)"
	require.Equal(t, expected, string(got.Raw()))
}

// TestAppendMarshalSQLDepthLimit verifies that AppendMarshalSQL enforces
// the same nesting depth limit as Parser.Parse.
func TestAppendMarshalSQLDepthLimit(t *testing.T) {
	for _, tc := range []struct {
		depth   int
		wantErr bool
	}{
		{depth: 299, wantErr: false},
		{depth: 300, wantErr: false},
		{depth: 301, wantErr: true},
	} {
		input := strings.Repeat("[", tc.depth) + strings.Repeat("]", tc.depth)

		var p Parser
		_, parseErr := p.Parse(input)

		buf := &bytes2.Buffer{}
		appendErr := AppendMarshalSQL(buf, []byte(input))

		if tc.wantErr {
			assert.Error(t, parseErr, "depth %d: parser should reject", tc.depth)
			assert.Error(t, appendErr, "depth %d: AppendMarshalSQL should reject", tc.depth)
		} else {
			assert.NoError(t, parseErr, "depth %d: parser should accept", tc.depth)
			assert.NoError(t, appendErr, "depth %d: AppendMarshalSQL should accept", tc.depth)
		}
	}
}

// TestAppendMarshalSQLNumberGrammar verifies that AppendMarshalSQL rejects
// malformed JSON numbers that a naive character-class scanner would accept.
func TestAppendMarshalSQLNumberGrammar(t *testing.T) {
	malformed := []string{
		`1+2`,
		`1-2`,
		`1..2`,
		`1e+`,
		`1e`,
		`--1`,
	}
	for _, input := range malformed {
		buf := &bytes2.Buffer{}
		err := AppendMarshalSQL(buf, []byte(input))
		assert.Error(t, err, "malformed number %q should be rejected", input)
	}

	valid := []string{
		`0`, `42`, `-1`, `3.14`, `-0.5`,
		`1e10`, `1E10`, `1e+10`, `1e-10`, `1.5e2`,
	}
	for _, input := range valid {
		buf := &bytes2.Buffer{}
		err := AppendMarshalSQL(buf, []byte(input))
		assert.NoError(t, err, "valid number %q should be accepted", input)
	}
}

// TestAppendMarshalSQLTrailingBackslash verifies that a backslash as the
// last byte of a string is rejected rather than causing an out-of-bounds read.
func TestAppendMarshalSQLTrailingBackslash(t *testing.T) {
	inputs := []string{
		`"trailing\`,      // backslash is last byte, no closing quote
		`{"key": "val\"}`, // backslash before quote looks like escaped quote, string never closes
	}
	for _, input := range inputs {
		buf := &bytes2.Buffer{}
		err := AppendMarshalSQL(buf, []byte(input))
		assert.Error(t, err, "input %q should be rejected", input)
		assert.ErrorContains(t, err, "unterminated string", "input %q", input)
	}
}
