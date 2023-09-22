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
	"testing"

	"github.com/stretchr/testify/require"
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
