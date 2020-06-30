/*
Copyright 2019 The Vitess Authors.

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

package trace

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
)

func TestExtractMapFromString(t *testing.T) {
	type testCase struct {
		str      string
		expected opentracing.TextMapCarrier
		err      bool
	}

	tests := []testCase{{
		str: "apa=12:banan=x-tracing-backend-12",
		expected: map[string]string{
			"apa":   "12",
			"banan": "x-tracing-backend-12",
		},
	}, {
		str:      `uber-trace-id=123\:456\:789\:1`,
		expected: map[string]string{"uber-trace-id": "123:456:789:1"},
	}, {
		str: `key:`,
		err: true,
	}, {
		str:      ``,
		expected: map[string]string{},
	}, {
		str:      `=`,
		expected: map[string]string{"": ""},
	}, {
		str:      `so\=confusing=42`,
		expected: map[string]string{"so=confusing": "42"},
	}, {
		str:      `key=\=42\=`,
		expected: map[string]string{"key": "=42="},
	}, {
		str:      `key=\\`,
		expected: map[string]string{"key": `\`},
	}, {
		str: `key=\r`,
		err: true,
	}, {
		str: `key=r\`,
		err: true,
	}}

	for _, tc := range tests {
		t.Run(tc.str, func(t *testing.T) {
			result, err := extractMapFromString(tc.str)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
