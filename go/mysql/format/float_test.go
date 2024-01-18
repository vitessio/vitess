/*
Copyright 2024 The Vitess Authors.

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

package format

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected []byte
	}{
		{
			name:     "positive integer",
			input:    123.0,
			expected: []byte("123"),
		},
		{
			name:     "negative integer",
			input:    -456.0,
			expected: []byte("-456"),
		},
		{
			name:     "positive float",
			input:    12.34,
			expected: []byte("12.34"),
		},
		{
			name:     "negative float",
			input:    -56.789,
			expected: []byte("-56.789"),
		},
		{
			name:     "large positive exponent",
			input:    1.23456789e15,
			expected: []byte("1.23456789e15"),
		},
		{
			name:     "large negative exponent",
			input:    1.23456789e-15,
			expected: []byte("0.00000000000000123456789"),
		},
		{
			name:     "zero",
			input:    0.0,
			expected: []byte("0"),
		},
		{
			name:     "positive infinity",
			input:    math.Inf(1),
			expected: []byte("+Inf"),
		},
		{
			name:     "negative infinity",
			input:    math.Inf(-1),
			expected: []byte("-Inf"),
		},
		{
			name:     "NaN",
			input:    math.NaN(),
			expected: []byte("NaN"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatFloat(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
