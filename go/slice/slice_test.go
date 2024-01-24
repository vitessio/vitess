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

package slice

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAll(t *testing.T) {
	testCases := []struct {
		name     string
		input    []int
		fn       func(int) bool
		expected bool
	}{
		{
			name:     "EmptySlice",
			input:    []int{},
			fn:       func(i int) bool { return i > 0 },
			expected: true,
		},
		{
			name:     "AllElementsTrue",
			input:    []int{1, 2, 3},
			fn:       func(i int) bool { return i > 0 },
			expected: true,
		},
		{
			name:     "SomeElementsFalse",
			input:    []int{1, 2, 0},
			fn:       func(i int) bool { return i > 0 },
			expected: false,
		},
		{
			name:     "SingleElementFalse",
			input:    []int{0},
			fn:       func(i int) bool { return i > 0 },
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := All(tc.input, tc.fn)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestAny(t *testing.T) {
	testCases := []struct {
		name     string
		input    []int
		fn       func(int) bool
		expected bool
	}{
		{
			name:     "ReturnsTrue",
			input:    []int{1, 2, 3, 4, 5},
			fn:       func(n int) bool { return n == 3 },
			expected: true,
		},
		{
			name:     "EmptySliceInput",
			input:    []int{},
			fn:       func(n int) bool { return n > 0 },
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Any(tc.input, tc.fn)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMap(t *testing.T) {
	testCases := []struct {
		name     string
		input    []int
		fn       func(int) int
		expected []int
	}{
		{
			name:     "AppliesFunction",
			input:    []int{1, 2, 3, 4, 5},
			fn:       func(n int) int { return n * 2 },
			expected: []int{2, 4, 6, 8, 10},
		},
		{
			name:     "EmptySliceInput",
			input:    nil,
			fn:       func(n int) int { return n },
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Map(tc.input, tc.fn)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFilter(t *testing.T) {
	testCases := []struct {
		name     string
		input    []int
		fn       func(int) bool
		expected []int
	}{
		{
			name:     "Filtering",
			input:    []int{1, 2, 3, 4, 5},
			fn:       func(i int) bool { return i%2 == 0 },
			expected: []int{2, 4},
		},
		{
			name:     "AllElementsFilteredOut",
			input:    []int{1, 3, 5},
			fn:       func(i int) bool { return i%2 == 0 },
			expected: []int{},
		},
		{
			name:     "EmptySliceInput",
			input:    nil,
			fn:       func(n int) bool { return n > 0 },
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Filter(tc.input, tc.fn)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMapWithError(t *testing.T) {
	testCases := []struct {
		name        string
		input       []int
		fn          func(int) (int, error)
		expected    []int
		expectedErr string
	}{
		{
			name:     "SuccessfulMapping",
			input:    []int{1, 2, 3},
			fn:       func(i int) (int, error) { return i * 2, nil },
			expected: []int{2, 4, 6},
		},
		{
			name:        "ErrorReturned",
			input:       []int{1, 2, 3},
			fn:          func(i int) (int, error) { return 0, errors.New("error") },
			expectedErr: "error",
		},
		{
			name:     "EmptySliceInput",
			input:    nil,
			fn:       func(n int) (int, error) { return n, nil },
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := MapWithError(tc.input, tc.fn)
			if tc.expectedErr != "" {
				assert.EqualError(t, err, tc.expectedErr)
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
