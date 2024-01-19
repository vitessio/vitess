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

package sqlescape

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeID(t *testing.T) {
	testcases := []struct {
		in, out string
	}{
		{
			in:  "aa",
			out: "`aa`",
		},
		{
			in:  "a`a",
			out: "`a``a`",
		},
		{
			in:  "`aa`",
			out: "```aa```",
		},
	}

	for _, tc := range testcases {
		out := EscapeID(tc.in)
		assert.Equal(t, tc.out, out)
	}
}

var scratch string

func BenchmarkEscapeID(b *testing.B) {
	testcases := []string{
		"aa", "a`a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	for _, tc := range testcases {
		name := tc
		if len(name) > 10 {
			name = "long"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				scratch = EscapeID(tc)
			}
		})
	}
}

func TestEscapeIDs(t *testing.T) {
	testCases := []struct {
		input    []string
		expected []string
	}{
		{
			input:    []string{"abc", "def", "ghi"},
			expected: []string{"`abc`", "`def`", "`ghi`"},
		},
		{
			input:    []string{"abc", "a`a", "`ghi`"},
			expected: []string{"`abc`", "`a``a`", "```ghi```"},
		},
		{
			input:    []string{},
			expected: []string{},
		},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%v", tt.input), func(t *testing.T) {
			out := EscapeIDs(tt.input)
			assert.Equal(t, tt.expected, out)
		})
	}
}

func TestUnescapeID(t *testing.T) {
	testcases := []struct {
		in, out string
	}{
		{
			in:  "`aa`",
			out: "aa",
		},
		{
			in:  "`a``a`",
			out: "a`a",
		},
		{
			in:  "```aa```",
			out: "`aa`",
		},
		{
			in:  "aa",
			out: "aa",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			out := UnescapeID(tc.in)
			assert.Equal(t, tc.out, out)
		})
	}
}
