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
package fastparse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseInt64(t *testing.T) {
	testcases := []struct {
		input    string
		base     int
		expected int64
		err      string
	}{
		{
			input:    "0",
			base:     10,
			expected: 0,
		},
		{
			input:    "1",
			base:     10,
			expected: 1,
		},
		{
			input:    "1",
			base:     2,
			expected: 1,
		},
		{
			input:    "10",
			base:     2,
			expected: 2,
		},
		{
			input:    "9223372036854775807",
			base:     10,
			expected: 9223372036854775807,
		},
		{
			input:    "7fffffffffffffff",
			base:     16,
			expected: 9223372036854775807,
		},
		{
			input:    "7FFFFFFFFFFFFFFF",
			base:     16,
			expected: 9223372036854775807,
		},
		{
			input:    "8000000000000000",
			base:     16,
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "8000000000000000": overflow`,
		},
		{
			input:    "80.1",
			base:     16,
			expected: 128,
			err:      `unparsed tail left after parsing int64 from "80.1": ".1"`,
		},
		{
			input:    "9223372036854775807trailing",
			base:     10,
			expected: 9223372036854775807,
			err:      `unparsed tail left after parsing int64 from "9223372036854775807trailing": "trailing"`,
		},
		{
			input:    "9223372036854775808",
			base:     10,
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "9223372036854775808": overflow`,
		},
		{
			input:    "9223372036854775808trailing",
			base:     10,
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "9223372036854775808trailing": overflow`,
		},
		{
			input:    "-9223372036854775807",
			base:     10,
			expected: -9223372036854775807,
		},
		{
			input:    "-9223372036854775807.1",
			base:     10,
			expected: -9223372036854775807,
			err:      `unparsed tail left after parsing int64 from "-9223372036854775807.1": ".1"`,
		},
		{
			input:    "-9223372036854775808",
			base:     10,
			expected: -9223372036854775808,
		},
		{
			input:    "-9223372036854775808.1",
			base:     10,
			expected: -9223372036854775808,
			err:      `unparsed tail left after parsing int64 from "-9223372036854775808.1": ".1"`,
		},
		{
			input:    "-9223372036854775809",
			base:     10,
			expected: -9223372036854775808,
			err:      `cannot parse int64 from "-9223372036854775809": overflow`,
		},
		{
			input:    "18446744073709551615",
			base:     10,
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "18446744073709551615": overflow`,
		},
		{
			input:    "18446744073709551616",
			base:     10,
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "18446744073709551616": overflow`,
		},
		{
			input:    "1.1",
			base:     10,
			expected: 1,
			err:      `unparsed tail left after parsing int64 from "1.1": ".1"`,
		},
		{
			input:    "-1.1",
			base:     10,
			expected: -1,
			err:      `unparsed tail left after parsing int64 from "-1.1": ".1"`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			val, err := ParseInt64(tc.input, tc.base)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, val)
			} else {
				require.Equal(t, tc.expected, val)
				require.EqualError(t, err, tc.err)
			}
		})
	}
}

func TestParseUint64(t *testing.T) {
	testcases := []struct {
		input    string
		base     int
		expected uint64
		err      string
	}{
		{
			input:    "0",
			base:     10,
			expected: 0,
		},
		{
			input:    "1",
			base:     10,
			expected: 1,
		},
		{
			input:    "1",
			base:     2,
			expected: 1,
		},
		{
			input:    "10",
			base:     2,
			expected: 2,
		},
		{
			input:    "9223372036854775807",
			base:     10,
			expected: 9223372036854775807,
		},
		{
			input:    "9223372036854775807trailing",
			base:     10,
			expected: 9223372036854775807,
			err:      `unparsed tail left after parsing uint64 from "9223372036854775807trailing": "trailing"`,
		},
		{
			input:    "9223372036854775808",
			base:     10,
			expected: 9223372036854775808,
		},
		{
			input:    "9223372036854775808trailing",
			base:     10,
			expected: 9223372036854775808,
			err:      `unparsed tail left after parsing uint64 from "9223372036854775808trailing": "trailing"`,
		},
		{
			input:    "18446744073709551615",
			base:     10,
			expected: 18446744073709551615,
		},
		{
			input:    "ffffffffffffffff",
			base:     16,
			expected: 18446744073709551615,
		},
		{
			input:    "FFFFFFFFFFFFFFFF",
			base:     16,
			expected: 18446744073709551615,
		},
		{
			input:    "18446744073709551615.1",
			base:     10,
			expected: 18446744073709551615,
			err:      `unparsed tail left after parsing uint64 from "18446744073709551615.1": ".1"`,
		},
		{
			input:    "ff.1",
			base:     16,
			expected: 255,
			err:      `unparsed tail left after parsing uint64 from "ff.1": ".1"`,
		},
		{
			input:    "18446744073709551616",
			base:     10,
			expected: 18446744073709551615,
			err:      `cannot parse uint64 from "18446744073709551616": overflow`,
		},
		{
			input:    "1.1",
			base:     10,
			expected: 1,
			err:      `unparsed tail left after parsing uint64 from "1.1": ".1"`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			val, err := ParseUint64(tc.input, tc.base)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, val)
			} else {
				require.Equal(t, tc.expected, val)
				require.EqualError(t, err, tc.err)
			}
		})
	}
}
