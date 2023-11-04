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
	"math"
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
			input:    " 10",
			base:     10,
			expected: 10,
		},
		{
			input:    " 10 ",
			base:     10,
			expected: 10,
		},
		{
			input:    " 10 1",
			base:     10,
			expected: 10,
			err:      `unparsed tail left after parsing int64 from " 10 1": "1"`,
		},
		{
			input:    " -10 ",
			base:     10,
			expected: -10,
		},
		{
			input:    " -10 1",
			base:     10,
			expected: -10,
			err:      `unparsed tail left after parsing int64 from " -10 1": "1"`,
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
			input:    "31415926535897932384",
			base:     10,
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "31415926535897932384": overflow`,
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
		{
			input:    "\t 42 \t",
			base:     10,
			expected: 42,
		},
		{
			input:    "\t 42 \n",
			base:     10,
			expected: 42,
			err:      `unparsed tail left after parsing int64 from "\t 42 \n": "\n"`,
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
			input:    " 10",
			base:     10,
			expected: 10,
		},
		{
			input:    " 10 ",
			base:     10,
			expected: 10,
		},
		{
			input:    " 10 1",
			base:     10,
			expected: 10,
			err:      `unparsed tail left after parsing uint64 from " 10 1": "1"`,
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
			input:    "31415926535897932384",
			base:     10,
			expected: 18446744073709551615,
			err:      `cannot parse uint64 from "31415926535897932384": overflow`,
		},
		{
			input:    "1.1",
			base:     10,
			expected: 1,
			err:      `unparsed tail left after parsing uint64 from "1.1": ".1"`,
		},
		{
			input:    "\t 42 \t",
			base:     10,
			expected: 42,
		},
		{
			input:    "\t 42 \n",
			base:     10,
			expected: 42,
			err:      `unparsed tail left after parsing uint64 from "\t 42 \n": "\n"`,
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

func TestParseFloat64(t *testing.T) {
	testcases := []struct {
		input    string
		expected float64
		err      string
	}{
		{
			input:    "0",
			expected: 0,
		},
		{
			input:    "1",
			expected: 1,
		},
		{
			input:    "1",
			expected: 1,
		},
		{
			input:    "10",
			expected: 10,
		},
		{
			input:    "-",
			expected: 0.0,
			err:      `strconv.ParseFloat: parsing "-": invalid syntax`,
		},
		{
			input:    " 10",
			expected: 10,
		},
		{
			input:    " 10 ",
			expected: 10,
		},
		{
			input:    " 10 1",
			expected: 10,
			err:      `unparsed tail left after parsing float64 from " 10 1": "1"`,
		},
		{
			input:    "9223372036854775807",
			expected: 9223372036854775807,
		},
		{
			input:    "80.1",
			expected: 80.1,
		},
		{
			input:    "9223372036854775807trailing",
			expected: 9223372036854775807,
			err:      `unparsed tail left after parsing float64 from "9223372036854775807trailing": "trailing"`,
		},
		{
			input:    " 9223372036854775807trailing",
			expected: 9223372036854775807,
			err:      `unparsed tail left after parsing float64 from " 9223372036854775807trailing": "trailing"`,
		},
		{
			input:    " 9223372036854775807",
			expected: 9223372036854775807,
		},
		{
			input:    "9223372036854775808",
			expected: 9223372036854775808,
		},
		{
			input:    "9223372036854775808trailing",
			expected: 9223372036854775808,
			err:      `unparsed tail left after parsing float64 from "9223372036854775808trailing": "trailing"`,
		},
		{
			input:    "-9223372036854775807",
			expected: -9223372036854775807,
		},
		{
			input:    "-9223372036854775807.1",
			expected: -9223372036854775807.1,
		},
		{
			input:    "-9223372036854775808",
			expected: -9223372036854775808,
		},
		{
			input:    "-9223372036854775808.1",
			expected: -9223372036854775808.1,
		},
		{
			input:    "-9223372036854775809",
			expected: -9223372036854775809,
		},
		{
			input:    "18446744073709551615",
			expected: 18446744073709551615,
		},
		{
			input:    "18446744073709551616",
			expected: 18446744073709551616,
		},
		{
			input:    "1.1",
			expected: 1.1,
		},
		{
			input:    "-1.1",
			expected: -1.1,
		},
		{
			input:    "1e100",
			expected: 1e+100,
		},
		{
			input:    "1e+100",
			expected: 1e+100,
		},
		{
			input:    "1e22",
			expected: 1e22,
		},
		{
			input:    "1e-22",
			expected: 1e-22,
		},
		{
			input:    "1e-100",
			expected: 1e-100,
		},
		{
			input:    "1e308",
			expected: 1e308,
		},
		{
			input:    "-1e308",
			expected: -1e308,
		},
		{
			input:    "1e408",
			expected: math.MaxFloat64,
			err:      `strconv.ParseFloat: parsing "1e408": value out of range`,
		},
		{
			input:    "-1e408",
			expected: -math.MaxFloat64,
			err:      `strconv.ParseFloat: parsing "-1e408": value out of range`,
		},
		{
			input:    "1e-308",
			expected: 1e-308,
		},
		{
			input:    "0.1.99",
			expected: 0.1,
			err:      `unparsed tail left after parsing float64 from "0.1.99": ".99"`,
		},
		{
			input:    "\t 42.10 \t",
			expected: 42.10,
		},
		{
			input:    "\t 42.10 \n",
			expected: 42.10,
			err:      `unparsed tail left after parsing float64 from "\t 42.10 \n": "\n"`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			val, err := ParseFloat64(tc.input)
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

func TestParseStringToFloat(t *testing.T) {
	tcs := []struct {
		str string
		val float64
	}{
		{str: ""},
		{str: " "},
		{str: "1", val: 1},
		{str: "1.10", val: 1.10},
		{str: "    6.87", val: 6.87},
		{str: "93.66  ", val: 93.66},
		{str: "\t 42.10 \n ", val: 42.10},
		{str: "1.10aa", val: 1.10},
		{str: ".", val: 0.00},
		{str: ".99", val: 0.99},
		{str: "..99", val: 0},
		{str: "1.", val: 1},
		{str: "0.1.99", val: 0.1},
		{str: "0.", val: 0},
		{str: "8794354", val: 8794354},
		{str: "    10  ", val: 10},
		{str: "2266951196291479516", val: 2266951196291479516},
		{str: "abcd123", val: 0},
	}

	for _, tc := range tcs {
		t.Run(tc.str, func(t *testing.T) {
			got, _ := ParseFloat64(tc.str)
			require.EqualValues(t, tc.val, got)
		})
	}
}
