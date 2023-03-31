package fastparse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseInt64(t *testing.T) {
	testcases := []struct {
		input    string
		expected int64
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
			input:    "9223372036854775807",
			expected: 9223372036854775807,
		},
		{
			input:    "9223372036854775807trailing",
			expected: 9223372036854775807,
			err:      `unparsed tail left after parsing int64 from "9223372036854775807trailing": "trailing"`,
		},
		{
			input:    "9223372036854775808",
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "9223372036854775808"`,
		},
		{
			input:    "9223372036854775808trailing",
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "9223372036854775808trailing"`,
		},
		{
			input:    "-9223372036854775807",
			expected: -9223372036854775807,
		},
		{
			input:    "-9223372036854775807.1",
			expected: -9223372036854775807,
			err:      `unparsed tail left after parsing int64 from "-9223372036854775807.1": ".1"`,
		},
		{
			input:    "-9223372036854775808",
			expected: -9223372036854775808,
		},
		{
			input:    "-9223372036854775808.1",
			expected: -9223372036854775808,
			err:      `unparsed tail left after parsing int64 from "-9223372036854775808.1": ".1"`,
		},
		{
			input:    "-9223372036854775809",
			expected: -9223372036854775808,
			err:      `cannot parse int64 from "-9223372036854775809"`,
		},
		{
			input:    "18446744073709551615",
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "18446744073709551615"`,
		},
		{
			input:    "18446744073709551616",
			expected: 9223372036854775807,
			err:      `cannot parse int64 from "18446744073709551616"`,
		},
		{
			input:    "1.1",
			expected: 1,
			err:      `unparsed tail left after parsing int64 from "1.1": ".1"`,
		},
		{
			input:    "-1.1",
			expected: -1,
			err:      `unparsed tail left after parsing int64 from "-1.1": ".1"`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			val, err := ParseInt64(tc.input)
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
		expected uint64
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
			input:    "9223372036854775807",
			expected: 9223372036854775807,
		},
		{
			input:    "9223372036854775807trailing",
			expected: 9223372036854775807,
			err:      `unparsed tail left after parsing uint64 from "9223372036854775807trailing": "trailing"`,
		},
		{
			input:    "9223372036854775808",
			expected: 9223372036854775808,
		},
		{
			input:    "9223372036854775808trailing",
			expected: 9223372036854775808,
			err:      `unparsed tail left after parsing uint64 from "9223372036854775808trailing": "trailing"`,
		},
		{
			input:    "18446744073709551615",
			expected: 18446744073709551615,
		},
		{
			input:    "18446744073709551615.1",
			expected: 18446744073709551615,
			err:      `unparsed tail left after parsing uint64 from "18446744073709551615.1": ".1"`,
		},
		{
			input:    "18446744073709551616",
			expected: 18446744073709551615,
			err:      `cannot parse uint64 from "18446744073709551616"`,
		},
		{
			input:    "1.1",
			expected: 1,
			err:      `unparsed tail left after parsing uint64 from "1.1": ".1"`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			val, err := ParseUint64(tc.input)
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
