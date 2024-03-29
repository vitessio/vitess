/*
Copyright 2022 The Vitess Authors.

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

package bitset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingletons(t *testing.T) {
	for i := 0; i < 40; i++ {
		bs := Single(i)

		require.Equal(t, 1, bs.Popcount())
		require.Equal(t, i, bs.SingleBit())

		var called bool
		bs.ForEach(func(offset int) {
			require.False(t, called)
			require.Equal(t, i, offset)
			called = true
		})
		require.True(t, called)
	}
}

func TestSingleBitReturnsNegativeOne(t *testing.T) {
	bs := Bitset("\x0F")
	result := bs.SingleBit()

	assert.Equal(t, -1, result)
}

func TestToBitsetPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			require.NotNil(t, r, "Expected panic, but none occurred")
		}
	}()

	byteEndsWithZero := []byte{8, 0}

	_ = toBitset(byteEndsWithZero)
}

func TestBuild(t *testing.T) {
	tt := []struct {
		name string
		bits []int
		want Bitset
	}{
		{"Empty Bits", []int{}, ""},
		{"Single Bit", []int{3}, "\x08"},
		{"Multiple Bits", []int{1, 3, 5, 7}, "\xAA"},
		{"Large Bits", []int{10, 11, 12}, "\x00\x1C"},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := Build(tc.bits...)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAnd(t *testing.T) {
	tt := []struct {
		name     string
		bs1, bs2 Bitset
		expected Bitset
	}{
		{
			name:     "Two NonEmpty",
			bs1:      Build(1, 2, 3, 4, 5),
			bs2:      Build(3, 4, 5, 6, 7),
			expected: Build(3, 4, 5),
		},
		{
			name:     "One Empty",
			bs1:      Build(1, 2, 3, 4, 5),
			bs2:      Build(),
			expected: "",
		},
		{
			name:     "Both Empty",
			bs1:      Build(),
			bs2:      Build(),
			expected: "",
		},
		{
			name:     "Different Word Sizes",
			bs1:      Build(1, 2, 3, 4, 5, 33),
			bs2:      Build(3, 4, 5, 6, 7),
			expected: Build(3, 4, 5),
		},
		{
			name:     "One Empty One NonEmpty",
			bs1:      Build(),
			bs2:      Build(3, 4, 5, 6, 7),
			expected: "",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.bs1.And(tc.bs2)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestAndNot(t *testing.T) {
	tt := []struct {
		name   string
		bs1    Bitset
		bs2    Bitset
		result Bitset
	}{
		{
			"Empty AndNot Empty",
			"",
			Build(1, 2, 3),
			"",
		},
		{
			"NonEmpty And Empty",
			Build(1, 2, 3),
			"",
			Build(1, 2, 3),
		},
		{
			"NonEmpty And NotEmpty",
			Build(1, 2, 3),
			Build(2, 3, 4),
			Build(1),
		},
		{
			"Common BitsSet AndNot",
			Build(1, 2, 3, 4, 5, 6, 7, 8),
			Build(3, 4, 5, 6, 7, 8, 9, 10),
			Build(1, 2),
		},
		{
			"bs1 Greater than bs2",
			Build(1, 2, 3, 4, 5, 6, 7, 8),
			Build(2, 3, 4),
			Build(1, 5, 6, 7, 8),
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.bs1.AndNot(tc.bs2)
			assert.Equal(t, tc.result, got)
		})
	}
}

func TestOr(t *testing.T) {
	tt := []struct {
		name   string
		bs1    Bitset
		bs2    Bitset
		result Bitset
	}{
		{
			"Empty Or Empty",
			"",
			"",
			"",
		},
		{
			"Empty Or NonEmpty",
			"",
			Build(1, 2, 3),
			Build(1, 2, 3),
		},
		{
			"NonEmpty Or Empty",
			Build(1, 2, 3),
			"",
			Build(1, 2, 3),
		},
		{
			"NonEmpty Or NonEmpty",
			Build(1, 2, 3),
			Build(4, 5, 6),
			Build(1, 2, 3, 4, 5, 6),
		},
		{
			"Common BitsSet",
			Build(1, 2, 3, 4),
			Build(3, 4, 5, 6),
			Build(1, 2, 3, 4, 5, 6),
		},
		{
			"Bs1 Larger Than Bs2",
			Build(3, 4, 5, 6, 7, 8, 9, 10),
			Build(1, 2),
			Build(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.bs1.Or(tc.bs2)
			assert.Equal(t, tc.result, got)
		})
	}
}

func TestSet(t *testing.T) {
	tt := []struct {
		name   string
		bs     Bitset
		offset int
		result Bitset
	}{
		{
			"Set On Empty Bitset",
			"",
			3,
			Build(3),
		},
		{
			"Set On NonEmpty Bitset",
			Build(1, 2, 3),
			10,
			Build(1, 2, 3, 10),
		},
		{
			"Set On Existing Bit",
			Build(1, 2, 3, 4),
			3,
			Build(1, 2, 3, 4),
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.bs.Set(tc.offset)
			assert.Equal(t, tc.result, got)
		})
	}
}

func TestIsContainedBy(t *testing.T) {
	tt := []struct {
		name     string
		bs1      Bitset
		bs2      Bitset
		expected bool
	}{
		{
			"Empty Is Contained By Empty",
			"",
			"",
			true,
		},
		{
			"Empty Is Contained By NonEmpty",
			"",
			Build(1, 2, 3),
			true,
		},
		{
			"NonEmpty Is Contained By Empty",
			Build(1, 2, 3),
			"",
			false,
		},
		{
			"Subset Is Contained By Superset",
			Build(1, 2, 3),
			Build(1, 2, 3, 4, 5, 6),
			true,
		},
		{
			"Not Contained",
			Build(1, 2, 3),
			Build(4, 5, 6),
			false,
		},
		{
			"Equal Bitsets",
			Build(1, 2, 3),
			Build(1, 2, 3),
			true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.bs1.IsContainedBy(tc.bs2)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestOverlaps(t *testing.T) {
	tt := []struct {
		name     string
		bs1      Bitset
		bs2      Bitset
		expected bool
	}{
		{
			"Empty Does Not Overlap Empty",
			"",
			"",
			false,
		},
		{
			"Empty Does Not Overlap NonEmpty",
			"",
			Build(1, 2, 3),
			false,
		},
		{
			"NonEmpty Does Not Overlap Empty",
			Build(1, 2, 3),
			"",
			false,
		},
		{
			"Common Bits Overlap",
			Build(1, 2, 3, 4),
			Build(3, 4, 5, 6),
			true,
		},
		{
			"No Common Bits Do Not Overlap",
			Build(1, 2, 3, 4),
			Build(5, 6, 7, 8),
			false,
		},
		{
			"Partial Overlap",
			Build(1, 2, 3, 4, 5),
			Build(4, 5, 6),
			true,
		},
		{
			"Equal Bitsets Overlap",
			Build(1, 2, 3),
			Build(1, 2, 3),
			true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.bs1.Overlaps(tc.bs2)
			assert.Equal(t, tc.expected, got)
		})
	}
}
