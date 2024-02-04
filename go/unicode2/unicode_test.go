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

package unicode2

import (
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	// Test for no range tables
	rt := Merge()
	assert.Equal(t, &unicode.RangeTable{}, rt)

	testCases := []struct {
		rt1      *unicode.RangeTable
		rt2      *unicode.RangeTable
		expected *unicode.RangeTable
	}{
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 67, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 2000, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 68, Hi: 71, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 2001, Hi: 3000, Stride: 1},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 71, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 3000, Stride: 1},
				},
				LatinOffset: 1,
			},
		},
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 70, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 2100, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 68, Hi: 72, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 2000, Hi: 3100, Stride: 1},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 72, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 3100, Stride: 1},
				},
				LatinOffset: 1,
			},
		},
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 65, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1000, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 70, Hi: 70, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 2000, Hi: 2001, Stride: 1},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 70, Stride: 5},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1000, Stride: 1},
					{Lo: 2000, Hi: 2001, Stride: 1},
				},
				LatinOffset: 1,
			},
		},
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 65, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1000, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 70, Hi: 71, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 2000, Hi: 2000, Stride: 1},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 65, Stride: 1},
					{Lo: 70, Hi: 71, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 2000, Stride: 1000},
				},
				LatinOffset: 2,
			},
		},
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 68, Stride: 1},
					{Lo: 100, Hi: 104, Stride: 2},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1004, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 69, Hi: 75, Stride: 2},
				},
				R32: []unicode.Range32{
					{Lo: 1005, Hi: 2000, Stride: 2},
					{Lo: 2003, Hi: 2006, Stride: 3},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 69, Stride: 1},
					{Lo: 71, Hi: 75, Stride: 2},
					{Lo: 100, Hi: 104, Stride: 2},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1005, Stride: 1},
					{Lo: 1007, Hi: 2000, Stride: 2},
					{Lo: 2003, Hi: 2006, Stride: 3},
				},
				LatinOffset: 3,
			},
		},
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 78, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 2000, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 75, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1500, Stride: 1},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 78, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 2000, Stride: 1},
				},
				LatinOffset: 1,
			},
		},
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 78, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 2000, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 75, Stride: 8},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1500, Stride: 3},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 78, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 2000, Stride: 1},
				},
				LatinOffset: 1,
			},
		},
		{
			rt1: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 78, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1500, Stride: 1},
				},
			},
			rt2: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 79, Hi: 84, Stride: 8},
				},
				R32: []unicode.Range32{
					{Lo: 1501, Hi: 2000, Stride: 500},
				},
			},
			expected: &unicode.RangeTable{
				R16: []unicode.Range16{
					{Lo: 65, Hi: 79, Stride: 1},
				},
				R32: []unicode.Range32{
					{Lo: 1000, Hi: 1501, Stride: 1},
				},
				LatinOffset: 1,
			},
		},
	}

	for _, tc := range testCases {
		rt := Merge(tc.rt1, tc.rt2)

		assert.Equal(t, tc.expected, rt)
	}
}
