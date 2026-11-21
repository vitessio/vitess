/*
Copyright 2026 The Vitess Authors.

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

package evalengine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMathMulOverflow covers the integer multiplication helpers at the edges
// of the int64/uint64 ranges, where a wrapped product must be reported as out
// of range instead of returned.
func TestMathMulOverflow(t *testing.T) {
	t.Run("ii", func(t *testing.T) {
		cases := []struct {
			v1, v2  int64
			want    int64
			wantErr bool
		}{
			{-1, math.MinInt64, 0, true},
			{math.MinInt64, -1, 0, true},
			{1 << 32, 1 << 31, 0, true},
			{-1, math.MaxInt64, -math.MaxInt64, false},
			{math.MinInt64, 1, math.MinInt64, false},
			{-(1 << 32), 1 << 31, 0, true},
			{1 << 32, -(1 << 31), 0, true},
			{2, -(1 << 62), 0, true},
			{-(1 << 62), 2, 0, true},
			{1, math.MinInt64, math.MinInt64, false},
			{0, math.MinInt64, 0, false},
			{math.MinInt64, 0, 0, false},
		}
		for _, c := range cases {
			got, err := mathMul_ii0(c.v1, c.v2)
			if c.wantErr {
				require.Error(t, err, "%d * %d = %d, want out of range error", c.v1, c.v2, got)
				continue
			}
			if assert.NoError(t, err, "%d * %d", c.v1, c.v2) {
				assert.Equal(t, c.want, got, "%d * %d", c.v1, c.v2)
			}
		}
	})

	t.Run("uu", func(t *testing.T) {
		cases := []struct {
			v1, v2  uint64
			want    uint64
			wantErr bool
		}{
			{4294967297, 4294967297, 0, true}, // (2^32+1)^2 wraps to 2^33+1
			{1 << 32, 1 << 32, 0, true},
			{3, 6148914691236517206, 0, true},
			{math.MaxUint64, 2, 0, true},
			{4294967295, 4294967297, math.MaxUint64, false}, // (2^32-1)(2^32+1) = 2^64-1
			{math.MaxUint64, 1, math.MaxUint64, false},
			{1 << 32, 1 << 31, 1 << 63, false},
			{0, math.MaxUint64, 0, false},
		}
		for _, c := range cases {
			got, err := mathMul_uu0(c.v1, c.v2)
			if c.wantErr {
				require.Error(t, err, "%d * %d = %d, want out of range error", c.v1, c.v2, got)
				continue
			}
			if assert.NoError(t, err, "%d * %d", c.v1, c.v2) {
				assert.Equal(t, c.want, got, "%d * %d", c.v1, c.v2)
			}
		}
	})
}
