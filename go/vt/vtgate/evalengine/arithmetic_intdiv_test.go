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
)

// TestMathIntDivSignedUnsignedBoundary covers the signed/unsigned DIV helpers
// at the extremes of the int64/uint64 ranges. A BIGINT UNSIGNED result is only
// valid when the quotient truncates to 0; any negative quotient is out of range.
func TestMathIntDivSignedUnsignedBoundary(t *testing.T) {
	const maxUint = uint64(math.MaxUint64)
	const absMinInt64 = uint64(1) << 63 // |math.MinInt64| == 2^63

	t.Run("iu", func(t *testing.T) {
		cases := []struct {
			v1      int64
			v2      uint64
			want    uint64
			wantErr bool
		}{
			// |v1| < v2 -> 0
			{math.MinInt64, absMinInt64 + 1, 0, false},
			{-5, 10, 0, false},
			{-10, maxUint, 0, false},
			// |v1| >= v2 -> out of range (quotient is negative)
			{math.MinInt64, absMinInt64 - 1, 0, true},   // |v1|=2^63 > 2^63-1
			{math.MinInt64, absMinInt64, 0, true},       // |v1|=2^63 == v2
			{-(math.MaxInt64), math.MaxInt64, 0, true},  // equal magnitudes
			{-10, 3, 0, true},
			// v1 >= 0 path unchanged
			{10, 3, 3, false},
			{0, 5, 0, false},
		}
		for _, c := range cases {
			got, err := mathIntDiv_iu0(c.v1, c.v2)
			if (err != nil) != c.wantErr {
				t.Errorf("mathIntDiv_iu0(%d, %d) err=%v, wantErr=%v", c.v1, c.v2, err, c.wantErr)
				continue
			}
			if err == nil && got != c.want {
				t.Errorf("mathIntDiv_iu0(%d, %d) = %d, want %d", c.v1, c.v2, got, c.want)
			}
		}
	})

	t.Run("ui", func(t *testing.T) {
		cases := []struct {
			v1      uint64
			v2      int64
			want    uint64
			wantErr bool
		}{
			// v1 < |v2| -> 0
			{absMinInt64 - 1, math.MinInt64, 0, false}, // 2^63-1 < 2^63
			{2, -3, 0, false},
			// v1 >= |v2| -> out of range
			{absMinInt64, math.MinInt64, 0, true}, // 2^63 == |v2|
			{maxUint, -1, 0, true},
			{5, -3, 0, true},
			// v2 > 0 path unchanged
			{10, 3, 3, false},
		}
		for _, c := range cases {
			got, err := mathIntDiv_ui0(c.v1, c.v2)
			if (err != nil) != c.wantErr {
				t.Errorf("mathIntDiv_ui0(%d, %d) err=%v, wantErr=%v", c.v1, c.v2, err, c.wantErr)
				continue
			}
			if err == nil && got != c.want {
				t.Errorf("mathIntDiv_ui0(%d, %d) = %d, want %d", c.v1, c.v2, got, c.want)
			}
		}
	})
}
