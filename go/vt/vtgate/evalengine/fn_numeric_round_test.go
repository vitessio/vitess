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
)

// TestRoundIntegerOverflow checks ROUND(int, negative) near the int64/uint64
// limits. MySQL 8.4 returns an out of range error when the rounded value does
// not fit; the expected values below come from MySQL 8.4.
func TestRoundIntegerOverflow(t *testing.T) {
	t.Run("signed", func(t *testing.T) {
		cases := []struct {
			v, round int64
			want     int64
			wantErr  bool
		}{
			{math.MaxInt64, -1, 0, true},
			{math.MaxInt64 - 1, -1, 0, true},
			{math.MinInt64, -1, 0, true},
			{-9223372036854775805, -1, 0, true},
			{math.MaxInt64, -19, 0, true},
			{-math.MaxInt64, -19, 0, true},
			{9223372036854775804, -1, 9223372036854775800, false},
			{-9223372036854775804, -1, -9223372036854775800, false},
			{math.MaxInt64, -20, 0, false},
			{1234, -2, 1200, false},
			{-1250, -2, -1300, false},
			{math.MaxInt64, 0, math.MaxInt64, false},
		}
		for _, c := range cases {
			got, err := roundSigned(c.v, c.round)
			if c.wantErr {
				assert.Error(t, err, "ROUND(%d, %d) = %d, want out of range error", c.v, c.round, got)
				continue
			}
			if assert.NoError(t, err, "ROUND(%d, %d)", c.v, c.round) {
				assert.Equal(t, c.want, got, "ROUND(%d, %d)", c.v, c.round)
			}
		}
	})

	t.Run("unsigned", func(t *testing.T) {
		cases := []struct {
			v       uint64
			round   int64
			want    uint64
			wantErr bool
		}{
			{math.MaxUint64, -1, 0, true},
			{math.MaxUint64, -19, 0, true},
			{math.MaxUint64, -2, 18446744073709551600, false},
			{math.MaxUint64, -20, 0, false},
			{math.MaxInt64, -19, 10000000000000000000, false},
			{1250, -2, 1300, false},
		}
		for _, c := range cases {
			got, err := roundUnsigned(c.v, c.round)
			if c.wantErr {
				assert.Error(t, err, "ROUND(%d, %d) = %d, want out of range error", c.v, c.round, got)
				continue
			}
			if assert.NoError(t, err, "ROUND(%d, %d)", c.v, c.round) {
				assert.Equal(t, c.want, got, "ROUND(%d, %d)", c.v, c.round)
			}
		}
	})
}
