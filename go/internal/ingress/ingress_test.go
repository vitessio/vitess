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

package ingress

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSplitBytesByWeight verifies request ingress bytes are attributed by
// positive weights while preserving the original total.
func TestSplitBytesByWeight(t *testing.T) {
	tests := []struct {
		name        string
		total       uint64
		weights     []int
		allocations []uint64
	}{
		{
			name:        "empty",
			total:       10,
			weights:     nil,
			allocations: nil,
		},
		{
			name:        "zero total",
			total:       0,
			weights:     []int{1, 2},
			allocations: []uint64{0, 0},
		},
		{
			name:        "positive weights",
			total:       30,
			weights:     []int{1, 2},
			allocations: []uint64{10, 20},
		},
		{
			name:        "remainder goes to last positive weight",
			total:       3,
			weights:     []int{1, 1, 0},
			allocations: []uint64{1, 2, 0},
		},
		{
			name:        "all non-positive weights",
			total:       5,
			weights:     []int{0, -1, 0},
			allocations: []uint64{2, 2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.allocations, SplitBytesByWeight(tt.total, tt.weights))
		})
	}
}

// TestNextStatementBytes verifies that statements handed out one at a time
// get the same shares SplitBytesByWeight gives them up front, with the last
// statement taking the remainder.
func TestNextStatementBytes(t *testing.T) {
	remaining := uint64(27)
	first := NextStatementBytes(remaining, "select 1", "select 222222")
	remaining -= first
	last := NextStatementBytes(remaining, "select 222222", "")

	assert.Equal(t, uint64(10), first)
	assert.Equal(t, uint64(17), last)
	assert.Equal(t, []uint64{first, last}, SplitBytesByWeight(27, []int{8, 13}))
	assert.Equal(t, uint64(5), NextStatementBytes(5, "select 1", ""), "a single statement takes it all")
}
