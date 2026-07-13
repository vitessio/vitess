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

// SplitBytesByWeight distributes request-level ingress bytes using
// caller-provided weights. Zero-weight entries receive no bytes unless all
// entries have zero weight, in which case the bytes are split evenly.
//
// The result always sums to total; any rounding remainder is assigned to the
// last positive-weight entry.
func SplitBytesByWeight(total uint64, weights []int) []uint64 {
	if len(weights) == 0 {
		return nil
	}
	allocations := make([]uint64, len(weights))
	if total == 0 {
		return allocations
	}

	var totalWeight uint64
	for _, weight := range weights {
		if weight > 0 {
			totalWeight += uint64(weight)
		}
	}
	if totalWeight == 0 {
		splitBytesEvenly(total, allocations)
		return allocations
	}

	lastPositiveWeight := -1
	for i, weight := range weights {
		if weight > 0 {
			lastPositiveWeight = i
		}
	}
	var allocated uint64
	for i, weight := range weights {
		if i == lastPositiveWeight {
			allocations[i] = total - allocated
			break
		}
		if weight > 0 {
			allocations[i] = total * uint64(weight) / totalWeight
		}
		allocated += allocations[i]
	}
	return allocations
}

func splitBytesEvenly(total uint64, allocations []uint64) {
	base := total / uint64(len(allocations))
	remainder := total % uint64(len(allocations))
	for i := range allocations {
		allocations[i] = base
		if uint64(i) < remainder {
			allocations[i]++
		}
	}
}
