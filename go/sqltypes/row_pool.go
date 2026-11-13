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

package sqltypes

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// A VStreamRows stream reuses one set of *querypb.Row across packets on both
// ends: the rowStreamer fills them in place with RowToProto3Inplace, and the
// gRPC client decodes every packet into one pooled VStreamRowsResponse whose
// Row.UnmarshalVT appends into Values[:0] and Lengths[:0]. Reuse keeps
// steady-state packets allocation-free, but each slot's capacity only ever
// grows to the largest row it has held, so a long stream retains the sum of
// the per-slot high-water marks until it ends (vitessio/vitess#20954).
//
// The helpers below bound that retention with a budget derived from the
// packet just processed, not from historical capacities: a pool may keep
// RowPoolHeadroom times the footprint of the live packet (never less than
// RowPoolMinBytes) and is dropped as a whole once it retains more. The drop
// is O(1) and the next packet reallocates only what it needs.

const (
	// RowPoolHeadroom is how many times the live packet's footprint a reused
	// row pool may retain before it is dropped. append grows a slice's
	// capacity by up to 2x, so 2 is the floor for allocation-free steady
	// state; 4 leaves room for row sizes to vary between packets without
	// churning the pool.
	RowPoolHeadroom = 4

	// RowPoolMinBytes is the floor of the budget: a pool this small is never
	// worth dropping, however small the packets that use it.
	RowPoolMinBytes = 1 << 20

	// rowFixedBytes approximates the fixed cost of one reused Row: the struct
	// and its two slice headers. It is part of the budget's per-row footprint
	// so metadata-heavy packets of tiny rows are not under-budgeted.
	rowFixedBytes = 64
)

// RowPoolBudget returns how many bytes a reused row pool may retain after
// carrying a packet of rowCount rows whose column values total valueBytes
// and which each hold fieldCount lengths.
func RowPoolBudget(valueBytes, rowCount, fieldCount int) int {
	footprint := valueBytes + rowCount*(fieldCount*8+rowFixedBytes)
	return max(RowPoolHeadroom*footprint, RowPoolMinBytes)
}

// RowPoolPacketBudget returns RowPoolBudget for the packet currently held in
// rows, i.e. its first len(rows) slots.
func RowPoolPacketBudget(rows []*querypb.Row) int {
	valueBytes, fieldCount := 0, 0
	for _, r := range rows {
		if r == nil {
			continue
		}
		valueBytes += len(r.Values)
		fieldCount = max(fieldCount, len(r.Lengths))
	}
	return RowPoolBudget(valueBytes, len(rows), fieldCount)
}

// RowPoolRetained returns the bytes a reused row pool keeps alive: the
// capacity of every slot's Values and Lengths. It scans rows[:cap(rows)]
// rather than rows, because a response's ResetVT truncates Rows to [:0] but
// keeps the *Row elements behind it, and they still own their buffers.
func RowPoolRetained(rows []*querypb.Row) int {
	retained := 0
	for _, r := range rows[:cap(rows)] {
		if r == nil {
			continue
		}
		retained += cap(r.Values) + cap(r.Lengths)*8
	}
	return retained
}
