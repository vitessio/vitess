/*
Copyright 2021 The Vitess Authors.

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

package semantics

import (
	"fmt"
	"math/bits"
)

type largeTableSet struct {
	tables []uint64
}

func (ts *largeTableSet) overlapsSmall(small uint64) bool {
	return ts.tables[0]&uint64(small) != 0
}

func minlen(a, b []uint64) int {
	if len(a) < len(b) {
		return len(a)
	}
	return len(b)
}

func (ts *largeTableSet) overlaps(b *largeTableSet) bool {
	min := minlen(ts.tables, b.tables)
	for t := 0; t < min; t++ {
		if ts.tables[t]&b.tables[t] != 0 {
			return true
		}
	}
	return false
}

func (ts *largeTableSet) containsSmall(small uint64) bool {
	return small&ts.tables[0] == small
}

func (ts *largeTableSet) isContainedBy(b *largeTableSet) bool {
	if len(ts.tables) > len(b.tables) {
		return false
	}
	for i, t := range ts.tables {
		if t&b.tables[i] != t {
			return false
		}
	}
	return true
}

func (ts *largeTableSet) popcount() (count int) {
	for _, t := range ts.tables {
		count += bits.OnesCount64(t)
	}
	return
}

func (ts *largeTableSet) merge(other *largeTableSet) *largeTableSet {
	small, large := ts.tables, other.tables
	if len(small) > len(large) {
		small, large = large, small
	}

	merged := make([]uint64, len(large))
	m := 0

	for m < len(small) {
		merged[m] = small[m] | large[m]
		m++
	}
	for m < len(large) {
		merged[m] = large[m]
		m++
	}

	return &largeTableSet{merged}
}

func (ts *largeTableSet) mergeSmall(small uint64) *largeTableSet {
	merged := make([]uint64, len(ts.tables))
	copy(merged, ts.tables)
	merged[0] |= small
	return &largeTableSet{merged}
}

func (ts *largeTableSet) mergeInPlace(other *largeTableSet) {
	if len(other.tables) > len(ts.tables) {
		merged := make([]uint64, len(other.tables))
		copy(merged, ts.tables)
		ts.tables = merged
	}
	for i := range other.tables {
		ts.tables[i] |= other.tables[i]
	}
}

func (ts *largeTableSet) mergeSmallInPlace(small uint64) {
	ts.tables[0] |= small
}

func (ts *largeTableSet) tableOffset() (offset int) {
	var found bool
	for chunk, t := range ts.tables {
		if t == 0 {
			continue
		}
		if found || bits.OnesCount64(t) != 1 {
			return -1
		}
		offset = chunk*64 + bits.TrailingZeros64(t)
		found = true
	}
	return
}

func (ts *largeTableSet) add(tableidx int) {
	chunk := tableidx / 64
	offset := tableidx % 64

	if len(ts.tables) <= chunk {
		tables := make([]uint64, chunk+1)
		copy(tables, ts.tables)
		ts.tables = tables
	}

	ts.tables[chunk] |= 1 << offset
}

func (ts *largeTableSet) foreach(callback func(int)) {
	for idx, bitset := range ts.tables {
		for bitset != 0 {
			t := bitset & -bitset
			r := bits.TrailingZeros64(bitset)
			callback(idx*64 + r)
			bitset ^= t
		}
	}
}

func newLargeTableSet(small uint64, tableidx int) *largeTableSet {
	chunk := tableidx / 64
	offset := tableidx % 64

	tables := make([]uint64, chunk+1)
	tables[0] = small
	tables[chunk] |= 1 << offset

	return &largeTableSet{tables}
}

// TableSet is how a set of tables is expressed.
// Tables get unique bits assigned in the order that they are encountered during semantic analysis.
// This TableSet implementation is optimized for sets of less than 64 tables, but can grow to support an arbitrary
// large amount of tables.
type TableSet struct {
	small uint64
	large *largeTableSet
}

func (ts TableSet) Format(f fmt.State, verb rune) {
	first := true
	fmt.Fprintf(f, "TableSet{")
	ts.ForEachTable(func(tid int) {
		if first {
			fmt.Fprintf(f, "%d", tid)
			first = false
		} else {
			fmt.Fprintf(f, ",%d", tid)
		}
	})
	fmt.Fprintf(f, "}")
}

// IsOverlapping returns true if at least one table exists in both sets
func (ts TableSet) IsOverlapping(other TableSet) bool {
	switch {
	case ts.large == nil && other.large == nil:
		return ts.small&other.small != 0
	case ts.large == nil:
		return other.large.overlapsSmall(ts.small)
	case other.large == nil:
		return ts.large.overlapsSmall(other.small)
	default:
		return ts.large.overlaps(other.large)
	}
}

// IsSolvedBy returns true if all of `ts` is contained in `other`
func (ts TableSet) IsSolvedBy(other TableSet) bool {
	switch {
	case ts.large == nil && other.large == nil:
		return ts.small&other.small == ts.small
	case ts.large == nil:
		return other.large.containsSmall(ts.small)
	case other.large == nil:
		// if we're a large table and other is not, we cannot be contained by other
		return false
	default:
		return ts.large.isContainedBy(other.large)
	}
}

// NumberOfTables returns the number of bits set
func (ts TableSet) NumberOfTables() int {
	if ts.large == nil {
		return bits.OnesCount64(ts.small)
	}
	return ts.large.popcount()
}

// TableOffset returns the offset in the Tables array from TableSet
func (ts TableSet) TableOffset() int {
	if ts.large == nil {
		if bits.OnesCount64(ts.small) != 1 {
			return -1
		}
		return bits.TrailingZeros64(ts.small)
	}
	return ts.large.tableOffset()
}

// ForEachTable calls the given callback with the indices for all tables in this TableSet
func (ts TableSet) ForEachTable(callback func(int)) {
	if ts.large == nil {
		bitset := ts.small
		for bitset != 0 {
			t := bitset & -bitset
			callback(bits.TrailingZeros64(bitset))
			bitset ^= t
		}
	} else {
		ts.large.foreach(callback)
	}
}

// Constituents returns a slice with the indices for all tables in this TableSet
func (ts TableSet) Constituents() (result []int) {
	ts.ForEachTable(func(t int) {
		result = append(result, t)
	})
	return
}

// Merge creates a TableSet that contains both inputs
func (ts TableSet) Merge(other TableSet) TableSet {
	switch {
	case ts.large == nil && other.large == nil:
		return TableSet{small: ts.small | other.small}
	case ts.large == nil:
		return TableSet{large: other.large.mergeSmall(ts.small)}
	case other.large == nil:
		return TableSet{large: ts.large.mergeSmall(other.small)}
	default:
		return TableSet{large: ts.large.merge(other.large)}
	}
}

// MergeInPlace merges all the tables in `other` into this TableSet
func (ts *TableSet) MergeInPlace(other TableSet) {
	switch {
	case ts.large == nil && other.large == nil:
		ts.small |= other.small
	case ts.large == nil:
		ts.large = other.large.mergeSmall(ts.small)
	case other.large == nil:
		ts.large.mergeSmallInPlace(other.small)
	default:
		ts.large.mergeInPlace(other.large)
	}
}

// AddTable adds the given table to this set
func (ts *TableSet) AddTable(tableidx int) {
	switch {
	case ts.large == nil && tableidx < 64:
		ts.small |= 1 << tableidx
	case ts.large == nil:
		ts.large = newLargeTableSet(ts.small, tableidx)
	default:
		ts.large.add(tableidx)
	}
}

// SingleTableSet creates a TableSet that contains only the given table
func SingleTableSet(tableidx int) TableSet {
	if tableidx < 64 {
		return TableSet{small: 1 << tableidx}
	}
	return TableSet{large: newLargeTableSet(0x0, tableidx)}
}

// MergeTableSets merges all the given TableSet into a single one
func MergeTableSets(tss ...TableSet) (result TableSet) {
	for _, t := range tss {
		result.MergeInPlace(t)
	}
	return
}

func TableSetFromIds(tids ...int) (ts TableSet) {
	for _, tid := range tids {
		ts.AddTable(tid)
	}
	return
}
