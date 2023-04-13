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

	"vitess.io/vitess/go/vt/vtgate/semantics/bitset"
)

// TableSet is how a set of tables is expressed.
// Tables get unique bits assigned in the order that they are encountered during semantic analysis.
type TableSet bitset.Bitset

// Format formats the TableSet.
func (ts TableSet) Format(f fmt.State, _ rune) {
	first := true
	fmt.Fprintf(f, "TableSet{")
	bitset.Bitset(ts).ForEach(func(tid int) {
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
	return bitset.Bitset(ts).Overlaps(bitset.Bitset(other))
}

// IsSolvedBy returns true if all of `ts` is contained in `other`
func (ts TableSet) IsSolvedBy(other TableSet) bool {
	return bitset.Bitset(ts).IsContainedBy(bitset.Bitset(other))
}

// NumberOfTables returns the number of bits set
func (ts TableSet) NumberOfTables() int {
	return bitset.Bitset(ts).Popcount()
}

// NonEmpty returns true if there are tables in the tableset
func (ts TableSet) NonEmpty() bool {
	return !ts.IsEmpty()
}

// IsEmpty returns true if there are no tables in the tableset
func (ts TableSet) IsEmpty() bool {
	return len(ts) == 0
}

// TableOffset returns the offset in the Tables array from TableSet
func (ts TableSet) TableOffset() int {
	return bitset.Bitset(ts).SingleBit()
}

// ForEachTable calls the given callback with the indices for all tables in this TableSet
func (ts TableSet) ForEachTable(callback func(int)) {
	bitset.Bitset(ts).ForEach(callback)
}

// Constituents returns a slice with the indices for all tables in this TableSet
func (ts TableSet) Constituents() (result []TableSet) {
	ts.ForEachTable(func(t int) {
		result = append(result, SingleTableSet(t))
	})
	return
}

// Merge creates a TableSet that contains both inputs
func (ts TableSet) Merge(other TableSet) TableSet {
	return TableSet(bitset.Bitset(ts).Or(bitset.Bitset(other)))
}

// Remove returns a new TableSet with all the tables in `other` removed
func (ts TableSet) Remove(other TableSet) TableSet {
	return TableSet(bitset.Bitset(ts).AndNot(bitset.Bitset(other)))
}

// KeepOnly removes all the tables not in `other` from this TableSet
func (ts TableSet) KeepOnly(other TableSet) TableSet {
	return TableSet(bitset.Bitset(ts).And(bitset.Bitset(other)))
}

// WithTable returns a new TableSet that contains this table too
func (ts TableSet) WithTable(tableidx int) TableSet {
	return TableSet(bitset.Bitset(ts).Set(tableidx))
}

// SingleTableSet creates a TableSet that contains only the given table
func SingleTableSet(tableidx int) TableSet {
	return TableSet(bitset.Single(tableidx))
}

// EmptyTableSet creates an empty TableSet
func EmptyTableSet() TableSet {
	return ""
}

// MergeTableSets merges all the given TableSet into a single one
func MergeTableSets(tss ...TableSet) TableSet {
	var result bitset.Bitset
	for _, t := range tss {
		result = result.Or(bitset.Bitset(t))
	}
	return TableSet(result)
}

// TableSetFromIds returns TableSet for all the id passed in argument.
func TableSetFromIds(tids ...int) (ts TableSet) {
	return TableSet(bitset.Build(tids...))
}
