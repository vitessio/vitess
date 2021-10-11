/*
Copyright 2020 The Vitess Authors.

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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	F1 = SingleTableSet(0)
	F2 = SingleTableSet(1)
	F3 = SingleTableSet(2)

	F12  = F1.Merge(F2)
	F123 = F12.Merge(F3)
)

func TestTableSet_IsOverlapping(t *testing.T) {
	assert.True(t, F12.IsOverlapping(F12))
	assert.True(t, F1.IsOverlapping(F12))
	assert.True(t, F12.IsOverlapping(F1))
	assert.False(t, F3.IsOverlapping(F12))
	assert.False(t, F12.IsOverlapping(F3))
}

func TestTableSet_IsSolvedBy(t *testing.T) {
	assert.True(t, F1.IsSolvedBy(F12))
	assert.False(t, (F12).IsSolvedBy(F1))
	assert.False(t, F3.IsSolvedBy(F12))
	assert.False(t, (F12).IsSolvedBy(F3))
}

func TestTableSet_Constituents(t *testing.T) {
	assert.Equal(t, []int{0, 1, 2}, (F123).Constituents())
	assert.Equal(t, []int{0, 1}, (F12).Constituents())
	assert.Equal(t, []int{0, 2}, (F1.Merge(F3)).Constituents())
	assert.Equal(t, []int{1, 2}, (F2.Merge(F3)).Constituents())
	assert.Empty(t, TableSet{}.Constituents())
}

func TestTableSet_TableOffset(t *testing.T) {
	assert.Equal(t, 0, F1.TableOffset())
	assert.Equal(t, 1, F2.TableOffset())
	assert.Equal(t, 2, F3.TableOffset())
}

func TestTableSet_LargeTablesConstituents(t *testing.T) {
	const GapSize = 32

	var ts TableSet
	var expected []int
	var table int

	for t := 0; t < 256; t++ {
		table += rand.Intn(GapSize) + 1
		expected = append(expected, table)
		ts.AddTable(table)
	}

	assert.Equal(t, expected, ts.Constituents())
}

func TestTabletSet_LargeMergeInPlace(t *testing.T) {
	const SetRange = 256
	const Blocks = 64

	var tablesets = make([]TableSet, 64)

	for i := range tablesets {
		ts := &tablesets[i]
		setrng := i * SetRange

		for tid := 0; tid < SetRange; tid++ {
			ts.AddTable(setrng + tid)
		}
	}

	var result TableSet
	for _, ts := range tablesets {
		result.MergeInPlace(ts)
	}

	var expected = make([]int, SetRange*Blocks)
	for tid := range expected {
		expected[tid] = tid
	}

	assert.Equal(t, expected, result.Constituents())
}

func TestTabletSet_LargeMerge(t *testing.T) {
	const SetRange = 256
	const Blocks = 64

	var tablesets = make([]TableSet, 64)

	for i := range tablesets {
		ts := &tablesets[i]
		setrng := i * SetRange

		for tid := 0; tid < SetRange; tid++ {
			ts.AddTable(setrng + tid)
		}
	}

	var result TableSet
	for _, ts := range tablesets {
		result = result.Merge(ts)
	}

	var expected = make([]int, SetRange*Blocks)
	for tid := range expected {
		expected[tid] = tid
	}

	assert.Equal(t, expected, result.Constituents())
}

func TestTableSet_LargeOffset(t *testing.T) {
	for tid := 0; tid < 1024; tid++ {
		ts := SingleTableSet(tid)
		assert.Equal(t, tid, ts.TableOffset())
	}
}
