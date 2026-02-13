/*
Copyright 2025 The Vitess Authors.

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

package sortio

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func intRow(v int64) sqltypes.Row {
	return sqltypes.Row{sqltypes.NewInt64(v)}
}

func intLess(a, b sqltypes.Row) bool {
	av, _ := a[0].ToInt64()
	bv, _ := b[0].ToInt64()
	return av < bv
}

func TestLoserTree_Empty(t *testing.T) {
	tree := newLoserTree(nil, intLess)
	assert.Equal(t, 0, tree.Len())
}

func TestLoserTree_SingleSource(t *testing.T) {
	entries := []mergeEntry{
		{row: intRow(42), source: 0},
	}
	tree := newLoserTree(entries, intLess)

	require.Equal(t, 1, tree.Len())
	row, source := tree.Winner()
	assert.Equal(t, int64(42), mustInt64(t, row))
	assert.Equal(t, 0, source)

	// Replace with a new value
	tree.Replace(intRow(99))
	row, source = tree.Winner()
	assert.Equal(t, int64(99), mustInt64(t, row))
	assert.Equal(t, 0, source)

	// Remove the only source
	tree.Remove()
	assert.Equal(t, 0, tree.Len())
}

func TestLoserTree_TwoSources(t *testing.T) {
	entries := []mergeEntry{
		{row: intRow(5), source: 0},
		{row: intRow(3), source: 1},
	}
	tree := newLoserTree(entries, intLess)

	row, source := tree.Winner()
	assert.Equal(t, int64(3), mustInt64(t, row))
	assert.Equal(t, 1, source)

	// Replace winner (source 1) with 7 — now source 0 (5) should win
	tree.Replace(intRow(7))
	row, source = tree.Winner()
	assert.Equal(t, int64(5), mustInt64(t, row))
	assert.Equal(t, 0, source)

	// Replace winner (source 0) with 10 — source 1 (7) should win
	tree.Replace(intRow(10))
	row, source = tree.Winner()
	assert.Equal(t, int64(7), mustInt64(t, row))
	assert.Equal(t, 1, source)
}

func TestLoserTree_MergeOrder(t *testing.T) {
	// Simulate merging 4 sorted sources:
	// Source 0: [1, 5, 9]
	// Source 1: [2, 6, 10]
	// Source 2: [3, 7, 11]
	// Source 3: [4, 8, 12]
	sources := [][]int64{
		{1, 5, 9},
		{2, 6, 10},
		{3, 7, 11},
		{4, 8, 12},
	}
	pos := []int{0, 0, 0, 0}

	entries := make([]mergeEntry, 4)
	for i := range 4 {
		entries[i] = mergeEntry{row: intRow(sources[i][0]), source: i}
		pos[i] = 1
	}

	tree := newLoserTree(entries, intLess)

	var result []int64
	for tree.Len() > 0 {
		row, source := tree.Winner()
		val := mustInt64(t, row)
		result = append(result, val)

		if pos[source] < len(sources[source]) {
			tree.Replace(intRow(sources[source][pos[source]]))
			pos[source]++
		} else {
			tree.Remove()
		}
	}

	expected := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	assert.Equal(t, expected, result)
}

func TestLoserTree_UnevenSources(t *testing.T) {
	// Source 0: [1]
	// Source 1: [2, 3, 4, 5]
	// Source 2: [6]
	sources := [][]int64{
		{1},
		{2, 3, 4, 5},
		{6},
	}
	pos := []int{0, 0, 0}

	entries := make([]mergeEntry, 3)
	for i := range 3 {
		entries[i] = mergeEntry{row: intRow(sources[i][0]), source: i}
		pos[i] = 1
	}

	tree := newLoserTree(entries, intLess)

	var result []int64
	for tree.Len() > 0 {
		row, source := tree.Winner()
		result = append(result, mustInt64(t, row))

		if pos[source] < len(sources[source]) {
			tree.Replace(intRow(sources[source][pos[source]]))
			pos[source]++
		} else {
			tree.Remove()
		}
	}

	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, result)
}

func TestLoserTree_AllSameValues(t *testing.T) {
	entries := []mergeEntry{
		{row: intRow(5), source: 0},
		{row: intRow(5), source: 1},
		{row: intRow(5), source: 2},
	}
	tree := newLoserTree(entries, intLess)

	// All equal — should still produce 3 values
	count := 0
	for tree.Len() > 0 {
		row, _ := tree.Winner()
		assert.Equal(t, int64(5), mustInt64(t, row))
		tree.Remove()
		count++
	}
	assert.Equal(t, 3, count)
}

func TestLoserTree_15Way(t *testing.T) {
	// Simulate FinalMergeWay=15 sources
	k := 15
	n := 100 // rows per source
	sources := make([][]int64, k)
	for i := range k {
		sources[i] = make([]int64, n)
		for j := range n {
			sources[i][j] = int64(j*k + i) // interleaved values
		}
	}

	entries := make([]mergeEntry, k)
	pos := make([]int, k)
	for i := range k {
		entries[i] = mergeEntry{row: intRow(sources[i][0]), source: i}
		pos[i] = 1
	}

	tree := newLoserTree(entries, intLess)

	var result []int64
	for tree.Len() > 0 {
		row, source := tree.Winner()
		result = append(result, mustInt64(t, row))

		if pos[source] < n {
			tree.Replace(intRow(sources[source][pos[source]]))
			pos[source]++
		} else {
			tree.Remove()
		}
	}

	require.Len(t, result, k*n)
	// Verify sorted order
	for i := 1; i < len(result); i++ {
		assert.LessOrEqual(t, result[i-1], result[i], "out of order at index %d", i)
	}
}

func mustInt64(t *testing.T, row sqltypes.Row) int64 {
	t.Helper()
	v, err := row[0].ToInt64()
	require.NoError(t, err)
	return v
}
