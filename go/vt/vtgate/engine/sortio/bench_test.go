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
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func benchFields() []*querypb.Field {
	return []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "name", Type: querypb.Type_VARCHAR},
	}
}

func benchComparison() evalengine.Comparison {
	return evalengine.Comparison{
		evalengine.OrderByParams{
			Col:             0,
			WeightStringCol: -1,
			Type:            evalengine.NewType(querypb.Type_INT64, collations.CollationBinaryID),
		},
	}
}

func benchRow(id int64) sqltypes.Row {
	return sqltypes.Row{
		sqltypes.NewInt64(id),
		sqltypes.NewVarChar(fmt.Sprintf("name-%d-padding-to-make-it-realistic", id)),
	}
}

// generateRows creates n rows in random order, each ~50 bytes encoded.
func generateRows(n int) []sqltypes.Row {
	rows := make([]sqltypes.Row, n)
	perm := rand.Perm(n)
	for i, p := range perm {
		rows[i] = benchRow(int64(p))
	}
	return rows
}

func BenchmarkRowCodec_Encode(b *testing.B) {
	codec := NewRowCodec(benchFields())
	row := benchRow(42)
	var buf bytes.Buffer

	for b.Loop() {
		buf.Reset()
		codec.Encode(&buf, row)
	}
}

func BenchmarkRowCodec_Decode(b *testing.B) {
	codec := NewRowCodec(benchFields())
	row := benchRow(42)
	var buf bytes.Buffer
	codec.Encode(&buf, row)
	encoded := buf.Bytes()

	for b.Loop() {
		r := bytes.NewReader(encoded)
		_, err := codec.Decode(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComparisonSort is the baseline: the existing in-memory sort path
// (Comparison.Sort) with no SpillSorter overhead.
func BenchmarkComparisonSort(b *testing.B) {
	cmp := benchComparison()
	n := 1000

	source := make([]sqltypes.Row, n)
	perm := rand.Perm(n)
	for i, p := range perm {
		source[i] = benchRow(int64(p))
	}

	for b.Loop() {
		rows := make([]sqltypes.Row, n)
		copy(rows, source)
		cmp.Sort(rows)
	}
}

func BenchmarkSpillSort_NoSpill(b *testing.B) {
	fields := benchFields()
	cmp := benchComparison()
	n := 1000

	// Pre-generate rows
	rows := make([]sqltypes.Row, n)
	perm := rand.Perm(n)
	for i, p := range perm {
		rows[i] = benchRow(int64(p))
	}

	for b.Loop() {
		ss := NewSpillSorter(cmp, fields, 1024*1024, "")
		for _, row := range rows {
			if err := ss.Add(row); err != nil {
				b.Fatal(err)
			}
		}
		err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		ss.Close()
	}
}

func BenchmarkSpillSort_OneSpill(b *testing.B) {
	fields := benchFields()
	cmp := benchComparison()
	n := 1000

	rows := make([]sqltypes.Row, n)
	perm := rand.Perm(n)
	for i, p := range perm {
		rows[i] = benchRow(int64(p))
	}

	// Calculate a buffer that will cause exactly ~2 runs
	codec := NewRowCodec(benchFields())
	totalSize := int64(0)
	for _, row := range rows {
		totalSize += int64(codec.EncodedSize(row))
	}
	bufSize := totalSize / 2

	for b.Loop() {
		ss := NewSpillSorter(cmp, fields, bufSize, "")
		for _, row := range rows {
			if err := ss.Add(row); err != nil {
				b.Fatal(err)
			}
		}
		err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		ss.Close()
	}
}

// BenchmarkSort10MB_InMemory sorts ~10MB of row data purely in memory
// using Comparison.Sort (the pre-existing path).
func BenchmarkSort10MB_InMemory(b *testing.B) {
	cmp := benchComparison()
	source := generateRows(200_000) // ~10MB at ~50 bytes/row

	for b.Loop() {
		rows := make([]sqltypes.Row, len(source))
		copy(rows, source)
		cmp.Sort(rows)
	}
}

// BenchmarkSort10MB_SpillSort runs SpillSorter at various buffer sizes.
func BenchmarkSort10MB_SpillSort(b *testing.B) {
	fields := benchFields()
	cmp := benchComparison()
	source := generateRows(200_000) // ~10MB at ~50 bytes/row

	for _, bufSize := range []int64{256 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("buf_%dKB", bufSize/1024), func(b *testing.B) {
			for b.Loop() {
				ss := NewSpillSorter(cmp, fields, bufSize, "")
				for _, row := range source {
					if err := ss.Add(row); err != nil {
						b.Fatal(err)
					}
				}
				err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
				ss.Close()
			}
		})
	}
}

// TestSort10MB_PeakMemory measures peak live heap usage for in-memory sort
// vs spill sort with various buffer sizes, sorting ~10MB of row data.
func TestSort10MB_PeakMemory(t *testing.T) {
	n := 200_000

	// --- In-memory sort: accumulate all rows, then sort ---
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	rows := make([]sqltypes.Row, 0, n)
	for i := range n {
		rows = append(rows, benchRow(int64(i)))
	}

	// Measure heap with all rows live (peak for in-memory approach)
	runtime.GC()
	var peak runtime.MemStats
	runtime.ReadMemStats(&peak)
	inMemHeap := peak.HeapAlloc - before.HeapAlloc

	benchComparison().Sort(rows)
	runtime.KeepAlive(rows)
	rows = nil //nolint:ineffassign
	runtime.GC()

	t.Logf("In-memory sort peak live heap: %.2f MB", float64(inMemHeap)/(1024*1024))

	// --- Spill sort at various buffer sizes ---
	for _, bufSize := range []int64{256 * 1024, 1024 * 1024} {
		runtime.GC()
		runtime.ReadMemStats(&before)

		ss := NewSpillSorter(benchComparison(), benchFields(), bufSize, "")
		for i := range n {
			if err := ss.Add(benchRow(int64(i))); err != nil {
				t.Fatal(err)
			}
		}

		// Measure heap after all adds — most data is on disk, only last partial run in buffer
		runtime.GC()
		runtime.ReadMemStats(&peak)
		spillHeap := peak.HeapAlloc - before.HeapAlloc

		err := ss.Finish(context.Background(), func(row sqltypes.Row) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		ss.Close()

		t.Logf("Spill sort (%4dKB buf) peak:   %.2f MB  (%.1fx less)", bufSize/1024, float64(spillHeap)/(1024*1024), float64(inMemHeap)/float64(spillHeap))
	}
}

// BenchmarkMerge_Heap benchmarks the evalengine.Merger (binary heap) for K-way merging.
// K=15 sources each with N/K sorted rows are merged to isolate merge overhead.
func BenchmarkMerge_Heap(b *testing.B) {
	k := FinalMergeWay // 15
	n := 200_000
	cmp := benchComparison()

	// Build K sorted sources
	sources := makeSortedSources(k, n)

	for b.Loop() {
		merger := &evalengine.Merger{Compare: cmp}
		pos := make([]int, k)
		for i := range k {
			merger.Push(intRow(sources[i][0]), i)
			pos[i] = 1
		}
		merger.Init()

		for merger.Len() > 0 {
			_, source := merger.Pop()
			if pos[source] < len(sources[source]) {
				merger.Push(intRow(sources[source][pos[source]]), source)
				pos[source]++
			}
		}
	}
}

// BenchmarkMerge_LoserTree benchmarks the loserTree for K-way merging.
// Same setup as BenchmarkMerge_Heap for direct comparison.
func BenchmarkMerge_LoserTree(b *testing.B) {
	k := FinalMergeWay // 15
	n := 200_000
	cmp := benchComparison()

	// Build K sorted sources
	sources := makeSortedSources(k, n)

	for b.Loop() {
		entries := make([]mergeEntry, k)
		pos := make([]int, k)
		for i := range k {
			entries[i] = mergeEntry{row: intRow(sources[i][0]), source: i}
			pos[i] = 1
		}
		tree := newLoserTree(entries, cmp.Less)

		for tree.Len() > 0 {
			_, source := tree.Winner()
			if pos[source] < len(sources[source]) {
				tree.Replace(intRow(sources[source][pos[source]]))
				pos[source]++
			} else {
				tree.Remove()
			}
		}
	}
}

// makeSortedSources creates k sorted int64 slices with interleaved values
// totaling n elements across all sources.
func makeSortedSources(k, n int) [][]int64 {
	perSource := n / k
	sources := make([][]int64, k)
	for i := range k {
		sources[i] = make([]int64, perSource)
		for j := range perSource {
			sources[i][j] = int64(j*k + i)
		}
	}
	return sources
}

func BenchmarkSpillSort_ManySpills(b *testing.B) {
	fields := benchFields()
	cmp := benchComparison()
	n := 1000

	rows := make([]sqltypes.Row, n)
	perm := rand.Perm(n)
	for i, p := range perm {
		rows[i] = benchRow(int64(p))
	}

	// Tiny buffer to force many spills
	bufSize := int64(256)

	for b.Loop() {
		ss := NewSpillSorter(cmp, fields, bufSize, "")
		for _, row := range rows {
			if err := ss.Add(row); err != nil {
				b.Fatal(err)
			}
		}
		err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		ss.Close()
	}
}
