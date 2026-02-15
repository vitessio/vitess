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
	"text/tabwriter"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// crossoverRow creates a row with an INT64 sort key and a VARCHAR payload.
// The encoded size is approximately payloadSize + 10 bytes of framing overhead.
func crossoverRow(id int64, payloadSize int) sqltypes.Row {
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + (id+int64(i))%26)
	}
	return sqltypes.Row{
		sqltypes.NewInt64(id),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, payload),
	}
}

func generateCrossoverRows(n, payloadSize int) []sqltypes.Row {
	rows := make([]sqltypes.Row, n)
	perm := rand.Perm(n)
	for i, p := range perm {
		rows[i] = crossoverRow(int64(p), payloadSize)
	}
	return rows
}

// Fixed buffer sizes to test — these are the values someone would actually configure.
var bufferSizes = []int64{
	256 * 1024,      // 256KB (vtgate default)
	512 * 1024,      // 512KB
	1024 * 1024,     // 1MB
	2 * 1024 * 1024, // 2MB
	4 * 1024 * 1024, // 4MB
}

// BenchmarkCrossover measures in-memory sort vs SpillSorter across varying
// row sizes and fixed buffer sizes. Use this to find the crossover point
// where spill sort becomes faster than in-memory sort.
//
// Run:
//
//	go test -bench=BenchmarkCrossover -benchtime=3s -timeout=30m ./go/vt/vtgate/engine/sortio/
//
// Filter to a specific row size:
//
//	go test -bench='BenchmarkCrossover/row47B' -benchtime=3s ./go/vt/vtgate/engine/sortio/
func BenchmarkCrossover(b *testing.B) {
	type scenario struct {
		payloadSize int
		totalMB     int
	}

	scenarios := []scenario{
		{40, 10},  // ~50B encoded, 10MB total (~200K rows)
		{40, 50},  // ~50B encoded, 50MB total (~1M rows)
		{40, 200}, // ~50B encoded, 200MB total (~4.4M rows)
		{190, 10}, // ~200B encoded, 10MB total (~50K rows)
		{190, 50}, // ~200B encoded, 50MB total (~250K rows)
		{990, 10}, // ~1KB encoded, 10MB total (~10K rows)
		{990, 50}, // ~1KB encoded, 50MB total (~50K rows)
	}

	for _, sc := range scenarios {
		fields := benchFields()
		cmp := benchComparison()
		codec := NewRowCodec(fields)

		sampleRow := crossoverRow(0, sc.payloadSize)
		encodedSize := codec.EncodedSize(sampleRow)
		nRows := (sc.totalMB * 1024 * 1024) / encodedSize
		totalBytes := int64(nRows * encodedSize)

		source := generateCrossoverRows(nRows, sc.payloadSize)
		prefix := fmt.Sprintf("row%dB/data%dMB", encodedSize, sc.totalMB)

		// In-memory baseline: copy + sort with tiny weights (same as MemorySort.TryExecute)
		b.Run(prefix+"/inmem", func(b *testing.B) {
			b.SetBytes(totalBytes)
			for b.Loop() {
				rows := make([]sqltypes.Row, len(source))
				copy(rows, source)
				result := &sqltypes.Result{Fields: fields, Rows: rows}
				if err := cmp.SortResult(result); err != nil {
					b.Fatal(err)
				}
			}
		})

		// SpillSorter at each fixed buffer size
		for _, bufSize := range bufferSizes {
			nRuns := max(totalBytes/bufSize, 1)
			b.Run(fmt.Sprintf("%s/spill_buf%s_%druns", prefix, humanBytes(bufSize), nRuns), func(b *testing.B) {
				b.SetBytes(totalBytes)
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
}

// TestCrossoverTable runs a quick timed comparison and prints a formatted
// table showing the speed ratio of spill sort vs in-memory sort at fixed
// buffer sizes across various row sizes and data volumes. Both paths use
// pre-allocated rows, so GC pressure is identical — this isolates the pure
// sorting mechanism overhead.
//
// A ratio > 1.0 means spill sort is slower; < 1.0 means spill sort wins.
//
// Heap columns show live heap measured after data ingestion (GC'd).
// For the pre-allocated test, values are shared with the source slice,
// so InMemHeap reflects the copied Row slice headers only.
//
// Run:
//
//	go test -run=TestCrossoverTable -v -count=1 -timeout=15m ./go/vt/vtgate/engine/sortio/
func TestCrossoverTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crossover analysis in short mode")
	}

	type scenario struct {
		payloadSize int
		totalMB     int
	}

	scenarios := []scenario{
		{40, 10},
		{40, 50},
		{40, 200},
		{190, 10},
		{190, 50},
		{990, 10},
		{990, 50},
	}

	for _, sc := range scenarios {
		fields := benchFields()
		cmp := benchComparison()
		codec := NewRowCodec(fields)

		sampleRow := crossoverRow(0, sc.payloadSize)
		encodedSize := codec.EncodedSize(sampleRow)
		nRows := (sc.totalMB * 1024 * 1024) / encodedSize
		totalBytes := int64(nRows * encodedSize)

		source := generateCrossoverRows(nRows, sc.payloadSize)

		inmemTime := measureOp(3, func() {
			rows := make([]sqltypes.Row, len(source))
			copy(rows, source)
			result := &sqltypes.Result{Fields: fields, Rows: rows}
			cmp.SortResult(result)
		})

		inmemHeap := measureHeap(func() any {
			rows := make([]sqltypes.Row, len(source))
			copy(rows, source)
			result := &sqltypes.Result{Fields: fields, Rows: rows}
			cmp.SortResult(result)
			return result
		})

		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\n=== Row: %dB encoded | Data: %dMB | Rows: %d | InMem heap: %s ===\n",
			encodedSize, sc.totalMB, nRows, humanBytes(inmemHeap))

		w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
		fmt.Fprintf(w, "BufSize\tRows/Buf\tRuns\tInMemory\tSpill\tRatio\tSpillHeap\t\n")
		fmt.Fprintf(w, "-------\t--------\t----\t--------\t-----\t-----\t---------\t\n")

		for _, bufSize := range bufferSizes {
			nRuns := max(totalBytes/bufSize, 1)
			rowsPerBuf := bufSize / int64(encodedSize)

			spillTime := measureOp(3, func() {
				ss := NewSpillSorter(cmp, fields, bufSize, "")
				for _, row := range source {
					if err := ss.Add(row); err != nil {
						t.Fatal(err)
					}
				}
				err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				ss.Close()
			})

			var ss *SpillSorter
			spillHeap := measureHeap(func() any {
				ss = NewSpillSorter(cmp, fields, bufSize, "")
				for _, row := range source {
					if err := ss.Add(row); err != nil {
						t.Fatal(err)
					}
				}
				return ss
			})
			ss.Close()

			ratio := float64(spillTime) / float64(inmemTime)
			marker := ""
			if ratio <= 1.0 {
				marker = " <-- spill wins"
			}

			fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\t%.2fx\t%s%s\t\n",
				humanBytes(bufSize), rowsPerBuf, nRuns,
				inmemTime.Round(time.Millisecond),
				spillTime.Round(time.Millisecond),
				ratio, humanBytes(spillHeap), marker)
		}
		w.Flush()
		t.Log(buf.String())
	}
}

// TestStreamingCrossoverTable simulates the production streaming scenario.
// In-memory sort must generate and hold ALL rows before sorting.
// Spill sort receives rows one at a time; after spilling, earlier rows can be GC'd.
// This reveals the GC pressure crossover where bounded memory wins.
//
// Heap columns here are more meaningful than in TestCrossoverTable because
// there is no shared source slice — the in-memory path holds all rows while
// the spill path only holds a buffer's worth.
//
// Run:
//
//	go test -run=TestStreamingCrossoverTable -v -count=1 -timeout=15m ./go/vt/vtgate/engine/sortio/
func TestStreamingCrossoverTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping streaming crossover analysis in short mode")
	}

	type scenario struct {
		payloadSize int
		totalMB     int
	}

	scenarios := []scenario{
		{40, 10},
		{40, 50},
		{40, 200},
		{190, 10},
		{190, 50},
		{190, 200},
	}

	for _, sc := range scenarios {
		fields := benchFields()
		cmp := benchComparison()
		codec := NewRowCodec(fields)

		sampleRow := crossoverRow(0, sc.payloadSize)
		encodedSize := codec.EncodedSize(sampleRow)
		nRows := (sc.totalMB * 1024 * 1024) / encodedSize
		totalBytes := int64(nRows * encodedSize)

		// Only pre-generate the permutation, not the rows themselves.
		perm := rand.Perm(nRows)

		// In-memory: generate all rows into a slice, then sort.
		inmemTime := measureOp(3, func() {
			rows := make([]sqltypes.Row, nRows)
			for i, p := range perm {
				rows[i] = crossoverRow(int64(p), sc.payloadSize)
			}
			result := &sqltypes.Result{Fields: fields, Rows: rows}
			cmp.SortResult(result)
		})

		inmemHeap := measureHeap(func() any {
			rows := make([]sqltypes.Row, nRows)
			for i, p := range perm {
				rows[i] = crossoverRow(int64(p), sc.payloadSize)
			}
			result := &sqltypes.Result{Fields: fields, Rows: rows}
			cmp.SortResult(result)
			return result
		})

		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\n=== STREAMING: Row: %dB | Data: %dMB | Rows: %d | InMem heap: %s ===\n",
			encodedSize, sc.totalMB, nRows, humanBytes(inmemHeap))

		w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
		fmt.Fprintf(w, "BufSize\tRows/Buf\tRuns\tInMemory\tSpill\tRatio\tSpillHeap\t\n")
		fmt.Fprintf(w, "-------\t--------\t----\t--------\t-----\t-----\t---------\t\n")

		for _, bufSize := range bufferSizes {
			nRuns := max(totalBytes/bufSize, 1)
			rowsPerBuf := bufSize / int64(encodedSize)

			spillTime := measureOp(3, func() {
				ss := NewSpillSorter(cmp, fields, bufSize, "")
				for _, p := range perm {
					if err := ss.Add(crossoverRow(int64(p), sc.payloadSize)); err != nil {
						t.Fatal(err)
					}
				}
				err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				ss.Close()
			})

			var ss *SpillSorter
			spillHeap := measureHeap(func() any {
				ss = NewSpillSorter(cmp, fields, bufSize, "")
				for _, p := range perm {
					if err := ss.Add(crossoverRow(int64(p), sc.payloadSize)); err != nil {
						t.Fatal(err)
					}
				}
				return ss
			})
			ss.Close()

			ratio := float64(spillTime) / float64(inmemTime)
			marker := ""
			if ratio <= 1.0 {
				marker = " <-- spill wins"
			}

			fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\t%.2fx\t%s%s\t\n",
				humanBytes(bufSize), rowsPerBuf, nRuns,
				inmemTime.Round(time.Millisecond),
				spillTime.Round(time.Millisecond),
				ratio, humanBytes(spillHeap), marker)
		}
		w.Flush()
		t.Log(buf.String())
	}
}

// measureOp returns the minimum duration across n runs of op.
// A warmup iteration and GC run first to reduce noise.
func measureOp(n int, op func()) time.Duration {
	op() // warmup
	runtime.GC()

	var best time.Duration
	for range n {
		start := time.Now()
		op()
		d := time.Since(start)
		if best == 0 || d < best {
			best = d
		}
	}
	return best
}

// measureHeap runs op() and returns the live heap delta.
// The returned value from op is kept alive during measurement so the GC
// doesn't reclaim the data we're trying to measure.
func measureHeap(op func() any) int64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	result := op()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	runtime.KeepAlive(result)

	if after.HeapAlloc > before.HeapAlloc {
		return int64(after.HeapAlloc - before.HeapAlloc)
	}
	return 0
}

func humanBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.0fKB", float64(b)/1024)
	default:
		return fmt.Sprintf("%dB", b)
	}
}
