/*
Copyright 2017 Google Inc.

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

package worker

import (
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// singlePk presents a table with a primary key that is a single integer column.
var singlePk = []*querypb.Field{{Name: "id", Type: sqltypes.Int32}, {Name: "msg", Type: sqltypes.Char}}

// multiPk presents a table with a primary key with multiple (two) integer columns.
var multiPk = []*querypb.Field{{Name: "id", Type: sqltypes.Int32}, {Name: "sub_id", Type: sqltypes.Int32}, {Name: "msg", Type: sqltypes.Char}}

// fakeResultReader returns generated Result structs.
// It implements the ResultReader interface.
type fakeResultReader struct {
	fields    []*querypb.Field
	createRow rowFactory
	// rangeInterval is the interval between two range starts.
	rangeInterval int
	// rangeLength is the length of *our* part within the range. It's usually shorter than rangeInterval.
	rangeLength   int
	rowsPerResult int
	rowsTotal     int

	// nextRangeStart is the start of the next range. It points at the start of
	// *our* part of the range i.e. different fakeResultReader have different
	// range starts.
	nextRangeStart int
	// currentIndex is the current index within the current range.
	currentIndex int
	rowsReturned int
	closed       bool
}

// newFakeResultReader returns a new FakeResultReader.
//
// "distribution" specifies how many consecutive rows each input should own.
// For example, [10, 1] means that the first input has the rows (more
// precisely primary keys) from 1 to 10, second input has 11, first input has
// 12 to 21, second input has 22 and so on.
//
// "index" specifies the index in "distribution" i.e. which input we are.
//
// "iterations" specifies how often the distribution should be repeated e.g.
// a [1, 1] distribution and 10 iterations means that each input will return
// 1*10 rows.
func newFakeResultReader(fields []*querypb.Field, index int, distribution []int, iterations int) *fakeResultReader {
	createRow := createRowSinglePk
	if reflect.DeepEqual(fields, multiPk) {
		createRow = createRowMultiPk
	}

	// Compute our start within the range.
	rangeStart := 0
	for i, rangeLength := range distribution {
		if index == i {
			break
		}
		rangeStart += rangeLength
	}

	// Compute the total length of the range across all inputs.
	rangeInterval := 0
	for _, rangeLength := range distribution {
		rangeInterval += rangeLength
	}

	rowsTotal := iterations * distribution[index]
	rowsPerResult := ResultSizeRows
	if rowsPerResult > rowsTotal {
		rowsPerResult = rowsTotal
	}

	return &fakeResultReader{
		fields:         fields,
		createRow:      createRow,
		rangeInterval:  rangeInterval,
		rangeLength:    distribution[index],
		rowsPerResult:  rowsPerResult,
		rowsTotal:      rowsTotal,
		nextRangeStart: rangeStart,
	}
}

// Fields returns the field information. It is part of the ResultReader interface.
func (f *fakeResultReader) Fields() []*querypb.Field {
	return f.fields
}

// Close closes nothing
func (f *fakeResultReader) Close(ctx context.Context) {
	f.closed = true
}

// Next returns the next fake result. It is part of the ResultReader interface.
func (f *fakeResultReader) Next() (*sqltypes.Result, error) {
	if f.rowsReturned == f.rowsTotal {
		return nil, io.EOF
	}

	var rows [][]sqltypes.Value
	rowsCount := 0
	for rowsCount < f.rowsPerResult {
		if f.rowsReturned == f.rowsTotal {
			break
		}

		// We covered our part of the range. Move to the start of the next range.
		if f.currentIndex == f.rangeLength {
			f.currentIndex = 0
			f.nextRangeStart += f.rangeInterval
		}

		id := f.nextRangeStart + f.currentIndex
		rows = append(rows, f.createRow(id))

		rowsCount++
		f.currentIndex++
		f.rowsReturned++
	}

	return &sqltypes.Result{
		Fields:       f.fields,
		RowsAffected: uint64(rowsCount),
		Rows:         rows,
	}, nil
}

type rowFactory func(id int) []sqltypes.Value

func createRowSinglePk(id int) []sqltypes.Value {
	idValue := sqltypes.NewInt64(int64(id))
	return []sqltypes.Value{
		idValue,
		sqltypes.NewVarBinary(fmt.Sprintf("msg %d", id)),
	}
}

func createRowMultiPk(id int) []sqltypes.Value {
	// Map id from the one dimensional space to a two-dimensional space.
	// Examples: 0, 1, 2, 3, 4 => (0, 0), (0, 1), (1, 0), (1, 1), (2, 0)
	newID := id / 2
	subID := id % 2
	return []sqltypes.Value{
		sqltypes.NewInt64(int64(newID)),
		sqltypes.NewInt64(int64(subID)),
		sqltypes.NewVarBinary(fmt.Sprintf("msg %d", id)),
	}
}

// mergedResults returns Result(s) with rows in the range [0, rowsTotal).
func mergedResults(fields []*querypb.Field, rowsTotal, rowsPerResult int) []*sqltypes.Result {
	createRow := createRowSinglePk
	if reflect.DeepEqual(fields, multiPk) {
		createRow = createRowMultiPk
	}
	if rowsPerResult > rowsTotal {
		rowsPerResult = rowsTotal
	}
	var results []*sqltypes.Result
	var rows [][]sqltypes.Value
	for id := 0; id < rowsTotal; id++ {
		rows = append(rows, createRow(id))

		// Last row in the Result or last row in total.
		if id%rowsPerResult == (rowsPerResult-1) || id == (rowsTotal-1) {
			results = append(results, &sqltypes.Result{
				Fields:       fields,
				RowsAffected: uint64(len(rows)),
				Rows:         rows,
			})
			rows = make([][]sqltypes.Value, 0)
		}
	}
	return results
}

// TestResultMerger tests the ResultMerger component by using multiple
// FakeResultReader as input and comparing the output.
// Rows in the test are generated with a single or multi-column primary key.
// The primary key column(s) have an integer type and start at 0.
func TestResultMerger(t *testing.T) {
	testcases := []struct {
		desc    string
		inputs  []ResultReader
		want    []*sqltypes.Result
		multiPk bool
	}{
		{
			desc: "2 inputs, 2 rows, even distribution",
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{1, 1}, 1),
				newFakeResultReader(singlePk, 1, []int{1, 1}, 1),
			},
			want: mergedResults(singlePk, 2, ResultSizeRows),
		},
		{
			desc: "2 inputs, enough rows such that ResultMerger must chunk its output into 4 results",
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{1, 1}, 2000),
				newFakeResultReader(singlePk, 1, []int{1, 1}, 2000),
			},
			want: mergedResults(singlePk, 4000, ResultSizeRows),
		},
		{
			desc: "2 inputs, last Result returned by ResultMerger is partial i.e. it has less than 'ResultSizeRows' rows",
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{ResultSizeRows + 1, ResultSizeRows}, 1),
				newFakeResultReader(singlePk, 1, []int{ResultSizeRows + 1, ResultSizeRows}, 1),
			},
			want: mergedResults(singlePk, 2*ResultSizeRows+1, ResultSizeRows),
		},
		{
			desc: "2 inputs, 11 rows, 10:1 distribution",
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{10, 1}, 1),
				newFakeResultReader(singlePk, 1, []int{10, 1}, 1),
			},
			want: mergedResults(singlePk, 11, ResultSizeRows),
		},
		{
			desc: "2 inputs, 11 rows, 1:10 distribution",
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{1, 10}, 1),
				newFakeResultReader(singlePk, 1, []int{1, 10}, 1),
			},
			want: mergedResults(singlePk, 11, ResultSizeRows),
		},
		{
			desc: "3 inputs, 3 rows, even distribution",
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{1, 1, 1}, 1),
				newFakeResultReader(singlePk, 1, []int{1, 1, 1}, 1),
				newFakeResultReader(singlePk, 2, []int{1, 1, 1}, 1),
			},
			want: mergedResults(singlePk, 3, ResultSizeRows),
		},
		{
			desc: "2 inputs, 4 rows, multi-column primary key",
			inputs: []ResultReader{
				// Note: The order of the inputs is reversed on purpose to verify that
				// the sort also considers the "sub_id" column.
				newFakeResultReader(multiPk, 1, []int{1, 1}, 2),
				newFakeResultReader(multiPk, 0, []int{1, 1}, 2),
			},
			want:    mergedResults(multiPk, 4, ResultSizeRows),
			multiPk: true,
		},
		{
			desc: "2 inputs, 4 rows including 1 duplicate row",
			// NOTE: In practice, this case should not happen because the inputs should be disjoint.
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{2, 1}, 1),
				newFakeResultReader(singlePk, 1, []int{1, 2}, 1),
			},
			want: func() []*sqltypes.Result {
				// Rows: 0, 1, 2
				results := mergedResults(singlePk, 3, ResultSizeRows)
				// Duplicate Row 1. New Rows: 0, 1, 1, 2
				results[0].Rows = append(results[0].Rows[:2], results[0].Rows[1:]...)
				results[0].RowsAffected = 4
				return results
			}(),
		},
		{
			desc: "2 inputs, 2 rows, row with id 1 is 'missing' i.e. a gap",
			inputs: []ResultReader{
				newFakeResultReader(singlePk, 0, []int{1, 1, 1}, 1),
				newFakeResultReader(singlePk, 2, []int{1, 1, 1}, 1),
			},
			want: func() []*sqltypes.Result {
				// Rows: 0, 1, 2
				results := mergedResults(singlePk, 3, ResultSizeRows)
				// Remove row 1. New Rows: 0, 2
				results[0].Rows = append(results[0].Rows[:1], results[0].Rows[2:]...)
				results[0].RowsAffected = 2
				return results
			}(),
		},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("checking testcase: %v", tc.desc), func(inner *testing.T) {
			pkFieldCount := 1
			if tc.multiPk {
				pkFieldCount = 2
			}
			rm, err := NewResultMerger(tc.inputs, pkFieldCount)
			if err != nil {
				inner.Fatal(err)
			}

			// Consume all merged Results.
			var got []*sqltypes.Result
			for {
				result, err := rm.Next()
				if err != nil {
					if err == io.EOF {
						break
					} else {
						inner.Fatal(err)
					}
				}
				got = append(got, result)
			}

			rm.Close(context.Background())

			if !reflect.DeepEqual(got, tc.want) {
				for i := range got {
					if i == len(tc.want) {
						// got has more Results than want. Avoid index out of range errors.
						break
					}
					if got[i].RowsAffected != tc.want[i].RowsAffected {
						inner.Logf("deviating RowsAffected value for Result at index: %v got = %v, want = %v", i, got[i].RowsAffected, tc.want[i].RowsAffected)
					}
					inner.Logf("deviating Rows for Result at index: %v got = %v, want = %v", i, got[i].Rows, tc.want[i].Rows)
				}
				if len(tc.want)-len(got) > 0 {
					for i := len(got); i < len(tc.want); i++ {
						inner.Logf("missing Result in got: %v", tc.want[i].Rows)
					}
				}
				if len(got)-len(tc.want) > 0 {
					for i := len(tc.want); i < len(got); i++ {
						inner.Logf("unnecessary extra Result in got: %v", got[i].Rows)
					}
				}
				inner.Fatalf("ResultMerger testcase '%v' failed. See output above for different rows.", tc.desc)
			}

			for _, x := range tc.inputs {
				fake := x.(*fakeResultReader)
				if !fake.closed {
					inner.Fatal("expected inputs to be closed by now")
				}
			}
		})
	}
}

// memoryResultReader is a ResultReader implementation which fully consumes
// the stream of another ResultReader and stores it in memory.
// It is used in benchmarks to exclude the CPU time and memory allocations
// of e.g. the fakeResultReader implementation.
type memoryResultReader struct {
	fields       []*querypb.Field
	results      []*sqltypes.Result
	currentIndex int
}

func newMemoryResultReader(input ResultReader) *memoryResultReader {
	m := &memoryResultReader{
		fields: input.Fields(),
	}
	for {
		result, err := input.Next()
		if err != nil {
			if err == io.EOF {
				return m
			}
			panic(err)
		}
		m.results = append(m.results, result)
	}
}

func (m *memoryResultReader) Fields() []*querypb.Field {
	return m.fields
}

func (m *memoryResultReader) Next() (*sqltypes.Result, error) {
	if m.currentIndex == len(m.results) {
		return nil, io.EOF
	}
	result := m.results[m.currentIndex]
	m.currentIndex++
	return result, nil
}

func (m *memoryResultReader) Close(ctx context.Context) {
	// intentionally blank. we have nothing we need to close
}

// benchmarkResult is a package level variable whose sole purpose is to
// reference output from the Benchmark* functions below.
// This was suggested by http://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go
// to avoid that a compiler optimization eliminates our function call away.
var benchmarkResult *sqltypes.Result

func BenchmarkResultMerger_2Inputs_1to1(b *testing.B) {
	benchmarkResultMerger(b, []int{1, 1})
}

func BenchmarkResultMerger_2Inputs_1000to1000(b *testing.B) {
	benchmarkResultMerger(b, []int{1000, 1000})
}

func BenchmarkResultMerger_4Inputs_1to1(b *testing.B) {
	benchmarkResultMerger(b, []int{1, 1, 1, 1})
}

func BenchmarkResultMerger_4Inputs_1000to1000(b *testing.B) {
	benchmarkResultMerger(b, []int{1000, 1000, 1000, 1000})
}

func BenchmarkResultMerger_8Inputs_1to1(b *testing.B) {
	benchmarkResultMerger(b, []int{1, 1, 1, 1, 1, 1, 1, 1})
}

func BenchmarkResultMerger_8Inputs_1000to1000(b *testing.B) {
	benchmarkResultMerger(b, []int{1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000})
}

func benchmarkResultMerger(b *testing.B, distribution []int) {
	if b.N < len(distribution) {
		b.Logf("b.N = %8d: skipping benchmark because b.N is lower than the number of inputs: %v", b.N, len(distribution))
		return
	}

	start := time.Now()

	inputs := make([]ResultReader, len(distribution))
	actualRowCount := 0
	for i := range distribution {
		rowsPerInput := b.N / len(distribution)
		if rowsPerInput < distribution[i] {
			b.Logf("b.N = %8d: adjusting the number of rows per distribution from: %v to: %v", b.N, distribution[i], rowsPerInput)
			distribution[i] = rowsPerInput
		}
		iterations := b.N / distribution[i] / len(distribution)
		if iterations == 0 {
			b.Logf("b.N = %8d: skipping benchmark because b.N is lower than the number of rows per input: %v", b.N, distribution[i])
			return
		}
		actualRowCount += distribution[i] * iterations
		inputs[i] = newMemoryResultReader(
			newFakeResultReader(singlePk, i, distribution, iterations))
	}
	rm, err := NewResultMerger(inputs, 1 /* pkFieldCount */)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("b.N = %8d: Preloaded %v ResultReader with %10d rows total. Took %4.1f seconds.", b.N, len(distribution), actualRowCount, time.Since(start).Seconds())

	b.ResetTimer()

	// Merge all rows.
	var lastResult *sqltypes.Result
	for {
		result, err := rm.Next()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				b.Fatal(err)
			}
		}
		lastResult = result
	}
	benchmarkResult = lastResult
}
