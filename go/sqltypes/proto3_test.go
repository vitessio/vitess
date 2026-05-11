/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestResult(t *testing.T) {
	fields := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	sqlResult := &Result{
		Fields:          fields,
		InsertID:        1,
		InsertIDChanged: true,
		RowsAffected:    2,
		Rows: [][]Value{{
			TestValue(VarChar, "aa"),
			TestValue(Int64, "1"),
			TestValue(Float64, "2"),
		}, {
			MakeTrusted(VarChar, []byte("bb")),
			NULL,
			NULL,
		}},
	}
	p3Result := &querypb.QueryResult{
		Fields:          fields,
		InsertId:        1,
		InsertIdChanged: true,
		RowsAffected:    2,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("aa12"),
		}, {
			Lengths: []int64{2, -1, -1},
			Values:  []byte("bb"),
		}},
	}
	p3converted := ResultToProto3(sqlResult)
	assert.True(t, proto.Equal(p3converted, p3Result), "P3:\n%v, want\n%v", p3converted, p3Result)

	reverse := Proto3ToResult(p3Result)
	assert.True(t, reverse.Equal(sqlResult), "reverse:\n%#v, want\n%#v", reverse, sqlResult)

	// Test custom fields.
	fields[1].Type = VarBinary
	sqlResult.Rows[0][1] = TestValue(VarBinary, "1")
	reverse = CustomProto3ToResult(fields, p3Result)
	assert.True(t, reverse.Equal(sqlResult), "reverse:\n%#v, want\n%#v", reverse, sqlResult)
}

func TestResults(t *testing.T) {
	fields1 := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	fields2 := []*querypb.Field{{
		Name: "col11",
		Type: VarChar,
	}, {
		Name: "col12",
		Type: Int64,
	}, {
		Name: "col13",
		Type: Float64,
	}}
	sqlResults := []*Result{{
		Fields:          fields1,
		InsertID:        1,
		InsertIDChanged: true,
		RowsAffected:    2,
		Rows: [][]Value{{
			TestValue(VarChar, "aa"),
			TestValue(Int64, "1"),
			TestValue(Float64, "2"),
		}},
	}, {
		Fields:          fields2,
		InsertID:        3,
		InsertIDChanged: true,
		RowsAffected:    4,
		Rows: [][]Value{{
			TestValue(VarChar, "bb"),
			TestValue(Int64, "3"),
			TestValue(Float64, "4"),
		}},
	}}
	p3Results := []*querypb.QueryResult{{
		Fields:          fields1,
		InsertId:        1,
		InsertIdChanged: true,
		RowsAffected:    2,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("aa12"),
		}},
	}, {
		Fields:          fields2,
		InsertId:        3,
		InsertIdChanged: true,
		RowsAffected:    4,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("bb34"),
		}},
	}}
	p3converted := ResultsToProto3(sqlResults)
	assert.True(t, Proto3ResultsEqual(p3converted, p3Results), "P3:\n%v, want\n%v", p3converted, p3Results)

	reverse := Proto3ToResults(p3Results)
	assert.True(t, ResultsEqual(reverse, sqlResults), "reverse:\n%#v, want\n%#v", reverse, sqlResults)
}

func TestQueryReponses(t *testing.T) {
	fields1 := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	fields2 := []*querypb.Field{{
		Name: "col11",
		Type: VarChar,
	}, {
		Name: "col12",
		Type: Int64,
	}, {
		Name: "col13",
		Type: Float64,
	}}

	queryResponses := []QueryResponse{
		{
			QueryResult: &Result{
				Fields:          fields1,
				InsertID:        1,
				InsertIDChanged: true,
				RowsAffected:    2,
				Rows: [][]Value{{
					TestValue(VarChar, "aa"),
					TestValue(Int64, "1"),
					TestValue(Float64, "2"),
				}},
			},
			QueryError: nil,
		}, {
			QueryResult: &Result{
				Fields:          fields2,
				InsertID:        3,
				InsertIDChanged: true,
				RowsAffected:    4,
				Rows: [][]Value{{
					TestValue(VarChar, "bb"),
					TestValue(Int64, "3"),
					TestValue(Float64, "4"),
				}},
			},
			QueryError: nil,
		}, {
			QueryResult: nil,
			QueryError:  vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "deadline exceeded"),
		},
	}

	p3ResultWithError := []*querypb.ResultWithError{
		{
			Error: nil,
			Result: &querypb.QueryResult{
				Fields:          fields1,
				InsertId:        1,
				InsertIdChanged: true,
				RowsAffected:    2,
				Rows: []*querypb.Row{{
					Lengths: []int64{2, 1, 1},
					Values:  []byte("aa12"),
				}},
			},
		}, {
			Error: nil,
			Result: &querypb.QueryResult{
				Fields:          fields2,
				InsertId:        3,
				InsertIdChanged: true,
				RowsAffected:    4,
				Rows: []*querypb.Row{{
					Lengths: []int64{2, 1, 1},
					Values:  []byte("bb34"),
				}},
			},
		}, {
			Error: &vtrpcpb.RPCError{
				Message: "deadline exceeded",
				Code:    vtrpcpb.Code_DEADLINE_EXCEEDED,
			},
			Result: nil,
		},
	}
	p3converted := QueryResponsesToProto3(queryResponses)
	assert.True(t, Proto3QueryResponsesEqual(p3converted, p3ResultWithError), "P3:\n%v, want\n%v", p3converted, p3ResultWithError)

	reverse := Proto3ToQueryReponses(p3ResultWithError)
	assert.True(t, QueryResponsesEqual(reverse, queryResponses), "reverse:\n%#v, want\n%#v", reverse, queryResponses)
}

func TestProto3ValuesEqual(t *testing.T) {
	for _, tc := range []struct {
		v1, v2   []*querypb.Value
		expected bool
	}{
		{
			v1: []*querypb.Value{
				{
					Type:  0,
					Value: []byte{0, 1},
				},
			},
			v2: []*querypb.Value{
				{
					Type:  0,
					Value: []byte{0, 1},
				},
				{
					Type:  1,
					Value: []byte{0, 1, 2},
				},
			},
			expected: false,
		},
		{
			v1: []*querypb.Value{
				{
					Type:  0,
					Value: []byte{0, 1},
				},
				{
					Type:  1,
					Value: []byte{0, 1, 2},
				},
			},
			v2: []*querypb.Value{
				{
					Type:  0,
					Value: []byte{0, 1},
				},
				{
					Type:  1,
					Value: []byte{0, 1, 2},
				},
			},
			expected: true,
		},
		{
			v1: []*querypb.Value{
				{
					Type:  0,
					Value: []byte{0, 1},
				},
				{
					Type:  1,
					Value: []byte{0, 1},
				},
			},
			v2: []*querypb.Value{
				{
					Type:  0,
					Value: []byte{0, 1},
				},
				{
					Type:  1,
					Value: []byte{0, 1, 2},
				},
			},
			expected: false,
		},
	} {
		require.Equal(t, tc.expected, Proto3ValuesEqual(tc.v1, tc.v2))
	}
}

func TestResultToProto3_CachedRows(t *testing.T) {
	fields := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}}
	result := &Result{
		Fields:       fields,
		RowsAffected: 2,
		Rows: [][]Value{{
			TestValue(VarChar, "aa"),
			TestValue(Int64, "1"),
		}, {
			TestValue(VarChar, "bb"),
			TestValue(Int64, "2"),
		}},
	}

	uncached := ResultToProto3(result)
	require.Len(t, uncached.Rows, 2)

	result.CacheProto3Rows()

	cached := ResultToProto3(result)
	require.True(t, proto.Equal(uncached, cached), "cached and uncached proto3 results differ")

	require.Same(t, cached.Rows[0], result.proto3Rows[0])
	require.Same(t, cached.Rows[1], result.proto3Rows[1])
}

func TestResultToProto3_NilAndEmptyCache(t *testing.T) {
	require.Nil(t, ResultToProto3(nil))
	var nilResult *Result
	nilResult.CacheProto3Rows() // does not panic

	result := &Result{
		Fields: []*querypb.Field{{Name: "col1", Type: VarChar}},
	}
	result.CacheProto3Rows()
	require.Nil(t, result.proto3Rows)

	p3 := ResultToProto3(result)
	require.NotNil(t, p3)
	require.Empty(t, p3.Rows)
}

func TestCopy_DoesNotPropagateProto3RowCache(t *testing.T) {
	result := &Result{
		Fields: []*querypb.Field{{Name: "col1", Type: Int64}},
		Rows: [][]Value{{
			TestValue(Int64, "42"),
		}},
	}
	result.CacheProto3Rows()
	require.NotNil(t, result.proto3Rows)

	require.Nil(t, result.ShallowCopy().proto3Rows)
	require.Nil(t, result.Copy().proto3Rows)
}

func TestMutations_InvalidateCachedProto3Rows(t *testing.T) {
	resultWithProto3FieldPopulated := func(t *testing.T) *Result {
		t.Helper()
		result := &Result{
			Fields: []*querypb.Field{{Name: "col1", Type: VarChar}},
			Rows: [][]Value{{
				TestValue(VarChar, "hello"),
			}},
		}
		result.CacheProto3Rows()
		require.NotNil(t, result.proto3Rows)
		return result
	}

	t.Run("AppendResult", func(t *testing.T) {
		result := resultWithProto3FieldPopulated(t)
		result.AppendResult(&Result{
			Rows: [][]Value{{
				TestValue(VarChar, "world"),
			}},
		})
		require.Nil(t, result.proto3Rows, "AppendResult must invalidate proto3Rows cache")

		p3 := ResultToProto3(result)
		require.Len(t, p3.Rows, 2)
	})

	t.Run("Repair", func(t *testing.T) {
		result := resultWithProto3FieldPopulated(t)
		result.Repair([]*querypb.Field{{Name: "col1", Type: VarBinary}})
		require.Nil(t, result.proto3Rows, "Repair must invalidate proto3Rows cache")

		p3 := ResultToProto3(result)
		require.Len(t, p3.Rows, 1)
	})
}

// makeTestResult builds a Result with numRows rows, each containing 5 columns
// of mixed types. Row values are deterministic so benchmarks are reproducible.
func makeTestResult(numRows int) *Result {
	fields := MakeTestFields(
		"col1|col2|col3|col4|col5",
		"int64|varchar|varchar|float64|int64",
	)
	rows := make([][]Value, numRows)
	for i := range rows {
		rows[i] = []Value{
			TestValue(Int64, strconv.Itoa(i)),
			TestValue(VarChar, fmt.Sprintf("val-%06d", i)),
			TestValue(VarChar, fmt.Sprintf("val-%06d-longervalue", i)),
			TestValue(Float64, fmt.Sprintf("%d.%02d", i/100, i%100)),
			TestValue(Int64, strconv.Itoa(i%2)),
		}
	}
	return &Result{
		Fields:       fields,
		RowsAffected: uint64(numRows),
		Rows:         rows,
	}
}

// BenchmarkResultToProto3 measures per-call allocation cost of ResultToProto3
// with and without CacheProto3Rows, across different result sizes.
func BenchmarkResultToProto3(b *testing.B) {
	for _, numRows := range []int{100, 1000, 10000} {
		result := makeTestResult(numRows)

		b.Run(fmt.Sprintf("rows=%d/uncached", numRows), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ResultToProto3(result)
			}
		})

		b.Run(fmt.Sprintf("rows=%d/cached", numRows), func(b *testing.B) {
			result.CacheProto3Rows()
			b.ReportAllocs()
			for b.Loop() {
				ResultToProto3(result)
			}
		})
	}
}

// BenchmarkConsolidationFanOut simulates the consolidator scenario: one shared
// Result is read by N concurrent goroutines calling ResultToProto3. Reports
// total heap bytes allocated across all waiters.
func BenchmarkConsolidationFanOut(b *testing.B) {
	const numRows = 10000

	for _, waiters := range []int{1, 10, 50} {
		result := makeTestResult(numRows)

		b.Run(fmt.Sprintf("waiters=%d/uncached", waiters), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var wg sync.WaitGroup
				wg.Add(waiters)
				for range waiters {
					go func() {
						defer wg.Done()
						ResultToProto3(result)
					}()
				}
				wg.Wait()
			}
		})

		b.Run(fmt.Sprintf("waiters=%d/cached", waiters), func(b *testing.B) {
			result.CacheProto3Rows()
			b.ReportAllocs()
			for b.Loop() {
				var wg sync.WaitGroup
				wg.Add(waiters)
				for range waiters {
					go func() {
						defer wg.Done()
						ResultToProto3(result)
					}()
				}
				wg.Wait()
			}
		})

		// Report aggregate heap delta for a single iteration so the savings
		// are visible in absolute terms, not just per-op.
		b.Run(fmt.Sprintf("waiters=%d/heap-delta", waiters), func(b *testing.B) {
			for _, cached := range []bool{false, true} {
				label := "uncached"
				r := makeTestResult(numRows)
				if cached {
					label = "cached"
					r.CacheProto3Rows()
				}
				b.Run(label, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						runtime.GC()
						var before runtime.MemStats
						runtime.ReadMemStats(&before)

						var wg sync.WaitGroup
						wg.Add(waiters)
						for range waiters {
							go func() {
								defer wg.Done()
								ResultToProto3(r)
							}()
						}
						wg.Wait()

						var after runtime.MemStats
						runtime.ReadMemStats(&after)
						b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "heap-bytes/op")
					}
				})
			}
		})
	}
}
