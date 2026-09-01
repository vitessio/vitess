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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	resultRunsVCursor struct {
		*loggingVCursor

		result          *sqltypes.Result
		runs            []*sqltypes.Result
		errs            []error
		multiShardCalls int
		resultRunsCalls int
	}
)

func (vc *resultRunsVCursor) ExecuteMultiShard(
	context.Context,
	Primitive,
	[]*srvtopo.ResolvedShard,
	[]*querypb.BoundQuery,
	bool,
	bool,
	bool,
) (*sqltypes.Result, []error) {
	vc.multiShardCalls++
	return vc.result, vc.errs
}

func (vc *resultRunsVCursor) ExecuteMultiShardWithResultRuns(
	context.Context,
	Primitive,
	[]*srvtopo.ResolvedShard,
	[]*querypb.BoundQuery,
	bool,
	bool,
	bool,
) (*sqltypes.Result, []*sqltypes.Result, []error) {
	vc.resultRunsCalls++
	return vc.result, vc.runs, vc.errs
}

// TestRouteMergeResultRuns proves a marked route merges shard-sorted runs.
func TestRouteMergeResultRuns(t *testing.T) {
	runs := []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "0", "2", "4", "6", "8"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "3", "5", "7", "9"),
	}
	route := newResultRunsRoute()
	vc := &resultRunsVCursor{
		loggingVCursor: &loggingVCursor{shards: []string{"-80", "80-"}},
		result:         combineResultRuns(runs[1], runs[0]),
		runs:           runs,
	}

	got, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	require.Equal(t, 1, vc.resultRunsCalls)
	expectResult(t, got, sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
}

// TestRouteMergeResultRunsBoundaries proves the full-sort crossover and the
// cases that need no merge tree.
func TestRouteMergeResultRunsBoundaries(t *testing.T) {
	t.Run("small multi-run result", func(t *testing.T) {
		runs := []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "0", "2", "4", "6"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "3", "5", "7"),
		}
		route := newResultRunsRoute()
		flat := combineResultRuns(runs[1], runs[0])

		got, merged, err := route.mergeResultRuns(flat, runs)
		require.NoError(t, err)
		require.False(t, merged)
		require.Same(t, flat, got)

		vc := &resultRunsVCursor{
			loggingVCursor: &loggingVCursor{shards: []string{"-80", "80-"}},
			result:         flat,
			runs:           runs,
		}
		got, err = route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		expectResult(t, got, sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "0", "1", "2", "3", "4", "5", "6", "7"))
	})

	t.Run("large dominant run copies the remaining tail", func(t *testing.T) {
		runs := []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "0", "2", "4", "6", "8", "10", "12", "14", "16", "18", "20", "22", "24", "26", "28", "30", "32", "34", "36", "38"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "3", "5", "7"),
		}
		route := newResultRunsRoute()
		flat := combineResultRuns(runs...)

		want, err := route.sort(flat.Copy())
		require.NoError(t, err)
		got, merged, err := route.mergeResultRuns(flat, runs)
		require.NoError(t, err)
		require.True(t, merged)
		expectResult(t, got, want)
	})

	t.Run("single non-empty run", func(t *testing.T) {
		runs := []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2", "3", "4"),
		}
		route := newResultRunsRoute()
		flat := combineResultRuns(runs...)

		got, merged, err := route.mergeResultRuns(flat, runs)
		require.NoError(t, err)
		require.True(t, merged)
		expectResult(t, got, flat)
	})

	t.Run("globally ordered runs", func(t *testing.T) {
		runs := []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "0", "1", "2", "3", "4"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "5", "6", "7", "8", "9"),
		}
		route := newResultRunsRoute()
		flat := combineResultRuns(runs[1], runs[0])

		got, merged, err := route.mergeResultRuns(flat, runs)
		require.NoError(t, err)
		require.True(t, merged)
		expectResult(t, got, combineResultRuns(runs...))
	})

	t.Run("empty and exhausted runs", func(t *testing.T) {
		runs := []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "0"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "3", "5", "7", "9", "11", "13", "15"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "2", "4", "6", "8", "10", "12", "14", "16"),
		}
		route := newResultRunsRoute()
		flat := combineResultRuns(runs[3], runs[2], runs[1])
		want, err := route.sort(flat.Copy())
		require.NoError(t, err)

		got, merged, err := route.mergeResultRuns(flat, runs)
		require.NoError(t, err)
		require.True(t, merged)
		expectResult(t, got, want)
	})
}

// TestRouteMergeResultRunsPartialAndFallback proves partial results and missing
// run data keep the existing warning and full-sort behavior.
func TestRouteMergeResultRunsPartialAndFallback(t *testing.T) {
	t.Run("falls back for missing or mismatched run data", func(t *testing.T) {
		route := newResultRunsRoute()

		got, merged, err := route.mergeResultRuns(nil, nil)
		require.NoError(t, err)
		require.False(t, merged)
		require.Nil(t, got)

		flat := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "3", "1", "2")
		got, merged, err = route.mergeResultRuns(flat, []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2"),
		})
		require.NoError(t, err)
		require.False(t, merged)
		require.Same(t, flat, got)
	})

	t.Run("partial success truncates merged rows and records warnings", func(t *testing.T) {
		runs := []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|sort_key", "int64|int64"), "1|10", "3|30"),
			nil,
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|sort_key", "int64|int64"), "2|20", "4|40"),
		}
		route := newResultRunsRoute()
		route.OrderBy[0].Col = 1
		route.TruncateColumnCount = 1
		route.ScatterErrorsAsWarnings = true
		vc := &resultRunsVCursor{
			loggingVCursor: &loggingVCursor{shards: []string{"-80", "80-c0", "c0-"}},
			result:         combineResultRuns(runs[0], runs[2]),
			runs:           runs,
			errs:           []error{vterrors.New(vtrpcpb.Code_INTERNAL, "shard failed")},
		}

		got, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		require.Equal(t, 1, vc.resultRunsCalls)
		require.Len(t, vc.warnings, 1)
		expectResult(t, got, sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2", "3", "4"))
	})

	t.Run("falls back to a full sort when runs are unavailable or incomplete", func(t *testing.T) {
		route := newResultRunsRoute()
		vc := &resultRunsVCursor{
			loggingVCursor: &loggingVCursor{shards: []string{"-80", "80-"}},
			result: sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"),
				"3", "1", "2"),
			runs: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "3", "1", "2"),
			},
		}

		got, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		require.Equal(t, 1, vc.resultRunsCalls)
		require.Equal(t, 0, vc.multiShardCalls)
		expectResult(t, got, sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2", "3"))
	})

	t.Run("marked route falls back when the VCursor cannot return runs", func(t *testing.T) {
		route := newResultRunsRoute()
		vc := &loggingVCursor{
			shards: []string{"-80", "80-"},
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "3", "1", "2"),
			},
		}

		got, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		expectResult(t, got, sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2", "3"))
	})

	t.Run("returns merge comparison errors", func(t *testing.T) {
		runs := []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("name", "varchar"), "a", "c", "e", "g", "i"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("name", "varchar"), "b", "d", "f", "h", "j"),
		}
		route := newResultRunsRoute()
		route.OrderBy = evalengine.Comparison{{
			Col:             0,
			WeightStringCol: -1,
			Type:            evalengine.NewType(sqltypes.VarChar, collations.Unknown),
			CollationEnv:    collations.MySQL8(),
		}}
		vc := &resultRunsVCursor{
			loggingVCursor: &loggingVCursor{shards: []string{"-80", "80-"}},
			result:         combineResultRuns(runs[1], runs[0]),
			runs:           runs,
		}

		got, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, "cannot compare strings, collation is unknown or unsupported (collation ID: 0)")
		require.Nil(t, got)
	})

	t.Run("unmarked routes use the original flat-result path", func(t *testing.T) {
		route := newResultRunsRoute()
		route.ShardResultIsSorted = false
		vc := &resultRunsVCursor{
			loggingVCursor: &loggingVCursor{shards: []string{"-80", "80-"}},
			result: sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"),
				"3", "1", "2"),
		}

		got, err := route.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		require.Equal(t, 0, vc.resultRunsCalls)
		require.Equal(t, 1, vc.multiShardCalls)
		expectResult(t, got, sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2", "3"))
	})
}

// TestRouteMergeResultRunsComparison proves the merge uses each supported
// ordering comparison in the same way as a full sort.
func TestRouteMergeResultRunsComparison(t *testing.T) {
	collationID, _ := collations.MySQL8().LookupID("utf8mb4_hu_0900_ai_ci")
	tests := []struct {
		name    string
		orderBy evalengine.Comparison
		runs    []*sqltypes.Result
		hasTies bool
	}{
		{
			name: "ascending duplicate keys and empty shard",
			orderBy: evalengine.Comparison{{
				Col:             1,
				WeightStringCol: -1,
			}},
			runs: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|sort_key", "int64|int64"), "10|1", "30|3", "31|3", "50|5", "70|7"),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|sort_key", "int64|int64"), "20|2", "32|3", "40|4", "60|6", "80|8"),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|sort_key", "int64|int64")),
			},
			hasTies: true,
		},
		{
			name: "descending",
			orderBy: evalengine.Comparison{{
				Col:             0,
				WeightStringCol: -1,
				Desc:            true,
			}},
			runs: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "9", "7", "5", "3", "1"),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "10", "8", "6", "4", "2"),
			},
		},
		{
			name: "weight strings",
			orderBy: evalengine.Comparison{{
				Col:             1,
				WeightStringCol: 0,
			}},
			runs: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("weight_string|name", "varbinary|varchar"), "a|a", "f|p", "k|q", "p|r", "v|x"),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("weight_string|name", "varbinary|varchar"), "c|t", "g|d", "l|e", "q|f", "w|g"),
			},
		},
		{
			name: "collation",
			orderBy: evalengine.Comparison{{
				Col:  0,
				Type: evalengine.NewType(sqltypes.VarChar, collationID),
			}},
			runs: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("name", "varchar"), "c", "cs", "d", "e", "f"),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("name", "varchar"), "c", "cs", "d", "e", "f"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route := newResultRunsRoute()
			route.OrderBy = tt.orderBy
			runs := copyResultRuns(tt.runs)
			flat := combineResultRuns(runs...)
			want, err := route.sort(flat.Copy())
			require.NoError(t, err)

			got, merged, err := route.mergeResultRuns(flat, runs)
			require.NoError(t, err)
			require.True(t, merged)
			if tt.hasTies {
				require.Equal(t, want.Fields, got.Fields)
				require.ElementsMatch(t, want.Rows, got.Rows)
				requireResultRowsOrdered(t, route.OrderBy, got.Rows)
				return
			}
			expectResult(t, got, want)
		})
	}
}

func requireResultRowsOrdered(t *testing.T, orderBy evalengine.Comparison, rows []sqltypes.Row) {
	t.Helper()
	for row := 1; row < len(rows); row++ {
		require.LessOrEqual(t, orderBy.Compare(rows[row-1], rows[row]), 0)
	}
}

func newResultRunsRoute() *Route {
	route := NewRoute(Scatter, &vindexes.Keyspace{Name: "ks", Sharded: true}, "select id from user order by id", "select id from user where 1 != 1")
	route.OrderBy = evalengine.Comparison{{Col: 0, WeightStringCol: -1}}
	route.ShardResultIsSorted = true
	return route
}

func combineResultRuns(runs ...*sqltypes.Result) *sqltypes.Result {
	result := new(sqltypes.Result)
	for _, run := range runs {
		if run != nil {
			result.AppendResult(run)
		}
	}
	return result
}

func copyResultRuns(runs []*sqltypes.Result) []*sqltypes.Result {
	copies := make([]*sqltypes.Result, len(runs))
	for i, run := range runs {
		if run != nil {
			copies[i] = run.Copy()
		}
	}
	return copies
}
