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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func TestUnionSortTruncatingFilter(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, truncate := range []int{0, 1, 2} {
			for _, project := range []string{"none", "reorder", "identity"} {
				t.Run(fmt.Sprintf("stream=%t/truncate=%d/project=%s", stream, truncate, project), func(t *testing.T) {
					fields := sqltypes.MakeTestFields("name|id|allowed|key", "varchar|int64|int64|varbinary")
					predicate, err := evalengine.Translate(sqlparser.NewColName("allowed"), &evalengine.Config{Collation: 255, ResolveColumn: evalengine.FieldResolver(fields).Column, Environment: vtenv.NewTestEnv()})
					require.NoError(t, err)
					filter := &Filter{Input: unionSortTestRoute(t, "select name, id, allowed from review3_alias"), Predicate: predicate, Truncate: truncate}
					var input Primitive = filter
					width := truncate
					if width == 0 {
						width = 3
					}
					orderCol := 0
					wantFields := fields[:width]
					wantRows := sqltypes.MakeTestResult(fields, "a|2|1|a", "z|1|1|z").Truncate(width).Rows
					if project == "reorder" {
						// Reorder and repeat columns above a truncating filter.
						columns := []int{width - 1, 0, 0}
						projection := &SimpleProjection{Input: filter, Cols: columns, ColNames: []string{"last", "first", "copy"}}
						input = projection
						want := projection.buildResult(&sqltypes.Result{Fields: wantFields, Rows: wantRows})
						wantFields, wantRows = want.Fields, want.Rows
						orderCol = 1
					}
					if project == "identity" {
						projection := &SimpleProjection{Input: filter, ColNames: make([]string, width)}
						projection.ColNames[0] = "renamed"
						input = projection
						wantFields = projection.buildFields(&sqltypes.Result{Fields: wantFields})
					}

					sort := &MemorySort{Input: NewConcatenate([]Primitive{input}, nil), OrderBy: evalengine.Comparison{{Col: orderCol, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Unknown, 0), CollationEnv: collations.MySQL8()}}, CollationQueries: []Primitive{
						&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("collation|coercibility", "varchar|int64"), "utf8mb4_general_ci|2")}},
					}}
					vc := &loggingVCursor{shards: []string{"0"}, results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "z|1|1|z", "a|2|1|a", "0|0|0|0")}}
					vc.inTx = true
					var result *sqltypes.Result
					if stream {
						result, err = wrapStreamExecute(sort, vc, nil, true)
					} else {
						result, err = sort.TryExecute(t.Context(), vc, nil, true)
					}
					require.NoError(t, err)
					require.Len(t, result.Fields, len(wantFields))
					for i, field := range wantFields {
						assert.Equal(t, field.Name, result.Fields[i].Name)
						assert.Equal(t, field.Type, result.Fields[i].Type)
					}
					assert.Equal(t, wantRows, result.Rows)
					assert.Equal(t, truncate, filter.Truncate, "cached filter must remain unchanged")
				})
			}
		}
	}
}

func TestUnionSortSQLScope(t *testing.T) {
	env := collations.MySQL8()
	source := unionSortTestRoute(t, "select name, id as c0 from review3_alias order by name")
	source.OrderBy = evalengine.Comparison{{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.VarChar, env.LookupByName("utf8mb4_general_ci")), CollationEnv: env}}
	generated, _, _, err := unionSortRoute(source, []int{0}, []collations.TypedCollation{{Collation: env.LookupByName("utf8mb4_general_ci")}}, []collations.TypedCollation{{Collation: env.LookupByName("utf8mb4_general_ci")}}, env)
	require.NoError(t, err)
	timeout := unionSortTestRoute(t, "select /*+ MAX_EXECUTION_TIME(5) SET_VAR(sort_buffer_size=32768) RESOURCE_GROUP(rg) NO_ICP(a) QB_NAME(q) */ sum(a.id * b.id) as n from review3_load a cross join review3_load b")
	timed, _, _, err := unionSortRoute(timeout, []int{0}, []collations.TypedCollation{{Collation: 63}}, nil, env)
	require.NoError(t, err)
	t.Logf("alias wrapper: %s", generated.Query)
	t.Logf("timeout wrapper: %s", timed.Query)
	assert.Equal(t, sqlparser.Comments{"/*+ MAX_EXECUTION_TIME(5) SET_VAR(sort_buffer_size=32768) RESOURCE_GROUP(rg) NO_MERGE(__union_values) */"}, timed.QueryStatement.(*sqlparser.Select).Comments.GetComments())
	assert.Contains(t, timed.Query, "NO_ICP(a) QB_NAME(q)")
	assert.Contains(t, generated.Query, "order by __union_values.c0")
}

func TestUnionSortPadSpaceTie(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			fields := sqltypes.MakeTestFields("name|id|name_key|id_key", "varchar|int64|varbinary|varbinary")
			vc := &loggingVCursor{shards: []string{"0"}, results: []*sqltypes.Result{
				sqltypes.MakeTestResult(fields, "a|2|\x00A|2", "a |1|\x00A\x00 |1"),
			}}
			vc.inTx = true
			sort := &MemorySort{
				Input: NewConcatenate([]Primitive{unionSortTestRoute(t, "select name, id from review3_alias")}, nil),
				OrderBy: evalengine.Comparison{
					{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Unknown, 0), CollationEnv: collations.MySQL8()},
					{Col: 1, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Int64, 63), CollationEnv: collations.MySQL8()},
				},
				CollationQueries:    []Primitive{&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("collation|coercibility|collation2|coercibility2", "varchar|int64|varchar|int64"), "utf8mb4_general_ci|2|binary|5")}}},
				TruncateColumnCount: 2,
			}
			var result *sqltypes.Result
			var err error
			if stream {
				result, err = wrapStreamExecute(sort, vc, nil, true)
			} else {
				result, err = sort.TryExecute(t.Context(), vc, nil, true)
			}
			require.NoError(t, err)
			t.Logf("actual sorted rows=%v", result.Rows)
			assert.Equal(t, "1", result.Rows[0][1].ToString(), "MySQL PAD SPACE ties must fall through to the numeric key")
		})
	}
}

func TestUnionSortDifferentArmHelpers(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			left := unionSortTestRoute(t, "select name,id,weight_string(name),weight_string(id) from test")
			right := unionSortTestRoute(t, "select name,id,weight_string(name) from test")
			sort := &MemorySort{Input: NewConcatenate([]Primitive{&Limit{Input: left, Count: evalengine.NewLiteralInt(1)}, right}, []int{2}), TruncateColumnCount: 2, OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: 2, Type: evalengine.NewType(sqltypes.Unknown, 0)}}, CollationQueries: []Primitive{
				&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("collation|coercibility", "varchar|int64"), "utf8mb4_general_ci|2")}},
				&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("collation|coercibility", "varchar|int64"), "utf8mb4_general_ci|4")}},
			}}
			vc := &loggingVCursor{shards: []string{"0"}, results: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|id|old1|old2|key", "varchar|int64|varbinary|varbinary|varbinary"), "a|1|old|old|a"),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|id|old1|key", "varchar|int64|varbinary|varbinary"), "z|2|old|z"),
			}}
			vc.inTx = true
			var result *sqltypes.Result
			var err error
			if stream {
				result, err = wrapStreamExecute(sort, vc, nil, true)
			} else {
				result, err = sort.TryExecute(t.Context(), vc, nil, true)
			}
			require.NoError(t, err)
			require.Len(t, result.Fields, 2)
			assert.Equal(t, sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|id", "varchar|int64"), "a|1", "z|2").Rows, result.Rows)
		})
	}
}

// The final sort cannot recover a row discarded by an arm-local LIMIT. Route
// must compare the server keys under the arm's collation before that limit.
func TestUnionSortArmLimitPadSpace(t *testing.T) {
	route := unionSortTestRoute(t, "select name,id from test order by name,id")
	route.Opcode = Scatter
	route.OrderBy = evalengine.Comparison{{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Unknown, 0)}, {Col: 1, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Unknown, 0)}}
	other := unionSortTestRoute(t, "select name,id from other")
	other.Opcode = Scatter
	sort := &MemorySort{Input: NewConcatenate([]Primitive{&Limit{Input: route, Count: evalengine.NewLiteralInt(1)}, other}, nil), TruncateColumnCount: 2, OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Unknown, 0)}}, CollationQueries: []Primitive{
		&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("global|coercibility|local1|coercibility1|local2|coercibility2", "varchar|int64|varchar|int64|varchar|int64"), "utf8mb4_general_ci|2|utf8mb4_general_ci|2|binary|5")}},
		&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("global|coercibility", "varchar|int64"), "utf8mb4_bin|2")}},
	}}
	vc := &loggingVCursor{shards: []string{"-80", "80-"}, results: []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|id|global_key|local_key", "varchar|int64|varbinary|varbinary"), "a|5|\x00\x00a|\x00A", "a |4|\x00\x00a\x00\x00 |\x00A\x00 "),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|id|global_key", "varchar|int64|varbinary")),
	}}
	vc.inTx = true
	result, err := sort.TryExecute(t.Context(), vc, nil, true)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "4", result.Rows[0][1].ToString())
	assert.Nil(t, route.OrderBy[0].PadSpaceWeight)
	assert.Equal(t, collations.Unknown, route.OrderBy[0].Type.Collation())
}

func TestUnionSortConcurrentPlanReuse(t *testing.T) {
	env := collations.MySQL8()
	route := unionSortTestRoute(t, "select name,id from test")
	metadata := unionSortTestRoute(t, "select 'metadata'")
	sort := &MemorySort{Input: NewConcatenate([]Primitive{route}, nil), OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Unknown, 0)}, {Col: 1, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Int64, 63)}}, CollationQueries: []Primitive{metadata}}
	t.Cleanup(func() {
		assert.Equal(t, sqltypes.Unknown, sort.OrderBy[0].Type.Type())
		assert.Nil(t, sort.OrderBy[0].PadSpaceWeight)
		assert.Equal(t, 2, route.QueryStatement.(*sqlparser.Select).GetColumnCount())
	})
	for run := range 4 {
		for _, stream := range []bool{false, true} {
			for _, name := range []string{"utf8mb4_general_ci", "utf8mb4_0900_bin"} {
				t.Run(fmt.Sprintf("run=%d/stream=%t/%s", run, stream, name), func(t *testing.T) {
					t.Parallel()
					coll := colldata.Lookup(env.LookupByName(name))
					key := string(coll.WeightString(nil, []byte("a"), 0))
					spaced := string(coll.WeightString(nil, []byte("a "), 0))
					vc := &loggingVCursor{shards: []string{"0"}, results: []*sqltypes.Result{
						sqltypes.MakeTestResult(sqltypes.MakeTestFields("collation|coercibility|collation2|coercibility2", "varchar|int64|varchar|int64"), name+"|2|binary|5"),
						sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|id|key|id_key", "varchar|int64|varbinary|varbinary"), "a|2|"+key+"|2", "a |1|"+spaced+"|1"),
					}}
					vc.inTx = true
					var result *sqltypes.Result
					var err error
					if stream {
						result, err = wrapStreamExecute(sort, vc, nil, true)
					} else {
						result, err = sort.TryExecute(t.Context(), vc, nil, true)
					}
					require.NoError(t, err)
					want := "1"
					if name == "utf8mb4_0900_bin" {
						want = "2"
					}
					assert.Equal(t, want, result.Rows[0][1].ToString())
				})
			}
		}
	}
}
