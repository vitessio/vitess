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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// A cached sort plan must resolve metadata separately for every execution.
// Its result fields describe the connection's encoding on both executions.
func TestUnionSortCollationsPerExecution(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			fields := []*querypb.Field{
				{Name: "name", Type: sqltypes.VarChar, Charset: 255},
				{Name: "weight", Type: sqltypes.VarBinary, Charset: collations.CollationBinaryID},
				{Name: "common_weight", Type: sqltypes.VarBinary, Charset: collations.CollationBinaryID},
			}
			metadataFields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
			vc := &loggingVCursor{shards: []string{"0"}, results: []*sqltypes.Result{
				sqltypes.MakeTestResult(fields, "a|wrong_a|1", "B|wrong_B|2"),
				sqltypes.MakeTestResult(fields, "a|wrong_a|2", "B|wrong_B|1"),
			}}
			vc.inTx = true
			route := unionSortTestRoute(t, "select name, weight_string(name) from test")
			metadata := &fakePrimitive{results: []*sqltypes.Result{
				sqltypes.MakeTestResult(metadataFields, "utf8mb4_general_ci|2"),
				sqltypes.MakeTestResult(metadataFields, "utf8mb4_bin|2"),
			}}
			sort := &MemorySort{
				Input: NewConcatenate([]Primitive{route}, nil), CollationQueries: []Primitive{metadata}, TruncateColumnCount: 1,
				OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: 1, Type: evalengine.NewType(sqltypes.Unknown, collations.Unknown), CollationEnv: collations.MySQL8()}},
			}
			for _, want := range [][]string{{"a", "B"}, {"B", "a"}} {
				var result *sqltypes.Result
				var err error
				if stream {
					result, err = wrapStreamExecute(sort, vc, nil, true)
				} else {
					result, err = sort.TryExecute(t.Context(), vc, nil, true)
				}
				require.NoError(t, err)
				require.Len(t, result.Fields, 1)
				require.Len(t, result.Rows, len(want))
				for i, row := range result.Rows {
					require.Len(t, row, 1, "temporary sort keys must not escape")
					assert.Equal(t, want[i], row[0].ToString())
				}
				assert.EqualValues(t, 255, result.Fields[0].Charset, "preserve the wire encoding")
			}
			assert.Equal(t, sqltypes.Unknown, sort.OrderBy[0].Type.Type())
			assert.Equal(t, 0, sort.OrderBy[0].Col)
			assert.Equal(t, 1, sort.OrderBy[0].WeightStringCol)
			assert.Equal(t, "select name, weight_string(name) from test", route.Query)
			assert.Equal(t, 2, route.QueryStatement.(*sqlparser.Select).GetColumnCount())
			queries := strings.Join(vc.log, "\n")
			assert.Contains(t, queries, "collate utf8mb4_general_ci")
			assert.Contains(t, queries, "collate utf8mb4_bin")
		})
	}
}

func TestUnionSortCollationCoercibility(t *testing.T) {
	metadataFields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			constant := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(metadataFields, "utf8mb4_0900_ai_ci|4")}}
			column := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(metadataFields, "utf8mb4_general_ci|2")}}
			queries := []Primitive{constant, column}
			if reverse {
				queries = []Primitive{column, constant}
			}
			sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}, CollationQueries: queries}
			resolved, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, nil)
			require.NoError(t, err)
			require.Len(t, resolved, 1)
			id, ok := collations.MySQL8().LookupID("utf8mb4_general_ci")
			require.True(t, ok)
			assert.Equal(t, id, resolved[0].Collation)
			assert.Equal(t, collations.CoerceImplicit, resolved[0].Coercibility)
		})
	}
}

func unionSortTestRoute(t *testing.T, query string) *Route {
	t.Helper()
	statement, err := sqlparser.NewTestParser().Parse(query)
	require.NoError(t, err)
	route := NewRoute(Unsharded, &vindexes.Keyspace{Name: "ks"}, query, query)
	route.QueryStatement = statement
	return route
}

func TestUnionSortMetadataPreservesBindings(t *testing.T) {
	fields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
	metadata := &fakePrimitive{
		writeBindVar: "__vals",
		results:      []*sqltypes.Result{sqltypes.MakeTestResult(fields, "utf8mb4_general_ci|2")},
	}
	sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}, CollationQueries: []Primitive{metadata}}
	bindings := map[string]*querypb.BindVariable{"kept": sqltypes.Int64BindVariable(7)}
	_, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, bindings)
	require.NoError(t, err)
	require.Len(t, bindings, 1)
	assert.True(t, proto.Equal(sqltypes.Int64BindVariable(7), bindings["kept"]))
}

func TestUnionSortRejectsIncompatibleCollations(t *testing.T) {
	fields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
	for _, operands := range [][]string{
		{"utf8mb4_general_ci|0", "utf8mb4_bin|0"},
		{"utf8mb4_general_ci|2", "utf8mb4_unicode_ci|2"},
	} {
		t.Run(fmt.Sprint(operands), func(t *testing.T) {
			sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}}
			for _, operand := range operands {
				sort.CollationQueries = append(sort.CollationQueries, &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, operand)}})
			}
			_, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, nil)
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
			mysqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
			require.True(t, ok)
			assert.Equal(t, sqlerror.ERCantAggregateNCollations, mysqlErr.Number())
		})
	}
}

func TestUnionSortEmptyDestinationUsesExistingShard(t *testing.T) {
	fields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
	vc := &loggingVCursor{
		shardSession: []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "20-"}}},
		results:      []*sqltypes.Result{sqltypes.MakeTestResult(fields, "utf8mb4_general_ci|2")},
	}
	route := NewRoute(None, &vindexes.Keyspace{Name: "ks", Sharded: true}, "metadata", "metadata")
	sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}, CollationQueries: []Primitive{route}}
	resolved, err := sort.resolveUnionCollations(t.Context(), vc, nil)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	vc.ExpectLog(t, []string{"ExecuteMultiShard ks.20-: metadata {} false false"})
}

func TestUnionSortCollationCharsets(t *testing.T) {
	fields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
	for _, tc := range []struct {
		name     string
		operands []string
		want     string
	}{
		{"unsupported arm", []string{"big5_chinese_ci|4", "utf8mb4_0900_ai_ci|0"}, "utf8mb4_0900_ai_ci"},
		{"repeated unsupported arm", []string{"big5_chinese_ci|4", "big5_chinese_ci|4", "utf8mb4_0900_ai_ci|0"}, "utf8mb4_0900_ai_ci"},
		{"ASCII column", []string{"ascii_general_ci|2", "latin1_swedish_ci|2"}, "latin1_swedish_ci"},
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", tc.name, reverse), func(t *testing.T) {
				sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}}
				for i := range tc.operands {
					if reverse {
						i = len(tc.operands) - 1 - i
					}
					sort.CollationQueries = append(sort.CollationQueries, &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, tc.operands[i])}})
				}
				resolved, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, nil)
				require.NoError(t, err)
				require.Len(t, resolved, 1)
				assert.Equal(t, collations.MySQL8().LookupByName(tc.want), resolved[0].Collation)
			})
		}
	}
	t.Run("unsupported common comparator", func(t *testing.T) {
		sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}, CollationQueries: []Primitive{
			&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "big5_chinese_ci|2")}},
		}}
		_, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, nil)
		require.ErrorContains(t, err, "VT12001")
	})
}

func TestUnionSortCachedSizeIncludesMetadata(t *testing.T) {
	metadata := &Route{Query: "short"}
	sort := &MemorySort{CollationQueries: []Primitive{metadata}}
	before := sort.CachedSize(true)
	metadata.Query = strings.Repeat("metadata", 1024)
	assert.GreaterOrEqual(t, sort.CachedSize(true)-before, int64(len(metadata.Query)-64), "plan-cache cost must include retained metadata SQL")
}

func TestUnionSortConcatenatedTextAndBlob(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      sqltypes.Type
		metadata string
		want     []string
	}{
		{"text", sqltypes.Text, "utf8mb4_general_ci|2", []string{"a", "B"}},
		{"blob", sqltypes.Blob, "binary|2", []string{"B", "a"}},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				charset := uint32(255)
				if tc.typ == sqltypes.Blob {
					charset = 63
				}
				fields := []*querypb.Field{{Name: "name", Type: tc.typ, Charset: charset}, {Name: "key", Type: sqltypes.VarBinary, Charset: 63}}
				aKey, bKey := "1", "2"
				if tc.typ == sqltypes.Blob {
					aKey, bKey = "a", "B"
				}
				vc := &loggingVCursor{shards: []string{"0"}, results: []*sqltypes.Result{
					sqltypes.MakeTestResult(fields, "a|"+aKey),
					sqltypes.MakeTestResult(fields, "B|"+bKey),
				}}
				vc.inTx = true
				metadataFields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
				sort := &MemorySort{
					Input: NewConcatenate([]Primitive{
						unionSortTestRoute(t, "select name from test where id = 1"),
						unionSortTestRoute(t, "select name from test where id = 4"),
					}, nil),
					CollationQueries: []Primitive{
						&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(metadataFields, tc.metadata)}},
						&fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(metadataFields, tc.metadata)}},
					},
					OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Unknown, collations.Unknown), CollationEnv: collations.MySQL8()}},
				}
				var result *sqltypes.Result
				var err error
				if stream {
					result, err = wrapStreamExecute(sort, vc, nil, true)
				} else {
					result, err = sort.TryExecute(t.Context(), vc, nil, true)
				}
				require.NoError(t, err)
				require.Len(t, result.Rows, 2)
				assert.Equal(t, tc.want[0], result.Rows[0][0].ToString())
				assert.Equal(t, tc.want[1], result.Rows[1][0].ToString())
			})
		}
	}
}

func TestUnionSortUnsupportedBinaryPrecedence(t *testing.T) {
	env := collations.MySQL8()
	big5, _ := env.LookupID("big5_chinese_ci")
	for left := collations.CoerceExplicit; left <= collations.CoerceIgnorable; left++ {
		for right := collations.CoerceExplicit; right <= collations.CoerceIgnorable; right++ {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/%d/reverse=%t", left, right, reverse), func(t *testing.T) {
					a := collations.TypedCollation{Collation: big5, Coercibility: left, Repertoire: collations.RepertoireUnicode}
					b := collations.TypedCollation{Collation: collations.CollationBinaryID, Coercibility: right, Repertoire: collations.RepertoireUnicode}
					// Both Latin-1 and Big5 are non-Unicode charsets. Binary
					// precedence must follow the existing supported-pair merge.
					ref := a
					ref.Collation = env.LookupByName("latin1_swedish_ci")
					var want collations.TypedCollation
					var wantErr error
					if reverse {
						want, _, _, wantErr = colldata.Merge(env, b, ref, colldata.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
						a, b = b, a
					} else {
						want, _, _, wantErr = colldata.Merge(env, ref, b, colldata.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
					}
					got, err := mergeUnionSortCollations(env, a, b)
					if wantErr != nil {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					if want.Collation == ref.Collation {
						want.Collation = big5
					}
					assert.Equal(t, want, got)
				})
			}
		}
	}
}

func TestUnionSortInputWrappers(t *testing.T) {
	route := unionSortTestRoute(t, "select name, id, weight_string(name) from test where id in ::__vals order by name limit 2 for update")
	route.TruncateColumnCount = 2
	route.OrderBy = evalengine.Comparison{{Col: 0, WeightStringCol: 2, Type: evalengine.NewType(sqltypes.Unknown, collations.Unknown), CollationEnv: collations.MySQL8()}}
	projection := &SimpleProjection{Input: route, Cols: []int{1, 0}, ColNames: []string{"id", "name"}}
	sort := &MemorySort{Input: NewConcatenate([]Primitive{&Limit{Input: &Filter{Input: projection}}}, nil), OrderBy: evalengine.Comparison{{Col: 1}, {Col: 0}}}
	resolved := []collations.TypedCollation{{Collation: collations.MySQL8().LookupByName("utf8mb4_general_ci")}, {Collation: collations.CollationBinaryID}}
	input, err := sort.unionSortInput(resolved, collations.MySQL8())
	require.NoError(t, err)
	copyRoute := Find(func(p Primitive) bool { _, ok := p.(*Route); return ok }, input).(*Route)
	assert.Contains(t, copyRoute.Query, "NO_MERGE(__union_values)")
	assert.Contains(t, copyRoute.Query, "weight_string(convert(c0 using utf8mb4) collate utf8mb4_general_ci)")
	assert.Contains(t, copyRoute.Query, "cast(c1 as binary)")
	assert.Contains(t, copyRoute.Query, "::__vals", "preserve routing-specific bindings in the data query")
	assert.Contains(t, copyRoute.Query, "limit 2 for update", "preserve arm-local selection and locks")
	assert.Contains(t, copyRoute.Query, "order by c0 asc", "preserve the per-shard order used by Route's merge")
	assert.Equal(t, 0, copyRoute.TruncateColumnCount)
	assert.Equal(t, 2, route.TruncateColumnCount)
	copyRoute.OrderBy[0].Col = 1
	assert.Equal(t, 0, route.OrderBy[0].Col, "comparison state belongs to this execution")
	warming, changed := removeForUpdateLocks(copyRoute.QueryStatement)
	assert.True(t, changed)
	assert.NotContains(t, warming, "for update")
	assert.Contains(t, warming, "weight_string(convert(c0 using utf8mb4)", "warming uses the generated data statement")
	assert.Equal(t, []int{1, 0}, projection.Cols)
	assert.Equal(t, 3, route.QueryStatement.(*sqlparser.Select).GetColumnCount())
}
