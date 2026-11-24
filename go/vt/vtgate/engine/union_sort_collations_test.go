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
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
			}
			metadataFields := sqltypes.MakeTestFields("collation|coercibility", "varchar|int64")
			input := &fakePrimitive{results: []*sqltypes.Result{
				sqltypes.MakeTestResult(fields, "a|wrong_a", "B|wrong_B"),
				sqltypes.MakeTestResult(fields, "a|wrong_a", "B|wrong_B"),
			}}
			metadata := &fakePrimitive{results: []*sqltypes.Result{
				sqltypes.MakeTestResult(metadataFields, "utf8mb4_general_ci|2"),
				sqltypes.MakeTestResult(metadataFields, "utf8mb4_bin|2"),
			}}
			sort := &MemorySort{
				Input: input, CollationQueries: []Primitive{metadata}, TruncateColumnCount: 1,
				OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: 1, Type: evalengine.NewType(sqltypes.Unknown, collations.Unknown), CollationEnv: collations.MySQL8()}},
			}
			for _, want := range [][]string{{"a", "B"}, {"B", "a"}} {
				var result *sqltypes.Result
				var err error
				if stream {
					result, err = wrapStreamExecute(sort, &noopVCursor{}, nil, true)
				} else {
					result, err = sort.TryExecute(t.Context(), &noopVCursor{}, nil, true)
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

func TestUnionSortKeyCharsetConversion(t *testing.T) {
	fields := []*querypb.Field{{Name: "name", Type: sqltypes.VarChar, Charset: 255}}
	latin1, ok := collations.MySQL8().LookupID("latin1_swedish_ci")
	require.True(t, ok)
	order, sortFields, keys, err := prepareUnionSort(evalengine.Comparison{{Col: 0}}, fields, []collations.TypedCollation{{Collation: latin1}})
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, 1, order[0].Col)
	assert.EqualValues(t, latin1, sortFields[1].Charset)
	input := []sqltypes.Value{sqltypes.NewVarChar("Ångström")}
	keyed, err := appendUnionSortKeys(input, keys)
	require.NoError(t, err)
	require.Len(t, keyed, 2)
	assert.Equal(t, "Ångström", keyed[0].ToString())
	assert.Equal(t, []byte{0xc5, 'n', 'g', 's', 't', 'r', 0xf6, 'm'}, keyed[1].Raw())
	assert.Len(t, input, 1)
	nulls, err := appendUnionSortKeys([]sqltypes.Value{sqltypes.NULL}, keys)
	require.NoError(t, err)
	assert.True(t, nulls[1].IsNull())
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
