/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func makeTestResults(fields []*querypb.Field, gen []sqltypes.RandomGenerator, N int) []*sqltypes.Result {
	result := &sqltypes.Result{Fields: fields}

	for range N {
		row := make([]sqltypes.Value, 0, len(fields))
		for _, f := range gen {
			row = append(row, f())
		}
		result.Rows = append(result.Rows, row)
	}

	return []*sqltypes.Result{result}
}

func benchmarkName(fields []*querypb.Field) string {
	var buf strings.Builder
	for i, f := range fields {
		if i > 0 {
			buf.WriteByte('_')
		}
		fmt.Fprintf(&buf, "%s(%s)", f.Name, f.Type.String())
	}
	return buf.String()
}

func BenchmarkScalarAggregate(b *testing.B) {
	rand_i64 := sqltypes.RandomGenerators[sqltypes.Int64]
	rand_i64small := func() sqltypes.Value {
		return sqltypes.NewInt64(rand.Int64N(1024))
	}
	rand_f64 := sqltypes.RandomGenerators[sqltypes.Float64]
	rand_dec := sqltypes.RandomGenerators[sqltypes.Decimal]
	rand_bin := sqltypes.RandomGenerators[sqltypes.VarBinary]

	cases := []struct {
		fields []*querypb.Field
		gen    []sqltypes.RandomGenerator
		params []*AggregateParams
	}{
		{
			fields: sqltypes.MakeTestFields("count", "int64"),
			gen:    []sqltypes.RandomGenerator{rand_i64},
			params: []*AggregateParams{
				{Opcode: AggregateCount, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("sum_small", "int64"),
			gen:    []sqltypes.RandomGenerator{rand_i64small},
			params: []*AggregateParams{
				{Opcode: AggregateSum, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("sum", "int64"),
			gen:    []sqltypes.RandomGenerator{rand_i64},
			params: []*AggregateParams{
				{Opcode: AggregateSum, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("sum", "float64"),
			gen:    []sqltypes.RandomGenerator{rand_f64},
			params: []*AggregateParams{
				{Opcode: AggregateSum, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("sum", "decimal"),
			gen:    []sqltypes.RandomGenerator{rand_dec},
			params: []*AggregateParams{
				{Opcode: AggregateSum, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("min", "int64"),
			gen:    []sqltypes.RandomGenerator{rand_i64},
			params: []*AggregateParams{
				{Opcode: AggregateMin, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("min", "float64"),
			gen:    []sqltypes.RandomGenerator{rand_f64},
			params: []*AggregateParams{
				{Opcode: AggregateMin, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("min", "decimal"),
			gen:    []sqltypes.RandomGenerator{rand_dec},
			params: []*AggregateParams{
				{Opcode: AggregateMin, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("min", "varbinary"),
			gen:    []sqltypes.RandomGenerator{rand_bin},
			params: []*AggregateParams{
				{Opcode: AggregateMin, Col: 0},
			},
		},
		{
			fields: sqltypes.MakeTestFields("keyspace|gtid|shard", "varchar|varchar|varchar"),
			gen: []sqltypes.RandomGenerator{
				func() sqltypes.Value {
					return sqltypes.NewVarChar("keyspace")
				},
				func() sqltypes.Value {
					return sqltypes.NewVarChar(uuid.New().String())
				},
				func() sqltypes.Value {
					return sqltypes.NewVarChar(fmt.Sprintf("%x-%x", rand.IntN(256), rand.IntN(256)))
				},
			},
			params: []*AggregateParams{
				{Opcode: AggregateGtid, Col: 1},
			},
		},
	}

	for _, tc := range cases {
		b.Run(benchmarkName(tc.fields), func(b *testing.B) {
			results := makeTestResults(tc.fields, tc.gen, 10000)

			fp := &fakePrimitive{
				allResultsInOneCall: true,
				results:             results,
			}
			oa := &ScalarAggregate{
				Aggregates: tc.params,
				Input:      fp,
			}

			b.Run("TryExecute", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					fp.rewind()
					_, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, true)
					if err != nil {
						panic(err)
					}
				}
			})
		})
	}
}

// TestGroupConcatSeparatorCharset concatenates the GROUP_CONCAT of two
// shards. The separator is in the character set of the values: for a
// latin1_swedish_ci column l with 'Müller' and 'MYLLER', MySQL 8.0.46 gives,
// read with character_set_results = NULL,
//
//	HEX(GROUP_CONCAT(l ORDER BY id SEPARATOR 'ü'))   4DFC6C6C6572FC4D55454C4C4552
//	HEX(GROUP_CONCAT(l ORDER BY id SEPARATOR '中'))  4DFC6C6C65723F4D55454C4C4552
func TestGroupConcatSeparatorCharset(t *testing.T) {
	latin1Swedish := collations.MySQL8().LookupByName("latin1_swedish_ci")
	utf8mb4 := collations.MySQL8().DefaultConnectionCharset()
	rows := func(collation collations.ID, values ...string) *sqltypes.Result {
		qr := &sqltypes.Result{Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32, Charset: uint32(collations.CollationBinaryID)},
			{Name: "l", Type: sqltypes.VarChar, Charset: uint32(collation), ColumnLength: 10},
		}}
		for i, value := range values {
			qr.Rows = append(qr.Rows, sqltypes.Row{sqltypes.NewInt32(int32(i)), sqltypes.MakeTrusted(sqltypes.VarChar, []byte(value))})
		}
		return qr
	}
	tests := []struct {
		name      string
		rows      *sqltypes.Result
		convert   bool
		separator string
		value     string
	}{
		{"latin1 values", rows(latin1Swedish, "M\xfcller", "MYLLER"), true, "ü", "M\xfcller\xfcMYLLER"},
		{"latin1 values, a separator that latin1 does not have", rows(latin1Swedish, "M\xfcller", "MYLLER"), true, "中", "M\xfcller?MYLLER"},
		{"utf8mb4 values", rows(utf8mb4, "Müller", "MYLLER"), true, "ü", "MüllerüMYLLER"},
		{"utf8mb4 values, without the flag", rows(utf8mb4, "Müller", "MYLLER"), false, "ü", "MüllerüMYLLER"},
	}
	for _, tc := range tests {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, stream %v", tc.name, stream), func(t *testing.T) {
				param := NewAggregateParam(AggregateGroupConcat, 1, nil, "", collations.MySQL8())
				param.Func = &sqlparser.GroupConcatExpr{Separator: tc.separator}
				aggregate := &ScalarAggregate{
					Aggregates: []*AggregateParams{param},
					Input:      &fakePrimitive{results: []*sqltypes.Result{tc.rows}},
				}
				vcursor := &noopVCursor{convertResultCharset: tc.convert}
				var qr *sqltypes.Result
				var err error
				if stream {
					qr, err = wrapStreamExecute(aggregate, vcursor, map[string]*querypb.BindVariable{}, true)
				} else {
					qr, err = aggregate.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, true)
				}
				require.NoError(t, err)
				require.Len(t, qr.Rows, 1)
				assert.Equal(t, tc.value, qr.Rows[0][1].ToString())
			})
		}
	}
}
