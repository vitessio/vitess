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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func makeTestResults(fields []*querypb.Field, gen []sqltypes.RandomGenerator, N int) []*sqltypes.Result {
	result := &sqltypes.Result{Fields: fields}

	for i := 0; i < N; i++ {
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
	var rand_i64 = sqltypes.RandomGenerators[sqltypes.Int64]
	var rand_i64small = func() sqltypes.Value {
		return sqltypes.NewInt64(rand.Int64N(1024))
	}
	var rand_f64 = sqltypes.RandomGenerators[sqltypes.Float64]
	var rand_dec = sqltypes.RandomGenerators[sqltypes.Decimal]
	var rand_bin = sqltypes.RandomGenerators[sqltypes.VarBinary]

	var cases = []struct {
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
