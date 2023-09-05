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

package evalengine

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

func TestWeightStrings(t *testing.T) {
	const Length = 1000

	type item struct {
		value  sqltypes.Value
		weight string
	}

	var cases = []struct {
		name  string
		gen   func() sqltypes.Value
		types []sqltypes.Type
		col   collations.ID
		len   int
		prec  int
	}{
		{name: "int64", gen: sqltypes.RandomGenerators[sqltypes.Int64], types: []sqltypes.Type{sqltypes.Int64, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "uint64", gen: sqltypes.RandomGenerators[sqltypes.Uint64], types: []sqltypes.Type{sqltypes.Uint64, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "float64", gen: sqltypes.RandomGenerators[sqltypes.Float64], types: []sqltypes.Type{sqltypes.Float64, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "varchar", gen: sqltypes.RandomGenerators[sqltypes.VarChar], types: []sqltypes.Type{sqltypes.VarChar, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationUtf8mb4ID},
		{name: "varbinary", gen: sqltypes.RandomGenerators[sqltypes.VarBinary], types: []sqltypes.Type{sqltypes.VarBinary, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "decimal", gen: sqltypes.RandomGenerators[sqltypes.Decimal], types: []sqltypes.Type{sqltypes.Decimal, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID, len: 20, prec: 10},
		{name: "json", gen: sqltypes.RandomGenerators[sqltypes.TypeJSON], types: []sqltypes.Type{sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "date", gen: sqltypes.RandomGenerators[sqltypes.Date], types: []sqltypes.Type{sqltypes.Date, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "datetime", gen: sqltypes.RandomGenerators[sqltypes.Datetime], types: []sqltypes.Type{sqltypes.Datetime, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "timestamp", gen: sqltypes.RandomGenerators[sqltypes.Timestamp], types: []sqltypes.Type{sqltypes.Timestamp, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "time", gen: sqltypes.RandomGenerators[sqltypes.Time], types: []sqltypes.Type{sqltypes.Time, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
	}

	for _, tc := range cases {
		for _, typ := range tc.types {
			t.Run(fmt.Sprintf("%s/%v", tc.name, typ), func(t *testing.T) {
				items := make([]item, 0, Length)
				for i := 0; i < Length; i++ {
					v := tc.gen()
					w, _, err := WeightString(nil, v, typ, tc.col, tc.len, tc.prec)
					require.NoError(t, err)

					items = append(items, item{value: v, weight: string(w)})
				}

				slices.SortFunc(items, func(a, b item) int {
					if a.weight < b.weight {
						return -1
					} else if a.weight > b.weight {
						return 1
					} else {
						return 0
					}
				})

				for i := 0; i < Length-1; i++ {
					a := items[i]
					b := items[i+1]

					v1, err := valueToEvalCast(a.value, typ, tc.col)
					require.NoError(t, err)
					v2, err := valueToEvalCast(b.value, typ, tc.col)
					require.NoError(t, err)

					cmp, err := evalCompareNullSafe(v1, v2)
					require.NoError(t, err)

					if cmp > 0 {
						t.Fatalf("expected %v [pos=%d] to come after %v [pos=%d]\nav = %v\nbv = %v",
							a.value, i, b.value, i+1,
							[]byte(a.weight), []byte(b.weight),
						)
					}
				}
			})
		}
	}
}
