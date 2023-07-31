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
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

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
		{name: "int64", gen: randomInt64, types: []sqltypes.Type{sqltypes.Int64, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "uint64", gen: randomUint64, types: []sqltypes.Type{sqltypes.Uint64, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "float64", gen: randomFloat64, types: []sqltypes.Type{sqltypes.Float64, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "varchar", gen: randomVarChar, types: []sqltypes.Type{sqltypes.VarChar, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationUtf8mb4ID},
		{name: "varbinary", gen: randomVarBinary, types: []sqltypes.Type{sqltypes.VarBinary, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "decimal", gen: randomDecimal, types: []sqltypes.Type{sqltypes.Decimal, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID, len: 20, prec: 10},
		{name: "json", gen: randomJSON, types: []sqltypes.Type{sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "date", gen: randomDate, types: []sqltypes.Type{sqltypes.Date, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "datetime", gen: randomDatetime, types: []sqltypes.Type{sqltypes.Datetime, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "timestamp", gen: randomTimestamp, types: []sqltypes.Type{sqltypes.Timestamp, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
		{name: "time", gen: randomTime, types: []sqltypes.Type{sqltypes.Time, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID},
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

func randomVarBinary() sqltypes.Value { return sqltypes.NewVarBinary(string(randomBytes())) }
func randomFloat64() sqltypes.Value {
	return sqltypes.NewFloat64(rand.NormFloat64())
}

func randomBytes() []byte {
	const Dictionary = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, 4+rand.Intn(256))
	for i := range b {
		b[i] = Dictionary[rand.Intn(len(Dictionary))]
	}
	return b
}

func randomJSON() sqltypes.Value {
	var j string
	switch rand.Intn(6) {
	case 0:
		j = "null"
	case 1:
		i := rand.Int63()
		if rand.Int()&0x1 == 1 {
			i = -i
		}
		j = strconv.FormatInt(i, 10)
	case 2:
		j = strconv.FormatFloat(rand.NormFloat64(), 'g', -1, 64)
	case 3:
		j = strconv.Quote(string(randomBytes()))
	case 4:
		j = "true"
	case 5:
		j = "false"
	}
	v, err := sqltypes.NewJSON(j)
	if err != nil {
		panic(err)
	}
	return v
}
