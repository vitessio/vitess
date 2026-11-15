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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vthash"
)

func TestTinyWeightStrings(t *testing.T) {
	const Length = 10000

	cases := []struct {
		typ    sqltypes.Type
		gen    func() sqltypes.Value
		col    collations.ID
		len    int
		prec   int
		values *EnumSetValues
	}{
		{typ: sqltypes.Int32, gen: sqltypes.RandomGenerators[sqltypes.Int32], col: collations.CollationBinaryID},
		{typ: sqltypes.Int64, gen: sqltypes.RandomGenerators[sqltypes.Int64], col: collations.CollationBinaryID},
		{typ: sqltypes.Uint32, gen: sqltypes.RandomGenerators[sqltypes.Uint32], col: collations.CollationBinaryID},
		{typ: sqltypes.Uint64, gen: sqltypes.RandomGenerators[sqltypes.Uint64], col: collations.CollationBinaryID},
		{typ: sqltypes.Float64, gen: sqltypes.RandomGenerators[sqltypes.Float64], col: collations.CollationBinaryID},
		{typ: sqltypes.VarChar, gen: sqltypes.RandomGenerators[sqltypes.VarChar], col: collations.CollationUtf8mb4ID},
		{typ: sqltypes.VarBinary, gen: sqltypes.RandomGenerators[sqltypes.VarBinary], col: collations.CollationBinaryID},
		{typ: sqltypes.Decimal, gen: sqltypes.RandomGenerators[sqltypes.Decimal], col: collations.CollationBinaryID, len: 20, prec: 10},
		{typ: sqltypes.TypeJSON, gen: sqltypes.RandomGenerators[sqltypes.TypeJSON], col: collations.CollationBinaryID},
		{typ: sqltypes.Enum, gen: sqltypes.RandomGenerators[sqltypes.Enum], col: collations.CollationBinaryID, values: &EnumSetValues{"'xxsmall'", "'xsmall'", "'small'", "'medium'", "'large'", "'xlarge'", "'xxlarge'"}},
		{typ: sqltypes.Set, gen: sqltypes.RandomGenerators[sqltypes.Set], col: collations.CollationBinaryID, values: &EnumSetValues{"'a'", "'b'", "'c'", "'d'", "'e'", "'f'", "'g'"}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v", tc.typ), func(t *testing.T) {
			field := &querypb.Field{
				Type:         tc.typ,
				ColumnLength: uint32(tc.len),
				Charset:      uint32(tc.col),
				Decimals:     uint32(tc.prec),
			}
			weight := TinyWeighter(field, collations.MySQL8(), tc.col)
			require.NotNil(t, weight, "could not generate Tiny Weight function")

			items := make([]sqltypes.Value, 0, Length)
			for range Length {
				v := tc.gen()
				weight(&v)
				items = append(items, v)
			}

			var fastComparisons int
			var fullComparisons int
			slices.SortFunc(items, func(a, b sqltypes.Value) int {
				if cmp := a.TinyWeightCmp(b); cmp != 0 {
					fastComparisons++
					return cmp
				}

				cmp, err := NullsafeCompare(a, b, collations.MySQL8(), tc.col, tc.values)
				require.NoError(t, err)

				fullComparisons++
				return cmp
			})

			for i := range Length - 1 {
				a := items[i]
				b := items[i+1]

				cmp, err := NullsafeCompare(a, b, collations.MySQL8(), tc.col, tc.values)
				require.NoError(t, err)

				if cmp > 0 {
					require.Failf(t, "out of order", "expected %v [pos=%d] to come after %v [pos=%d]\n%v | %032b\n%v | %032b", a, i, b, i+1, a, a.TinyWeight(), b, b.TinyWeight())
				}
			}

			t.Logf("%d fast comparisons, %d full comparisons (%.02f%% were fast)", fastComparisons, fullComparisons, 100.0*float64(fastComparisons)/float64(fastComparisons+fullComparisons))
		})
	}
}

func TestWeightStrings(t *testing.T) {
	const Length = 1000

	type item struct {
		value  sqltypes.Value
		weight string
	}

	cases := []struct {
		name   string
		gen    func() sqltypes.Value
		types  []sqltypes.Type
		col    collations.ID
		len    int
		prec   int
		values *EnumSetValues
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
		{name: "enum", gen: sqltypes.RandomGenerators[sqltypes.Enum], types: []sqltypes.Type{sqltypes.Enum, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID, values: &EnumSetValues{"'xxsmall'", "'xsmall'", "'small'", "'medium'", "'large'", "'xlarge'", "'xxlarge'"}},
		{name: "set", gen: sqltypes.RandomGenerators[sqltypes.Set], types: []sqltypes.Type{sqltypes.Set, sqltypes.VarChar, sqltypes.TypeJSON}, col: collations.CollationBinaryID, values: &EnumSetValues{"'a'", "'b'", "'c'", "'d'", "'e'", "'f'", "'g'"}},
	}

	for _, tc := range cases {
		for _, typ := range tc.types {
			t.Run(fmt.Sprintf("%s/%v", tc.name, typ), func(t *testing.T) {
				items := make([]item, 0, Length)
				for range Length {
					v := tc.gen()
					w, _, err := WeightString(nil, v, typ, collations.MySQL8(), tc.col, tc.len, tc.prec, tc.values, 0)
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

				for i := range Length - 1 {
					a := items[i]
					b := items[i+1]

					v1, err := valueToEvalCast(a.value, typ, tc.col, tc.values, 0)
					require.NoError(t, err)
					v2, err := valueToEvalCast(b.value, typ, tc.col, tc.values, 0)
					require.NoError(t, err)

					cmp, err := evalCompareNullSafe(v1, v2, collations.MySQL8())
					require.NoError(t, err)

					if cmp > 0 {
						require.Failf(t, "out of order", "expected %v [pos=%d] to come after %v [pos=%d]\nav = %v\nbv = %v",
							a.value, i, b.value, i+1,
							[]byte(a.weight), []byte(b.weight),
						)
					}
				}
			})
		}
	}
}

// TestWeightStringsRowCharset takes the weight and the hash of text of
// columns that are not utf8mb4, for an environment whose rows are in utf8mb4,
// as the ones of a vtgate with --convert-result-charset are. The values have
// to be converted to the character set of the collation first. The expected
// weights are WEIGHT_STRING() of MySQL 8.0.46 and 8.4.13: 'é' in
// latin1_swedish_ci has the weight of 'E', 45, where its two utf8mb4 bytes
// taken as latin1 have the weights 41 A9.
//
// For an environment with the row character set binary, which is the default,
// the text is in the character set of its collation already, as VReplication
// and VDiff hand it over, and is not converted: the same bytes get the weight
// and the hash of the latin1 text they are.
func TestWeightStringsRowCharset(t *testing.T) {
	tcases := []struct {
		value     string
		collation string
		weight    string
		sameAs    string
		// unconverted is the weight of the bytes of value taken as text of
		// the collation, and empty when they are not valid text of it.
		unconverted string
	}{
		{"é", "latin1_swedish_ci", "\x45", "E", "\x41\xa9"},
		{"Müller", "latin1_swedish_ci", "MYLLER", "MYLLER", "MA\xbcLLER"},
		{"中", "gb2312_chinese_ci", "\xd6\xd0", "中", ""},
	}
	plain := collations.MySQL8()
	conn := plain.WithRowCharset(collations.CollationUtf8mb4ID)
	for _, tcase := range tcases {
		t.Run(tcase.value+"/"+tcase.collation, func(t *testing.T) {
			collation := plain.LookupByName(tcase.collation)
			value := sqltypes.NewVarChar(tcase.value)
			sameAs := sqltypes.NewVarChar(tcase.sameAs)

			weight, _, err := WeightString(nil, value, sqltypes.VarChar, conn, collation, 0, 0, nil, 0)
			require.NoError(t, err)
			assert.Equal(t, tcase.weight, string(weight))

			if tcase.unconverted != "" {
				weight, _, err = WeightString(nil, value, sqltypes.VarChar, plain, collation, 0, 0, nil, 0)
				require.NoError(t, err)
				assert.Equal(t, tcase.unconverted, string(weight))
			}

			if tiny := TinyWeighter(&querypb.Field{Type: sqltypes.VarChar}, conn, collation); tiny != nil {
				tiny(&value)
				tiny(&sameAs)
				assert.Equal(t, sameAs.TinyWeight(), value.TinyWeight())
			}

			var h1, h2, h3 vthash.Hasher
			require.NoError(t, NullsafeHashcode128(&h1, value, conn, collation, sqltypes.VarChar, 0, nil))
			require.NoError(t, NullsafeHashcode128(&h2, sameAs, conn, collation, sqltypes.VarChar, 0, nil))
			assert.Equal(t, h2.Sum128(), h1.Sum128())
			if tcase.value != tcase.sameAs {
				require.NoError(t, NullsafeHashcode128(&h3, value, plain, collation, sqltypes.VarChar, 0, nil))
				assert.NotEqual(t, h2.Sum128(), h3.Sum128())
			}
		})
	}
}
