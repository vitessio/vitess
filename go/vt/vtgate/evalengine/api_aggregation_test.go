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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestMinMax(t *testing.T) {
	tcases := []struct {
		type_    sqltypes.Type
		coll     collations.ID
		values   []sqltypes.Value
		min, max sqltypes.Value
		err      error
	}{
		{
			type_:  sqltypes.Int64,
			values: []sqltypes.Value{},
			min:    sqltypes.NULL,
			max:    sqltypes.NULL,
		},
		{
			type_:  sqltypes.Int64,
			values: []sqltypes.Value{NULL, NULL},
			min:    sqltypes.NULL,
			max:    sqltypes.NULL,
		},
		{
			type_:  sqltypes.Int64,
			values: []sqltypes.Value{NULL, NewInt64(1)},
			min:    NewInt64(1),
			max:    NewInt64(1),
		},
		{
			type_:  sqltypes.Int64,
			values: []sqltypes.Value{NewInt64(1), NewInt64(2)},
			min:    NewInt64(1),
			max:    NewInt64(2),
		},
		{
			type_:  sqltypes.VarChar,
			values: []sqltypes.Value{TestValue(sqltypes.VarChar, "aa"), TestValue(sqltypes.VarChar, "bb")},
			err:    vterrors.New(vtrpcpb.Code_UNKNOWN, "cannot compare strings, collation is unknown or unsupported (collation ID: 0)"),
		},
		{
			type_:  sqltypes.VarBinary,
			values: []sqltypes.Value{sqltypes.NewVarBinary("a"), sqltypes.NewVarBinary("b")},
			min:    sqltypes.NewVarBinary("a"),
			max:    sqltypes.NewVarBinary("b"),
		},
		{
			type_:  sqltypes.Decimal,
			values: []sqltypes.Value{sqltypes.NewDecimal("1.001"), sqltypes.NewDecimal("2.1")},
			min:    sqltypes.NewDecimal("1.001"),
			max:    sqltypes.NewDecimal("2.1"),
		},
		{
			// accent insensitive
			type_: sqltypes.VarChar,
			coll:  getCollationID("utf8mb4_0900_as_ci"),
			values: []sqltypes.Value{
				sqltypes.NewVarChar("ǍḄÇ"),
				sqltypes.NewVarChar("ÁḆĈ"),
			},
			min: sqltypes.NewVarChar("ÁḆĈ"),
			max: sqltypes.NewVarChar("ǍḄÇ"),
		},
		{
			// kana sensitive
			type_: sqltypes.VarChar,
			coll:  getCollationID("utf8mb4_ja_0900_as_cs_ks"),
			values: []sqltypes.Value{
				sqltypes.NewVarChar("\xE3\x81\xAB\xE3\x81\xBB\xE3\x82\x93\xE3\x81\x94"),
				sqltypes.NewVarChar("\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4"),
			},
			min: sqltypes.NewVarChar("\xE3\x81\xAB\xE3\x81\xBB\xE3\x82\x93\xE3\x81\x94"),
			max: sqltypes.NewVarChar("\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\xB3\xE3\x82\xB4"),
		},
		{
			// non breaking space
			type_: sqltypes.VarChar,
			coll:  getCollationID("utf8mb4_0900_as_cs"),
			values: []sqltypes.Value{
				sqltypes.NewVarChar("abc "),
				sqltypes.NewVarChar("abc\u00a0"),
			},
			min: sqltypes.NewVarChar("abc "),
			max: sqltypes.NewVarChar("abc\u00a0"),
		},
		{
			type_: sqltypes.VarChar,
			coll:  getCollationID("utf8mb4_hu_0900_ai_ci"),
			// "cs" counts as a separate letter, where c < cs < d
			values: []sqltypes.Value{
				sqltypes.NewVarChar("c"),
				sqltypes.NewVarChar("cs"),
			},
			min: sqltypes.NewVarChar("c"),
			max: sqltypes.NewVarChar("cs"),
		},
		{
			type_: sqltypes.VarChar,
			coll:  getCollationID("utf8mb4_hu_0900_ai_ci"),
			// "cs" counts as a separate letter, where c < cs < d
			values: []sqltypes.Value{
				sqltypes.NewVarChar("cukor"),
				sqltypes.NewVarChar("csak"),
			},
			min: sqltypes.NewVarChar("cukor"),
			max: sqltypes.NewVarChar("csak"),
		},
	}
	for i, tcase := range tcases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Run("Min", func(t *testing.T) {
				agg := NewAggregationMinMax(tcase.type_, collations.MySQL8(), tcase.coll)

				for _, v := range tcase.values {
					err := agg.Min(v)
					if err != nil {
						if tcase.err != nil {
							return
						}
						require.NoError(t, err)
					}
				}

				utils.MustMatch(t, agg.Result(), tcase.min)
			})

			t.Run("Max", func(t *testing.T) {
				agg := NewAggregationMinMax(tcase.type_, collations.MySQL8(), tcase.coll)

				for _, v := range tcase.values {
					err := agg.Max(v)
					if err != nil {
						if tcase.err != nil {
							return
						}
						require.NoError(t, err)
					}
				}

				utils.MustMatch(t, agg.Result(), tcase.max)
			})
		})
	}
}
