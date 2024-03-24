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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

func TestEvalengineTypeAggregations(t *testing.T) {
	aggregationCases := []struct {
		types  []sqltypes.Type
		result sqltypes.Type
	}{
		{[]sqltypes.Type{sqltypes.Int64, sqltypes.Int32, sqltypes.Float64}, sqltypes.Float64},
		{[]sqltypes.Type{sqltypes.Int64, sqltypes.Decimal, sqltypes.Float64}, sqltypes.Float64},
		{[]sqltypes.Type{sqltypes.Int64, sqltypes.Int32, sqltypes.Decimal}, sqltypes.Decimal},
		{[]sqltypes.Type{sqltypes.Int64, sqltypes.Int32, sqltypes.Int64}, sqltypes.Int64},
		{[]sqltypes.Type{sqltypes.Int32, sqltypes.Int16, sqltypes.Int8}, sqltypes.Int32},
		{[]sqltypes.Type{sqltypes.Int32, sqltypes.Uint16, sqltypes.Uint8}, sqltypes.Int32},
		{[]sqltypes.Type{sqltypes.Int32, sqltypes.Uint16, sqltypes.Uint32}, sqltypes.Int64},
		{[]sqltypes.Type{sqltypes.Int32, sqltypes.Uint16, sqltypes.Uint64}, sqltypes.Decimal},
		{[]sqltypes.Type{sqltypes.Bit, sqltypes.Bit, sqltypes.Bit}, sqltypes.Bit},
		{[]sqltypes.Type{sqltypes.Bit, sqltypes.Int32, sqltypes.Float64}, sqltypes.Float64},
		{[]sqltypes.Type{sqltypes.Bit, sqltypes.Decimal, sqltypes.Float64}, sqltypes.Float64},
		{[]sqltypes.Type{sqltypes.Bit, sqltypes.Int32, sqltypes.Decimal}, sqltypes.Decimal},
		{[]sqltypes.Type{sqltypes.Bit, sqltypes.Int32, sqltypes.Int64}, sqltypes.Int64},
		{[]sqltypes.Type{sqltypes.Char, sqltypes.VarChar}, sqltypes.VarChar},
		{[]sqltypes.Type{sqltypes.Char, sqltypes.Char}, sqltypes.VarChar},
		{[]sqltypes.Type{sqltypes.Char, sqltypes.VarChar, sqltypes.VarBinary}, sqltypes.VarBinary},
		{[]sqltypes.Type{sqltypes.Char, sqltypes.Char, sqltypes.Set, sqltypes.Enum}, sqltypes.VarChar},
		{[]sqltypes.Type{sqltypes.TypeJSON, sqltypes.TypeJSON}, sqltypes.TypeJSON},
		{[]sqltypes.Type{sqltypes.Geometry, sqltypes.Geometry}, sqltypes.Geometry},
		{[]sqltypes.Type{sqltypes.Text, sqltypes.Text}, sqltypes.Blob},
		{[]sqltypes.Type{sqltypes.Blob, sqltypes.Blob}, sqltypes.Blob},
	}
	collEnv := collations.MySQL8()
	for i, tc := range aggregationCases {
		t.Run(fmt.Sprintf("%d.%v", i, tc.result), func(t *testing.T) {
			var typer TypeAggregator

			for _, tt := range tc.types {
				// this test only aggregates binary collations because textual collation
				// aggregation is tested in the `mysql/collations` package

				err := typer.Add(NewType(tt, collations.CollationBinaryID), collEnv)
				require.NoError(t, err)
			}

			res := typer.Type()
			require.Equalf(t, tc.result, res.Type(), "expected aggregate(%v) = %v, got %v", tc.types, tc.result, res.Type())
		})
	}
}
