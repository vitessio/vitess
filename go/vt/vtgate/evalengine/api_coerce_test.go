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

package evalengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// TestCastToIntegral checks that CastTo rounds non-integers like MySQL stores them,
// keeps integers exact, also above 2^53 where a float64 would round them,
// and fails for the values that MySQL rejects in strict mode.
func TestCastToIntegral(t *testing.T) {
	testcases := []struct {
		name   string
		value  sqltypes.Value
		typ    sqltypes.Type
		expect string
		code   vtrpcpb.Code
	}{
		{name: "float", value: sqltypes.NewFloat64(1.5), typ: sqltypes.Int64, expect: "2"},
		{name: "fractional text", value: sqltypes.NewVarChar("1.5"), typ: sqltypes.Int64, expect: "2"},
		{name: "fractional binary", value: sqltypes.NewVarBinary("1.5"), typ: sqltypes.Int64, expect: "2"},
		{name: "large signed text", value: sqltypes.NewVarChar("9007199254740993"), typ: sqltypes.Int64, expect: "9007199254740993"},
		{name: "large negative text", value: sqltypes.NewVarChar("-9007199254740993"), typ: sqltypes.Int64, expect: "-9007199254740993"},
		{name: "large unsigned text", value: sqltypes.NewVarChar("18446744073709551615"), typ: sqltypes.Uint64, expect: "18446744073709551615"},
		{name: "large binary", value: sqltypes.NewVarBinary("9007199254740993"), typ: sqltypes.Int64, expect: "9007199254740993"},

		// a double rounds half to even
		{name: "double 2.5", value: sqltypes.NewFloat64(2.5), typ: sqltypes.Int64, expect: "2"},
		{name: "double 0.5", value: sqltypes.NewFloat64(0.5), typ: sqltypes.Int64, expect: "0"},
		{name: "double -2.5", value: sqltypes.NewFloat64(-2.5), typ: sqltypes.Int64, expect: "-2"},
		// a decimal and a string round half away from zero
		{name: "decimal 2.5", value: sqltypes.NewDecimal("2.5"), typ: sqltypes.Int64, expect: "3"},
		{name: "decimal -2.5", value: sqltypes.NewDecimal("-2.5"), typ: sqltypes.Int64, expect: "-3"},
		{name: "text 2.5", value: sqltypes.NewVarChar("2.5"), typ: sqltypes.Int64, expect: "3"},

		// MySQL fails these with errors 1366, 1265 and 1264
		{name: "not a number", value: sqltypes.NewVarChar("abc"), typ: sqltypes.Int64, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "empty text", value: sqltypes.NewVarChar(""), typ: sqltypes.Int64, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "trailing text", value: sqltypes.NewVarChar("12abc"), typ: sqltypes.Int64, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "too large", value: sqltypes.NewFloat64(1e30), typ: sqltypes.Int64, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "too large for signed", value: sqltypes.NewUint64(9223372036854775808), typ: sqltypes.Int64, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "negative under unsigned", value: sqltypes.NewFloat64(-2.5), typ: sqltypes.Uint64, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "negative integer under unsigned", value: sqltypes.NewInt64(-1), typ: sqltypes.Uint64, code: vtrpcpb.Code_INVALID_ARGUMENT},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CastTo(tc.value, NewType(tc.typ, collations.CollationBinaryID), 0)
			if tc.code != vtrpcpb.Code_OK {
				require.Error(t, err)
				assert.Equal(t, tc.code, vterrors.Code(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expect, got.ToString())
			assert.Equal(t, tc.typ, got.Type())
		})
	}
}
