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

		// narrower integral types keep their own range
		{name: "tinyint max", value: sqltypes.NewFloat64(127.4), typ: sqltypes.Int8, expect: "127"},
		{name: "tinyint min", value: sqltypes.NewFloat64(-128), typ: sqltypes.Int8, expect: "-128"},
		{name: "tinyint too large", value: sqltypes.NewFloat64(300.4), typ: sqltypes.Int8, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "tinyint too small", value: sqltypes.NewFloat64(-129), typ: sqltypes.Int8, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "unsigned tinyint max", value: sqltypes.NewVarChar("255"), typ: sqltypes.Uint8, expect: "255"},
		{name: "unsigned tinyint too large", value: sqltypes.NewVarChar("256"), typ: sqltypes.Uint8, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "mediumint too large", value: sqltypes.NewDecimal("8388608"), typ: sqltypes.Int24, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "int too large", value: sqltypes.NewFloat64(3e9), typ: sqltypes.Int32, code: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "unsigned int max", value: sqltypes.NewFloat64(4294967295), typ: sqltypes.Uint32, expect: "4294967295"},

		// a value that isn't a number goes through the generic cast
		{name: "datetime", value: sqltypes.NewDatetime("2026-11-14 12:34:56"), typ: sqltypes.Int64, expect: "20261114123456"},
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

// TestCastToErrorValue checks that a binary value is escaped in the error.
func TestCastToErrorValue(t *testing.T) {
	_, err := CastTo(sqltypes.NewVarBinary("1\xff"), NewType(sqltypes.Int64, collations.CollationBinaryID), 0)
	require.EqualError(t, err, `Incorrect integer value: '1\xFF'`)

	_, err = CastTo(sqltypes.NewFloat64(-1), NewType(sqltypes.Uint64, collations.CollationBinaryID), 0)
	require.EqualError(t, err, `BIGINT UNSIGNED value is out of range in '-1'`)
}
