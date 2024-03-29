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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

func TestHashCodes(t *testing.T) {
	var cases = []struct {
		static, dynamic sqltypes.Value
		equal           bool
		err             error
	}{
		{sqltypes.NewFloat64(-1), sqltypes.NewVarChar("-1"), true, nil},
		{sqltypes.NewDecimal("-1"), sqltypes.NewVarChar("-1"), true, nil},
		{sqltypes.NewDate("2000-01-01"), sqltypes.NewInt64(20000101), true, nil},
		{sqltypes.NewDatetime("2000-01-01 11:22:33"), sqltypes.NewInt64(20000101112233), true, nil},
		{sqltypes.NewTime("11:22:33"), sqltypes.NewInt64(112233), true, nil},
		{sqltypes.NewInt64(20000101), sqltypes.NewDate("2000-01-01"), true, nil},
		{sqltypes.NewInt64(20000101112233), sqltypes.NewDatetime("2000-01-01 11:22:33"), true, nil},
		{sqltypes.NewInt64(112233), sqltypes.NewTime("11:22:33"), true, nil},
		{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"1": "foo", "2": "bar"}`)), sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"2": "bar", "1": "foo"}`)), true, nil},
		{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"1": "foo", "2": "bar"}`)), sqltypes.NewVarChar(`{"2": "bar", "1": "foo"}`), false, nil},
		{sqltypes.NewVarChar(`{"2": "bar", "1": "foo"}`), sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"1": "foo", "2": "bar"}`)), false, nil},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v %s %v", tc.static, equality(tc.equal).Operator(), tc.dynamic), func(t *testing.T) {
			cmp, err := NullsafeCompare(tc.static, tc.dynamic, collations.MySQL8(), collations.CollationUtf8mb4ID)
			require.NoError(t, err)
			require.Equalf(t, tc.equal, cmp == 0, "got %v %s %v (expected %s)", tc.static, equality(cmp == 0).Operator(), tc.dynamic, equality(tc.equal))

			h1, err := NullsafeHashcode(tc.static, collations.CollationUtf8mb4ID, tc.static.Type(), 0)
			require.NoError(t, err)

			h2, err := NullsafeHashcode(tc.dynamic, collations.CollationUtf8mb4ID, tc.static.Type(), 0)
			require.ErrorIs(t, err, tc.err)

			assert.Equalf(t, tc.equal, h1 == h2, "HASH(%v) %s HASH(%v) (expected %s)", tc.static, equality(h1 == h2).Operator(), tc.dynamic, equality(tc.equal))
		})
	}
}

// The following test tries to produce lots of different values and compares them both using hash code and compare,
// to make sure that these two methods agree on what values are equal
func TestHashCodesRandom(t *testing.T) {
	tested := 0
	equal := 0
	collation := collations.MySQL8().LookupByName("utf8mb4_general_ci")
	endTime := time.Now().Add(1 * time.Second)
	for time.Now().Before(endTime) {
		tested++
		v1, v2 := sqltypes.TestRandomValues()
		cmp, err := NullsafeCompare(v1, v2, collations.MySQL8(), collation)
		require.NoErrorf(t, err, "%s compared with %s", v1.String(), v2.String())
		typ, err := coerceTo(v1.Type(), v2.Type())
		require.NoError(t, err)

		hash1, err := NullsafeHashcode(v1, collation, typ, 0)
		require.NoError(t, err)
		hash2, err := NullsafeHashcode(v2, collation, typ, 0)
		require.NoError(t, err)
		if cmp == 0 {
			equal++
			require.Equalf(t, hash1, hash2, "values %s and %s are considered equal but produce different hash codes: %d & %d (%v)", v1.String(), v2.String(), hash1, hash2, typ)
		}
	}
	t.Logf("tested %d values, with %d equalities found\n", tested, equal)
}

type equality bool

func (e equality) Operator() string {
	if e {
		return "="
	}
	return "!="
}

func (e equality) String() string {
	if e {
		return "equal"
	}
	return "not equal"
}

func TestHashCodes128(t *testing.T) {
	var cases = []struct {
		static, dynamic sqltypes.Value
		equal           bool
		err             error
	}{
		{sqltypes.NewInt64(-1), sqltypes.NewUint64(^uint64(0)), false, nil},
		{sqltypes.NewUint64(^uint64(0)), sqltypes.NewInt64(-1), false, nil},
		{sqltypes.NewInt64(-1), sqltypes.NewVarChar("-1"), true, nil},
		{sqltypes.NewVarChar("-1"), sqltypes.NewInt64(-1), true, nil},
		{sqltypes.NewInt64(23), sqltypes.NewFloat64(23.0), true, nil},
		{sqltypes.NewInt64(23), sqltypes.NewFloat64(23.1), false, ErrHashCoercionIsNotExact},
		{sqltypes.NewUint64(^uint64(0)), sqltypes.NewFloat64(-1.0), false, ErrHashCoercionIsNotExact},
		{sqltypes.NewUint64(42), sqltypes.NewFloat64(42.0), true, nil},
		{sqltypes.NewDate("2000-01-01"), sqltypes.NewInt64(20000101), true, nil},
		{sqltypes.NewDatetime("2000-01-01 11:22:33"), sqltypes.NewInt64(20000101112233), true, nil},
		{sqltypes.NewTime("11:22:33"), sqltypes.NewInt64(112233), true, nil},
		{sqltypes.NewInt64(20000101), sqltypes.NewDate("2000-01-01"), true, nil},
		{sqltypes.NewInt64(20000101112233), sqltypes.NewDatetime("2000-01-01 11:22:33"), true, nil},
		{sqltypes.NewInt64(112233), sqltypes.NewTime("11:22:33"), true, nil},
		{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"1": "foo", "2": "bar"}`)), sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"2": "bar", "1": "foo"}`)), true, nil},
		{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"1": "foo", "2": "bar"}`)), sqltypes.NewVarChar(`{"2": "bar", "1": "foo"}`), false, nil},
		{sqltypes.NewVarChar(`{"2": "bar", "1": "foo"}`), sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"1": "foo", "2": "bar"}`)), false, nil},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v %s %v", tc.static, equality(tc.equal).Operator(), tc.dynamic), func(t *testing.T) {
			cmp, err := NullsafeCompare(tc.static, tc.dynamic, collations.MySQL8(), collations.CollationUtf8mb4ID)
			require.NoError(t, err)
			require.Equalf(t, tc.equal, cmp == 0, "got %v %s %v (expected %s)", tc.static, equality(cmp == 0).Operator(), tc.dynamic, equality(tc.equal))

			hasher1 := vthash.New()
			err = NullsafeHashcode128(&hasher1, tc.static, collations.CollationUtf8mb4ID, tc.static.Type(), 0)
			require.NoError(t, err)

			hasher2 := vthash.New()
			err = NullsafeHashcode128(&hasher2, tc.dynamic, collations.CollationUtf8mb4ID, tc.static.Type(), 0)
			require.ErrorIs(t, err, tc.err)

			h1 := hasher1.Sum128()
			h2 := hasher2.Sum128()
			assert.Equalf(t, tc.equal, h1 == h2, "HASH(%v) %s HASH(%v) (expected %s)", tc.static, equality(h1 == h2).Operator(), tc.dynamic, equality(tc.equal))
		})
	}
}

// The following test tries to produce lots of different values and compares them both using hash code and compare,
// to make sure that these two methods agree on what values are equal
func TestHashCodesRandom128(t *testing.T) {
	tested := 0
	equal := 0
	collation := collations.MySQL8().LookupByName("utf8mb4_general_ci")
	endTime := time.Now().Add(1 * time.Second)
	for time.Now().Before(endTime) {
		tested++
		v1, v2 := sqltypes.TestRandomValues()
		cmp, err := NullsafeCompare(v1, v2, collations.MySQL8(), collation)
		require.NoErrorf(t, err, "%s compared with %s", v1.String(), v2.String())
		typ, err := coerceTo(v1.Type(), v2.Type())
		require.NoError(t, err)

		hasher1 := vthash.New()
		err = NullsafeHashcode128(&hasher1, v1, collation, typ, 0)
		require.NoError(t, err)
		hasher2 := vthash.New()
		err = NullsafeHashcode128(&hasher2, v2, collation, typ, 0)
		require.NoError(t, err)
		if cmp == 0 {
			equal++
			hash1 := hasher1.Sum128()
			hash2 := hasher2.Sum128()
			require.Equalf(t, hash1, hash2, "values %s and %s are considered equal but produce different hash codes: %x & %x (%v)", v1.String(), v2.String(), hash1, hash2, typ)
		}
	}
	t.Logf("tested %d values, with %d equalities found\n", tested, equal)
}

// coerceTo takes two input types, and decides how they should be coerced before compared
func coerceTo(v1, v2 sqltypes.Type) (sqltypes.Type, error) {
	if v1 == v2 {
		return v1, nil
	}
	if sqltypes.IsNull(v1) || sqltypes.IsNull(v2) {
		return sqltypes.Null, nil
	}
	if (sqltypes.IsTextOrBinary(v1)) && (sqltypes.IsTextOrBinary(v2)) {
		return sqltypes.VarChar, nil
	}
	if sqltypes.IsDateOrTime(v1) {
		return v1, nil
	}
	if sqltypes.IsDateOrTime(v2) {
		return v2, nil
	}

	if sqltypes.IsNumber(v1) || sqltypes.IsNumber(v2) {
		switch {
		case sqltypes.IsTextOrBinary(v1) || sqltypes.IsTextOrBinary(v2):
			return sqltypes.Float64, nil
		case sqltypes.IsFloat(v2) || v2 == sqltypes.Decimal || sqltypes.IsFloat(v1) || v1 == sqltypes.Decimal:
			return sqltypes.Float64, nil
		case sqltypes.IsSigned(v1):
			switch {
			case sqltypes.IsUnsigned(v2):
				return sqltypes.Uint64, nil
			case sqltypes.IsSigned(v2):
				return sqltypes.Int64, nil
			default:
				return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
			}
		case sqltypes.IsUnsigned(v1):
			switch {
			case sqltypes.IsSigned(v2) || sqltypes.IsUnsigned(v2):
				return sqltypes.Uint64, nil
			default:
				return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
			}
		}
	}
	return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
}
