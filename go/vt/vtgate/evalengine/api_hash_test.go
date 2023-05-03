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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vthash"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

func TestHashCodes(t *testing.T) {
	var cases = []struct {
		static, dynamic sqltypes.Value
		equal           bool
		err             error
	}{
		{sqltypes.NewInt64(-1), sqltypes.NewUint64(^uint64(0)), true, nil},
		{sqltypes.NewUint64(^uint64(0)), sqltypes.NewInt64(-1), true, nil},
		{sqltypes.NewFloat64(-1), sqltypes.NewVarChar("-1"), true, nil},
		{sqltypes.NewDecimal("-1"), sqltypes.NewVarChar("-1"), true, nil},
	}

	for _, tc := range cases {
		cmp, err := NullsafeCompare(tc.static, tc.dynamic, collations.CollationUtf8mb4ID)
		require.NoError(t, err)
		require.Equalf(t, tc.equal, cmp == 0, "got %v %s %v (expected %s)", tc.static, equality(cmp == 0).Operator(), tc.dynamic, equality(tc.equal))

		h1, err := NullsafeHashcode(tc.static, collations.CollationUtf8mb4ID, tc.static.Type())
		require.NoError(t, err)

		h2, err := NullsafeHashcode(tc.dynamic, collations.CollationUtf8mb4ID, tc.static.Type())
		require.ErrorIs(t, err, tc.err)

		assert.Equalf(t, tc.equal, h1 == h2, "HASH(%v) %s HASH(%v) (expected %s)", tc.static, equality(h1 == h2).Operator(), tc.dynamic, equality(tc.equal))
	}
}

// The following test tries to produce lots of different values and compares them both using hash code and compare,
// to make sure that these two methods agree on what values are equal
func TestHashCodesRandom(t *testing.T) {
	tested := 0
	equal := 0
	collation := collations.Local().LookupByName("utf8mb4_general_ci").ID()
	endTime := time.Now().Add(1 * time.Second)
	for time.Now().Before(endTime) {
		tested++
		v1, v2 := randomValues()
		cmp, err := NullsafeCompare(v1, v2, collation)
		require.NoErrorf(t, err, "%s compared with %s", v1.String(), v2.String())
		typ, err := CoerceTo(v1.Type(), v2.Type())
		require.NoError(t, err)

		hash1, err := NullsafeHashcode(v1, collation, typ)
		require.NoError(t, err)
		hash2, err := NullsafeHashcode(v2, collation, typ)
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
		{sqltypes.NewInt64(-1), sqltypes.NewUint64(^uint64(0)), true, nil},
		{sqltypes.NewUint64(^uint64(0)), sqltypes.NewInt64(-1), true, nil},
		{sqltypes.NewInt64(-1), sqltypes.NewVarChar("-1"), true, nil},
		{sqltypes.NewVarChar("-1"), sqltypes.NewInt64(-1), true, nil},
		{sqltypes.NewInt64(23), sqltypes.NewFloat64(23.0), true, nil},
		{sqltypes.NewInt64(23), sqltypes.NewFloat64(23.1), false, ErrHashCoercionIsNotExact},
		{sqltypes.NewUint64(^uint64(0)), sqltypes.NewFloat64(-1.0), false, ErrHashCoercionIsNotExact},
		{sqltypes.NewUint64(42), sqltypes.NewFloat64(42.0), true, nil},
	}

	for _, tc := range cases {
		cmp, err := NullsafeCompare(tc.static, tc.dynamic, collations.CollationUtf8mb4ID)
		require.NoError(t, err)
		require.Equalf(t, tc.equal, cmp == 0, "got %v %s %v (expected %s)", tc.static, equality(cmp == 0).Operator(), tc.dynamic, equality(tc.equal))

		hasher1 := vthash.New()
		err = NullsafeHashcode128(&hasher1, tc.static, collations.CollationUtf8mb4ID, tc.static.Type())
		require.NoError(t, err)

		hasher2 := vthash.New()
		err = NullsafeHashcode128(&hasher2, tc.dynamic, collations.CollationUtf8mb4ID, tc.static.Type())
		require.ErrorIs(t, err, tc.err)

		h1 := hasher1.Sum128()
		h2 := hasher2.Sum128()
		assert.Equalf(t, tc.equal, h1 == h2, "HASH(%v) %s HASH(%v) (expected %s)", tc.static, equality(h1 == h2).Operator(), tc.dynamic, equality(tc.equal))
	}
}

// The following test tries to produce lots of different values and compares them both using hash code and compare,
// to make sure that these two methods agree on what values are equal
func TestHashCodesRandom128(t *testing.T) {
	tested := 0
	equal := 0
	collation := collations.Local().LookupByName("utf8mb4_general_ci").ID()
	endTime := time.Now().Add(1 * time.Second)
	for time.Now().Before(endTime) {
		tested++
		v1, v2 := randomValues()
		cmp, err := NullsafeCompare(v1, v2, collation)
		require.NoErrorf(t, err, "%s compared with %s", v1.String(), v2.String())
		typ, err := CoerceTo(v1.Type(), v2.Type())
		require.NoError(t, err)

		hasher1 := vthash.New()
		err = NullsafeHashcode128(&hasher1, v1, collation, typ)
		require.NoError(t, err)
		hasher2 := vthash.New()
		err = NullsafeHashcode128(&hasher2, v2, collation, typ)
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

func randomValues() (sqltypes.Value, sqltypes.Value) {
	if rand.Int()%2 == 0 {
		// create a single value, and turn it into two different types
		v := rand.Int()
		return randomNumericType(v), randomNumericType(v)
	}

	// just produce two arbitrary random values and compare
	return randomValue(), randomValue()
}

func randomNumericType(i int) sqltypes.Value {
	r := rand.Intn(len(numericTypes))
	return numericTypes[r](i)

}

var numericTypes = []func(int) sqltypes.Value{
	func(i int) sqltypes.Value { return sqltypes.NULL },
	func(i int) sqltypes.Value { return sqltypes.NewInt8(int8(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewInt32(int32(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewInt64(int64(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewUint64(uint64(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewUint32(uint32(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewFloat64(float64(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewDecimal(fmt.Sprintf("%d", i)) },
	func(i int) sqltypes.Value { return sqltypes.NewVarChar(fmt.Sprintf("%d", i)) },
	func(i int) sqltypes.Value { return sqltypes.NewVarChar(fmt.Sprintf("  %f aa", float64(i))) },
}

var randomGenerators = []func() sqltypes.Value{
	randomNull,
	randomInt8,
	randomInt32,
	randomInt64,
	randomUint64,
	randomUint32,
	randomVarChar,
	randomComplexVarChar,
	randomDecimal,
}

func randomValue() sqltypes.Value {
	r := rand.Intn(len(randomGenerators))
	return randomGenerators[r]()
}

func randomNull() sqltypes.Value    { return sqltypes.NULL }
func randomInt8() sqltypes.Value    { return sqltypes.NewInt8(int8(rand.Intn(255))) }
func randomInt32() sqltypes.Value   { return sqltypes.NewInt32(rand.Int31()) }
func randomInt64() sqltypes.Value   { return sqltypes.NewInt64(rand.Int63()) }
func randomUint32() sqltypes.Value  { return sqltypes.NewUint32(rand.Uint32()) }
func randomUint64() sqltypes.Value  { return sqltypes.NewUint64(rand.Uint64()) }
func randomDecimal() sqltypes.Value { return sqltypes.NewDecimal(fmt.Sprintf("%d", rand.Int63())) }
func randomVarChar() sqltypes.Value { return sqltypes.NewVarChar(fmt.Sprintf("%d", rand.Int63())) }

func randomComplexVarChar() sqltypes.Value {
	return sqltypes.NewVarChar(fmt.Sprintf(" \t %f apa", float64(rand.Intn(1000))*1.10))
}
