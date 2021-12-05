/*
Copyright 2021 The Vitess Authors.

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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

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
			require.Equalf(t, hash1, hash2, "values %s and %s are considered equal but produce different hash codes: %d & %d", v1.String(), v2.String(), hash1, hash2)
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
func randomVarChar() sqltypes.Value { return sqltypes.NewVarChar(fmt.Sprintf("%d", rand.Int63())) }
func randomComplexVarChar() sqltypes.Value {
	return sqltypes.NewVarChar(fmt.Sprintf(" \t %f apa", float64(rand.Intn(1000))*1.10))
}
