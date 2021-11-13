/*
Copyright 2019 The Vitess Authors.

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

func TestHashCodesRandom(t *testing.T) {
	tested := 0
	equal := 0
	endTime := time.Now().Add(1 * time.Second) // run the test for 10 seconds
	for time.Now().Before(endTime) {
		t.Run(fmt.Sprintf("%d", tested), func(t *testing.T) {
			tested++
			v1, v2 := randomValues()
			cmp, err := NullsafeCompare(v1, v2, collations.Unknown)
			require.NoErrorf(t, err, "%s compared with %s", v1.String(), v2.String())
			hash1, err := NullsafeHashcode(v1, collations.Unknown)
			require.NoError(t, err, "hashCode for "+v1.String())
			hash2, err := NullsafeHashcode(v2, collations.Unknown)
			require.NoError(t, err, "hashCode for "+v2.String())
			if cmp == 0 {
				equal++
				require.Equalf(t, hash1, hash2, "values %s and %s are considered equal but produce different hash codes: %d & %d", v1.String(), v2.String(), hash1, hash2)
			}
		})
	}
	fmt.Printf("tested %d values, with %d equalities found\n", tested, equal)
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
	func(i int) sqltypes.Value { return sqltypes.NewInt8(int8(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewInt32(int32(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewInt64(int64(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewFloat64(float64(i)) },
	func(i int) sqltypes.Value { return sqltypes.NewDecimal(fmt.Sprintf("%d", i)) },
}

var randomGenerators = []func() sqltypes.Value{
	randomNull,
	randomInt8,
	randomInt32,
}

func randomValue() sqltypes.Value {
	r := rand.Intn(len(randomGenerators))
	return randomGenerators[r]()
}

func randomNull() sqltypes.Value {
	return sqltypes.NULL
}

func randomInt8() sqltypes.Value {
	return sqltypes.NewInt8(int8(rand.Intn(255)))
}

func randomInt32() sqltypes.Value {
	return sqltypes.NewInt32(rand.Int31())
}
