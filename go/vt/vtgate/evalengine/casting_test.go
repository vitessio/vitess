/*
Copyright 2020 The Vitess Authors.

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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestEvalResultToBooleanStrict(t *testing.T) {
	trueValues := []*EvalResult{{
		typ3:    sqltypes.Int64,
		numval3: 1,
	}, {
		typ3:    sqltypes.Uint64,
		numval3: 1,
	}, {
		typ3:    sqltypes.Int8,
		numval3: 1,
	}}

	falseValues := []*EvalResult{{
		typ3:    sqltypes.Int64,
		numval3: 0,
	}, {
		typ3:    sqltypes.Uint64,
		numval3: 0,
	}, {
		typ3:    sqltypes.Int8,
		numval3: 0,
	}}

	invalid := []*EvalResult{{
		typ3:   sqltypes.VarChar,
		bytes3: []byte("foobar"),
	}, {
		typ3:    sqltypes.Float32,
		numval3: math.Float64bits(1.0),
	}, {
		typ3:    sqltypes.Int64,
		numval3: 12,
	}}

	for _, res := range trueValues {
		name := res.debugString()
		t.Run(fmt.Sprintf("ToBooleanStrict() %s expected true (success)", name), func(t *testing.T) {
			result, err := res.ToBooleanStrict()
			require.NoError(t, err, name)
			require.Equal(t, true, result, name)
		})
	}
	for _, res := range falseValues {
		name := res.debugString()
		t.Run(fmt.Sprintf("ToBooleanStrict() %s expected false (success)", name), func(t *testing.T) {
			result, err := res.ToBooleanStrict()
			require.NoError(t, err, name)
			require.Equal(t, false, result, name)
		})
	}
	for _, res := range invalid {
		name := res.debugString()
		t.Run(fmt.Sprintf("ToBooleanStrict() %s  expected fail", name), func(t *testing.T) {
			_, err := res.ToBooleanStrict()
			require.Error(t, err)
		})
	}
}
