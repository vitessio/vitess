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
)

func TestEvalResultToBooleanStrict(t *testing.T) {
	trueValues := []eval{
		newEvalInt64(1),
		newEvalUint64(1),
	}

	falseValues := []eval{
		newEvalInt64(0),
		newEvalUint64(0),
	}

	invalid := []eval{
		newEvalText([]byte("foobar"), collationBinary),
		newEvalFloat(1.0),
		newEvalInt64(12),
	}

	for _, res := range trueValues {
		name := evalToSQLValue(res).String()
		t.Run(fmt.Sprintf("ToBooleanStrict() %s expected true (success)", name), func(t *testing.T) {
			result, err := (&EvalResult{res}).ToBooleanStrict()
			require.NoError(t, err, name)
			require.Equal(t, true, result, name)
		})
	}
	for _, res := range falseValues {
		name := evalToSQLValue(res).String()
		t.Run(fmt.Sprintf("ToBooleanStrict() %s expected false (success)", name), func(t *testing.T) {
			result, err := (&EvalResult{res}).ToBooleanStrict()
			require.NoError(t, err, name)
			require.Equal(t, false, result, name)
		})
	}
	for _, res := range invalid {
		name := evalToSQLValue(res).String()
		t.Run(fmt.Sprintf("ToBooleanStrict() %s  expected fail", name), func(t *testing.T) {
			_, err := (&EvalResult{res}).ToBooleanStrict()
			require.Error(t, err)
		})
	}
}
