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

package engine

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestComparer(t *testing.T) {
	tests := []struct {
		comparer comparer
		row1     []sqltypes.Value
		row2     []sqltypes.Value
		output   int
	}{
		{
			comparer: comparer{
				orderBy:      0,
				weightString: -1,
				desc:         true,
			},
			row1: []sqltypes.Value{
				sqltypes.NewInt64(23),
			},
			row2: []sqltypes.Value{
				sqltypes.NewInt64(34),
			},
			output: 1,
		}, {
			comparer: comparer{
				orderBy:      0,
				weightString: -1,
				desc:         false,
			},
			row1: []sqltypes.Value{
				sqltypes.NewInt64(23),
			},
			row2: []sqltypes.Value{
				sqltypes.NewInt64(23),
			},
			output: 0,
		}, {
			comparer: comparer{
				orderBy:      0,
				weightString: -1,
				desc:         false,
			},
			row1: []sqltypes.Value{
				sqltypes.NewInt64(23),
			},
			row2: []sqltypes.Value{
				sqltypes.NewInt64(12),
			},
			output: 1,
		}, {
			comparer: comparer{
				orderBy:      1,
				weightString: 0,
				desc:         false,
			},
			row1: []sqltypes.Value{
				sqltypes.NewInt64(23),
				sqltypes.NewVarChar("b"),
			},
			row2: []sqltypes.Value{
				sqltypes.NewInt64(34),
				sqltypes.NewVarChar("a"),
			},
			output: -1,
		}, {
			comparer: comparer{
				orderBy:      1,
				weightString: 0,
				desc:         true,
			},
			row1: []sqltypes.Value{
				sqltypes.NewInt64(23),
				sqltypes.NewVarChar("A"),
			},
			row2: []sqltypes.Value{
				sqltypes.NewInt64(23),
				sqltypes.NewVarChar("a"),
			},
			output: 0,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got, err := test.comparer.compare(test.row1, test.row2)
			require.NoError(t, err)
			require.Equal(t, test.output, got)
		})
	}
}
