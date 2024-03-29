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

package sqltypes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var TestRows = []string{
	"[]",
	"[[INT64(1)]]",
	"[[DECIMAL(6)]]",
	"[[DECIMAL(5)]]",
	"[[DECIMAL(6)]]",
	"[[DECIMAL(8)]]",
	"[[NULL]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)] [INT32(2) INT64(4) INT64(420)] [INT32(3) INT64(6) INT64(420)]]",
	"[[INT64(3) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT64(666) INT64(20) INT64(420)]]",
	"[[INT64(4)]]",
	"[[INT64(12) DECIMAL(7900)]]",
	"[[INT64(3) INT64(4)]]",
	"[[INT32(3)]]",
	"[[INT32(2)]]",
	"[[INT64(3) INT64(4)]]",
	"[[INT32(100) INT64(1) INT64(2)] [INT32(200) INT64(1) INT64(1)] [INT32(300) INT64(1) INT64(1)]]",
	"[[INT64(1) INT64(1)]]",
	"[[INT64(0) INT64(0)]]",
	"[[DECIMAL(2.0000)]]",
	"[[INT32(100) DECIMAL(1.0000)] [INT32(200) DECIMAL(2.0000)] [INT32(300) DECIMAL(3.0000)]]",
	"[[INT64(3) DECIMAL(2.0000)]]",
	"[[INT64(3) INT64(4)]]",
	"[[INT32(100) INT64(1) INT64(2)] [INT32(200) INT64(1) INT64(1)] [INT32(300) INT64(1) INT64(1)]]",
	"[[INT64(1) INT64(1)]]",
	"[[DECIMAL(6)]]",
	"[[FLOAT64(6)]]",
	"[[INT32(3)]]",
	"[[FLOAT64(3)]]",
	"[[INT32(1)]]",
	"[[FLOAT64(1)]]",
	"[[DECIMAL(6) FLOAT64(1)]]",
	"[[INT32(2) DECIMAL(14)]]",
	"[[INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(1) INT64(20)] [INT32(1) INT64(10)] [INT32(4) INT64(20)] [INT32(2) INT64(10)] [INT32(9) INT64(20)] [INT32(3) INT64(10)]]",
	"[[INT32(2) INT32(4)]]",
	"[[INT32(5) INT32(4)] [INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(5) INT32(4)] [INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(2) INT32(4)] [INT32(5) INT32(4)]]",
	"[[INT64(2) DECIMAL(2)] [INT64(1) DECIMAL(0)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(3)]]",
	"[[INT64(1) INT64(3)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(10)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(10)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(10)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(10)]]",
	"[[VARCHAR(\"albumQ\") INT32(4)] [VARCHAR(\"albumY\") INT32(1)] [VARCHAR(\"albumY\") INT32(2)] [VARCHAR(\"albumX\") INT32(2)] [VARCHAR(\"albumX\") INT32(3)] [VARCHAR(\"albumX\") INT32(1)]]",
	"[[VARCHAR(\"albumQ\") INT32(4)] [VARCHAR(\"albumY\") INT32(1)] [VARCHAR(\"albumY\") INT32(2)] [VARCHAR(\"albumX\") INT32(2)] [VARCHAR(\"albumX\") INT32(3)] [VARCHAR(\"albumX\") INT32(1)]]",
	"[[INT64(2)]]",
	"[[INT32(1) INT32(100)]]",
	"[[INT32(1) INT32(100)]]",
	"[[INT64(2)]]",
	"[[INT64(2)]]",
	"[[INT64(2)]]",
	"[[INT64(2)]]",
	"[[UINT32(70)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(20)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(20)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(20)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(20)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(10)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(10)]]",
	"[]",
	"[]",
	"[[INT64(1) NULL] [INT64(2) INT64(2)]]",
	"[[INT64(1) INT64(1)] [INT64(2) NULL]]",
	"[[INT64(1) INT64(1)]]",
	"[[INT64(1) INT64(8)] [INT64(1) INT64(9)]]",
	"[[INT64(1)] [INT64(2)]]",
	"[[INT64(1)]]",
	"[[INT64(4)] [INT64(8)] [INT64(12)]]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[]",
	"[]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[]",
	"[]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[[DECIMAL(2) INT64(1)]]",
	"[[NULL INT64(0)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[NULL INT64(0)]]",
	"[]",
	"[[NULL INT64(0)]]",
	"[]",
	"[[DECIMAL(3) INT64(3)]]",
	"[[DECIMAL(2) INT64(1)] [DECIMAL(1) INT64(1)] [DECIMAL(0) INT64(1)]]",
	"[[NULL INT64(0)]]",
	"[]",
	"[[DECIMAL(423) INT64(4)]]",
	"[[DECIMAL(423) INT64(4)]]",
	"[[DECIMAL(420) INT64(1)] [DECIMAL(2) INT64(1)] [DECIMAL(1) INT64(1)] [DECIMAL(0) INT64(1)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(2) INT64(4)]]",
	"[[INT64(2) INT64(4)]]",
	"[[INT64(1) INT64(1)] [INT64(1) INT64(2)] [INT64(1) INT64(3)]]",
	"[[INT64(1) INT64(1)] [INT64(1) INT64(2)] [INT64(1) INT64(3)] [INT64(1) INT64(4)] [INT64(1) INT64(5)] [INT64(1) INT64(6)]]",
	"[[INT64(1) INT64(1)] [INT64(1) INT64(2)] [INT64(1) INT64(3)]]",
}

func TestRowParsing(t *testing.T) {
	for _, r := range TestRows {
		output, err := ParseRows(r)
		require.NoError(t, err)
		outputstr := fmt.Sprintf("%v", output)
		require.Equal(t, r, outputstr, "did not roundtrip")
	}
}

func TestRowsEquals(t *testing.T) {
	tests := []struct {
		name        string
		left, right string
		expectedErr string
	}{
		{
			name:  "Both equal",
			left:  "[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]",
			right: "[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]",
		},
		{
			name:        "length mismatch",
			left:        "[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]",
			right:       "[[INT64(2)] [INT64(2)] [INT64(1)]]",
			expectedErr: "results differ: expected 4 rows in result, got 3\n\twant: [[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]\n\tgot:  [[INT64(2)] [INT64(2)] [INT64(1)]]",
		},
		{
			name:        "elements mismatch",
			left:        "[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]",
			right:       "[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(4)]]",
			expectedErr: "results differ: row [INT64(1)] is missing from result\n\twant: [[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]\n\tgot:  [[INT64(1)] [INT64(2)] [INT64(2)] [INT64(4)]]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left, err := ParseRows(tt.left)
			require.NoError(t, err)

			right, err := ParseRows(tt.right)
			require.NoError(t, err)

			err = RowsEquals(left, right)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func TestRowsEqualStr(t *testing.T) {
	tests := []struct {
		name        string
		want        string
		got         []Row
		expectedErr string
	}{
		{
			name: "Unknown type",
			want: "[[RANDOM(1)]]",
			got: []Row{
				{
					NewInt64(1),
				},
			},
			expectedErr: "malformed row assertion: unknown SQL type \"RANDOM\" at <input>:1:3",
		},
		{
			name: "Invalid row",
			want: "[[INT64(1]]",
			got: []Row{
				{
					NewInt64(1),
				},
			},
			expectedErr: "malformed row assertion: unexpected token ']' at <input>:1:10",
		},
		{
			name: "Both equal",
			want: "[[INT64(1)]]",
			got: []Row{
				{
					NewInt64(1),
				},
			},
		},
		{
			name: "length mismatch",
			want: "[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]",
			got: []Row{
				{
					NewInt64(1),
				},
			},
			expectedErr: "results differ: expected 4 rows in result, got 1\n\twant: [[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]\n\tgot:  [[INT64(1)]]",
		},
		{
			name: "elements mismatch",
			want: "[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]",
			got: []Row{
				{
					NewInt64(1),
				},
				{
					NewInt64(1),
				},
				{
					NewInt64(1),
				},
				{
					NewInt64(1),
				},
			},
			expectedErr: "results differ: row [INT64(2)] is missing from result\n\twant: [[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]\n\tgot:  [[INT64(1)] [INT64(1)] [INT64(1)] [INT64(1)]]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RowsEqualsStr(tt.want, tt.got)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}
