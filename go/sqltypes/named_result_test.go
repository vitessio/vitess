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

package sqltypes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestToNamedResult(t *testing.T) {
	require.Nil(t, ToNamedResult(nil))

	in := &Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: Int64,
		}, {
			Name: "status",
			Type: VarChar,
		}, {
			Name: "uid",
			Type: Uint64,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{TestValue(Int64, "0"), TestValue(VarChar, "s0"), TestValue(Uint64, "0")},
			{TestValue(Int64, "1"), TestValue(VarChar, "s1"), TestValue(Uint64, "1")},
			{TestValue(Int64, "2"), TestValue(VarChar, "s2"), TestValue(Uint64, "2")},
		},
	}
	named := in.Named()
	for i := range in.Rows {
		require.Equal(t, in.Rows[i][0], named.Rows[i]["id"])
		require.Equal(t, int64(i), named.Rows[i].AsInt64("id", 0))

		require.Equal(t, in.Rows[i][1], named.Rows[i]["status"])
		require.Equal(t, fmt.Sprintf("s%d", i), named.Rows[i].AsString("status", "notfound"))

		require.Equal(t, in.Rows[i][2], named.Rows[i]["uid"])
		require.Equal(t, uint64(i), named.Rows[i].AsUint64("uid", 0))
	}
}

func TestToNumericTypes(t *testing.T) {
	row := RowNamedValues{
		"test": Value{
			val: []byte("0x1234"),
		},
	}
	tests := []struct {
		name        string
		fieldName   string
		expectedErr string
	}{
		{
			name:        "random fieldName",
			fieldName:   "random",
			expectedErr: "No such field in RowNamedValues",
		},
		{
			name:        "right fieldName",
			fieldName:   "test",
			expectedErr: "Cannot convert value to desired type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := row.ToInt(tt.fieldName)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			_, err = row.ToInt32(tt.fieldName)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			_, err = row.ToInt64(tt.fieldName)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			_, err = row.ToUint64(tt.fieldName)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			_, err = row.ToFloat64(tt.fieldName)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			_, err = row.ToBool(tt.fieldName)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestToBytes(t *testing.T) {
	row := RowNamedValues{
		"test": Value{
			val: []byte("0x1234"),
		},
	}

	_, err := row.ToBytes("random")
	require.ErrorContains(t, err, "No such field in RowNamedValues")

	val, err := row.ToBytes("test")
	require.NoError(t, err)
	require.Equal(t, []byte{0x30, 0x78, 0x31, 0x32, 0x33, 0x34}, val)
}

func TestRow(t *testing.T) {
	row := RowNamedValues{}
	tests := []struct {
		name        string
		res         *NamedResult
		expectedRow RowNamedValues
	}{
		{
			name:        "empty results",
			res:         &NamedResult{},
			expectedRow: nil,
		},
		{
			name: "non-empty results",
			res: &NamedResult{
				Rows: []RowNamedValues{row},
			},
			expectedRow: row,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedRow, tt.res.Row())
		})
	}
}
