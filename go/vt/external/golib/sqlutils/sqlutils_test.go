/*
   Copyright 2024 The Vitess Authors.

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

package sqlutils

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRowMap(t *testing.T) {
	tt := []struct {
		name     string
		rowMap   RowMap
		expected any
	}{
		{
			"GetString",
			RowMap{"key": CellData{String: "value"}},
			"value",
		},
		{
			"GetInt64",
			RowMap{"key": CellData{String: "123"}},
			int64(123),
		},
		{
			"GetInt32",
			RowMap{"key": CellData{String: "42"}},
			int32(42),
		},
		{
			"GetNullInt64",
			RowMap{"key": CellData{String: "789"}},
			sql.NullInt64{Int64: 789, Valid: true},
		},
		{
			"GetNullInt64 Error",
			RowMap{"key": CellData{String: "foo"}},
			sql.NullInt64{Valid: false},
		},
		{
			"GetInt",
			RowMap{"key": CellData{String: "456"}},
			456,
		},
		{
			"GetUint",
			RowMap{"key": CellData{String: "123"}},
			uint(123),
		},
		{
			"GetUint64",
			RowMap{"key": CellData{String: "999"}},
			uint64(999),
		},
		{
			"GetUint32",
			RowMap{"key": CellData{String: "888"}},
			uint32(888),
		},
		{
			"GetBool",
			RowMap{"key": CellData{String: "1"}},
			true,
		},
		{
			"GetTime",
			RowMap{"key": CellData{String: "2024-01-24 12:34:56.789"}},
			time.Date(2024, time.January, 24, 12, 34, 56, 789000000, time.UTC),
		},
		{
			"GetTime Error",
			RowMap{"key": CellData{String: "invalid_time_format"}},
			time.Time{},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.name {
			case "GetString":
				assert.Equal(t, tc.expected, tc.rowMap.GetString("key"))
			case "GetInt64":
				assert.Equal(t, tc.expected, tc.rowMap.GetInt64("key"))
			case "GetInt32":
				assert.Equal(t, tc.expected, tc.rowMap.GetInt32("key"))
			case "GetNullInt64":
				assert.Equal(t, tc.expected, tc.rowMap.GetNullInt64("key"))
			case "GetNullInt64 Error":
				assert.Equal(t, tc.expected, tc.rowMap.GetNullInt64("key"))
			case "GetInt":
				assert.Equal(t, tc.expected, tc.rowMap.GetInt("key"))
			case "GetUint":
				assert.Equal(t, tc.expected, tc.rowMap.GetUint("key"))
			case "GetUint64":
				assert.Equal(t, tc.expected, tc.rowMap.GetUint64("key"))
			case "GetUint32":
				assert.Equal(t, tc.expected, tc.rowMap.GetUint32("key"))
			case "GetBool":
				assert.Equal(t, tc.expected, tc.rowMap.GetBool("key"))
			case "GetTime":
				assert.Equal(t, tc.expected, tc.rowMap.GetTime("key"))
			case "GetTime Error":
				assert.Equal(t, tc.expected, tc.rowMap.GetTime("key"))
			}
		})
	}
}

func TestNullString(t *testing.T) {
	cellData := CellData{String: "test_value", Valid: true}

	result := cellData.NullString()

	expected := &sql.NullString{String: "test_value", Valid: true}
	assert.Equal(t, expected, result)
}

func TestMarshalJSON(t *testing.T) {
	tt := []struct {
		name     string
		rowData  RowData
		expected string
	}{
		{"Valid", RowData{{String: "value", Valid: true}}, `["value"]`},
		{"Invalid", RowData{{String: "", Valid: false}}, "[null]"},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.rowData.MarshalJSON()
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, string(result))
		})
	}
}

func TestUnmarshalJSON(t *testing.T) {
	tt := []struct {
		name     string
		input    string
		expected CellData
		isError  bool
	}{
		{"Valid JSON", `"value"`, CellData{String: "value", Valid: true}, false},
		{"Invalid JSON", `"invalid_json`, CellData{}, true},
		{"Null JSON", `null`, CellData{String: "", Valid: true}, false}, //??
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var cellData CellData
			err := cellData.UnmarshalJSON([]byte(tc.input))

			if tc.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, cellData)
			}
		})
	}
}

func TestQueryRowsMap(t *testing.T) {
	tt := []struct {
		name      string
		db        *sql.DB
		query     string
		onRowFunc func(RowMap) error
		args      []any
		shouldErr bool
	}{
		{"Error", nil, "", nil, nil, true},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			err := QueryRowsMap(tc.db, tc.query, tc.onRowFunc, tc.args...)
			if tc.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExecNoPrepare(t *testing.T) {
	tt := []struct {
		name      string
		db        *sql.DB
		query     string
		args      []any
		shouldErr bool
		expect    sql.Result
	}{
		{"Error", nil, "", nil, true, nil},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			out, err := ExecNoPrepare(tc.db, tc.query, tc.args...)
			if tc.shouldErr {
				assert.Error(t, err)
				assert.Nil(t, out)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expect, out)
			}
		})
	}
}

func TestArgs(t *testing.T) {
	args := []any{1, "abc", true}
	expected := []any{1, "abc", true}
	result := Args(args...)
	assert.Equal(t, expected, result)
}

func TestNilIfZero(t *testing.T) {
	tt := []struct {
		name     string
		i        int64
		expected any
	}{
		{"NonZero", int64(42), int64(42)},
		{"Zero", int64(0), nil},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			result := NilIfZero(tc.i)
			assert.Equal(t, tc.expected, result)
		})
	}
}
