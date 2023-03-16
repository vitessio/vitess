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

package mysql

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONTypes(t *testing.T) {
	// most of these test cases have been taken from open source java/python adapters
	// like https://github.com/shyiko/mysql-binlog-connector-java/pull/119/files
	testcases := []struct {
		name     string
		data     []byte
		expected string
		isMap    bool
	}{{
		name:     "null",
		data:     []byte{},
		expected: `null`,
	}, {
		name:     "map, string value",
		data:     []byte{0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98},
		expected: `{"a":"b"}`,
	}, {
		name:     "map, int value",
		data:     []byte{0, 1, 0, 12, 0, 11, 0, 1, 0, 5, 2, 0, 97},
		expected: `{"a":2}`,
	}, {
		name:     "map, object value",
		data:     []byte{0, 1, 0, 29, 0, 11, 0, 4, 0, 0, 15, 0, 97, 115, 100, 102, 1, 0, 14, 0, 11, 0, 3, 0, 5, 123, 0, 102, 111, 111},
		expected: `{"asdf":{"foo":123}}`,
	}, {
		name:     "list of ints",
		data:     []byte{2, 2, 0, 10, 0, 5, 1, 0, 5, 2, 0},
		expected: `[1,2]`,
	}, {
		name:     "list of maps",
		data:     []byte{0, 4, 0, 60, 0, 32, 0, 1, 0, 33, 0, 1, 0, 34, 0, 2, 0, 36, 0, 2, 0, 12, 38, 0, 12, 40, 0, 12, 42, 0, 2, 46, 0, 97, 99, 97, 98, 98, 99, 1, 98, 1, 100, 3, 97, 98, 99, 2, 0, 14, 0, 12, 10, 0, 12, 12, 0, 1, 120, 1, 121},
		expected: `{"a":"b","c":"d","ab":"abc","bc":["x","y"]}`,
		isMap:    true,
	}, {
		name:     "list with one string",
		data:     []byte{2, 1, 0, 37, 0, 12, 8, 0, 0, 4, 104, 101, 114, 101},
		expected: `["here"]`,
	}, {
		name:     "list varied",
		data:     []byte{2, 3, 0, 37, 0, 12, 13, 0, 2, 18, 0, 12, 33, 0, 4, 104, 101, 114, 101, 2, 0, 15, 0, 12, 10, 0, 12, 12, 0, 1, 73, 2, 97, 109, 3, 33, 33, 33},
		expected: `["here",["I","am"],"!!!"]`,
	}, {
		name:     "string",
		data:     []byte{12, 13, 115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103},
		expected: `"scalar string"`,
	}, {
		name:     "map, long string value",
		data:     []byte{0, 1, 0, 149, 0, 11, 0, 6, 0, 12, 17, 0, 115, 99, 111, 112, 101, 115, 130, 1, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 56, 65, 65, 65, 66, 103, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 65, 65, 65, 65, 84, 216, 142, 184},
		expected: `{"scopes":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"}`,
	}, {
		// repeat the same string 10 times, to test the case where length of string
		// requires 2 bytes to store
		name: "long string",
		data: []byte{12, 130, 1,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103,
			115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103},
		expected: `"scalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar string"`,
	}, {
		name:     "bool true",
		data:     []byte{4, 1},
		expected: `true`,
	}, {
		name:     "bool false",
		data:     []byte{4, 2},
		expected: `false`,
	}, {
		name:     "bool null",
		data:     []byte{4, 0},
		expected: `null`,
	}, {
		name:     "uint16 max",
		data:     []byte{6, 255, 255},
		expected: `65535`,
	}, {
		name:     "uint32 max",
		data:     []byte{8, 255, 255, 255, 255},
		expected: `4294967295`,
	}, {
		name:     "uint64 max",
		data:     []byte{10, 255, 255, 255, 255, 255, 255, 255, 255},
		expected: `18446744073709551615`,
	}, {
		name:     "int16 -1",
		data:     []byte{5, 255, 255},
		expected: `-1`,
	}, {
		name:     "int32 -1",
		data:     []byte{7, 255, 255, 255, 255},
		expected: `-1`,
	}, {
		name:     "int64 -1",
		data:     []byte{9, 255, 255, 255, 255, 255, 255, 255, 255},
		expected: `-1`,
	}, {
		name:     "int16 max",
		data:     []byte{5, 255, 127},
		expected: `32767`,
	}, {
		name:     "int32 max",
		data:     []byte{7, 255, 255, 255, 127},
		expected: `2147483647`,
	}, {
		name:     "int64 max",
		data:     []byte{9, 255, 255, 255, 255, 255, 255, 255, 127},
		expected: `9223372036854775807`,
	}, {
		name:     "uint16/1",
		data:     []byte{6, 1, 0},
		expected: `1`,
	}, {
		name:     "int32/32768",
		data:     []byte{7, 0, 128, 0, 0},
		expected: `32768`,
	}, {
		name:     "int16/neg",
		data:     []byte{5, 0, 128},
		expected: `-32768`,
	}, {
		name:     "int32/neg",
		data:     []byte{7, 255, 127, 255, 255},
		expected: `-32769`,
	}, {
		name:     "uint32",
		data:     []byte{8, 0, 128, 0, 0},
		expected: `32768`,
	}, {
		name:     "int64",
		data:     []byte{9, 0, 0, 0, 128, 0, 0, 0, 0},
		expected: `2147483648`,
	}, {
		name:     "int32/neg",
		data:     []byte{7, 0, 0, 0, 128},
		expected: `-2147483648`,
	}, {
		name:     "int64/neg",
		data:     []byte{9, 255, 255, 255, 127, 255, 255, 255, 255},
		expected: `-2147483649`,
	}, {
		name:     "uint64",
		data:     []byte{10, 255, 255, 255, 255, 255, 255, 255, 255},
		expected: `18446744073709551615`,
	}, {
		name:     "int64/neg",
		data:     []byte{9, 0, 0, 0, 0, 0, 0, 0, 128},
		expected: `-9223372036854775808`,
	}, {
		name:     "double",
		data:     []byte{11, 110, 134, 27, 240, 249, 33, 9, 64},
		expected: `3.14159`,
	}, {
		name:     "empty map",
		data:     []byte{0, 0, 0, 4, 0},
		expected: `{}`,
	}, {
		name:     "empty list",
		data:     []byte{2, 0, 0, 4, 0},
		expected: `[]`,
	}, {
		// opaque, datetime
		name:     "datetime",
		data:     []byte{15, 12, 8, 0, 0, 0, 25, 118, 31, 149, 25},
		expected: `"2015-01-15 23:24:25.000000"`,
	}, {
		// opaque, time
		name:     "time",
		data:     []byte{15, 11, 8, 0, 0, 0, 25, 118, 1, 0, 0},
		expected: `"23:24:25.000000"`,
	}, {
		// opaque, time
		name:     "time2",
		data:     []byte{15, 11, 8, 192, 212, 1, 25, 118, 1, 0, 0},
		expected: `"23:24:25.120000"`,
	}, {
		// opaque, date
		name:     "date",
		data:     []byte{15, 10, 8, 0, 0, 0, 0, 0, 30, 149, 25},
		expected: `"2015-01-15"`,
	}, {
		// opaque, decimal
		name:     "decimal",
		data:     []byte{15, 246, 8, 13, 4, 135, 91, 205, 21, 4, 210},
		expected: `1.234567891234e+08`,
	}, {
		// opaque, bit field. Not yet implemented.
		name:     "bitfield: unimplemented",
		data:     []byte{15, 16, 2, 202, 254},
		expected: `opaque type 16 is not supported yet, data [2 202 254]`,
	}}
	for _, tc := range testcases {
		name := fmt.Sprintf("%s (%s)", tc.name, tc.expected)
		t.Run(name, func(t *testing.T) {
			val, err := getJSONValue(tc.data)
			if err != nil {
				require.Equal(t, tc.expected, err.Error())
				return
			}
			if tc.isMap { // map keys sorting order is not guaranteed, so we convert back to golang maps and compare
				var gotJSON, wantJSON map[string]any
				err = json.Unmarshal([]byte(val), &gotJSON)
				require.NoError(t, err)
				err = json.Unmarshal([]byte(tc.expected), &wantJSON)
				require.NoError(t, err)
				require.EqualValues(t, wantJSON, gotJSON)
				return
			}
			require.Equal(t, tc.expected, val)
		})
	}
}
