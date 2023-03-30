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

package json

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryJSON(t *testing.T) {
	testcases := []struct {
		name     string
		data     []byte
		expected *Value
	}{
		{
			name:     "null",
			data:     []byte{},
			expected: ValueNull,
		},
		{
			name:     `object {"a": "b"}`,
			data:     []byte{0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98},
			expected: &Value{t: TypeObject, o: Object{kvs: []kv{{k: "a", v: &Value{t: TypeString, s: "b"}}}}},
		},
		{
			name:     `object {"a": 2}`,
			data:     []byte{0, 1, 0, 12, 0, 11, 0, 1, 0, 5, 2, 0, 97},
			expected: &Value{t: TypeObject, o: Object{kvs: []kv{{k: "a", v: &Value{t: TypeNumber, i: true, s: "2"}}}}},
		},
		{
			name:     `nested object {"asdf":{"foo":123}}`,
			data:     []byte{0, 1, 0, 29, 0, 11, 0, 4, 0, 0, 15, 0, 97, 115, 100, 102, 1, 0, 14, 0, 11, 0, 3, 0, 5, 123, 0, 102, 111, 111},
			expected: &Value{t: TypeObject, o: Object{kvs: []kv{{k: "asdf", v: &Value{t: TypeObject, o: Object{kvs: []kv{{k: "foo", v: &Value{t: TypeNumber, i: true, s: "123"}}}}}}}}},
		},
		{
			name:     `array [1,2]`,
			data:     []byte{2, 2, 0, 10, 0, 5, 1, 0, 5, 2, 0},
			expected: &Value{t: TypeArray, a: []*Value{{t: TypeNumber, i: true, s: "1"}, {t: TypeNumber, i: true, s: "2"}}},
		},
		{
			name:     `complex {"a":"b","c":"d","ab":"abc","bc":["x","y"]}`,
			data:     []byte{0, 4, 0, 60, 0, 32, 0, 1, 0, 33, 0, 1, 0, 34, 0, 2, 0, 36, 0, 2, 0, 12, 38, 0, 12, 40, 0, 12, 42, 0, 2, 46, 0, 97, 99, 97, 98, 98, 99, 1, 98, 1, 100, 3, 97, 98, 99, 2, 0, 14, 0, 12, 10, 0, 12, 12, 0, 1, 120, 1, 121},
			expected: &Value{t: TypeObject, o: Object{kvs: []kv{{k: "a", v: &Value{t: TypeString, s: "b"}}, {k: "c", v: &Value{t: TypeString, s: "d"}}, {k: "ab", v: &Value{t: TypeString, s: "abc"}}, {k: "bc", v: &Value{t: TypeArray, a: []*Value{{t: TypeString, s: "x"}, {t: TypeString, s: "y"}}}}}}},
		},
		{
			name:     `array ["here"]`,
			data:     []byte{2, 1, 0, 37, 0, 12, 8, 0, 0, 4, 104, 101, 114, 101},
			expected: &Value{t: TypeArray, a: []*Value{{t: TypeString, s: "here"}}},
		},
		{
			name:     `array ["here",["I","am"],"!!!"]`,
			data:     []byte{2, 3, 0, 37, 0, 12, 13, 0, 2, 18, 0, 12, 33, 0, 4, 104, 101, 114, 101, 2, 0, 15, 0, 12, 10, 0, 12, 12, 0, 1, 73, 2, 97, 109, 3, 33, 33, 33},
			expected: &Value{t: TypeArray, a: []*Value{{t: TypeString, s: "here"}, {t: TypeArray, a: []*Value{{t: TypeString, s: "I"}, {t: TypeString, s: "am"}}}, {t: TypeString, s: "!!!"}}},
		},
		{
			name:     `scalar "scalar string"`,
			data:     []byte{12, 13, 115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103},
			expected: &Value{t: TypeString, s: "scalar string"},
		},
		{
			name:     `object {"scopes":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"}`,
			data:     []byte{0, 1, 0, 149, 0, 11, 0, 6, 0, 12, 17, 0, 115, 99, 111, 112, 101, 115, 130, 1, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 56, 65, 65, 65, 66, 103, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 65, 65, 65, 65, 84, 216, 142, 184},
			expected: &Value{t: TypeObject, o: Object{kvs: []kv{{k: "scopes", v: &Value{t: TypeString, s: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"}}}}},
		},
		{
			name: `scalar "scalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar string"`,
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
			expected: &Value{t: TypeString, s: "scalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar string"},
		},
		{
			name:     `true`,
			data:     []byte{4, 1},
			expected: ValueTrue,
		},
		{
			name:     `false`,
			data:     []byte{4, 2},
			expected: ValueFalse,
		},
		{
			name:     `null`,
			data:     []byte{4, 0},
			expected: ValueNull,
		},
		{
			name:     `-1`,
			data:     []byte{5, 255, 255},
			expected: &Value{t: TypeNumber, i: true, s: "-1"},
		},
		{
			name:     `1`,
			data:     []byte{6, 1, 0},
			expected: &Value{t: TypeNumber, i: true, s: "1"},
		},
		{
			name:     `32767`,
			data:     []byte{5, 255, 127},
			expected: &Value{t: TypeNumber, i: true, s: "32767"},
		},
		{
			name:     `32768`,
			data:     []byte{7, 0, 128, 0, 0},
			expected: &Value{t: TypeNumber, i: true, s: "32768"},
		},
		{
			name:     `-32769`,
			data:     []byte{7, 255, 127, 255, 255},
			expected: &Value{t: TypeNumber, i: true, s: "-32769"},
		},
		{
			name:     `2147483647`,
			data:     []byte{7, 255, 255, 255, 127},
			expected: &Value{t: TypeNumber, i: true, s: "2147483647"},
		},
		{
			name:     `32768`,
			data:     []byte{8, 0, 128, 0, 0},
			expected: &Value{t: TypeNumber, i: true, s: "32768"},
		},
		{
			name:     `2147483648`,
			data:     []byte{9, 0, 0, 0, 128, 0, 0, 0, 0},
			expected: &Value{t: TypeNumber, i: true, s: "2147483648"},
		},
		{
			name:     `-2147483648`,
			data:     []byte{7, 0, 0, 0, 128},
			expected: &Value{t: TypeNumber, i: true, s: "-2147483648"},
		},
		{
			name:     `-2147483649`,
			data:     []byte{9, 255, 255, 255, 127, 255, 255, 255, 255},
			expected: &Value{t: TypeNumber, i: true, s: "-2147483649"},
		},
		{
			name:     `18446744073709551615`,
			data:     []byte{10, 255, 255, 255, 255, 255, 255, 255, 255},
			expected: &Value{t: TypeNumber, i: true, s: "18446744073709551615"},
		},
		{
			name:     `-9223372036854775808`,
			data:     []byte{9, 0, 0, 0, 0, 0, 0, 0, 128},
			expected: &Value{t: TypeNumber, i: true, s: "-9223372036854775808"},
		},
		{
			name:     `3.14159`,
			data:     []byte{11, 110, 134, 27, 240, 249, 33, 9, 64},
			expected: &Value{t: TypeNumber, i: false, s: "3.14159"},
		},
		{
			name:     `empty object {}`,
			data:     []byte{0, 0, 0, 4, 0},
			expected: &Value{t: TypeObject, o: Object{kvs: nil}},
		},
		{
			name:     `empty array []`,
			data:     []byte{2, 0, 0, 4, 0},
			expected: &Value{t: TypeArray, a: nil},
		},
		{
			name:     `datetime "2015-01-15 23:24:25.000000"`,
			data:     []byte{15, 12, 8, 0, 0, 0, 25, 118, 31, 149, 25},
			expected: &Value{t: TypeDateTime, s: "2015-01-15 23:24:25.000000"},
		},
		{
			name:     `time "23:24:25.000000"`,
			data:     []byte{15, 11, 8, 0, 0, 0, 25, 118, 1, 0, 0},
			expected: &Value{t: TypeTime, s: "23:24:25.000000"},
		},
		{
			name:     `time "23:24:25.120000"`,
			data:     []byte{15, 11, 8, 192, 212, 1, 25, 118, 1, 0, 0},
			expected: &Value{t: TypeTime, s: "23:24:25.120000"},
		},
		{
			name:     `date "2015-01-15"`,
			data:     []byte{15, 10, 8, 0, 0, 0, 0, 0, 30, 149, 25},
			expected: &Value{t: TypeDate, s: "2015-01-15"},
		},
		{
			name:     `decimal "123456789.1234"`,
			data:     []byte{15, 246, 8, 13, 4, 135, 91, 205, 21, 4, 210},
			expected: &Value{t: TypeNumber, i: true, s: "123456789.1234"},
		},
		{
			name:     `bit literal [2 202 254]`,
			data:     []byte{15, 16, 2, 202, 254},
			expected: &Value{t: TypeBit, s: string([]byte{2, 202, 254})},
		},
		{
			name:     `opaque string [2 202 254]`,
			data:     []byte{15, 15, 2, 202, 254},
			expected: &Value{t: TypeBlob, s: string([]byte{2, 202, 254})},
		},
		{
			name:     `opaque blob [2 202 254]`,
			data:     []byte{15, 252, 2, 202, 254},
			expected: &Value{t: TypeBlob, s: string([]byte{2, 202, 254})},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := ParseMySQL(tc.data)
			require.NoError(t, err)
			require.Equal(t, tc.expected, val)
		})
	}
}

func TestMarshalJSONToSQL(t *testing.T) {
	testcases := []struct {
		name     string
		data     []byte
		expected string
	}{
		{
			name:     "null",
			data:     []byte{},
			expected: "CAST(null as JSON)",
		},
		{
			name:     `object {"a": "b"}`,
			data:     []byte{0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98},
			expected: `JSON_OBJECT(_utf8mb4'a', _utf8mb4'b')`,
		},
		{
			name:     `object {"a": 2}`,
			data:     []byte{0, 1, 0, 12, 0, 11, 0, 1, 0, 5, 2, 0, 97},
			expected: "JSON_OBJECT(_utf8mb4'a', 2)",
		},
		{
			name:     `nested object {"asdf":{"foo":123}}`,
			data:     []byte{0, 1, 0, 29, 0, 11, 0, 4, 0, 0, 15, 0, 97, 115, 100, 102, 1, 0, 14, 0, 11, 0, 3, 0, 5, 123, 0, 102, 111, 111},
			expected: `JSON_OBJECT(_utf8mb4'asdf', JSON_OBJECT(_utf8mb4'foo', 123))`,
		},
		{
			name:     `array [1,2]`,
			data:     []byte{2, 2, 0, 10, 0, 5, 1, 0, 5, 2, 0},
			expected: "JSON_ARRAY(1, 2)",
		},
		{
			name:     `complex {"a":"b","c":"d","ab":"abc","bc":["x","y"]}`,
			data:     []byte{0, 4, 0, 60, 0, 32, 0, 1, 0, 33, 0, 1, 0, 34, 0, 2, 0, 36, 0, 2, 0, 12, 38, 0, 12, 40, 0, 12, 42, 0, 2, 46, 0, 97, 99, 97, 98, 98, 99, 1, 98, 1, 100, 3, 97, 98, 99, 2, 0, 14, 0, 12, 10, 0, 12, 12, 0, 1, 120, 1, 121},
			expected: "JSON_OBJECT(_utf8mb4'a', _utf8mb4'b', _utf8mb4'c', _utf8mb4'd', _utf8mb4'ab', _utf8mb4'abc', _utf8mb4'bc', JSON_ARRAY(_utf8mb4'x', _utf8mb4'y'))",
		},
		{
			name:     `array ["here"]`,
			data:     []byte{2, 1, 0, 37, 0, 12, 8, 0, 0, 4, 104, 101, 114, 101},
			expected: "JSON_ARRAY(_utf8mb4'here')",
		},
		{
			name:     `array ["here",["I","am"],"!!!"]`,
			data:     []byte{2, 3, 0, 37, 0, 12, 13, 0, 2, 18, 0, 12, 33, 0, 4, 104, 101, 114, 101, 2, 0, 15, 0, 12, 10, 0, 12, 12, 0, 1, 73, 2, 97, 109, 3, 33, 33, 33},
			expected: "JSON_ARRAY(_utf8mb4'here', JSON_ARRAY(_utf8mb4'I', _utf8mb4'am'), _utf8mb4'!!!')",
		},
		{
			name:     `scalar "scalar string"`,
			data:     []byte{12, 13, 115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103},
			expected: `CAST(JSON_QUOTE(_utf8mb4'scalar string') as JSON)`,
		},
		{
			name:     `object {"scopes":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"}`,
			data:     []byte{0, 1, 0, 149, 0, 11, 0, 6, 0, 12, 17, 0, 115, 99, 111, 112, 101, 115, 130, 1, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 56, 65, 65, 65, 66, 103, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 65, 65, 65, 65, 84, 216, 142, 184},
			expected: "JSON_OBJECT(_utf8mb4'scopes', _utf8mb4'AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA')",
		},
		{
			name: `scalar "scalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar string"`,
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
			expected: `CAST(JSON_QUOTE(_utf8mb4'scalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar string') as JSON)`,
		},
		{
			name:     `true`,
			data:     []byte{4, 1},
			expected: `CAST(true as JSON)`,
		},
		{
			name:     `false`,
			data:     []byte{4, 2},
			expected: `CAST(false as JSON)`,
		},
		{
			name:     `null`,
			data:     []byte{4, 0},
			expected: `CAST(null as JSON)`,
		},
		{
			name:     `-1`,
			data:     []byte{5, 255, 255},
			expected: `CAST(-1 as JSON)`,
		},
		{
			name:     `1`,
			data:     []byte{6, 1, 0},
			expected: `CAST(1 as JSON)`,
		},
		{
			name:     `32767`,
			data:     []byte{5, 255, 127},
			expected: `CAST(32767 as JSON)`,
		},
		{
			name:     `32768`,
			data:     []byte{7, 0, 128, 0, 0},
			expected: `CAST(32768 as JSON)`,
		},
		{
			name:     `-32769`,
			data:     []byte{7, 255, 127, 255, 255},
			expected: `CAST(-32769 as JSON)`,
		},
		{
			name:     `2147483647`,
			data:     []byte{7, 255, 255, 255, 127},
			expected: `CAST(2147483647 as JSON)`,
		},
		{
			name:     `32768`,
			data:     []byte{8, 0, 128, 0, 0},
			expected: `CAST(32768 as JSON)`,
		},
		{
			name:     `2147483648`,
			data:     []byte{9, 0, 0, 0, 128, 0, 0, 0, 0},
			expected: `CAST(2147483648 as JSON)`,
		},
		{
			name:     `-2147483648`,
			data:     []byte{7, 0, 0, 0, 128},
			expected: `CAST(-2147483648 as JSON)`,
		},
		{
			name:     `-2147483649`,
			data:     []byte{9, 255, 255, 255, 127, 255, 255, 255, 255},
			expected: `CAST(-2147483649 as JSON)`,
		},
		{
			name:     `18446744073709551615`,
			data:     []byte{10, 255, 255, 255, 255, 255, 255, 255, 255},
			expected: `CAST(18446744073709551615 as JSON)`,
		},
		{
			name:     `-9223372036854775808`,
			data:     []byte{9, 0, 0, 0, 0, 0, 0, 0, 128},
			expected: `CAST(-9223372036854775808 as JSON)`,
		},
		{
			name:     `3.14159`,
			data:     []byte{11, 110, 134, 27, 240, 249, 33, 9, 64},
			expected: `CAST(3.14159 as JSON)`,
		},
		{
			name:     `empty object {}`,
			data:     []byte{0, 0, 0, 4, 0},
			expected: `JSON_OBJECT()`,
		},
		{
			name:     `empty array []`,
			data:     []byte{2, 0, 0, 4, 0},
			expected: `JSON_ARRAY()`,
		},
		{
			name:     `datetime "2015-01-15 23:24:25.000000"`,
			data:     []byte{15, 12, 8, 0, 0, 0, 25, 118, 31, 149, 25},
			expected: `CAST(timestamp '2015-01-15 23:24:25.000000' as JSON)`,
		},
		{
			name:     `time "23:24:25.000000"`,
			data:     []byte{15, 11, 8, 0, 0, 0, 25, 118, 1, 0, 0},
			expected: `CAST(time '23:24:25.000000' as JSON)`,
		},
		{
			name:     `time "23:24:25.120000"`,
			data:     []byte{15, 11, 8, 192, 212, 1, 25, 118, 1, 0, 0},
			expected: `CAST(time '23:24:25.120000' as JSON)`,
		},
		{
			name:     `date "2015-01-15"`,
			data:     []byte{15, 10, 8, 0, 0, 0, 0, 0, 30, 149, 25},
			expected: `CAST(date '2015-01-15' as JSON)`,
		},
		{
			name:     `decimal "123456789.1234"`,
			data:     []byte{15, 246, 8, 13, 4, 135, 91, 205, 21, 4, 210},
			expected: `CAST(123456789.1234 as JSON)`,
		},
		{
			name:     `bit literal [2 202 254]`,
			data:     []byte{15, 16, 2, 202, 254},
			expected: `CAST(b'101100101011111110' as JSON)`,
		},
		{
			name:     `opaque string [2 202 254]`,
			data:     []byte{15, 15, 2, 202, 254},
			expected: `CAST(x'02cafe' as JSON)`,
		},
		{
			name:     `opaque blob [2 202 254]`,
			data:     []byte{15, 252, 2, 202, 254},
			expected: `CAST(x'02cafe' as JSON)`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := ParseMySQL(tc.data)
			buf := val.MarshalSQLTo(nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(buf))
		})
	}
}
