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

package binlog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/json"
)

func jsonObject(entries map[string]*json.Value) *json.Value {
	var obj json.Object
	for k, v := range entries {
		obj.Set(k, v, json.Set)
	}
	return json.NewObject(obj)
}

func TestBinaryJSON(t *testing.T) {
	testcases := []struct {
		name     string
		data     []byte
		expected *json.Value
	}{
		{
			name:     "null",
			data:     []byte{},
			expected: json.ValueNull,
		},
		{
			name: `object {"a": "b"}`,
			data: []byte{0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98},
			// expected: &json.Value{t: json.TypeObject, o: json.Object{kvs: []json.kv{{k: "a", v: &json.Value{t: json.TypeString, s: "b"}}}}},
			expected: jsonObject(map[string]*json.Value{"a": json.NewString("b")}),
		},
		{
			name:     `object {"a": 2}`,
			data:     []byte{0, 1, 0, 12, 0, 11, 0, 1, 0, 5, 2, 0, 97},
			expected: jsonObject(map[string]*json.Value{"a": json.NewNumber("2", json.NumberTypeSigned)}),
		},
		{
			name:     `nested object {"asdf":{"foo":123}}`,
			data:     []byte{0, 1, 0, 29, 0, 11, 0, 4, 0, 0, 15, 0, 97, 115, 100, 102, 1, 0, 14, 0, 11, 0, 3, 0, 5, 123, 0, 102, 111, 111},
			expected: jsonObject(map[string]*json.Value{"asdf": jsonObject(map[string]*json.Value{"foo": json.NewNumber("123", json.NumberTypeSigned)})}),
		},
		{
			name:     `array [1,2]`,
			data:     []byte{2, 2, 0, 10, 0, 5, 1, 0, 5, 2, 0},
			expected: json.NewArray([]*json.Value{json.NewNumber("1", json.NumberTypeSigned), json.NewNumber("2", json.NumberTypeSigned)}),
		},
		{
			name: `complex {"a":"b","c":"d","ab":"abc","bc":["x","y"]}`,
			data: []byte{0, 4, 0, 60, 0, 32, 0, 1, 0, 33, 0, 1, 0, 34, 0, 2, 0, 36, 0, 2, 0, 12, 38, 0, 12, 40, 0, 12, 42, 0, 2, 46, 0, 97, 99, 97, 98, 98, 99, 1, 98, 1, 100, 3, 97, 98, 99, 2, 0, 14, 0, 12, 10, 0, 12, 12, 0, 1, 120, 1, 121},
			expected: jsonObject(map[string]*json.Value{
				"a":  json.NewString("b"),
				"c":  json.NewString("d"),
				"ab": json.NewString("abc"),
				"bc": json.NewArray([]*json.Value{json.NewString("x"), json.NewString("y")}),
			}),
		},
		{
			name:     `array ["here"]`,
			data:     []byte{2, 1, 0, 37, 0, 12, 8, 0, 0, 4, 104, 101, 114, 101},
			expected: json.NewArray([]*json.Value{json.NewString("here")}),
		},
		{
			name: `array ["here",["I","am"],"!!!"]`,
			data: []byte{2, 3, 0, 37, 0, 12, 13, 0, 2, 18, 0, 12, 33, 0, 4, 104, 101, 114, 101, 2, 0, 15, 0, 12, 10, 0, 12, 12, 0, 1, 73, 2, 97, 109, 3, 33, 33, 33},
			expected: json.NewArray([]*json.Value{
				json.NewString("here"),
				json.NewArray([]*json.Value{json.NewString("I"), json.NewString("am")}),
				json.NewString("!!!"),
			}),
		},
		{
			name:     `scalar "scalar string"`,
			data:     []byte{12, 13, 115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103},
			expected: json.NewString("scalar string"),
		},
		{
			name: `object {"scopes":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"}`,
			data: []byte{0, 1, 0, 149, 0, 11, 0, 6, 0, 12, 17, 0, 115, 99, 111, 112, 101, 115, 130, 1, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 69, 65, 65, 65, 65, 65, 65, 56, 65, 65, 65, 66, 103, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 65, 65, 65, 65, 84, 216, 142, 184},
			// expected: &json.Value{t: json.TypeObject, o: json.Object{kvs: []json.kv{{k: "scopes", v: &json.Value{t: json.TypeString, s: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"}}}}},
			expected: jsonObject(map[string]*json.Value{
				"scopes": json.NewString("AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"),
			}),
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
			expected: json.NewString("scalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar string"),
		},
		{
			name:     `true`,
			data:     []byte{4, 1},
			expected: json.ValueTrue,
		},
		{
			name:     `false`,
			data:     []byte{4, 2},
			expected: json.ValueFalse,
		},
		{
			name:     `null`,
			data:     []byte{4, 0},
			expected: json.ValueNull,
		},
		{
			name:     `-1`,
			data:     []byte{5, 255, 255},
			expected: json.NewNumber("-1", json.NumberTypeSigned),
		},
		{
			name:     `1`,
			data:     []byte{6, 1, 0},
			expected: json.NewNumber("1", json.NumberTypeUnsigned),
		},
		{
			name:     `32767`,
			data:     []byte{5, 255, 127},
			expected: json.NewNumber("32767", json.NumberTypeSigned),
		},
		{
			name:     `32768`,
			data:     []byte{7, 0, 128, 0, 0},
			expected: json.NewNumber("32768", json.NumberTypeSigned),
		},
		{
			name:     `-32769`,
			data:     []byte{7, 255, 127, 255, 255},
			expected: json.NewNumber("-32769", json.NumberTypeSigned),
		},
		{
			name:     `2147483647`,
			data:     []byte{7, 255, 255, 255, 127},
			expected: json.NewNumber("2147483647", json.NumberTypeSigned),
		},
		{
			name:     `32768`,
			data:     []byte{8, 0, 128, 0, 0},
			expected: json.NewNumber("32768", json.NumberTypeUnsigned),
		},
		{
			name:     `2147483648`,
			data:     []byte{9, 0, 0, 0, 128, 0, 0, 0, 0},
			expected: json.NewNumber("2147483648", json.NumberTypeSigned),
		},
		{
			name:     `-2147483648`,
			data:     []byte{7, 0, 0, 0, 128},
			expected: json.NewNumber("-2147483648", json.NumberTypeSigned),
		},
		{
			name:     `-2147483649`,
			data:     []byte{9, 255, 255, 255, 127, 255, 255, 255, 255},
			expected: json.NewNumber("-2147483649", json.NumberTypeSigned),
		},
		{
			name:     `18446744073709551615`,
			data:     []byte{10, 255, 255, 255, 255, 255, 255, 255, 255},
			expected: json.NewNumber("18446744073709551615", json.NumberTypeUnsigned),
		},
		{
			name:     `-9223372036854775808`,
			data:     []byte{9, 0, 0, 0, 0, 0, 0, 0, 128},
			expected: json.NewNumber("-9223372036854775808", json.NumberTypeSigned),
		},
		{
			name:     `3.14159`,
			data:     []byte{11, 110, 134, 27, 240, 249, 33, 9, 64},
			expected: json.NewNumber("3.14159", json.NumberTypeFloat),
		},
		{
			name:     `empty object {}`,
			data:     []byte{0, 0, 0, 4, 0},
			expected: json.NewObject(json.Object{}),
		},
		{
			name:     `empty array []`,
			data:     []byte{2, 0, 0, 4, 0},
			expected: json.NewArray(nil),
		},
		{
			name:     `datetime "2015-01-15 23:24:25.000000"`,
			data:     []byte{15, 12, 8, 0, 0, 0, 25, 118, 31, 149, 25},
			expected: json.NewDateTime("2015-01-15 23:24:25.000000"),
		},
		{
			name:     `time "23:24:25.000000"`,
			data:     []byte{15, 11, 8, 0, 0, 0, 25, 118, 1, 0, 0},
			expected: json.NewTime("23:24:25.000000"),
		},
		{
			name:     `time "23:24:25.120000"`,
			data:     []byte{15, 11, 8, 192, 212, 1, 25, 118, 1, 0, 0},
			expected: json.NewTime("23:24:25.120000"),
		},
		{
			name:     `date "2015-01-15"`,
			data:     []byte{15, 10, 8, 0, 0, 0, 0, 0, 30, 149, 25},
			expected: json.NewDate("2015-01-15"),
		},
		{
			name:     `decimal "123456789.1234"`,
			data:     []byte{15, 246, 8, 13, 4, 135, 91, 205, 21, 4, 210},
			expected: json.NewNumber("123456789.1234", json.NumberTypeDecimal),
		},
		{
			name:     `bit literal [2 202 254]`,
			data:     []byte{15, 16, 2, 202, 254},
			expected: json.NewBit(string([]byte{2, 202, 254})),
		},
		{
			name:     `opaque string [2 202 254]`,
			data:     []byte{15, 15, 2, 202, 254},
			expected: json.NewBlob(string([]byte{2, 202, 254})),
		},
		{
			name:     `opaque blob [2 202 254]`,
			data:     []byte{15, 252, 2, 202, 254},
			expected: json.NewBlob(string([]byte{2, 202, 254})),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := ParseBinaryJSON(tc.data)
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
			expected: "JSON_OBJECT(_utf8mb4'a', _utf8mb4'b', _utf8mb4'ab', _utf8mb4'abc', _utf8mb4'bc', JSON_ARRAY(_utf8mb4'x', _utf8mb4'y'), _utf8mb4'c', _utf8mb4'd')",
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
			expected: `CAST(x'02CAFE' as JSON)`,
		},
		{
			name:     `opaque blob [2 202 254]`,
			data:     []byte{15, 252, 2, 202, 254},
			expected: `CAST(x'02CAFE' as JSON)`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := ParseBinaryJSON(tc.data)
			buf := val.MarshalSQLTo(nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(buf))
		})
	}
}
