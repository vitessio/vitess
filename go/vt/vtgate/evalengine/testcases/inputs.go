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

package testcases

import (
	"math"
	"strconv"
)

var inputJSONObjects = []string{
	`[ { "a": 1 }, { "a": 2 } ]`,
	`{ "a" : "foo", "b" : [ true, { "c" : 123, "c" : 456 } ] }`,
	`{ "a" : "foo", "b" : [ true, { "c" : "123" } ] }`,
	`{ "a" : "foo", "b" : [ true, { "c" : 123 } ] }`,
	`{"a": 1, "b": 2, "c": {"d": 4}}`,
	`["a", {"b": [true, false]}, [10, 20]]`,
	`[10, 20, [30, 40]]`,
}

var inputJSONPaths = []string{
	"$**.b", "$.c", "$.b[1].c", "$.b[1].c", "$.b[1]", "$[0][0]", "$**[0]", "$.a[0]",
	"$[0].a[0]", "$**.a", "$[0][0][0].a", "$[*].b", "$[*].a", `$[1].b[0]`, `$[2][2]`,
	`$.a`, `$.e`, `$.b`, `$.c.d`, `$.a.d`, `$[0]`, `$[1]`, `$[2][*]`, `$`,
}

var inputJSONPrimitives = []string{
	`true`, `false`, `"true"`, `'false'`,
	`1`, `1.0`, `'1'`, `'1.0'`, `NULL`, `'NULL'`,
	`'foobar'`, `'foo\nbar'`, `'a'`, `JSON_OBJECT()`,
}

var inputBitwise = []string{
	"0", "1", "0xFF", "255", "1.0", "1.1", "-1", "-255", "7", "9", "13", "1.5", "-1.5",
	"0.0e0", "1.0e0", "255.0", "1.5e0", "-1.5e0", "1.1e0", "-1e0", "-255e0", "7e0", "9e0", "13e0",
	strconv.FormatUint(math.MaxUint64, 10),
	strconv.FormatUint(math.MaxInt64, 10),
	strconv.FormatUint(math.MaxInt64+1, 10),
	strconv.FormatInt(math.MinInt64, 10),
	"18446744073709551616",
	"-9223372036854775809",
	`"foobar"`, `"foobar1234"`, `"0"`, "0x1", "-0x1", "X'ff'", "X'00'",
	`"1abcd"`, "NULL", `_binary "foobar"`, `_binary "foobar1234"`,
	"64", "'64'", "_binary '64'", "X'40'", "_binary X'40'",
}

var inputComparisonElement = []string{"NULL", "-1", "0", "1",
	`'foo'`, `'bar'`, `'FOO'`, `'BAR'`,
	`'foo' collate utf8mb4_0900_as_cs`,
	`'FOO' collate utf8mb4_0900_as_cs`,
	`_latin1 'foo'`,
	`_latin1 'FOO'`,
}

var inputConversions = []string{
	"0", "1", "255",
	"0.0e0", "1.0e0", "1.5e0", "-1.5e0", "1.1e0", "-1.1e0", "-1.7e0",
	"0.0", "0.000", "1.5", "-1.5", "1.1", "1.7", "-1.1", "-1.7",
	`'foobar'`, `_utf8 'foobar'`, `''`, `_binary 'foobar'`,
	`0x0`, `0x1`, `0xff`, `X'00'`, `X'01'`, `X'ff'`,
	"NULL", "true", "false",
	"0xFF666F6F626172FF", "0x666F6F626172FF", "0xFF666F6F626172",
	"18446744073709540000e0",
	"-18446744073709540000e0",
	"JSON_OBJECT()", "JSON_ARRAY()",
}

const inputPi = "314159265358979323846264338327950288419716939937510582097494459"

var inputStrings = []string{
	"\"Å å\"",
	"NULL",
	"\"\"",
	"\"a\"",
	"\"abc\"",
	"1",
	"-1",
	"0123",
	"0xAACC",
	"3.1415926",
	"\"中文测试\"",
	"\"日本語テスト\"",
	"\"한국어 시험\"",
	"\"😊😂🤢\"",
	"'123'",
	"9223372036854775807",
	"-9223372036854775808",
	"999999999999999999999999",
	"-999999999999999999999999",
	"_latin1 X'ÂÄÌå'",
	"_binary 'Müller' ",
	"_utf8mb4 'abcABCÅå'",
	// TODO: support other multibyte encodings
	// "_dec8 'ÒòÅå'",
	// "_utf8mb3 'abcABCÅå'",
	// "_utf16 'AabcÅå'",
	// "_utf32 'AabcÅå'",
	// "_ucs2 'AabcÅå'",
}
