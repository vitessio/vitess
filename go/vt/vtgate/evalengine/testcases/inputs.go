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

	"vitess.io/vitess/go/mysql/format"
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
	"0", "1", "0xFF", "255", "1.0", "1.1", "-1", "-255", "7", "9", "13", "1.5", "-1.5", "'1.5'", "'-1.5'",
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

var radianInputs = []string{
	"0",
	"1",
	"-1",
	"'1.5'",
	"NULL",
	"'ABC'",
	"1.5e0",
	"-1.5e0",
	"9223372036854775810.4",
	"-9223372036854775810.4",
	string(format.FormatFloat(math.Pi)),
	string(format.FormatFloat(math.MaxFloat64)),
	string(format.FormatFloat(math.SmallestNonzeroFloat32)),
	string(format.FormatFloat(math.SmallestNonzeroFloat64)),
}

var inputComparisonElement = []string{
	"NULL", "-1", "0", "1",
	`'foo'`, `'bar'`, `'FOO'`, `'BAR'`,
	`'foo' collate utf8mb4_0900_as_cs`,
	`'FOO' collate utf8mb4_0900_as_cs`,
	`_latin1 'foo'`,
	`_latin1 'FOO'`,
}

var inputConversions = []string{
	"0", "1", "255", "' 0 '", "' 1 '", "' 255 '", `'\t1foo\t'`, "' 255 foo'",
	"0.0e0", "1.0e0", "1.5e0", "-1.5e0", "1.1e0", "-1.1e0", "-1.7e0",
	"0.0", "0.000", "1.5", "-1.5", "1.1", "1.7", "-1.1", "-1.7", "'1.5'", "'-1.5'",
	`'foobar'`, `_utf8 'foobar'`, `''`, `_binary 'foobar'`,
	`0x0`, `0x1`, `0xff`, `X'00'`, `X'01'`, `X'ff'`,
	"NULL", "true", "false",
	"0xFF666F6F626172FF", "0x666F6F626172FF", "0xFF666F6F626172",
	"9223372036854775807", "-9223372036854775808", "18446744073709551615",
	"18446744073709540000e0",
	"-18446744073709540000e0",
	"JSON_OBJECT()", "JSON_ARRAY()",
	"time '10:04:58'", "time '31:34:58'", "time '32:34:58'", "time '101:34:58'", "time '5 10:34:58'", "date '2000-01-01'",
	"timestamp '2000-01-01 10:34:58'", "timestamp '2000-01-01 10:34:58.123456'", "timestamp '2000-01-01 10:34:58.978654'",
	"20000101103458", "20000101103458.1234", "20000101103458.123456", "20000101", "103458", "103458.123456",
	"'20000101103458'", "'20000101103458.1234'", "'20000101103458.123456'", "'20000101'", "'103458'", "'103458.123456'",
	"'20000101103458foo'", "'20000101103458.1234foo'", "'20000101103458.123456foo'", "'20000101foo'", "'103458foo'", "'103458.123456foo'",
	"time '-10:04:58'", "time '-31:34:58'", "time '-32:34:58'",
	"time '-101:34:58'", "time '-5 10:34:58'",
	"'10:04:58'", "'101:34:58'", "'5 10:34:58'", "'2000-01-01'", "'2000-01-01 12:34:58'",
	"cast(0 as json)", "cast(1 as json)",
	"cast(true as json)", "cast(false as json)",
	"cast('{}' as json)", "cast('[]' as json)",
	"cast('null' as json)", "cast('true' as json)", "cast('false' as json)",
	// JSON numbers
	"cast(1 as json)", "cast(2 as json)", "cast(1.1 as json)", "cast(-1.1 as json)",
	"cast(9223372036854775807 as json)", "cast(18446744073709551615 as json)",
	"cast('1' as json)", "cast('2' as json)", "cast('1.1' as json)", "cast('-1.1' as json)",
	"cast('9223372036854775807' as json)", "cast('18446744073709551615' as json)",
	// JSON strings
	"cast('\"foo\"' as json)", "cast('\"bar\"' as json)", "cast('invalid' as json)",
	// JSON binary values
	"cast(_binary' \"foo\"' as json)", "cast(_binary '\"bar\"' as json)",
	"cast(0xFF666F6F626172FF as json)", "cast(0x666F6F626172FF as json)",
	"cast(0b01 as json)", "cast(0b001 as json)",
	// JSON arrays
	"cast('[\"a\"]' as json)", "cast('[\"ab\"]' as json)",
	"cast('[\"ab\", \"cd\", \"ef\"]' as json)", "cast('[\"ab\", \"ef\"]' as json)",
	// JSON objects
	"cast('{\"a\": 1, \"b\": 2}' as json)", "cast('{\"b\": 2, \"a\": 1}' as json)",
	"cast('{\"c\": 1, \"b\": 2}' as json)", "cast('{\"b\": 2, \"c\": 1}' as json)",
	"cast(' \"b\": 2}' as json)", "cast('\"a\": 1' as json)",
	// JSON date, datetime & time
	"cast(date '2000-01-01' as json)", "cast(date '2000-01-02' as json)",
	"cast(timestamp '2000-01-01 12:34:58' as json)",
	"cast(time '12:34:56' as json)", "cast(time '12:34:58' as json)", "cast(time '5 12:34:58' as json)",
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
	"_binary 'Müller' ",
	"_utf8mb4 'abcABCÅå'",
	// TODO: support other multibyte encodings
	// "_dec8 'ÒòÅå'",
	// "_utf8mb3 'abcABCÅå'",
	// "_utf16 'AabcÅå'",
	// "_utf32 'AabcÅå'",
	// "_ucs2 'AabcÅå'",
}

var inputConversionTypes = []string{
	"BINARY", "BINARY(1)", "BINARY(0)", "BINARY(16)", "BINARY(-1)",
	"CHAR", "CHAR(1)", "CHAR(0)", "CHAR(16)", "CHAR(-1)",
	"NCHAR", "NCHAR(1)", "NCHAR(0)", "NCHAR(16)", "NCHAR(-1)",
	"DECIMAL", "DECIMAL(0, 4)", "DECIMAL(12, 0)", "DECIMAL(12, 4)", "DECIMAL(60)", "DECIMAL(60, 6)",
	"DOUBLE", "REAL",
	"SIGNED", "UNSIGNED", "SIGNED INTEGER", "UNSIGNED INTEGER", "JSON",
	"DATE", "DATETIME", "TIME", "DATETIME(4)", "TIME(4)", "DATETIME(6)", "TIME(6)",
}

var dateFormats = []struct {
	c    byte
	expr string
}{
	{'a', "LEFT(DAYNAME(d),3)"},
	{'b', "LEFT(MONTHNAME(d),3)"},
	{'c', "MONTH(d)"},
	{'D', ""},
	{'d', "LPAD(DAYOFMONTH(d),0,2)"},
	{'e', "DAYOFMONTH(d)"},
	{'f', "LPAD(MICROSECOND(t),6,0)"},
	{'H', "LPAD(HOUR(t),2,0)"},
	{'h', ""},
	{'I', ""},
	{'i', "LPAD(MINUTE(t),2,0)"},
	{'j', ""},
	{'k', "HOUR(t)"},
	{'l', ""},
	{'M', "MONTHNAME(d)"},
	{'m', "LPAD(MONTH(d),2,0)"},
	{'p', ""},
	{'r', ""},
	{'S', "LPAD(SECOND(t),2,0)"},
	{'s', "LPAD(SECOND(t),2,0)"},
	{'T', ""},
	{'U', "LPAD(WEEK(d,0),2,0)"},
	{'u', "LPAD(WEEK(d,1),2,0)"},
	{'V', "RIGHT(YEARWEEK(d,2),2)"},
	{'v', "RIGHT(YEARWEEK(d,3),2)"},
	{'W', "DAYNAME(d)"},
	{'w', "DAYOFWEEK(d)-1"},
	{'X', "LEFT(YEARWEEK(d,2),4)"},
	{'x', "LEFT(YEARWEEK(d,3),4)"},
	{'Y', "YEAR(d)"},
	{'y', "RIGHT(YEAR(d),2)"},
	{'%', ""},
}

var inputTrimStrings = []string{
	"\" Å å\" ",
	"NULL",
	"\"\"",
	"\"a\"",
	"\"abc\"",
	"'abca'",
	"1",
	"-1",
	"0123",
	"0xAACC",
	"3.1415926",
	"\" 中文测试\"",
	"\"日本語テスト \"",
	"\"한국어 시험\"",
	"\" 😊😂🤢\r\t \"",
	"'123'",
	"9223372036854775807",
	"-9223372036854775808",
	"999999999999999999999999",
	"-999999999999999999999999",
	"_binary 'Müller\r\n' ",
	"_utf8mb4 '\nabcABCÅå '",
	// utf8mb4 version of the non-breaking space
	"_utf8mb4 0xC2A078C2A0",
	// TODO: support other multibyte encodings
	// latin1 version of the non-breaking space
	///"_latin1 0xA078A0",
}
