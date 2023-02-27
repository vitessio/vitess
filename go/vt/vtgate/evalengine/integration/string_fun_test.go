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

package integration

import (
	"fmt"
	"testing"
)

var cases = []string{
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
	"_dec8 'ÒòÅå'",
	"_binary 'Müller' ",
	"_utf8mb4 'abcABCÅå'",
	"_utf8mb3 'abcABCÅå'",
	"_utf16 'AabcÅå'",
	"_utf32 'AabcÅå'",
	"_ucs2 'AabcÅå'",
}

func TestBuiltinLowerandLcase(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()
	var cases = []string{
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
	}
	for _, str := range cases {
		query := fmt.Sprintf("LOWER (%s)", str)
		compareRemoteExpr(t, conn, query)

		query = fmt.Sprintf("LCASE(%s)", str)
		compareRemoteExpr(t, conn, query)
	}
}

func TestBuiltinUpperandUcase(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	var cases = []string{
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
	}

	for _, str := range cases {
		query := fmt.Sprintf("UPPER(%s)", str)
		compareRemoteExpr(t, conn, query)

		query = fmt.Sprintf("UCASE(%s)", str)
		compareRemoteExpr(t, conn, query)
	}
}

func TestBuiltinCharLength(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, str := range cases {
		query := fmt.Sprintf("CHAR_LENGTH(%s)", str)
		compareRemoteExpr(t, conn, query)

		query = fmt.Sprintf("CHARACTER_LENGTH(%s)", str)
		compareRemoteExpr(t, conn, query)
	}
}

func TestBuiltinLength(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, str := range cases {
		query := fmt.Sprintf("Length(%s)", str)
		compareRemoteExpr(t, conn, query)

		query = fmt.Sprintf("OCTET_LENGTH(%s)", str)
		compareRemoteExpr(t, conn, query)
	}
}

func TestBuiltinBitLength(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()
	for _, str := range cases {
		query := fmt.Sprintf("BIT_LENGTH(%s)", str)
		compareRemoteExpr(t, conn, query)
	}
}

func TestBuiltinASCII(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, str := range cases {
		query := fmt.Sprintf("ASCII(%s)", str)
		compareRemoteExpr(t, conn, query)
	}
}

func TestBuiltinRepeat(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()
	counts := []string{"-1", "1.2", "3"}
	cases := []string{
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
		"_utf8mb3 'abcABCÅå'",
	}
	for _, str := range cases {
		for _, cnt := range counts {
			query := fmt.Sprintf("repeat(%s, %s)", str, cnt)
			compareRemoteExpr(t, conn, query)
		}
	}
}
