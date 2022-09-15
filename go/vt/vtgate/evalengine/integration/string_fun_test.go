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
	"9223372036854775807",
	"-9223372036854775808",
	"999999999999999999999999",
	"-999999999999999999999999",
}

func TestBuiltinLowerandLcase(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, str := range cases {
		query := fmt.Sprintf("LOWER(%s)", str)
		compareRemoteExpr(t, conn, query)

		query = fmt.Sprintf("LCASE(%s)", str)
		compareRemoteExpr(t, conn, query)
	}
}

func TestBuiltinUpperandUcase(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

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
		query := fmt.Sprintf("Char_Length(%s)", str)
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
