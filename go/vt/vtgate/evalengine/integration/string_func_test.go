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

func TestBuiltinAscii(t *testing.T) {
	var elems = []string{
		"NULL",
		"\"a\"",
		"\"abc\"",
		"1",
		"-1",
		"0123",
		"0xAACC",
		"3.1415926",
		"9223372036854775807",
		"-9223372036854775808",
		"999999999999999999999999",
		"-999999999999999999999999",
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	t.Run("ASCII", func(t *testing.T) {
		for i := 0; i < len(elems); i++ {
			query := fmt.Sprintf("SELECT ASCII(%s)", elems[i])
			compareRemoteQuery(t, conn, query)
		}
	})
}

func TestBuiltinBin(t *testing.T) {
	var elems = []string{
		"NULL",
		"\"a\"",
		"\"101\"",
		"\"-101\"",
		"1",
		"-1",
		"20",
		"-100",
		"3.1415926",
		"9223372036854775807",
		"-9223372036854775808",
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	t.Run("BIN", func(t *testing.T) {
		for i := 0; i < len(elems); i++ {
			query := fmt.Sprintf("SELECT BIN(%s)", elems[i])
			compareRemoteQuery(t, conn, query)
		}
	})
}

func TestBuiltinBitLength(t *testing.T) {
	var elems = []string{
		"NULL",
		"\"a\"",
		"\"abc\"",
		"1",
		"-1",
		"20",
		"-100",
		"3.1415926",
		"9223372036854775807",
		"-9223372036854775808",
		"999999999999999999999999",
		"-999999999999999999999999",
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	t.Run("BIT_LENGTH", func(t *testing.T) {
		for i := 0; i < len(elems); i++ {
			query := fmt.Sprintf("SELECT BIT_LENGTH(%s)", elems[i])
			compareRemoteQuery(t, conn, query)
		}
	})
}
