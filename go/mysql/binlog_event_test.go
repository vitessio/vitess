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
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestQueryString(t *testing.T) {
	input := Query{
		Database: "test_database",
		Charset: &binlogdatapb.Charset{
			Client: 12,
			Conn:   34,
			Server: 56,
		},
		SQL: "sql",
	}
	want := `{Database: "test_database", Charset: client:12 conn:34 server:56 , SQL: "sql"}`
	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestQueryStringNilCharset(t *testing.T) {
	input := Query{
		Database: "test_database",
		Charset:  nil,
		SQL:      "sql",
	}
	want := `{Database: "test_database", Charset: <nil>, SQL: "sql"}`
	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestBinlogFormatIsZero(t *testing.T) {
	table := map[*BinlogFormat]bool{
		{}:                 true,
		{FormatVersion: 1}: false,
		{HeaderLength: 1}:  false,
	}
	for input, want := range table {
		if got := input.IsZero(); got != want {
			t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
		}
	}
}
