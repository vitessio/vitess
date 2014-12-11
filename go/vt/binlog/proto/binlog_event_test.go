// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"testing"

	mproto "github.com/henryanand/vitess/go/mysql/proto"
)

func TestQueryString(t *testing.T) {
	input := Query{
		Database: "test_database",
		Charset: &mproto.Charset{
			Client: 12,
			Conn:   34,
			Server: 56,
		},
		Sql: []byte("sql"),
	}
	want := `{Database: "test_database", Charset: &{12 34 56}, Sql: "sql"}`
	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestQueryStringNilCharset(t *testing.T) {
	input := Query{
		Database: "test_database",
		Charset:  nil,
		Sql:      []byte("sql"),
	}
	want := `{Database: "test_database", Charset: <nil>, Sql: "sql"}`
	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestBinlogFormatIsZero(t *testing.T) {
	table := map[BinlogFormat]bool{
		BinlogFormat{}:                 true,
		BinlogFormat{FormatVersion: 1}: false,
		BinlogFormat{HeaderLength: 1}:  false,
	}
	for input, want := range table {
		if got := input.IsZero(); got != want {
			t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
		}
	}
}
