// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/bson"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

type reflectBinlogTransaction struct {
	Statements []reflectStatement
	Timestamp  int64
	GTIDField  myproto.GTIDField
}

type extraBinlogTransaction struct {
	Extra      int
	Statements []reflectStatement
	Timestamp  int64
	GTIDField  myproto.GTIDField
}

type reflectStatement struct {
	Category int
	Charset  *mproto.Charset
	Sql      []byte
}

func TestBinlogTransaction(t *testing.T) {
	reflected, err := bson.Marshal(&reflectBinlogTransaction{
		Statements: []reflectStatement{
			{
				Category: 1,
				Charset:  &mproto.Charset{Client: 12, Conn: 34, Server: 56},
				Sql:      []byte("sql"),
			},
		},
		Timestamp: 456,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := BinlogTransaction{
		Statements: []Statement{
			{
				Category: 1,
				Charset:  &mproto.Charset{Client: 12, Conn: 34, Server: 56},
				Sql:      []byte("sql"),
			},
		},
		Timestamp: 456,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled BinlogTransaction
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("%#v != %#v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraBinlogTransaction{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

func TestStatementString(t *testing.T) {
	cs := &mproto.Charset{Client: 12, Conn: 34, Server: 56}
	table := map[string]Statement{
		`{Category: BL_UNRECOGNIZED, Charset: &{12 34 56}, Sql: "SQL"}`: Statement{Category: BL_UNRECOGNIZED, Charset: cs, Sql: []byte("SQL")},
		`{Category: BL_BEGIN, Charset: &{12 34 56}, Sql: "SQL"}`:        Statement{Category: BL_BEGIN, Charset: cs, Sql: []byte("SQL")},
		`{Category: BL_COMMIT, Charset: &{12 34 56}, Sql: "SQL"}`:       Statement{Category: BL_COMMIT, Charset: cs, Sql: []byte("SQL")},
		`{Category: BL_ROLLBACK, Charset: &{12 34 56}, Sql: "SQL"}`:     Statement{Category: BL_ROLLBACK, Charset: cs, Sql: []byte("SQL")},
		`{Category: BL_DML, Charset: &{12 34 56}, Sql: "SQL"}`:          Statement{Category: BL_DML, Charset: cs, Sql: []byte("SQL")},
		`{Category: BL_DDL, Charset: &{12 34 56}, Sql: "SQL"}`:          Statement{Category: BL_DDL, Charset: cs, Sql: []byte("SQL")},
		`{Category: BL_SET, Charset: &{12 34 56}, Sql: "SQL"}`:          Statement{Category: BL_SET, Charset: cs, Sql: []byte("SQL")},
		`{Category: 7, Charset: &{12 34 56}, Sql: "SQL"}`:               Statement{Category: 7, Charset: cs, Sql: []byte("SQL")},
	}
	for want, input := range table {
		if got := input.String(); got != want {
			t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
		}
	}
}
