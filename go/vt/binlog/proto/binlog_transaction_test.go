// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/bson"
)

type reflectBinlogTransaction struct {
	Statements []reflectStatement
	Timestamp  int64
	GroupId    int64
}

type extraBinlogTransaction struct {
	Extra      int
	Statements []reflectStatement
	Timestamp  int64
	GroupId    int64
}

type reflectStatement struct {
	Category int
	Sql      []byte
}

func TestBinlogTransaction(t *testing.T) {
	reflected, err := bson.Marshal(&reflectBinlogTransaction{
		Statements: []reflectStatement{
			{
				Category: 1,
				Sql:      []byte("sql"),
			},
		},
		Timestamp: 456,
		GroupId:   123,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := BinlogTransaction{
		Statements: []Statement{
			{
				Category: 1,
				Sql:      []byte("sql"),
			},
		},
		Timestamp: 456,
		GroupId:   123,
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
