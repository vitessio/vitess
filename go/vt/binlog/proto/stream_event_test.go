// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/bson"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

type reflectStreamEvent struct {
	Category         string
	TableName        string
	PrimaryKeyFields []mproto.Field
	PrimaryKeyValues [][]sqltypes.Value
	Sql              string
	Timestamp        int64
	GTIDField        myproto.GTIDField
}

type extraStreamEvent struct {
	Extra            int
	Category         string
	TableName        string
	PrimaryKeyFields []mproto.Field
	PrimaryKeyValues [][]sqltypes.Value
	Sql              string
	Timestamp        int64
	GTIDField        myproto.GTIDField
}

func TestStreamEvent(t *testing.T) {
	reflected, err := bson.Marshal(&reflectStreamEvent{
		Category:  "str1",
		TableName: "str2",
		PrimaryKeyFields: []mproto.Field{
			mproto.Field{
				Name: "str2",
				Type: mproto.VT_VARCHAR,
			},
			mproto.Field{
				Name: "str3",
				Type: mproto.VT_LONGLONG,
			},
			mproto.Field{
				Name: "str4",
				Type: mproto.VT_LONGLONG,
			},
		},
		PrimaryKeyValues: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("str5")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("18446744073709551615")),
			},
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("str6")),
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("18446744073709551614")),
			},
		},
		Sql:       "str7",
		Timestamp: 3,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := StreamEvent{
		Category:  "str1",
		TableName: "str2",
		PrimaryKeyFields: []mproto.Field{
			mproto.Field{
				Name: "str2",
				Type: mproto.VT_VARCHAR,
			},
			mproto.Field{
				Name: "str3",
				Type: mproto.VT_LONGLONG,
			},
			mproto.Field{
				Name: "str4",
				Type: mproto.VT_LONGLONG,
			},
		},
		PrimaryKeyValues: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("str5")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("18446744073709551615")),
			},
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("str6")),
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("18446744073709551614")),
			},
		},
		Sql:       "str7",
		Timestamp: 3,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled StreamEvent
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	want = fmt.Sprintf("%#v", custom)
	got = fmt.Sprintf("%#v", unmarshalled)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	extra, err := bson.Marshal(&extraStreamEvent{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}
