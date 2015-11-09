// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/proto/query"
)

type reflectStreamEvent struct {
	Category         string
	TableName        string
	PrimaryKeyFields []BSONField
	PrimaryKeyValues [][]sqltypes.Value
	Sql              string
	Timestamp        int64
	TransactionID    string
}

type extraStreamEvent struct {
	Extra            int
	Category         string
	TableName        string
	PrimaryKeyFields []BSONField
	PrimaryKeyValues [][]sqltypes.Value
	Sql              string
	Timestamp        int64
	TransactionID    string
}

func TestStreamEvent(t *testing.T) {
	reflected, err := bson.Marshal(&reflectStreamEvent{
		Category:  "str1",
		TableName: "str2",
		PrimaryKeyFields: []BSONField{
			BSONField{
				Name: "str2",
				Type: mysql.TypeVarString,
			},
			BSONField{
				Name: "str3",
				Type: mysql.TypeLonglong,
			},
			BSONField{
				Name: "str4",
				Type: mysql.TypeLonglong,
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
		PrimaryKeyFields: []*query.Field{
			&query.Field{
				Name: "str2",
				Type: sqltypes.VarChar,
			},
			&query.Field{
				Name: "str3",
				Type: sqltypes.Int64,
			},
			&query.Field{
				Name: "str4",
				Type: sqltypes.Int64,
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
	want = fmt.Sprintf("%+v", custom)
	got = fmt.Sprintf("%+v", unmarshalled)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
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
