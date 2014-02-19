// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/bson"
)

type reflectStreamEvent struct {
	Category   string
	TableName  string
	PKColNames []string
	PKValues   [][]interface{}
	Sql        string
	Timestamp  int64
	GroupId    int64
}

type extraStreamEvent struct {
	Extra      int
	Category   string
	TableName  string
	PKColNames []string
	PKValues   [][]interface{}
	Sql        string
	Timestamp  int64
	GroupId    int64
}

func TestStreamEvent(t *testing.T) {
	reflected, err := bson.Marshal(&reflectStreamEvent{
		Category:   "str1",
		TableName:  "str2",
		PKColNames: []string{"str3", "str4"},
		PKValues: [][]interface{}{
			[]interface{}{
				"str5", 1, uint64(0xffffffffffffffff),
			},
			[]interface{}{
				"str6", 2, uint64(0xfffffffffffffffe),
			},
		},
		Sql:       "str7",
		Timestamp: 3,
		GroupId:   8,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := StreamEvent{
		Category:   "str1",
		TableName:  "str2",
		PKColNames: []string{"str3", "str4"},
		PKValues: [][]interface{}{
			[]interface{}{
				"str5", 1, uint64(0xffffffffffffffff),
			},
			[]interface{}{
				"str6", 2, uint64(0xfffffffffffffffe),
			},
		},
		Sql:       "str7",
		Timestamp: 3,
		GroupId:   8,
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
