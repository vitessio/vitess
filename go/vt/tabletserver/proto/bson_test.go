// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"testing"

	"github.com/youtube/vitess/go/bson"
)

type reflectQuery struct {
	Sql           string
	BindVariables map[string]interface{}
	SessionId     int64
	TransactionId int64
}

type extraQuery struct {
	Extra         int
	Sql           string
	BindVariables map[string]interface{}
	SessionId     int64
	TransactionId int64
}

func TestQuery(t *testing.T) {
	reflected, err := bson.Marshal(&reflectQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		SessionId:     2,
		TransactionId: 1,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := Query{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		SessionId:     2,
		TransactionId: 1,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled Query
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.Sql != unmarshalled.Sql {
		t.Errorf("want %v, got %v", custom.Sql, unmarshalled.Sql)
	}
	if custom.TransactionId != unmarshalled.TransactionId {
		t.Errorf("want %v, got %v", custom.TransactionId, unmarshalled.TransactionId)
	}
	if custom.SessionId != unmarshalled.SessionId {
		t.Errorf("want %v, got %v", custom.SessionId, unmarshalled.SessionId)
	}
	if custom.BindVariables["val"].(int64) != unmarshalled.BindVariables["val"].(int64) {
		t.Errorf("want %v, got %v", custom.BindVariables["val"], unmarshalled.BindVariables["val"])
	}

	extra, err := bson.Marshal(&extraQuery{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectSession struct {
	SessionId     int64
	TransactionId int64
}

type extraSession struct {
	Extra         int
	SessionId     int64
	TransactionId int64
}

func TestSession(t *testing.T) {
	reflected, err := bson.Marshal(&reflectSession{
		SessionId:     2,
		TransactionId: 1,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := Session{
		SessionId:     2,
		TransactionId: 1,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled Session
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom != unmarshalled {
		t.Errorf("want %v, got %#v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraSession{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectBoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

type extraBoundQuery struct {
	Extra         int
	Sql           string
	BindVariables map[string]interface{}
}

func TestBoundQuery(t *testing.T) {
	reflected, err := bson.Marshal(&reflectBoundQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := BoundQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled BoundQuery
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.Sql != unmarshalled.Sql {
		t.Errorf("want %v, got %v", custom.Sql, unmarshalled.Sql)
	}
	if custom.BindVariables["val"].(int64) != unmarshalled.BindVariables["val"].(int64) {
		t.Errorf("want %v, got %v", custom.BindVariables["val"], unmarshalled.BindVariables["val"])
	}

	extra, err := bson.Marshal(&extraBoundQuery{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectQueryList struct {
	Queries       []BoundQuery
	SessionId     int64
	TransactionId int64
}

type extraQueryList struct {
	Extra         int
	Queries       []BoundQuery
	SessionId     int64
	TransactionId int64
}

func TestQueryList(t *testing.T) {
	reflected, err := bson.Marshal(&reflectQueryList{
		Queries: []BoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		SessionId:     2,
		TransactionId: 1,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := QueryList{
		Queries: []BoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		SessionId:     2,
		TransactionId: 1,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled QueryList
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.TransactionId != unmarshalled.TransactionId {
		t.Errorf("want %v, got %v", custom.TransactionId, unmarshalled.TransactionId)
	}
	if custom.SessionId != unmarshalled.SessionId {
		t.Errorf("want %v, got %v", custom.SessionId, unmarshalled.SessionId)
	}
	if custom.Queries[0].Sql != unmarshalled.Queries[0].Sql {
		t.Errorf("want %v, got %v", custom.Queries[0].Sql, unmarshalled.Queries[0].Sql)
	}
	if custom.Queries[0].BindVariables["val"].(int64) != unmarshalled.Queries[0].BindVariables["val"].(int64) {
		t.Errorf("want %v, got %v", custom.Queries[0].BindVariables["val"], unmarshalled.Queries[0].BindVariables["val"])
	}

	extra, err := bson.Marshal(&extraQueryList{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}
