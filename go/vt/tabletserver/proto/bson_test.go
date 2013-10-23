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
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

type badQuery struct {
	Extra         int
	Sql           string
	BindVariables map[string]interface{}
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

func TestQuery(t *testing.T) {
	reflected, err := bson.Marshal(&reflectQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		TransactionId: 1,
		ConnectionId:  2,
		SessionId:     3,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := Query{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		TransactionId: 1,
		ConnectionId:  2,
		SessionId:     3,
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

	unexpected, err := bson.Marshal(&badQuery{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}

type reflectSession struct {
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

type badSession struct {
	Extra         int
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

func TestSession(t *testing.T) {
	reflected, err := bson.Marshal(&reflectSession{
		TransactionId: 1,
		ConnectionId:  2,
		SessionId:     3,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := Session{
		TransactionId: 1,
		ConnectionId:  2,
		SessionId:     3,
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

	unexpected, err := bson.Marshal(&badSession{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}

type reflectBoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

type badBoundQuery struct {
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

	unexpected, err := bson.Marshal(&badBoundQuery{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}
