// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"testing"

	"github.com/youtube/vitess/go/bson"
)

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
