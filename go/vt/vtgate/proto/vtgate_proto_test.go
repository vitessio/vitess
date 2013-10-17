// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"testing"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/vt/topo"
)

type reflectSessionParams struct {
	TabletType topo.TabletType
}

type badSessionParams struct {
	Extra      int
	TabletType topo.TabletType
}

func TestSessionParams(t *testing.T) {
	reflected, err := bson.Marshal(&reflectSessionParams{topo.TabletType("master")})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := SessionParams{topo.TabletType("master")}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled SessionParams
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom != unmarshalled {
		t.Errorf("want %v, got %#v", custom, unmarshalled)
	}

	unexpected, err := bson.Marshal(&badSessionParams{})
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
	SessionId int64
}

type badSession struct {
	Extra     int
	SessionId int64
}

func TestSession(t *testing.T) {
	reflected, err := bson.Marshal(&reflectSession{1})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := Session{1}
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

	unexpected, err := bson.Marshal(&badSessionParams{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}

type reflectQueryShard struct {
	Sql           string
	BindVariables map[string]interface{}
	SessionId     int64
	Keyspace      string
	Shards        []string
}

type badQueryShard struct {
	Extra         int
	Sql           string
	BindVariables map[string]interface{}
	SessionId     int64
	Keyspace      string
	Shards        []string
}

func TestQueryShard(t *testing.T) {
	reflected, err := bson.Marshal(&reflectQueryShard{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		SessionId:     1,
		Keyspace:      "keyspace",
		Shards:        []string{"shard1", "shard2"},
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := QueryShard{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		SessionId:     1,
		Keyspace:      "keyspace",
		Shards:        []string{"shard1", "shard2"},
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled QueryShard
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.Sql != unmarshalled.Sql {
		t.Errorf("want %v, got %v", custom.Sql, unmarshalled.Sql)
	}
	if custom.SessionId != unmarshalled.SessionId {
		t.Errorf("want %v, got %v", custom.SessionId, unmarshalled.SessionId)
	}
	if custom.Keyspace != unmarshalled.Keyspace {
		t.Errorf("want %v, got %v", custom.Keyspace, unmarshalled.Keyspace)
	}
	if custom.BindVariables["val"].(int64) != unmarshalled.BindVariables["val"].(int64) {
		t.Errorf("want %v, got %v", custom.BindVariables["val"], unmarshalled.BindVariables["val"])
	}
	if custom.Shards[0] != unmarshalled.Shards[0] {
		t.Errorf("want %v, got %v", custom.Shards[0], unmarshalled.Shards[0])
	}
	if custom.Shards[1] != unmarshalled.Shards[1] {
		t.Errorf("want %v, got %v", custom.Shards[1], unmarshalled.Shards[1])
	}

	unexpected, err := bson.Marshal(&badQueryShard{})
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

type reflectBatchQueryShard struct {
	Queries   []reflectBoundQuery
	SessionId int64
	Keyspace  string
	Shards    []string
}

type badBatchQueryShard struct {
	Extra     int
	Queries   []reflectBoundQuery
	SessionId int64
	Keyspace  string
	Shards    []string
}

func TestBatchQueryShard(t *testing.T) {
	reflected, err := bson.Marshal(&reflectBatchQueryShard{
		Queries: []reflectBoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		SessionId: 1,
		Keyspace:  "keyspace",
		Shards:    []string{"shard1", "shard2"},
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := BatchQueryShard{
		Queries: []BoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		SessionId: 1,
		Keyspace:  "keyspace",
		Shards:    []string{"shard1", "shard2"},
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled BatchQueryShard
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.Queries[0].Sql != unmarshalled.Queries[0].Sql {
		t.Errorf("want %v, got %v", custom.Queries[0].Sql, unmarshalled.Queries[0].Sql)
	}
	if custom.SessionId != unmarshalled.SessionId {
		t.Errorf("want %v, got %v", custom.SessionId, unmarshalled.SessionId)
	}
	if custom.Keyspace != unmarshalled.Keyspace {
		t.Errorf("want %v, got %v", custom.Keyspace, unmarshalled.Keyspace)
	}
	if custom.Queries[0].BindVariables["val"].(int64) != unmarshalled.Queries[0].BindVariables["val"].(int64) {
		t.Errorf("want %v, got %v", custom.Queries[0].BindVariables["val"], unmarshalled.Queries[0].BindVariables["val"])
	}
	if custom.Shards[0] != unmarshalled.Shards[0] {
		t.Errorf("want %v, got %v", custom.Shards[0], unmarshalled.Shards[0])
	}
	if custom.Shards[1] != unmarshalled.Shards[1] {
		t.Errorf("want %v, got %v", custom.Shards[1], unmarshalled.Shards[1])
	}

	unexpected, err := bson.Marshal(&badBatchQueryShard{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}

type badTypeBatchQueryShard struct {
	Queries   string
	SessionId int64
	Keyspace  string
	Shards    []string
}

func TestBatchQueryShardBadType(t *testing.T) {
	unexpected, err := bson.Marshal(&badTypeBatchQueryShard{})
	if err != nil {
		t.Error(err)
	}
	var unmarshalled BatchQueryShard
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want := "Unexpected data type 5 for Queries"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}
