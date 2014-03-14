// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/bson"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

var commonSession = Session{
	InTransaction: true,
	ShardSessions: []*ShardSession{{
		Keyspace:      "a",
		Shard:         "0",
		TabletType:    topo.TabletType("replica"),
		TransactionId: 1,
	}, {
		Keyspace:      "b",
		Shard:         "1",
		TabletType:    topo.TabletType("master"),
		TransactionId: 2,
	}},
}

type reflectSession struct {
	InTransaction bool
	ShardSessions []*ShardSession
}

type extraSession struct {
	Extra         int
	InTransaction bool
	ShardSessions []*ShardSession
}

func TestSession(t *testing.T) {
	reflected, err := bson.Marshal(&reflectSession{
		InTransaction: true,
		ShardSessions: []*ShardSession{{
			Keyspace:      "a",
			Shard:         "0",
			TabletType:    topo.TabletType("replica"),
			TransactionId: 1,
		}, {
			Keyspace:      "b",
			Shard:         "1",
			TabletType:    topo.TabletType("master"),
			TransactionId: 2,
		}},
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := commonSession
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled Session
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraSession{})
	if err != nil {
		t.Fatal(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectQueryShard struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	Shards        []string
	TabletType    topo.TabletType
	Session       *Session
}

type extraQueryShard struct {
	Extra         int
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	Shards        []string
	TabletType    topo.TabletType
	Session       *Session
}

func TestQueryShard(t *testing.T) {
	reflected, err := bson.Marshal(&reflectQueryShard{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		Keyspace:      "keyspace",
		Shards:        []string{"shard1", "shard2"},
		TabletType:    topo.TabletType("replica"),
		Session:       &commonSession,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := QueryShard{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		Keyspace:      "keyspace",
		Shards:        []string{"shard1", "shard2"},
		TabletType:    topo.TabletType("replica"),
		Session:       &commonSession,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled QueryShard
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraQueryShard{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

func TestQueryResult(t *testing.T) {
	// We can't do the reflection test because bson
	// doesn't do it correctly for embedded fields.
	want := "o\x01\x00\x00" +
		"\x04Fields\x00*\x00\x00\x00" +
		"\x030\x00\"\x00\x00\x00" +
		"\x05Name\x00\x04\x00\x00\x00\x00name" +
		"\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
		"?RowsAffected\x00\x02\x00\x00\x00\x00\x00\x00\x00" +
		"?InsertId\x00\x03\x00\x00\x00\x00\x00\x00\x00" +
		"\x04Rows\x00 \x00\x00\x00" +
		"\x040\x00\x18\x00\x00\x00" +
		"\x050\x00\x01\x00\x00\x00" +
		"\x001\x051\x00\x02\x00\x00\x00\x00aa" +
		"\x00\x00" +
		"\x03Session\x00\xd0\x00\x00\x00" +
		"\bInTransaction\x00\x01" +
		"\x04ShardSessions\x00\xac\x00\x00\x00" +
		"\x030\x00Q\x00\x00\x00" +
		"\x05Keyspace\x00\x01\x00\x00\x00\x00a" +
		"\x05Shard\x00\x01\x00\x00\x00\x000" +
		"\x05TabletType\x00\a\x00\x00\x00\x00replica" +
		"\x12TransactionId\x00\x01\x00\x00\x00\x00\x00\x00\x00" +
		"\x00" +
		"\x031\x00P\x00\x00\x00" +
		"\x05Keyspace\x00\x01\x00\x00\x00\x00b" +
		"\x05Shard\x00\x01\x00\x00\x00\x001" +
		"\x05TabletType\x00\x06\x00\x00\x00\x00master" +
		"\x12TransactionId\x00\x02\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00" +
		"\x05Error\x00\x05\x00\x00\x00\x00error" +
		"\x00"

	custom := QueryResult{
		Fields:       []mproto.Field{{"name", 1}},
		RowsAffected: 2,
		InsertId:     3,
		Rows: [][]sqltypes.Value{
			{{sqltypes.String("1")}, {sqltypes.String("aa")}},
		},
		Session: &commonSession,
		Error:   "error",
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled QueryResult
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}
}

type reflectBoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

type reflectBatchQueryShard struct {
	Queries    []reflectBoundQuery
	Keyspace   string
	Shards     []string
	TabletType topo.TabletType
	Session    *Session
}

type extraBatchQueryShard struct {
	Extra      int
	Queries    []reflectBoundQuery
	Keyspace   string
	Shards     []string
	TabletType topo.TabletType
	Session    *Session
}

func TestBatchQueryShard(t *testing.T) {
	reflected, err := bson.Marshal(&reflectBatchQueryShard{
		Queries: []reflectBoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		Keyspace: "keyspace",
		Shards:   []string{"shard1", "shard2"},
		Session: &Session{InTransaction: true,
			ShardSessions: []*ShardSession{{
				Keyspace:      "a",
				Shard:         "0",
				TabletType:    topo.TabletType("replica"),
				TransactionId: 1,
			}, {
				Keyspace:      "b",
				Shard:         "1",
				TabletType:    topo.TabletType("master"),
				TransactionId: 2,
			}},
		},
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := BatchQueryShard{
		Queries: []tproto.BoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		Keyspace: "keyspace",
		Shards:   []string{"shard1", "shard2"},
		Session:  &commonSession,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled BatchQueryShard
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraBatchQueryShard{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type badTypeBatchQueryShard struct {
	Queries    string
	Keyspace   string
	Shards     []string
	TabletType topo.TabletType
	Session    *Session
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

type reflectQueryResultList struct {
	List    []mproto.QueryResult
	Session *Session
	Error   string
}

type extraQueryResultList struct {
	Extra   int
	List    []mproto.QueryResult
	Session *Session
	Error   string
}

func TestQueryResultList(t *testing.T) {
	reflected, err := bson.Marshal(&reflectQueryResultList{
		List: []mproto.QueryResult{{
			Fields:       []mproto.Field{{"name", 1}},
			RowsAffected: 2,
			InsertId:     3,
			Rows: [][]sqltypes.Value{
				{{sqltypes.String("1")}, {sqltypes.String("aa")}},
			},
		}},
		Session: &commonSession,
		Error:   "error",
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := QueryResultList{
		List: []mproto.QueryResult{{
			Fields:       []mproto.Field{{"name", 1}},
			RowsAffected: 2,
			InsertId:     3,
			Rows: [][]sqltypes.Value{
				{{sqltypes.String("1")}, {sqltypes.String("aa")}},
			},
		}},
		Session: &commonSession,
		Error:   "error",
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled QueryResultList
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraQueryResultList{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectKeyspaceIdQuery struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	KeyspaceIds   []string
	TabletType    topo.TabletType
	Session       *Session
}

type extraKeyspaceIdQuery struct {
	Extra         int
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	KeyspaceIds   []string
	TabletType    topo.TabletType
	Session       *Session
}

func TestKeyspaceIdQuery(t *testing.T) {
	reflected, err := bson.Marshal(&reflectKeyspaceIdQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		Keyspace:      "keyspace",
		KeyspaceIds:   []string{"10", "18"},
		TabletType:    "replica",
		Session:       &commonSession,
	})

	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := KeyspaceIdQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		Keyspace:      "keyspace",
		KeyspaceIds:   []string{"10", "18"},
		TabletType:    "replica",
		Session:       &commonSession,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled KeyspaceIdQuery
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraKeyspaceIdQuery{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectKeyRangeQuery struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	KeyRange      string
	TabletType    topo.TabletType
	Session       *Session
}

type extraKeyRangeQuery struct {
	Extra         int
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	KeyRange      string
	TabletType    topo.TabletType
	Session       *Session
}

func TestKeyRangeQuery(t *testing.T) {
	reflected, err := bson.Marshal(&reflectKeyRangeQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		Keyspace:      "keyspace",
		KeyRange:      "10-18",
		TabletType:    "replica",
		Session:       &commonSession,
	})

	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := KeyRangeQuery{
		Sql:           "query",
		BindVariables: map[string]interface{}{"val": int64(1)},
		Keyspace:      "keyspace",
		KeyRange:      "10-18",
		TabletType:    "replica",
		Session:       &commonSession,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled KeyRangeQuery
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraKeyRangeQuery{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectKeyspaceIdBatchQuery struct {
	Queries     []reflectBoundQuery
	Keyspace    string
	KeyspaceIds []string
	TabletType  topo.TabletType
	Session     *Session
}

type extraKeyspaceIdBatchQuery struct {
	Extra       int
	Queries     []reflectBoundQuery
	Keyspace    string
	KeyspaceIds []string
	TabletType  topo.TabletType
	Session     *Session
}

func TestKeyspaceIdBatchQuery(t *testing.T) {
	reflected, err := bson.Marshal(&reflectKeyspaceIdBatchQuery{
		Queries: []reflectBoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		Keyspace:    "keyspace",
		KeyspaceIds: []string{"10", "20"},
		Session: &Session{InTransaction: true,
			ShardSessions: []*ShardSession{{
				Keyspace:      "a",
				Shard:         "0",
				TabletType:    topo.TabletType("replica"),
				TransactionId: 1,
			}, {
				Keyspace:      "b",
				Shard:         "1",
				TabletType:    topo.TabletType("master"),
				TransactionId: 2,
			}},
		},
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := KeyspaceIdBatchQuery{
		Queries: []tproto.BoundQuery{{
			Sql:           "query",
			BindVariables: map[string]interface{}{"val": int64(1)},
		}},
		Keyspace:    "keyspace",
		KeyspaceIds: []string{"10", "20"},
		Session:     &commonSession,
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%+v, got\n%+v", want, got)
	}

	var unmarshalled KeyspaceIdBatchQuery
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%+v, got \n%+v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraKeyspaceIdBatchQuery{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type badTypeKeyspaceIdsBatchQuery struct {
	Queries     string
	Keyspace    string
	KeyspaceIds []string
	TabletType  topo.TabletType
	Session     *Session
}

func TestKeyspaceIdsBatchQueryBadType(t *testing.T) {
	unexpected, err := bson.Marshal(&badTypeKeyspaceIdsBatchQuery{})
	if err != nil {
		t.Error(err)
	}
	var unmarshalled KeyspaceIdBatchQuery
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want := "Unexpected data type 5 for Queries"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}
