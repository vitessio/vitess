// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	kproto "github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// Session represents the session state. It keeps track of
// the shards on which transactions are in progress, along
// with the corresponding tranaction ids.
type Session struct {
	InTransaction bool
	ShardSessions []*ShardSession
}

//go:generate bsongen -file $GOFILE -type Session -o session_bson.go

func (session *Session) String() string {
	return fmt.Sprintf("InTransaction: %v, ShardSession: %+v", session.InTransaction, session.ShardSessions)
}

// ShardSession represents the session state for a shard.
type ShardSession struct {
	Keyspace      string
	Shard         string
	TabletType    topo.TabletType
	TransactionId int64
}

//go:generate bsongen -file $GOFILE -type ShardSession -o shard_session_bson.go

func (shardSession *ShardSession) String() string {
	return fmt.Sprintf("Keyspace: %v, Shard: %v, TabletType: %v, TransactionId: %v", shardSession.Keyspace, shardSession.Shard, shardSession.TabletType, shardSession.TransactionId)
}

// Query represents a keyspace agnostic query request.
type Query struct {
	Sql           string
	BindVariables map[string]interface{}
	TabletType    topo.TabletType
	Session       *Session
}

//go:generate bsongen -file $GOFILE -type Query -o query_bson.go

// QueryShard represents a query request for the
// specified list of shards.
type QueryShard struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	Shards        []string
	TabletType    topo.TabletType
	Session       *Session
}

//go:generate bsongen -file $GOFILE -type QueryShard -o query_shard_bson.go

// KeyspaceIdQuery represents a query request for the
// specified list of keyspace IDs.
type KeyspaceIdQuery struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	KeyspaceIds   []kproto.KeyspaceId
	TabletType    topo.TabletType
	Session       *Session
}

//go:generate bsongen -file $GOFILE -type KeyspaceIdQuery -o keyspace_id_query_bson.go

// KeyRangeQuery represents a query request for the
// specified list of keyranges.
type KeyRangeQuery struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	KeyRanges     []kproto.KeyRange
	TabletType    topo.TabletType
	Session       *Session
}

//go:generate bsongen -file $GOFILE -type KeyRangeQuery -o key_range_query_bson.go

// EntityId represents a tuple of external_id and keyspace_id
type EntityId struct {
	ExternalID interface{}
	KeyspaceID kproto.KeyspaceId
}

//go:generate bsongen -file $GOFILE -type EntityId -o entity_id_bson.go

// EntityIdsQuery represents a query request for the specified KeyspaceId map.
type EntityIdsQuery struct {
	Sql               string
	BindVariables     map[string]interface{}
	Keyspace          string
	EntityColumnName  string
	EntityKeyspaceIDs []EntityId
	TabletType        topo.TabletType
	Session           *Session
}

//go:generate bsongen -file $GOFILE -type EntityIdsQuery -o entity_ids_query_bson.go

// QueryResult is mproto.QueryResult+Session (for now).
type QueryResult struct {
	Result  *mproto.QueryResult
	Session *Session
	Error   string
}

//go:generate bsongen -file $GOFILE -type QueryResult -o query_result_bson.go

// BatchQueryShard represents a batch query request
// for the specified shards.
type BatchQueryShard struct {
	Queries    []tproto.BoundQuery
	Keyspace   string
	Shards     []string
	TabletType topo.TabletType
	Session    *Session
}

//go:generate bsongen -file $GOFILE -type BatchQueryShard -o batch_query_shard_bson.go

// KeyspaceIdBatchQuery represents a batch query request
// for the specified keyspace IDs.
type KeyspaceIdBatchQuery struct {
	Queries     []tproto.BoundQuery
	Keyspace    string
	KeyspaceIds []kproto.KeyspaceId
	TabletType  topo.TabletType
	Session     *Session
}

//go:generate bsongen -file $GOFILE -type KeyspaceIdBatchQuery -o keyspace_id_batch_query_bson.go

// QueryResultList is mproto.QueryResultList+Session
type QueryResultList struct {
	List    []mproto.QueryResult
	Session *Session
	Error   string
}

// SplitQueryRequest is a request to split a query into multiple parts
type SplitQueryRequest struct {
	Keyspace   string
	Query      tproto.BoundQuery
	SplitCount int
}

// SplitQueryPart is a sub query of SplitQueryRequest.Query
type SplitQueryPart struct {
	Query *KeyRangeQuery
	Size  int64
}

// Result for SplitQueryRequest
type SplitQueryResult struct {
	Splits []SplitQueryPart
}
