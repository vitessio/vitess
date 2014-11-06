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

// EntityId represents a tuple of external_id and keyspace_id
type EntityId struct {
	ExternalID interface{}
	KeyspaceID kproto.KeyspaceId
}

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

// QueryResult is mproto.QueryResult+Session (for now).
type QueryResult struct {
	Result  *mproto.QueryResult
	Session *Session
	Error   string
}

// BatchQueryShard represents a batch query request
// for the specified shards.
type BatchQueryShard struct {
	Queries    []tproto.BoundQuery
	Keyspace   string
	Shards     []string
	TabletType topo.TabletType
	Session    *Session
}

// KeyspaceIdBatchQuery represents a batch query request
// for the specified keyspace IDs.
type KeyspaceIdBatchQuery struct {
	Queries     []tproto.BoundQuery
	Keyspace    string
	KeyspaceIds []kproto.KeyspaceId
	TabletType  topo.TabletType
	Session     *Session
}

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
