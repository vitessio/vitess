// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateservice provides to interface definition for the
// vtgate service
package vtgateservice

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// VTGateService is the interface implemented by the VTGate service,
// that RPC server implementations will call.
type VTGateService interface {
	// Regular query execution.
	// All these methods can change the provided session.
	Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error)
	ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, reply *proto.QueryResult) error
	ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, reply *proto.QueryResult) error
	ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, reply *proto.QueryResult) error
	ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, reply *proto.QueryResult) error
	ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, reply *proto.QueryResultList) error
	ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, reply *proto.QueryResultList) error

	// Streaming queries
	StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*proto.QueryResult) error) error
	StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, sendReply func(*proto.QueryResult) error) error
	StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, sendReply func(*proto.QueryResult) error) error
	StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, sendReply func(*proto.QueryResult) error) error

	// Transaction management
	Begin(ctx context.Context, outSession *vtgatepb.Session) error
	Commit(ctx context.Context, inSession *vtgatepb.Session) error
	Rollback(ctx context.Context, inSession *vtgatepb.Session) error

	// Map Reduce support
	SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*vtgatepb.SplitQueryResponse_Part, error)

	// Topology support
	GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error)

	// GetSrvShard is not part of the public API, but might be used
	// by some implementations.
	GetSrvShard(ctx context.Context, keyspace, shard string) (*topodatapb.SrvShard, error)

	// HandlePanic should be called with defer at the beginning of each
	// RPC implementation method, before calling any of the previous methods
	HandlePanic(err *error)
}
