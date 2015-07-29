// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateservice provides to interface definition for the
// vtgate service
package vtgateservice

import (
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

// VTGateService is the interface implemented by the VTGate service,
// that RPC server implementations will call.
type VTGateService interface {
	// Regular query execution
	Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) error
	ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) error
	ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error
	ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error
	ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error
	ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error
	ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error

	// Streaming queries
	StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*proto.QueryResult) error) error
	StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error
	StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error
	StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error

	// Transaction management
	Begin(ctx context.Context, outSession *proto.Session) error
	Commit(ctx context.Context, inSession *proto.Session) error
	Rollback(ctx context.Context, inSession *proto.Session) error

	// Map Reduce support
	SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error

	// Topology support
	GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error)

	// HandlePanic should be called with defer at the beginning of each
	// RPC implementation method, before calling any of the previous methods
	HandlePanic(err *error)
}
