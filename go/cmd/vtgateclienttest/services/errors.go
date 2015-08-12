// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"fmt"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// errorClient implements vtgateservice.VTGateService
// and returns specific errors. It is meant to test all possible error cases,
// and make sure all clients handle the errors correctly.
//
// So far, we understand:
// - "return integrity error": Execute* will return an integrity error.
// - "error": GetSrvKeyspace will return an error.
//
// TODO(alainjobart) Add throttling error.
// TODO(alainjobart) Add all errors the client may care about.
type errorClient struct {
	fallback vtgateservice.VTGateService
}

func newErrorClient(fallback vtgateservice.VTGateService) *errorClient {
	return &errorClient{
		fallback: fallback,
	}
}

func (c *errorClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.Execute returning integrity error (errno 1062)")
	}
	return c.fallback.Execute(ctx, sql, bindVariables, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteShard returning integrity error (errno 1062)")
	}
	return c.fallback.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds []key.KeyspaceId, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteKeyspaceIds returning integrity error (errno 1062)")
	}
	return c.fallback.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	if query.Sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteKeyRanges returning integrity error (errno 1062)")
	}
	return c.fallback.ExecuteKeyRanges(ctx, query, reply)
}

func (c *errorClient) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	if query.Sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteEntityIds returning integrity error (errno 1062)")
	}
	return c.fallback.ExecuteEntityIds(ctx, query, reply)
}

func (c *errorClient) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	if len(batchQuery.Queries) == 1 && batchQuery.Queries[0].Sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteBatchShard returning integrity error (errno 1062)")
	}
	return c.fallback.ExecuteBatchShard(ctx, batchQuery, reply)
}

func (c *errorClient) ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	if len(batchQuery.Queries) == 1 && batchQuery.Queries[0].Sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteBatchKeyspaceIds returning integrity error (errno 1062)")
	}
	return c.fallback.ExecuteBatchKeyspaceIds(ctx, batchQuery, reply)
}

func (c *errorClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecute(ctx, sql, bindVariables, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecuteKeyRanges(ctx, query, sendReply)
}

func (c *errorClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds []key.KeyspaceId, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, sendReply)
}

func (c *errorClient) Begin(ctx context.Context, outSession *proto.Session) error {
	return c.fallback.Begin(ctx, outSession)
}

func (c *errorClient) Commit(ctx context.Context, inSession *proto.Session) error {
	return c.fallback.Commit(ctx, inSession)
}

func (c *errorClient) Rollback(ctx context.Context, inSession *proto.Session) error {
	return c.fallback.Rollback(ctx, inSession)
}

func (c *errorClient) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return c.fallback.SplitQuery(ctx, req, reply)
}

func (c *errorClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	if keyspace == "error" {
		return nil, fmt.Errorf("vtgate test client, errorClient.GetSrvKeyspace returning error")
	}
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}

func (c *errorClient) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}
