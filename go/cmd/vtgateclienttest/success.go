// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"
)

// successClient implements vtgateservice.VTGateService
// and returns specific values. It is meant to test all possible success cases,
// and make sure all clients handle any corner case correctly.
type successClient struct {
	fallback vtgateservice.VTGateService
}

func newSuccessClient(fallback vtgateservice.VTGateService) *successClient {
	return &successClient{
		fallback: fallback,
	}
}

func (c *successClient) Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) error {
	return c.fallback.Execute(ctx, query, reply)
}

func (c *successClient) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	return c.fallback.ExecuteShard(ctx, query, reply)
}

func (c *successClient) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	return c.fallback.ExecuteKeyspaceIds(ctx, query, reply)
}

func (c *successClient) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	return c.fallback.ExecuteKeyRanges(ctx, query, reply)
}

func (c *successClient) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	return c.fallback.ExecuteEntityIds(ctx, query, reply)
}

func (c *successClient) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	return c.fallback.ExecuteBatchShard(ctx, batchQuery, reply)
}

func (c *successClient) ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	return c.fallback.ExecuteBatchKeyspaceIds(ctx, batchQuery, reply)
}

func (c *successClient) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecute(ctx, query, sendReply)
}

func (c *successClient) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecuteShard(ctx, query, sendReply)
}

func (c *successClient) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecuteKeyRanges(ctx, query, sendReply)
}

func (c *successClient) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	return c.fallback.StreamExecuteKeyspaceIds(ctx, query, sendReply)
}

func (c *successClient) Begin(ctx context.Context, outSession *proto.Session) error {
	return c.fallback.Begin(ctx, outSession)
}

func (c *successClient) Commit(ctx context.Context, inSession *proto.Session) error {
	return c.fallback.Commit(ctx, inSession)
}

func (c *successClient) Rollback(ctx context.Context, inSession *proto.Session) error {
	return c.fallback.Rollback(ctx, inSession)
}

func (c *successClient) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return c.fallback.SplitQuery(ctx, req, reply)
}

func (c *successClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	if keyspace == "big" {
		return &topo.SrvKeyspace{
			Partitions: map[topo.TabletType]*topo.KeyspacePartition{
				topo.TYPE_REPLICA: &topo.KeyspacePartition{
					ShardReferences: []topo.ShardReference{
						topo.ShardReference{
							Name: "shard0",
							KeyRange: key.KeyRange{
								Start: key.Uint64Key(0x4000000000000000).KeyspaceId(),
								End:   key.Uint64Key(0x8000000000000000).KeyspaceId(),
							},
						},
					},
				},
			},
			ShardingColumnName: "sharding_column_name",
			ShardingColumnType: key.KIT_UINT64,
			ServedFrom: map[topo.TabletType]string{
				topo.TYPE_MASTER: "other_keyspace",
			},
			SplitShardCount: 128,
		}, nil
	}
	if keyspace == "small" {
		return &topo.SrvKeyspace{}, nil
	}
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}

func (c *successClient) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}
