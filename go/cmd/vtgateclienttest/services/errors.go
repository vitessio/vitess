// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
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
	fallbackClient
}

func newErrorClient(fallback vtgateservice.VTGateService) *errorClient {
	return &errorClient{
		fallbackClient: newFallbackClient(fallback),
	}
}

func (c *errorClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.Execute returning integrity error (errno 1062)")
	}
	return c.fallbackClient.Execute(ctx, sql, bindVariables, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteShard returning integrity error (errno 1062)")
	}
	return c.fallbackClient.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteKeyspaceIds returning integrity error (errno 1062)")
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteKeyRanges returning integrity error (errno 1062)")
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteEntityIds returning integrity error (errno 1062)")
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if len(queries) == 1 && queries[0].Sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteBatchShards returning integrity error (errno 1062)")
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session, reply)
}

func (c *errorClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if len(queries) == 1 && queries[0].Sql == "return integrity error" {
		return fmt.Errorf("vtgate test client, errorClient.ExecuteBatchKeyspaceIds returning integrity error (errno 1062)")
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session, reply)
}

func (c *errorClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	if keyspace == "error" {
		return nil, fmt.Errorf("vtgate test client, errorClient.GetSrvKeyspace returning error")
	}
	return c.fallbackClient.GetSrvKeyspace(ctx, keyspace)
}
