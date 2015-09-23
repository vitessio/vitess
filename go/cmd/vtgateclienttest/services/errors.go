// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// errorClient implements vtgateservice.VTGateService
// and returns specific errors. It is meant to test all possible error cases,
// and make sure all clients handle the errors correctly.

// ErrorPrefix is the prefix to send with queries so they go through this service handler.
const ErrorPrefix = "error://"

type errorClient struct {
	fallbackClient
}

func newErrorClient(fallback vtgateservice.VTGateService) *errorClient {
	return &errorClient{
		fallbackClient: newFallbackClient(fallback),
	}
}

// requestToError returns an error for the given request, by looking at the
// request's prefix and requested error type. If the request doesn't match an
// error request, return nil.
func requestToError(received string) error {
	if !strings.HasPrefix(received, ErrorPrefix) {
		return nil
	}
	s := strings.TrimPrefix(received, ErrorPrefix)
	switch s {
	case "integrity error":
		return vterrors.FromError(
			vtrpc.ErrorCode_INTEGRITY_ERROR,
			errors.New("vtgate test client forced error: integrity error (errno 1062)"),
		)
	// request backlog and general throttling type errors
	case "transient error":
		return vterrors.FromError(
			vtrpc.ErrorCode_TRANSIENT_ERROR,
			errors.New("request_backlog: too many requests in flight: vtgate test client forced error"),
		)
	case "unknown error":
		return vterrors.FromError(
			vtrpc.ErrorCode_UNKNOWN_ERROR,
			errors.New("vtgate test client forced error: unknown error"),
		)
	default:
		return vterrors.FromError(
			vtrpc.ErrorCode_UNKNOWN_ERROR,
			fmt.Errorf("vtgate test client error request unrecognized: %v", received),
		)
	}
}

func (c *errorClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if err := requestToError(sql); err != nil {
		return err
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
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, reply)
}

func (c *errorClient) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if len(queries) == 1 {
		if err := requestToError(queries[0].Sql); err != nil {
			return err
		}
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session, reply)
}

func (c *errorClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if len(queries) == 1 {
		if err := requestToError(queries[0].Sql); err != nil {
			return err
		}
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session, reply)
}

func (c *errorClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecute(ctx, sql, bindVariables, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, sendReply)
}

func (c *errorClient) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error) {
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.SplitQuery(ctx, sql, keyspace, bindVariables, splitColumn, splitCount)
}

func (c *errorClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error) {
	if err := requestToError(keyspace); err != nil {
		return nil, err
	}
	return c.fallbackClient.GetSrvKeyspace(ctx, keyspace)
}
