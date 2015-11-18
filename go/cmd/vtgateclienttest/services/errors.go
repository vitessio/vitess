// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// errorClient implements vtgateservice.VTGateService
// and returns specific errors. It is meant to test all possible error cases,
// and make sure all clients handle the errors correctly.

const (
	// ErrorPrefix is the prefix to send with queries so they go through this service handler.
	ErrorPrefix = "error://"
	// PartialErrorPrefix is the prefix to send with queries so the RPC returns a partial error.
	// A partial error is when we return an error as part of the RPC response instead of via
	// the regular error channels. This occurs if an RPC partially succeeds, and therefore
	// requires some kind of response, but still needs to return an error.
	// VTGate Execute* calls do this: they always return a new session ID, but might also
	// return an error in the response.
	PartialErrorPrefix = "partialerror://"
)

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
func requestToError(request string) error {
	if !strings.HasPrefix(request, ErrorPrefix) {
		return nil
	}
	return trimmedRequestToError(strings.TrimPrefix(request, ErrorPrefix))
}

// requestToPartialError fills reply for a partial error if requested.
// It returns true if a partial error was requested, false otherwise.
// This partial error should only be returned by Execute* calls.
func requestToPartialError(request string, session *vtgatepb.Session, reply *proto.QueryResult) bool {
	if !strings.HasPrefix(request, PartialErrorPrefix) {
		return false
	}
	request = strings.TrimPrefix(request, PartialErrorPrefix)
	parts := strings.Split(request, "/")
	rpcErr := vterrors.RPCErrFromVtError(trimmedRequestToError(parts[0]))
	reply.Err = rpcErr
	reply.Session = session
	if len(parts) > 1 && parts[1] == "close transaction" {
		reply.Session.InTransaction = false
	}
	return true
}

// trimmedRequestToError returns an error for a trimmed request by looking at the
// requested error type. It assumes that prefix checking has already been done.
// If the received string doesn't match a known error, returns an unknown error.
func trimmedRequestToError(received string) error {
	switch received {
	case "bad input":
		return vterrors.FromError(
			vtrpcpb.ErrorCode_BAD_INPUT,
			errors.New("vtgate test client forced error: bad input"),
		)
	case "deadline exceeded":
		return vterrors.FromError(
			vtrpcpb.ErrorCode_DEADLINE_EXCEEDED,
			errors.New("vtgate test client forced error: deadline exceeded"),
		)
	case "integrity error":
		return vterrors.FromError(
			vtrpcpb.ErrorCode_INTEGRITY_ERROR,
			errors.New("vtgate test client forced error: integrity error (errno 1062)"),
		)
	// request backlog and general throttling type errors
	case "transient error":
		return vterrors.FromError(
			vtrpcpb.ErrorCode_TRANSIENT_ERROR,
			errors.New("request_backlog: too many requests in flight: vtgate test client forced error: transient error"),
		)
	case "unauthenticated":
		return vterrors.FromError(
			vtrpcpb.ErrorCode_UNAUTHENTICATED,
			errors.New("vtgate test client forced error: unauthenticated"),
		)
	case "unknown error":
		return vterrors.FromError(
			vtrpcpb.ErrorCode_UNKNOWN_ERROR,
			errors.New("vtgate test client forced error: unknown error"),
		)
	default:
		return vterrors.FromError(
			vtrpcpb.ErrorCode_UNKNOWN_ERROR,
			fmt.Errorf("vtgate test client error request unrecognized: %v", received),
		)
	}
}

func (c *errorClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	// not this will be refactored once we can change requestToPartialError signature.
	reply := &proto.QueryResult{}
	if requestToPartialError(sql, session, reply) {
		if reply.Session != nil {
			*session = *reply.Session
		}
		return reply.Result, vterrors.FromRPCError(reply.Err)
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.Execute(ctx, sql, bindVariables, tabletType, session, notInTransaction)
}

func (c *errorClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	reply := &proto.QueryResult{}
	if requestToPartialError(sql, session, reply) {
		if reply.Session != nil {
			*session = *reply.Session
		}
		return reply.Result, vterrors.FromRPCError(reply.Err)
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction)
}

func (c *errorClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	reply := &proto.QueryResult{}
	if requestToPartialError(sql, session, reply) {
		if reply.Session != nil {
			*session = *reply.Session
		}
		return reply.Result, vterrors.FromRPCError(reply.Err)
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction)
}

func (c *errorClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	reply := &proto.QueryResult{}
	if requestToPartialError(sql, session, reply) {
		if reply.Session != nil {
			*session = *reply.Session
		}
		return reply.Result, vterrors.FromRPCError(reply.Err)
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction)
}

func (c *errorClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	reply := &proto.QueryResult{}
	if requestToPartialError(sql, session, reply) {
		if reply.Session != nil {
			*session = *reply.Session
		}
		return reply.Result, vterrors.FromRPCError(reply.Err)
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction)
}

func (c *errorClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	if len(queries) == 1 {
		var partialReply proto.QueryResult
		if requestToPartialError(queries[0].Query.Sql, session, &partialReply) {
			if partialReply.Session != nil {
				*session = *partialReply.Session
			}
			return nil, vterrors.FromRPCError(partialReply.Err)
		}
		if err := requestToError(queries[0].Query.Sql); err != nil {
			return nil, err
		}
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session)
}

func (c *errorClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	if len(queries) == 1 {
		var partialReply proto.QueryResult
		if requestToPartialError(queries[0].Query.Sql, session, &partialReply) {
			if partialReply.Session != nil {
				*session = *partialReply.Session
			}
			return nil, vterrors.FromRPCError(partialReply.Err)
		}
		if err := requestToError(queries[0].Query.Sql); err != nil {
			return nil, err
		}
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session)
}

func (c *errorClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecute(ctx, sql, bindVariables, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, sendReply)
}

func (c *errorClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, sendReply)
}

func (c *errorClient) Begin(ctx context.Context, outSession *vtgatepb.Session) error {
	// The client sends the error request through the callerid, as there are no other parameters
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return err
	}
	return c.fallbackClient.Begin(ctx, outSession)
}

func (c *errorClient) Commit(ctx context.Context, inSession *vtgatepb.Session) error {
	// The client sends the error request through the callerid, as there are no other parameters
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return err
	}
	return c.fallbackClient.Commit(ctx, inSession)
}

func (c *errorClient) Rollback(ctx context.Context, inSession *vtgatepb.Session) error {
	// The client sends the error request through the callerid, as there are no other parameters
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return err
	}
	return c.fallbackClient.Rollback(ctx, inSession)
}

func (c *errorClient) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.SplitQuery(ctx, sql, keyspace, bindVariables, splitColumn, splitCount)
}

func (c *errorClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	if err := requestToError(keyspace); err != nil {
		return nil, err
	}
	return c.fallbackClient.GetSrvKeyspace(ctx, keyspace)
}

func (c *errorClient) GetSrvShard(ctx context.Context, keyspace, shard string) (*topodatapb.SrvShard, error) {
	if err := requestToError(keyspace); err != nil {
		return nil, err
	}
	return c.fallbackClient.GetSrvShard(ctx, keyspace, shard)
}
