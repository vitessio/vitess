// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcqueryservice

import (
	"sync"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	queryservicepb "github.com/youtube/vitess/go/vt/proto/queryservice"
)

// query is the gRPC query service implementation.
// It implements the queryservice.QueryServer interface.
type query struct {
	server queryservice.QueryService
}

// GetSessionId is part of the queryservice.QueryServer interface
func (q *query) GetSessionId(ctx context.Context, request *querypb.GetSessionIdRequest) (response *querypb.GetSessionIdResponse, err error) {
	defer q.server.HandlePanic(&err)

	sessionID, err := q.server.GetSessionId(request.Keyspace, request.Shard)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}

	return &querypb.GetSessionIdResponse{
		SessionId: sessionID,
	}, nil
}

// Execute is part of the queryservice.QueryServer interface
func (q *query) Execute(ctx context.Context, request *querypb.ExecuteRequest) (response *querypb.ExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	result, err := q.server.Execute(ctx, request.Target, request.Query.Sql, bv, request.SessionId, request.TransactionId)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	return &querypb.ExecuteResponse{
		Result: sqltypes.ResultToProto3(result),
	}, nil
}

// ExecuteBatch is part of the queryservice.QueryServer interface
func (q *query) ExecuteBatch(ctx context.Context, request *querypb.ExecuteBatchRequest) (response *querypb.ExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	bql, err := querytypes.Proto3ToBoundQueryList(request.Queries)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	results, err := q.server.ExecuteBatch(ctx, request.Target, bql, request.SessionId, request.AsTransaction, request.TransactionId)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	return &querypb.ExecuteBatchResponse{
		Results: sqltypes.ResultsToProto3(results),
	}, nil
}

// StreamExecute is part of the queryservice.QueryServer interface
func (q *query) StreamExecute(request *querypb.StreamExecuteRequest, stream queryservicepb.Query_StreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return tabletserver.ToGRPCError(err)
	}
	if err := q.server.StreamExecute(ctx, request.Target, request.Query.Sql, bv, request.SessionId, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.StreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	}); err != nil {
		return tabletserver.ToGRPCError(err)
	}
	return nil
}

// Begin is part of the queryservice.QueryServer interface
func (q *query) Begin(ctx context.Context, request *querypb.BeginRequest) (response *querypb.BeginResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	transactionID, err := q.server.Begin(ctx, request.Target, request.SessionId)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}

	return &querypb.BeginResponse{
		TransactionId: transactionID,
	}, nil
}

// Commit is part of the queryservice.QueryServer interface
func (q *query) Commit(ctx context.Context, request *querypb.CommitRequest) (response *querypb.CommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.Commit(ctx, request.Target, request.SessionId, request.TransactionId); err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	return &querypb.CommitResponse{}, nil
}

// Rollback is part of the queryservice.QueryServer interface
func (q *query) Rollback(ctx context.Context, request *querypb.RollbackRequest) (response *querypb.RollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.Rollback(ctx, request.Target, request.SessionId, request.TransactionId); err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}

	return &querypb.RollbackResponse{}, nil
}

// SplitQuery is part of the queryservice.QueryServer interface
func (q *query) SplitQuery(ctx context.Context, request *querypb.SplitQueryRequest) (response *querypb.SplitQueryResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	bq, err := querytypes.Proto3ToBoundQuery(request.Query)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	splits, err := q.server.SplitQuery(ctx, request.Target, bq.Sql, bq.BindVariables, request.SplitColumn, request.SplitCount, request.SessionId)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	qs, err := querytypes.QuerySplitsToProto3(splits)
	if err != nil {
		return nil, tabletserver.ToGRPCError(err)
	}
	return &querypb.SplitQueryResponse{Queries: qs}, nil
}

// StreamHealth is part of the queryservice.QueryServer interface
func (q *query) StreamHealth(request *querypb.StreamHealthRequest, stream queryservicepb.Query_StreamHealthServer) (err error) {
	defer q.server.HandlePanic(&err)

	c := make(chan *querypb.StreamHealthResponse, 10)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for shr := range c {
			// we send until the client disconnects
			if err := stream.Send(shr); err != nil {
				return
			}
		}
	}()

	id, err := q.server.StreamHealthRegister(c)
	if err != nil {
		close(c)
		wg.Wait()
		return err
	}
	wg.Wait()
	return q.server.StreamHealthUnregister(id)
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, func(qsc tabletserver.Controller) {
		if servenv.GRPCCheckServiceMap("queryservice") {
			queryservicepb.RegisterQueryServer(servenv.GRPCServer, &query{qsc.QueryService()})
		}
	})
}

// RegisterForTest should only be used by unit tests
func RegisterForTest(s *grpc.Server, server queryservice.QueryService) {
	queryservicepb.RegisterQueryServer(s, &query{server})
}
