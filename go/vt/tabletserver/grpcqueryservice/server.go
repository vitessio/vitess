// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcqueryservice

import (
	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vterrors"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	queryservicepb "github.com/youtube/vitess/go/vt/proto/queryservice"
)

// query is the gRPC query service implementation.
// It implements the queryservice.QueryServer interface.
type query struct {
	server queryservice.QueryService
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
		return nil, vterrors.ToGRPCError(err)
	}
	result, err := q.server.Execute(ctx, request.Target, request.Query.Sql, bv, request.TransactionId)
	if err != nil {
		return nil, vterrors.ToGRPCError(err)
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
		return nil, vterrors.ToGRPCError(err)
	}
	results, err := q.server.ExecuteBatch(ctx, request.Target, bql, request.AsTransaction, request.TransactionId)
	if err != nil {
		return nil, vterrors.ToGRPCError(err)
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
		return vterrors.ToGRPCError(err)
	}
	if err := q.server.StreamExecute(ctx, request.Target, request.Query.Sql, bv, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.StreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	}); err != nil {
		return vterrors.ToGRPCError(err)
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
	transactionID, err := q.server.Begin(ctx, request.Target)
	if err != nil {
		return nil, vterrors.ToGRPCError(err)
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
	if err := q.server.Commit(ctx, request.Target, request.TransactionId); err != nil {
		return nil, vterrors.ToGRPCError(err)
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
	if err := q.server.Rollback(ctx, request.Target, request.TransactionId); err != nil {
		return nil, vterrors.ToGRPCError(err)
	}

	return &querypb.RollbackResponse{}, nil
}

// BeginExecute is part of the queryservice.QueryServer interface
func (q *query) BeginExecute(ctx context.Context, request *querypb.BeginExecuteRequest) (response *querypb.BeginExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, vterrors.ToGRPCError(err)
	}

	result, transactionID, err := q.server.BeginExecute(ctx, request.Target, request.Query.Sql, bv)
	if err != nil {
		// if we have a valid transactionID, return the error in-band
		if transactionID != 0 {
			return &querypb.BeginExecuteResponse{
				Error:         vterrors.VtRPCErrorFromVtError(err),
				TransactionId: transactionID,
			}, nil
		}
		return nil, vterrors.ToGRPCError(err)
	}
	return &querypb.BeginExecuteResponse{
		Result:        sqltypes.ResultToProto3(result),
		TransactionId: transactionID,
	}, nil
}

// BeginExecuteBatch is part of the queryservice.QueryServer interface
func (q *query) BeginExecuteBatch(ctx context.Context, request *querypb.BeginExecuteBatchRequest) (response *querypb.BeginExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	bql, err := querytypes.Proto3ToBoundQueryList(request.Queries)
	if err != nil {
		return nil, vterrors.ToGRPCError(err)
	}

	results, transactionID, err := q.server.BeginExecuteBatch(ctx, request.Target, bql, request.AsTransaction)
	if err != nil {
		// if we have a valid transactionID, return the error in-band
		if transactionID != 0 {
			return &querypb.BeginExecuteBatchResponse{
				Error:         vterrors.VtRPCErrorFromVtError(err),
				TransactionId: transactionID,
			}, nil
		}
		return nil, vterrors.ToGRPCError(err)
	}
	return &querypb.BeginExecuteBatchResponse{
		Results:       sqltypes.ResultsToProto3(results),
		TransactionId: transactionID,
	}, nil
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
		return nil, vterrors.ToGRPCError(err)
	}
	splits := []querytypes.QuerySplit{}
	splits, err = queryservice.CallCorrectSplitQuery(
		q.server,
		request.UseSplitQueryV2,
		ctx,
		request.Target,
		bq.Sql,
		bq.BindVariables,
		request.SplitColumn,
		request.SplitCount,
		request.NumRowsPerQueryPart,
		request.Algorithm)
	if err != nil {
		return nil, vterrors.ToGRPCError(err)
	}
	qs, err := querytypes.QuerySplitsToProto3(splits)
	if err != nil {
		return nil, vterrors.ToGRPCError(err)
	}
	return &querypb.SplitQueryResponse{Queries: qs}, nil
}

// StreamHealth is part of the queryservice.QueryServer interface
func (q *query) StreamHealth(request *querypb.StreamHealthRequest, stream queryservicepb.Query_StreamHealthServer) (err error) {
	defer q.server.HandlePanic(&err)

	c := make(chan *querypb.StreamHealthResponse, 10)

	id, err := q.server.StreamHealthRegister(c)
	if err != nil {
		close(c)
		return err
	}

	for shr := range c {
		// we send until the client disconnects
		if err := stream.Send(shr); err != nil {
			break
		}
	}

	return q.server.StreamHealthUnregister(id)
}

// Register registers the implementation on the provide gRPC Server.
func Register(s *grpc.Server, server queryservice.QueryService) {
	queryservicepb.RegisterQueryServer(s, &query{server})
}
