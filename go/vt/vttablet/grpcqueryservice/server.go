// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcqueryservice

import (
	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
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
		return nil, vterrors.ToGRPC(err)
	}
	result, err := q.server.Execute(ctx, request.Target, request.Query.Sql, bv, request.TransactionId, request.Options)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
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
		return nil, vterrors.ToGRPC(err)
	}
	results, err := q.server.ExecuteBatch(ctx, request.Target, bql, request.AsTransaction, request.TransactionId, request.Options)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
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
		return vterrors.ToGRPC(err)
	}
	if err := q.server.StreamExecute(ctx, request.Target, request.Query.Sql, bv, request.Options, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.StreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	}); err != nil {
		return vterrors.ToGRPC(err)
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
		return nil, vterrors.ToGRPC(err)
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
		return nil, vterrors.ToGRPC(err)
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
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.RollbackResponse{}, nil
}

// Prepare is part of the queryservice.QueryServer interface
func (q *query) Prepare(ctx context.Context, request *querypb.PrepareRequest) (response *querypb.PrepareResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.Prepare(ctx, request.Target, request.TransactionId, request.Dtid); err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.PrepareResponse{}, nil
}

// CommitPrepared is part of the queryservice.QueryServer interface
func (q *query) CommitPrepared(ctx context.Context, request *querypb.CommitPreparedRequest) (response *querypb.CommitPreparedResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.CommitPrepared(ctx, request.Target, request.Dtid); err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.CommitPreparedResponse{}, nil
}

// RollbackPrepared is part of the queryservice.QueryServer interface
func (q *query) RollbackPrepared(ctx context.Context, request *querypb.RollbackPreparedRequest) (response *querypb.RollbackPreparedResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.RollbackPrepared(ctx, request.Target, request.Dtid, request.TransactionId); err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.RollbackPreparedResponse{}, nil
}

// CreateTransaction is part of the queryservice.QueryServer interface
func (q *query) CreateTransaction(ctx context.Context, request *querypb.CreateTransactionRequest) (response *querypb.CreateTransactionResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.CreateTransaction(ctx, request.Target, request.Dtid, request.Participants); err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.CreateTransactionResponse{}, nil
}

// StartCommit is part of the queryservice.QueryServer interface
func (q *query) StartCommit(ctx context.Context, request *querypb.StartCommitRequest) (response *querypb.StartCommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.StartCommit(ctx, request.Target, request.TransactionId, request.Dtid); err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.StartCommitResponse{}, nil
}

// SetRollback is part of the queryservice.QueryServer interface
func (q *query) SetRollback(ctx context.Context, request *querypb.SetRollbackRequest) (response *querypb.SetRollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.SetRollback(ctx, request.Target, request.Dtid, request.TransactionId); err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.SetRollbackResponse{}, nil
}

// ConcludeTransaction is part of the queryservice.QueryServer interface
func (q *query) ConcludeTransaction(ctx context.Context, request *querypb.ConcludeTransactionRequest) (response *querypb.ConcludeTransactionResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.ConcludeTransaction(ctx, request.Target, request.Dtid); err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.ConcludeTransactionResponse{}, nil
}

// ReadTransaction is part of the queryservice.QueryServer interface
func (q *query) ReadTransaction(ctx context.Context, request *querypb.ReadTransactionRequest) (response *querypb.ReadTransactionResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	result, err := q.server.ReadTransaction(ctx, request.Target, request.Dtid)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.ReadTransactionResponse{Metadata: result}, nil
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
		return nil, vterrors.ToGRPC(err)
	}

	result, transactionID, err := q.server.BeginExecute(ctx, request.Target, request.Query.Sql, bv, request.Options)
	if err != nil {
		// if we have a valid transactionID, return the error in-band
		if transactionID != 0 {
			return &querypb.BeginExecuteResponse{
				Error:         vterrors.ToVTRPC(err),
				TransactionId: transactionID,
			}, nil
		}
		return nil, vterrors.ToGRPC(err)
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
		return nil, vterrors.ToGRPC(err)
	}

	results, transactionID, err := q.server.BeginExecuteBatch(ctx, request.Target, bql, request.AsTransaction, request.Options)
	if err != nil {
		// if we have a valid transactionID, return the error in-band
		if transactionID != 0 {
			return &querypb.BeginExecuteBatchResponse{
				Error:         vterrors.ToVTRPC(err),
				TransactionId: transactionID,
			}, nil
		}
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.BeginExecuteBatchResponse{
		Results:       sqltypes.ResultsToProto3(results),
		TransactionId: transactionID,
	}, nil
}

// MessageStream is part of the queryservice.QueryServer interface
func (q *query) MessageStream(request *querypb.MessageStreamRequest, stream queryservicepb.Query_MessageStreamServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.MessageStream(ctx, request.Target, request.Name, func(qr *sqltypes.Result) error {
		return stream.Send(&querypb.MessageStreamResponse{
			Result: sqltypes.ResultToProto3(qr),
		})
	}); err != nil {
		return vterrors.ToGRPC(err)
	}
	return nil
}

// MessageAck is part of the queryservice.QueryServer interface
func (q *query) MessageAck(ctx context.Context, request *querypb.MessageAckRequest) (response *querypb.MessageAckResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	count, err := q.server.MessageAck(ctx, request.Target, request.Name, request.Ids)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.MessageAckResponse{
		Result: &querypb.QueryResult{
			RowsAffected: uint64(count),
		},
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
		return nil, vterrors.ToGRPC(err)
	}
	splits := []querytypes.QuerySplit{}
	splits, err = q.server.SplitQuery(
		ctx,
		request.Target,
		*bq,
		request.SplitColumn,
		request.SplitCount,
		request.NumRowsPerQueryPart,
		request.Algorithm)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	qs, err := querytypes.QuerySplitsToProto3(splits)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.SplitQueryResponse{Queries: qs}, nil
}

// StreamHealth is part of the queryservice.QueryServer interface
func (q *query) StreamHealth(request *querypb.StreamHealthRequest, stream queryservicepb.Query_StreamHealthServer) (err error) {
	defer q.server.HandlePanic(&err)
	if err = q.server.StreamHealth(stream.Context(), stream.Send); err != nil {
		return vterrors.ToGRPC(err)
	}
	return nil
}

// UpdateStream is part of the queryservice.QueryServer interface
func (q *query) UpdateStream(request *querypb.UpdateStreamRequest, stream queryservicepb.Query_UpdateStreamServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.UpdateStream(ctx, request.Target, request.Position, request.Timestamp, func(reply *querypb.StreamEvent) error {
		return stream.Send(&querypb.UpdateStreamResponse{
			Event: reply,
		})
	}); err != nil {
		return vterrors.ToGRPC(err)
	}
	return nil
}

// Register registers the implementation on the provide gRPC Server.
func Register(s *grpc.Server, server queryservice.QueryService) {
	queryservicepb.RegisterQueryServer(s, &query{server})
}
