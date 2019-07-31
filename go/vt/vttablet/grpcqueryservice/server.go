/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpcqueryservice

import (
	"google.golang.org/grpc"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
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
	result, err := q.server.Execute(ctx, request.Target, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.Options)
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
	results, err := q.server.ExecuteBatch(ctx, request.Target, request.Queries, request.AsTransaction, request.TransactionId, request.Options)
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
	err = q.server.StreamExecute(ctx, request.Target, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.Options, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.StreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	})
	return vterrors.ToGRPC(err)
}

// Begin is part of the queryservice.QueryServer interface
func (q *query) Begin(ctx context.Context, request *querypb.BeginRequest) (response *querypb.BeginResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	transactionID, err := q.server.Begin(ctx, request.Target, request.Options)
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

	result, transactionID, err := q.server.BeginExecute(ctx, request.Target, request.Query.Sql, request.Query.BindVariables, request.Options)
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

	results, transactionID, err := q.server.BeginExecuteBatch(ctx, request.Target, request.Queries, request.AsTransaction, request.Options)
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
	err = q.server.MessageStream(ctx, request.Target, request.Name, func(qr *sqltypes.Result) error {
		return stream.Send(&querypb.MessageStreamResponse{
			Result: sqltypes.ResultToProto3(qr),
		})
	})
	return vterrors.ToGRPC(err)
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
	splits, err := q.server.SplitQuery(
		ctx,
		request.Target,
		request.Query,
		request.SplitColumn,
		request.SplitCount,
		request.NumRowsPerQueryPart,
		request.Algorithm)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.SplitQueryResponse{Queries: splits}, nil
}

// StreamHealth is part of the queryservice.QueryServer interface
func (q *query) StreamHealth(request *querypb.StreamHealthRequest, stream queryservicepb.Query_StreamHealthServer) (err error) {
	defer q.server.HandlePanic(&err)
	err = q.server.StreamHealth(stream.Context(), stream.Send)
	return vterrors.ToGRPC(err)
}

// UpdateStream is part of the queryservice.QueryServer interface
func (q *query) UpdateStream(request *querypb.UpdateStreamRequest, stream queryservicepb.Query_UpdateStreamServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.UpdateStream(ctx, request.Target, request.Position, request.Timestamp, func(reply *querypb.StreamEvent) error {
		return stream.Send(&querypb.UpdateStreamResponse{
			Event: reply,
		})
	})
	return vterrors.ToGRPC(err)
}

// VStream is part of the queryservice.QueryServer interface
func (q *query) VStream(request *binlogdatapb.VStreamRequest, stream queryservicepb.Query_VStreamServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.VStream(ctx, request.Target, request.Position, request.Filter, func(events []*binlogdatapb.VEvent) error {
		return stream.Send(&binlogdatapb.VStreamResponse{
			Events: events,
		})
	})
	return vterrors.ToGRPC(err)
}

// VStreamRows is part of the queryservice.QueryServer interface
func (q *query) VStreamRows(request *binlogdatapb.VStreamRowsRequest, stream queryservicepb.Query_VStreamRowsServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.VStreamRows(ctx, request.Target, request.Query, request.Lastpk, stream.Send)
	return vterrors.ToGRPC(err)
}

// Register registers the implementation on the provide gRPC Server.
func Register(s *grpc.Server, server queryservice.QueryService) {
	queryservicepb.RegisterQueryServer(s, &query{server})
}
