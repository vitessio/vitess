/*
Copyright 2019 The Vitess Authors.

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
	"context"

	"google.golang.org/grpc"

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
	queryservicepb.UnimplementedQueryServer
	server queryservice.QueryService
}

var _ queryservicepb.QueryServer = (*query)(nil)

// Execute is part of the queryservice.QueryServer interface
func (q *query) Execute(ctx context.Context, request *querypb.ExecuteRequest) (response *querypb.ExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	result, err := q.server.Execute(ctx, request.Target, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.ReservedId, request.Options)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.ExecuteResponse{
		Result: sqltypes.ResultToProto3(result),
	}, nil
}

// StreamExecute is part of the queryservice.QueryServer interface
func (q *query) StreamExecute(request *querypb.StreamExecuteRequest, stream queryservicepb.Query_StreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.StreamExecute(ctx, request.Target, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.ReservedId, request.Options, func(reply *sqltypes.Result) error {
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
	state, err := q.server.Begin(ctx, request.Target, request.Options)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.BeginResponse{
		TransactionId:       state.TransactionID,
		TabletAlias:         state.TabletAlias,
		SessionStateChanges: state.SessionStateChanges,
	}, nil
}

// Commit is part of the queryservice.QueryServer interface
func (q *query) Commit(ctx context.Context, request *querypb.CommitRequest) (response *querypb.CommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	rID, err := q.server.Commit(ctx, request.Target, request.TransactionId)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.CommitResponse{ReservedId: rID}, nil
}

// Rollback is part of the queryservice.QueryServer interface
func (q *query) Rollback(ctx context.Context, request *querypb.RollbackRequest) (response *querypb.RollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	rID, err := q.server.Rollback(ctx, request.Target, request.TransactionId)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}

	return &querypb.RollbackResponse{ReservedId: rID}, nil
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
	state, result, err := q.server.BeginExecute(ctx, request.Target, request.PreQueries, request.Query.Sql, request.Query.BindVariables, request.ReservedId, request.Options)
	if err != nil {
		// if we have a valid transactionID, return the error in-band
		if state.TransactionID != 0 {
			return &querypb.BeginExecuteResponse{
				Error:         vterrors.ToVTRPC(err),
				TransactionId: state.TransactionID,
				TabletAlias:   state.TabletAlias,
			}, nil
		}
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.BeginExecuteResponse{
		Result:              sqltypes.ResultToProto3(result),
		TransactionId:       state.TransactionID,
		TabletAlias:         state.TabletAlias,
		SessionStateChanges: state.SessionStateChanges,
	}, nil
}

// BeginStreamExecute is part of the queryservice.QueryServer interface
func (q *query) BeginStreamExecute(request *querypb.BeginStreamExecuteRequest, stream queryservicepb.Query_BeginStreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	state, err := q.server.BeginStreamExecute(ctx, request.Target, request.PreQueries, request.Query.Sql, request.Query.BindVariables, request.ReservedId, request.Options, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.BeginStreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	})
	if err != nil {
		return vterrors.ToGRPC(err)
	}

	errInLastPacket := stream.Send(&querypb.BeginStreamExecuteResponse{
		TransactionId:       state.TransactionID,
		TabletAlias:         state.TabletAlias,
		SessionStateChanges: state.SessionStateChanges,
	})
	return vterrors.ToGRPC(errInLastPacket)
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

// StreamHealth is part of the queryservice.QueryServer interface
func (q *query) StreamHealth(request *querypb.StreamHealthRequest, stream queryservicepb.Query_StreamHealthServer) (err error) {
	defer q.server.HandlePanic(&err)
	err = q.server.StreamHealth(stream.Context(), stream.Send)
	return vterrors.ToGRPC(err)
}

// VStream is part of the queryservice.QueryServer interface
func (q *query) VStream(request *binlogdatapb.VStreamRequest, stream queryservicepb.Query_VStreamServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.VStream(ctx, request, func(events []*binlogdatapb.VEvent) error {
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
	err = q.server.VStreamRows(ctx, request, stream.Send)
	return vterrors.ToGRPC(err)
}

// VStreamResults is part of the queryservice.QueryServer interface
func (q *query) VStreamResults(request *binlogdatapb.VStreamResultsRequest, stream queryservicepb.Query_VStreamResultsServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.VStreamResults(ctx, request.Target, request.Query, stream.Send)
	return vterrors.ToGRPC(err)
}

// ReserveExecute implements the QueryServer interface
func (q *query) ReserveExecute(ctx context.Context, request *querypb.ReserveExecuteRequest) (response *querypb.ReserveExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	state, result, err := q.server.ReserveExecute(ctx, request.Target, request.PreQueries, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.Options)
	if err != nil {
		// if we have a valid reservedID, return the error in-band
		if state.ReservedID != 0 {
			return &querypb.ReserveExecuteResponse{
				Error:       vterrors.ToVTRPC(err),
				ReservedId:  state.ReservedID,
				TabletAlias: state.TabletAlias,
			}, nil
		}
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.ReserveExecuteResponse{
		Result:      sqltypes.ResultToProto3(result),
		ReservedId:  state.ReservedID,
		TabletAlias: state.TabletAlias,
	}, nil
}

// ReserveStreamExecute is part of the queryservice.QueryServer interface
func (q *query) ReserveStreamExecute(request *querypb.ReserveStreamExecuteRequest, stream queryservicepb.Query_ReserveStreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	state, err := q.server.ReserveStreamExecute(ctx, request.Target, request.PreQueries, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.Options, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.ReserveStreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	})
	if err != nil {
		return vterrors.ToGRPC(err)
	}

	errInLastPacket := stream.Send(&querypb.ReserveStreamExecuteResponse{
		ReservedId:  state.ReservedID,
		TabletAlias: state.TabletAlias,
	})
	return vterrors.ToGRPC(errInLastPacket)
}

// ReserveBeginExecute implements the QueryServer interface
func (q *query) ReserveBeginExecute(ctx context.Context, request *querypb.ReserveBeginExecuteRequest) (response *querypb.ReserveBeginExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	state, result, err := q.server.ReserveBeginExecute(ctx, request.Target, request.PreQueries, request.PostBeginQueries, request.Query.Sql, request.Query.BindVariables, request.Options)
	if err != nil {
		// if we have a valid reservedID, return the error in-band
		if state.ReservedID != 0 {
			return &querypb.ReserveBeginExecuteResponse{
				Error:               vterrors.ToVTRPC(err),
				TransactionId:       state.TransactionID,
				ReservedId:          state.ReservedID,
				TabletAlias:         state.TabletAlias,
				SessionStateChanges: state.SessionStateChanges,
			}, nil
		}
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.ReserveBeginExecuteResponse{
		Result:              sqltypes.ResultToProto3(result),
		TransactionId:       state.TransactionID,
		ReservedId:          state.ReservedID,
		TabletAlias:         state.TabletAlias,
		SessionStateChanges: state.SessionStateChanges,
	}, nil
}

// ReserveBeginStreamExecute is part of the queryservice.QueryServer interface
func (q *query) ReserveBeginStreamExecute(request *querypb.ReserveBeginStreamExecuteRequest, stream queryservicepb.Query_ReserveBeginStreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	state, err := q.server.ReserveBeginStreamExecute(ctx, request.Target, request.PreQueries, request.PostBeginQueries, request.Query.Sql, request.Query.BindVariables, request.Options, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.ReserveBeginStreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	})
	if err != nil {
		return vterrors.ToGRPC(err)
	}

	errInLastPacket := stream.Send(&querypb.ReserveBeginStreamExecuteResponse{
		ReservedId:          state.ReservedID,
		TransactionId:       state.TransactionID,
		TabletAlias:         state.TabletAlias,
		SessionStateChanges: state.SessionStateChanges,
	})
	return vterrors.ToGRPC(errInLastPacket)
}

// Release implements the QueryServer interface
func (q *query) Release(ctx context.Context, request *querypb.ReleaseRequest) (response *querypb.ReleaseResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.Release(ctx, request.Target, request.TransactionId, request.ReservedId)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	return &querypb.ReleaseResponse{}, nil
}

// Register registers the implementation on the provide gRPC Server.
func Register(s *grpc.Server, server queryservice.QueryService) {
	queryservicepb.RegisterQueryServer(s, &query{server: server})
}
