package drpcqueryservice

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	"storj.io/drpc"
)

type queryServer struct {
	server queryservice.QueryService
}

func (q *queryServer) Execute(ctx context.Context, request *querypb.ExecuteRequest) (response *querypb.ExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	result, err := q.server.Execute(ctx, request.Target, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.ReservedId, request.Options)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.ExecuteResponse{
		Result: sqltypes.ResultToProto3(result),
	}, nil
}

func (q *queryServer) ExecuteBatch(ctx context.Context, request *querypb.ExecuteBatchRequest) (response *querypb.ExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	results, err := q.server.ExecuteBatch(ctx, request.Target, request.Queries, request.AsTransaction, request.TransactionId, request.Options)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.ExecuteBatchResponse{
		Results: sqltypes.ResultsToProto3(results),
	}, nil
}

func (q *queryServer) StreamExecute(request *querypb.StreamExecuteRequest, stream queryservicepb.DRPCQuery_StreamExecuteStream) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.DRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.StreamExecute(ctx, request.Target, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.Options, func(reply *sqltypes.Result) error {
		return stream.Send(&querypb.StreamExecuteResponse{
			Result: sqltypes.ResultToProto3(reply),
		})
	})
	return vterrors.ToDRPC(err)
}

func (q *queryServer) Begin(ctx context.Context, request *querypb.BeginRequest) (response *querypb.BeginResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	transactionID, alias, err := q.server.Begin(ctx, request.Target, request.Options)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.BeginResponse{
		TransactionId: transactionID,
		TabletAlias:   alias,
	}, nil
}

func (q *queryServer) Commit(ctx context.Context, request *querypb.CommitRequest) (response *querypb.CommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	rID, err := q.server.Commit(ctx, request.Target, request.TransactionId)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.CommitResponse{ReservedId: rID}, nil
}

func (q *queryServer) Rollback(ctx context.Context, request *querypb.RollbackRequest) (response *querypb.RollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	rID, err := q.server.Rollback(ctx, request.Target, request.TransactionId)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.RollbackResponse{ReservedId: rID}, nil
}

func (q *queryServer) Prepare(ctx context.Context, request *querypb.PrepareRequest) (response *querypb.PrepareResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.Prepare(ctx, request.Target, request.TransactionId, request.Dtid); err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.PrepareResponse{}, nil
}

func (q *queryServer) CommitPrepared(ctx context.Context, request *querypb.CommitPreparedRequest) (response *querypb.CommitPreparedResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.CommitPrepared(ctx, request.Target, request.Dtid); err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.CommitPreparedResponse{}, nil
}

func (q *queryServer) RollbackPrepared(ctx context.Context, request *querypb.RollbackPreparedRequest) (response *querypb.RollbackPreparedResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.RollbackPrepared(ctx, request.Target, request.Dtid, request.TransactionId); err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.RollbackPreparedResponse{}, nil
}

func (q *queryServer) CreateTransaction(ctx context.Context, request *querypb.CreateTransactionRequest) (response *querypb.CreateTransactionResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.CreateTransaction(ctx, request.Target, request.Dtid, request.Participants); err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.CreateTransactionResponse{}, nil
}

func (q *queryServer) StartCommit(ctx context.Context, request *querypb.StartCommitRequest) (response *querypb.StartCommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.StartCommit(ctx, request.Target, request.TransactionId, request.Dtid); err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.StartCommitResponse{}, nil
}

func (q *queryServer) SetRollback(ctx context.Context, request *querypb.SetRollbackRequest) (response *querypb.SetRollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.SetRollback(ctx, request.Target, request.Dtid, request.TransactionId); err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.SetRollbackResponse{}, nil
}

func (q *queryServer) ConcludeTransaction(ctx context.Context, request *querypb.ConcludeTransactionRequest) (response *querypb.ConcludeTransactionResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.ConcludeTransaction(ctx, request.Target, request.Dtid); err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.ConcludeTransactionResponse{}, nil
}

func (q *queryServer) ReadTransaction(ctx context.Context, request *querypb.ReadTransactionRequest) (response *querypb.ReadTransactionResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	result, err := q.server.ReadTransaction(ctx, request.Target, request.Dtid)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}

	return &querypb.ReadTransactionResponse{Metadata: result}, nil
}

func (q *queryServer) BeginExecute(ctx context.Context, request *querypb.BeginExecuteRequest) (response *querypb.BeginExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	result, transactionID, alias, err := q.server.BeginExecute(ctx, request.Target, request.PreQueries, request.Query.Sql, request.Query.BindVariables, request.ReservedId, request.Options)
	if err != nil {
		// if we have a valid transactionID, return the error in-band
		if transactionID != 0 {
			return &querypb.BeginExecuteResponse{
				Error:         vterrors.ToVTRPC(err),
				TransactionId: transactionID,
				TabletAlias:   alias,
			}, nil
		}
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.BeginExecuteResponse{
		Result:        sqltypes.ResultToProto3(result),
		TransactionId: transactionID,
		TabletAlias:   alias,
	}, nil
}

func (q *queryServer) BeginExecuteBatch(ctx context.Context, request *querypb.BeginExecuteBatchRequest) (response *querypb.BeginExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	results, transactionID, alias, err := q.server.BeginExecuteBatch(ctx, request.Target, request.Queries, request.AsTransaction, request.Options)
	if err != nil {
		// if we have a valid transactionID, return the error in-band
		if transactionID != 0 {
			return &querypb.BeginExecuteBatchResponse{
				Error:         vterrors.ToVTRPC(err),
				TransactionId: transactionID,
				TabletAlias:   alias,
			}, nil
		}
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.BeginExecuteBatchResponse{
		Results:       sqltypes.ResultsToProto3(results),
		TransactionId: transactionID,
		TabletAlias:   alias,
	}, nil
}

func (q *queryServer) MessageStream(request *querypb.MessageStreamRequest, stream queryservicepb.DRPCQuery_MessageStreamStream) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.DRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.MessageStream(ctx, request.Target, request.Name, func(qr *sqltypes.Result) (err error) {
		return stream.Send(&querypb.MessageStreamResponse{
			Result: sqltypes.ResultToProto3(qr),
		})
	})
	return vterrors.ToDRPC(err)
}

func (q *queryServer) MessageAck(ctx context.Context, request *querypb.MessageAckRequest) (response *querypb.MessageAckResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	count, err := q.server.MessageAck(ctx, request.Target, request.Name, request.Ids)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.MessageAckResponse{
		Result: &querypb.QueryResult{
			RowsAffected: uint64(count),
		},
	}, nil
}

func (q *queryServer) ReserveExecute(ctx context.Context, request *querypb.ReserveExecuteRequest) (response *querypb.ReserveExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	result, reservedID, alias, err := q.server.ReserveExecute(ctx, request.Target, request.PreQueries, request.Query.Sql, request.Query.BindVariables, request.TransactionId, request.Options)
	if err != nil {
		// if we have a valid reservedID, return the error in-band
		if reservedID != 0 {
			return &querypb.ReserveExecuteResponse{
				Error:       vterrors.ToVTRPC(err),
				ReservedId:  reservedID,
				TabletAlias: alias,
			}, nil
		}
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.ReserveExecuteResponse{
		Result:      sqltypes.ResultToProto3(result),
		ReservedId:  reservedID,
		TabletAlias: alias,
	}, nil
}

func (q *queryServer) ReserveBeginExecute(ctx context.Context, request *querypb.ReserveBeginExecuteRequest) (response *querypb.ReserveBeginExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	result, transactionID, reservedID, alias, err := q.server.ReserveBeginExecute(ctx, request.Target, request.PreQueries, request.Query.Sql, request.Query.BindVariables, request.Options)
	if err != nil {
		// if we have a valid reservedID, return the error in-band
		if reservedID != 0 {
			return &querypb.ReserveBeginExecuteResponse{
				Error:         vterrors.ToVTRPC(err),
				TransactionId: transactionID,
				ReservedId:    reservedID,
				TabletAlias:   alias,
			}, nil
		}
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.ReserveBeginExecuteResponse{
		Result:        sqltypes.ResultToProto3(result),
		TransactionId: transactionID,
		ReservedId:    reservedID,
		TabletAlias:   alias,
	}, nil
}

func (q *queryServer) Release(ctx context.Context, request *querypb.ReleaseRequest) (response *querypb.ReleaseResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.DRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.Release(ctx, request.Target, request.TransactionId, request.ReservedId)
	if err != nil {
		return nil, vterrors.ToDRPC(err)
	}
	return &querypb.ReleaseResponse{}, nil
}

func (q *queryServer) StreamHealth(request *querypb.StreamHealthRequest, stream queryservicepb.DRPCQuery_StreamHealthStream) (err error) {
	defer q.server.HandlePanic(&err)
	err = q.server.StreamHealth(stream.Context(), stream.Send)
	return vterrors.ToDRPC(err)
}

func (q *queryServer) VStream(request *binlogdatapb.VStreamRequest, stream queryservicepb.DRPCQuery_VStreamStream) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.DRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.VStream(ctx, request.Target, request.Position, request.TableLastPKs, request.Filter, func(events []*binlogdatapb.VEvent) (err error) {
		return stream.Send(&binlogdatapb.VStreamResponse{
			Events: events,
		})
	})
	return vterrors.ToDRPC(err)
}

func (q *queryServer) VStreamRows(request *binlogdatapb.VStreamRowsRequest, stream queryservicepb.DRPCQuery_VStreamRowsStream) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.DRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.VStreamRows(ctx, request.Target, request.Query, request.Lastpk, stream.Send)
	return vterrors.ToDRPC(err)
}

func (q *queryServer) VStreamResults(request *binlogdatapb.VStreamResultsRequest, stream queryservicepb.DRPCQuery_VStreamResultsStream) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.DRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	err = q.server.VStreamResults(ctx, request.Target, request.Query, stream.Send)
	return vterrors.ToDRPC(err)
}

var _ queryservicepb.DRPCQueryServer = (*queryServer)(nil)

func Register(m drpc.Mux, server queryservice.QueryService) {
	queryservicepb.DRPCRegisterQuery(m, &queryServer{server: server})
}
